package durablequeue

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Status is the lifecycle state of a task.
type Status string

const (
	StatusPending   Status = "pending"
	StatusClaimed   Status = "claimed"
	StatusCompleted Status = "completed"
	StatusDead      Status = "dead"
)

var (
	ErrNoTask       = errors.New("queue: no claimable task")
	ErrTaskNotFound = errors.New("queue: task not found")
	ErrNotClaimed   = errors.New("queue: task is not currently claimed")
	ErrStaleToken   = errors.New("queue: lease token mismatch (stale or wrong token)")
	ErrLeaseExpired = errors.New("queue: lease has expired")
	ErrClosed       = errors.New("queue: closed")
	ErrEmptyID      = errors.New("queue: task id must not be empty")
)

// DefaultMaxAttempts is used when EnqueueOptions.MaxAttempts <= 0.
const DefaultMaxAttempts = 3

// DefaultLease is used when Claim is called with a non-positive lease.
const DefaultLease = 30 * time.Second

// Task is a unit of work. Payload is opaque to the queue.
type Task struct {
	ID          string    `json:"id"`
	Payload     []byte    `json:"payload,omitempty"`
	Status      Status    `json:"status"`
	Seq         uint64    `json:"seq"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
	NotBefore   time.Time `json:"not_before"`
	LeaseToken  string    `json:"lease_token,omitempty"`
	LeaseExpiry time.Time `json:"lease_expiry,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (t *Task) clone() *Task { c := *t; return &c }

// walRecord is the persisted envelope for every state mutation. Every
// mutation stores the full task state (upsert), which makes replay trivially
// idempotent when combined with the monotonically increasing LSN.
type walRecord struct {
	LSN  uint64 `json:"lsn"`
	Op   string `json:"op"`
	Task *Task  `json:"task"`
}

// Options configures Open.
type Options struct {
	Dir string
	Now func() time.Time
	// DisableSync skips fsync on the WAL (only for benchmarks/tests).
	DisableSync bool
}

// Queue is a durable, single-process, embeddable job queue. All exported
// methods are safe for concurrent use; a single mutex serializes mutations,
// and every mutation is appended to the WAL (and fsynced) before it becomes
// visible in memory.
type Queue struct {
	mu     sync.Mutex
	dir    string
	wal    *wal
	tasks  map[string]*Task
	lsn    uint64
	seq    uint64
	now    func() time.Time
	closed bool
}

// Open opens (or creates) the queue stored in opts.Dir. It loads the latest
// snapshot, truncates any torn WAL tail, and replays WAL records newer than
// the snapshot. Mid-file WAL corruption is reported as a CorruptionError.
func Open(opts Options) (*Queue, error) {
	if opts.Dir == "" {
		return nil, errors.New("queue: Options.Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o777); err != nil {
		return nil, err
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	q := &Queue{dir: opts.Dir, tasks: make(map[string]*Task), now: now}

	if err := q.loadSnapshot(); err != nil {
		return nil, err
	}
	w, err := openWAL(filepath.Join(opts.Dir, walFileName), !opts.DisableSync,
		func(lsn uint64, payload []byte) error {
			var rec walRecord
			if err := json.Unmarshal(payload, &rec); err != nil {
				return &CorruptionError{Offset: -1, Reason: "undecodable record: " + err.Error()}
			}
			if rec.Task == nil || rec.Task.ID == "" {
				return &CorruptionError{Offset: -1, Reason: "record without task"}
			}
			if rec.LSN <= q.lsn {
				return nil
			}
			q.apply(&rec)
			return nil
		})
	if err != nil {
		return nil, err
	}
	q.wal = w
	return q, nil
}

func (q *Queue) apply(rec *walRecord) {
	t := rec.Task.clone()
	q.tasks[t.ID] = t
	if t.Seq > q.seq {
		q.seq = t.Seq
	}
	if rec.LSN > q.lsn {
		q.lsn = rec.LSN
	}
}

// persistLocked appends the mutation to the WAL (durable) and only then
// applies it to the in-memory state. Caller must hold q.mu.
func (q *Queue) persistLocked(t *Task, op string) error {
	q.lsn++
	rec := walRecord{LSN: q.lsn, Op: op, Task: t}
	data, err := json.Marshal(&rec)
	if err != nil {
		q.lsn--
		return err
	}
	if err := q.wal.append(q.lsn, data); err != nil {
		q.lsn--
		return err
	}
	stored := t.clone()
	q.tasks[stored.ID] = stored
	if stored.Seq > q.seq {
		q.seq = stored.Seq
	}
	return nil
}

// EnqueueOptions controls Enqueue.
type EnqueueOptions struct {
	MaxAttempts int
	Delay       time.Duration
}

// Enqueue adds a task. Enqueue is idempotent by task ID: if a task with the
// same ID already exists (in any state), the existing task is returned with
// created=false and the queue is left unchanged.
func (q *Queue) Enqueue(id string, payload []byte, opts EnqueueOptions) (task *Task, created bool, err error) {
	if id == "" {
		return nil, false, ErrEmptyID
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, false, ErrClosed
	}
	if existing, ok := q.tasks[id]; ok {
		return existing.clone(), false, nil
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	now := q.now()
	q.seq++
	t := &Task{
		ID:          id,
		Payload:     append([]byte(nil), payload...),
		Status:      StatusPending,
		Seq:         q.seq,
		MaxAttempts: maxAttempts,
		NotBefore:   now.Add(opts.Delay),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := q.persistLocked(t, "enqueue"); err != nil {
		q.seq--
		return nil, false, err
	}
	return t.clone(), true, nil
}

// Claim atomically leases the oldest claimable task. A task is claimable when
// it is pending and its NotBefore has passed, or when its previous lease has
// expired (crash/timeout of the previous worker). The returned task carries a
// unique LeaseToken that must be presented to Ack/Fail; once the lease
// expires or the task is reclaimed, the token becomes stale and is rejected.
func (q *Queue) Claim(lease time.Duration) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	if lease <= 0 {
		lease = DefaultLease
	}
	now := q.now()
	var best *Task
	for _, t := range q.tasks {
		var claimable bool
		switch t.Status {
		case StatusPending:
			claimable = !t.NotBefore.After(now)
		case StatusClaimed:
			claimable = !t.LeaseExpiry.After(now)
		}
		if !claimable {
			continue
		}
		if best == nil || t.Seq < best.Seq {
			best = t
		}
	}
	if best == nil {
		return nil, ErrNoTask
	}
	t := *best
	t.Status = StatusClaimed
	t.Attempts++
	t.LeaseToken = newToken()
	t.LeaseExpiry = now.Add(lease)
	t.NotBefore = now
	t.UpdatedAt = now
	if err := q.persistLocked(&t, "claim"); err != nil {
		return nil, err
	}
	return &t, nil
}

// checkLeaseLocked validates that id is currently claimed with exactly this
// token and that the lease has not expired.
func (q *Queue) checkLeaseLocked(id, token string) (*Task, error) {
	t, ok := q.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	if t.Status != StatusClaimed {
		return nil, ErrNotClaimed
	}
	if t.LeaseToken != token {
		return nil, ErrStaleToken
	}
	if !t.LeaseExpiry.After(q.now()) {
		return nil, ErrLeaseExpired
	}
	return t, nil
}

// Ack marks a claimed task as completed. The token must match the current,
// unexpired lease.
func (q *Queue) Ack(id, token string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	t, err := q.checkLeaseLocked(id, token)
	if err != nil {
		return err
	}
	u := *t
	u.Status = StatusCompleted
	u.LeaseToken = ""
	u.LeaseExpiry = time.Time{}
	u.UpdatedAt = q.now()
	return q.persistLocked(&u, "ack")
}

// Fail releases a claimed task and schedules a retry after retryDelay. If the
// task has reached its MaxAttempts, it is moved to the dead-letter queue
// (StatusDead) instead and will never be claimed again.
func (q *Queue) Fail(id, token string, retryDelay time.Duration, cause string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	t, err := q.checkLeaseLocked(id, token)
	if err != nil {
		return err
	}
	u := *t
	u.LeaseToken = ""
	u.LeaseExpiry = time.Time{}
	u.LastError = cause
	u.UpdatedAt = q.now()
	if u.Attempts >= u.MaxAttempts {
		u.Status = StatusDead
		u.NotBefore = u.UpdatedAt
	} else {
		u.Status = StatusPending
		u.NotBefore = u.UpdatedAt.Add(retryDelay)
	}
	return q.persistLocked(&u, "fail")
}

// Get returns a copy of the task with the given ID.
func (q *Queue) Get(id string) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return t.clone(), nil
}

// List returns copies of all tasks, optionally filtered by status, ordered
// by FIFO sequence.
func (q *Queue) List(statuses ...Status) []*Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	var filter map[Status]bool
	if len(statuses) > 0 {
		filter = make(map[Status]bool, len(statuses))
		for _, s := range statuses {
			filter[s] = true
		}
	}
	out := make([]*Task, 0, len(q.tasks))
	for _, t := range q.tasks {
		if filter != nil && !filter[t.Status] {
			continue
		}
		out = append(out, t.clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// Stats summarizes the queue.
type Stats struct {
	Pending   int    `json:"pending"`
	Claimed   int    `json:"claimed"`
	Completed int    `json:"completed"`
	Dead      int    `json:"dead"`
	Total     int    `json:"total"`
	LSN       uint64 `json:"lsn"`
}

// Stats returns current per-status counts and the last applied LSN.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	var s Stats
	s.LSN = q.lsn
	for _, t := range q.tasks {
		s.Total++
		switch t.Status {
		case StatusPending:
			s.Pending++
		case StatusClaimed:
			s.Claimed++
		case StatusCompleted:
			s.Completed++
		case StatusDead:
			s.Dead++
		}
	}
	return s
}

// Close flushes and closes the WAL. It is safe to abandon a Queue without
// Close (simulating a crash): every mutation is already fsynced.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	return q.wal.close()
}

func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
