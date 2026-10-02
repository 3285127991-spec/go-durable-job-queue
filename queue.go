package durablequeue

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"durablequeue/wal"
)

const (
	walFileName  = "queue.wal"
	snapFileName = "queue.snap"
)

// ErrNotDead is returned when trying to requeue a task that is not in the
// dead letter queue.
var ErrNotDead = errors.New("queue: task is not dead")

// recordOp identifies the mutation kind stored in the WAL.
type recordOp string

const (
	opEnqueue recordOp = "enqueue"
	opClaim   recordOp = "claim"
	opAck     recordOp = "ack"
	opFail    recordOp = "fail"
	opDead    recordOp = "dead"
	opRequeue recordOp = "requeue"
)

// record is a single WAL entry: the full post-mutation state of one task,
// which makes replay a simple idempotent upsert.
type record struct {
	Op   recordOp `json:"op"`
	Task Task     `json:"task"`
}

// Queue is a durable, lease-based job queue backed by a write-ahead log and
// periodic snapshots in Options.Dir. All methods are safe for concurrent
// use by multiple goroutines.
type Queue struct {
	mu      sync.Mutex
	opts    Options
	log     *wal.WAL
	tasks   map[string]*Task
	order   []string // task IDs in insertion order
	lastSeq uint64
	closed  bool
}

// Open opens (or creates) a queue in opts.Dir, recovering state from the
// snapshot and the write-ahead log.
func Open(opts Options) (*Queue, error) {
	opts.withDefaults()
	if opts.Dir == "" {
		return nil, errors.New("queue: Options.Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o777); err != nil {
		return nil, err
	}
	q := &Queue{
		opts:  opts,
		tasks: make(map[string]*Task),
	}
	snapSeq, err := q.loadSnapshot()
	if err != nil {
		return nil, err
	}
	q.lastSeq = snapSeq

	log, records, err := wal.Open(filepath.Join(opts.Dir, walFileName), snapSeq+1, !opts.NoSync)
	if err != nil {
		return nil, err
	}
	q.log = log
	for _, rec := range records {
		if rec.Seq <= snapSeq {
			continue // already covered by the snapshot
		}
		var r record
		if err := json.Unmarshal(rec.Payload, &r); err != nil {
			return nil, fmt.Errorf("queue: decode wal record seq=%d: %w", rec.Seq, err)
		}
		q.apply(&r.Task)
		q.lastSeq = rec.Seq
	}
	return q, nil
}

// apply inserts or replaces a task in memory. Caller holds q.mu.
func (q *Queue) apply(t *Task) {
	if _, ok := q.tasks[t.ID]; !ok {
		q.order = append(q.order, t.ID)
	}
	c := *t
	q.tasks[t.ID] = &c
}

// commit appends the post-mutation task state to the WAL. Caller holds q.mu.
func (q *Queue) commit(op recordOp, t *Task) error {
	payload, err := json.Marshal(record{Op: op, Task: *t})
	if err != nil {
		return err
	}
	seq, err := q.log.Append(payload)
	if err != nil {
		return err
	}
	q.lastSeq = seq
	return nil
}

// Enqueue adds a task. It is idempotent on id: if a task with the same ID
// already exists (in any state), the existing task is returned and created
// is false. maxAttempts <= 0 uses DefaultMaxAttempts. delay defers the
// first delivery.
func (q *Queue) Enqueue(id string, payload []byte, maxAttempts int, delay time.Duration) (task *Task, created bool, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkOpen(); err != nil {
		return nil, false, err
	}
	if id == "" {
		return nil, false, errors.New("queue: task id is required")
	}
	if t, ok := q.tasks[id]; ok {
		c := t.clone()
		return &c, false, nil
	}
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	now := time.Now()
	t := &Task{
		ID:          id,
		Payload:     append([]byte(nil), payload...),
		Status:      StatusPending,
		MaxAttempts: maxAttempts,
		NotBefore:   now.Add(delay),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := q.commit(opEnqueue, t); err != nil {
		return nil, false, err
	}
	q.apply(t)
	c := t.clone()
	return &c, true, nil
}

// Claim leases the next available task for q.opts.LeaseDuration. It returns
// (nil, nil) when no task is available. A task whose previous lease expired
// becomes available again; each claim issues a fresh token, so a worker
// holding an old token can no longer operate on the task.
func (q *Queue) Claim() (*Task, error) {
	return q.ClaimWithLease(q.opts.LeaseDuration)
}

// ClaimWithLease is Claim with an explicit lease duration.
func (q *Queue) ClaimWithLease(lease time.Duration) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkOpen(); err != nil {
		return nil, err
	}
	if lease <= 0 {
		lease = q.opts.LeaseDuration
	}
	now := time.Now()
	for _, id := range q.order {
		t := q.tasks[id]
		if !t.Available(now) {
			continue
		}
		if t.Attempts >= t.MaxAttempts {
			// Delivered MaxAttempts times without an ack: dead-letter it.
			t.Status = StatusDead
			t.Token = ""
			t.UpdatedAt = now
			if err := q.commit(opDead, t); err != nil {
				return nil, err
			}
			continue
		}
		t.Attempts++
		t.Token = newToken()
		t.LeaseExpiry = now.Add(lease)
		t.Status = StatusClaimed
		t.UpdatedAt = now
		if err := q.commit(opClaim, t); err != nil {
			return nil, err
		}
		c := t.clone()
		return &c, nil
	}
	return nil, nil
}

// Ack marks a claimed task as done. The token must match the current lease
// and the lease must not be expired. Ack on an already-done task is a
// no-op so workers can safely retry acknowledgements.
func (q *Queue) Ack(id, token string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkOpen(); err != nil {
		return err
	}
	t, err := q.claimable(id, token)
	if err != nil {
		if errors.Is(err, ErrNotClaimed) && q.tasks[id] != nil && q.tasks[id].Status == StatusDone {
			return nil // idempotent ack
		}
		return err
	}
	t.Status = StatusDone
	t.Token = ""
	t.UpdatedAt = time.Now()
	return q.commit(opAck, t)
}

// Fail reports the current attempt as failed. If the task exhausted
// MaxAttempts it moves to the dead letter queue; otherwise it becomes
// pending again after an exponential backoff.
func (q *Queue) Fail(id, token string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkOpen(); err != nil {
		return err
	}
	t, err := q.claimable(id, token)
	if err != nil {
		return err
	}
	now := time.Now()
	t.Token = ""
	t.UpdatedAt = now
	if t.Attempts >= t.MaxAttempts {
		t.Status = StatusDead
	} else {
		t.Status = StatusPending
		t.NotBefore = now.Add(q.backoff(t.Attempts))
	}
	return q.commit(opFail, t)
}

// Requeue moves a dead-lettered task back to pending with zeroed attempts.
func (q *Queue) Requeue(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkOpen(); err != nil {
		return err
	}
	t, ok := q.tasks[id]
	if !ok {
		return ErrTaskNotFound
	}
	if t.Status != StatusDead {
		return ErrNotDead
	}
	now := time.Now()
	t.Status = StatusPending
	t.Attempts = 0
	t.Token = ""
	t.NotBefore = now
	t.UpdatedAt = now
	return q.commit(opRequeue, t)
}

// claimable validates that task id is currently claimed under token with an
// unexpired lease. Caller holds q.mu.
func (q *Queue) claimable(id, token string) (*Task, error) {
	t, ok := q.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	if t.Status != StatusClaimed {
		return nil, ErrNotClaimed
	}
	if t.Token != token {
		return nil, ErrTokenMismatch
	}
	if time.Now().After(t.LeaseExpiry) {
		return nil, ErrLeaseExpired
	}
	return t, nil
}

// backoff returns the retry delay after the given number of attempts.
func (q *Queue) backoff(attempts int) time.Duration {
	d := q.opts.BaseBackoff
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= q.opts.MaxBackoff {
			return q.opts.MaxBackoff
		}
	}
	if d > q.opts.MaxBackoff {
		return q.opts.MaxBackoff
	}
	return d
}

// Get returns a copy of the task with the given ID.
func (q *Queue) Get(id string) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkOpen(); err != nil {
		return nil, err
	}
	t, ok := q.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	c := t.clone()
	return &c, nil
}

// List returns copies of all tasks, optionally filtered by status, in
// insertion order.
func (q *Queue) List(status ...Status) []*Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	var filter map[Status]bool
	if len(status) > 0 {
		filter = make(map[Status]bool, len(status))
		for _, s := range status {
			filter[s] = true
		}
	}
	var out []*Task
	for _, id := range q.order {
		t := q.tasks[id]
		if filter != nil && !filter[t.Status] {
			continue
		}
		c := t.clone()
		out = append(out, &c)
	}
	return out
}

// Stats summarizes task counts by status.
type Stats struct {
	Pending int
	Claimed int
	Done    int
	Dead    int
	LastSeq uint64
}

// Stats returns current queue statistics.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	var s Stats
	s.LastSeq = q.lastSeq
	for _, t := range q.tasks {
		switch t.Status {
		case StatusPending:
			s.Pending++
		case StatusClaimed:
			s.Claimed++
		case StatusDone:
			s.Done++
		case StatusDead:
			s.Dead++
		}
	}
	return s
}

// Close flushes and closes the queue files.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	return q.log.Close()
}

func (q *Queue) checkOpen() error {
	if q.closed {
		return ErrClosed
	}
	return nil
}

func (t *Task) clone() Task {
	c := *t
	c.Payload = append([]byte(nil), t.Payload...)
	return c
}

func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	return hex.EncodeToString(b[:])
}
