package durablequeue

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock for lease/delay tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func openTestQueue(t *testing.T, clock *fakeClock) (*Queue, string) {
	t.Helper()
	dir := t.TempDir()
	opts := Options{Dir: dir}
	if clock != nil {
		opts.Now = clock.Now
	}
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return q, dir
}

func reopen(t *testing.T, dir string, clock *fakeClock) *Queue {
	t.Helper()
	opts := Options{Dir: dir}
	if clock != nil {
		opts.Now = clock.Now
	}
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return q
}

func TestEnqueueIdempotent(t *testing.T) {
	q, _ := openTestQueue(t, nil)
	defer q.Close()
	t1, created, err := q.Enqueue("job-1", []byte("first"), EnqueueOptions{})
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}
	t2, created, err := q.Enqueue("job-1", []byte("second"), EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("duplicate enqueue reported created=true")
	}
	if string(t2.Payload) != "first" {
		t.Fatalf("duplicate enqueue must not overwrite payload, got %q", t2.Payload)
	}
	if t1.Seq != t2.Seq {
		t.Fatalf("duplicate returned different task: seq %d vs %d", t1.Seq, t2.Seq)
	}
	if got := q.Stats().Total; got != 1 {
		t.Fatalf("total=%d, want 1", got)
	}
	if _, _, err := q.Enqueue("", nil, EnqueueOptions{}); !errors.Is(err, ErrEmptyID) {
		t.Fatalf("empty id: %v", err)
	}
}

func TestClaimExclusiveAndAck(t *testing.T) {
	q, _ := openTestQueue(t, nil)
	defer q.Close()
	q.Enqueue("a", []byte("A"), EnqueueOptions{})
	q.Enqueue("b", []byte("B"), EnqueueOptions{})
	task, err := q.Claim(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if task.ID != "a" {
		t.Fatalf("FIFO order violated: claimed %q", task.ID)
	}
	if task.Attempts != 1 || task.LeaseToken == "" {
		t.Fatalf("bad claimed task: %+v", task)
	}
	task2, err := q.Claim(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if task2.ID != "b" {
		t.Fatalf("expected b, got %q", task2.ID)
	}
	if _, err := q.Claim(time.Minute); !errors.Is(err, ErrNoTask) {
		t.Fatalf("expected ErrNoTask, got %v", err)
	}
	if err := q.Ack("a", "wrong-token"); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("wrong token: %v", err)
	}
	if err := q.Ack("a", task.LeaseToken); err != nil {
		t.Fatal(err)
	}
	got, _ := q.Get("a")
	if got.Status != StatusCompleted {
		t.Fatalf("status=%s", got.Status)
	}
	if _, created, _ := q.Enqueue("a", []byte("again"), EnqueueOptions{}); created {
		t.Fatal("completed task ID must remain deduped")
	}
}

func TestLeaseExpiryReclaimAndStaleToken(t *testing.T) {
	clock := newFakeClock()
	q, _ := openTestQueue(t, clock)
	defer q.Close()
	q.Enqueue("x", []byte("X"), EnqueueOptions{})
	first, err := q.Claim(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Second)
	if _, err := q.Claim(time.Second); !errors.Is(err, ErrNoTask) {
		t.Fatalf("before expiry: %v", err)
	}
	clock.Advance(6 * time.Second)
	second, err := q.Claim(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != "x" || second.LeaseToken == first.LeaseToken {
		t.Fatalf("reclaim broken: %+v", second)
	}
	if err := q.Ack("x", first.LeaseToken); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("stale token ack: %v", err)
	}
	if err := q.Fail("x", first.LeaseToken, 0, "old worker"); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("stale token fail: %v", err)
	}
	if err := q.Ack("x", second.LeaseToken); err != nil {
		t.Fatal(err)
	}
}

func TestAckAfterLeaseExpiryRejected(t *testing.T) {
	clock := newFakeClock()
	q, _ := openTestQueue(t, clock)
	defer q.Close()
	q.Enqueue("x", nil, EnqueueOptions{})
	task, _ := q.Claim(5 * time.Second)
	clock.Advance(6 * time.Second)
	if err := q.Ack("x", task.LeaseToken); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired lease ack: %v", err)
	}
}

func TestDelayedRetry(t *testing.T) {
	clock := newFakeClock()
	q, _ := openTestQueue(t, clock)
	defer q.Close()
	q.Enqueue("flaky", nil, EnqueueOptions{MaxAttempts: 3})
	task, _ := q.Claim(time.Minute)
	if err := q.Fail("flaky", task.LeaseToken, 30*time.Second, "boom"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(29 * time.Second)
	if _, err := q.Claim(time.Minute); !errors.Is(err, ErrNoTask) {
		t.Fatalf("before retry delay: %v", err)
	}
	clock.Advance(1 * time.Second)
	retry, err := q.Claim(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Attempts != 2 {
		t.Fatalf("attempts=%d, want 2", retry.Attempts)
	}
	got, _ := q.Get("flaky")
	if got.LastError != "boom" {
		t.Fatalf("last error %q", got.LastError)
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	q, _ := openTestQueue(t, nil)
	defer q.Close()
	q.Enqueue("doomed", nil, EnqueueOptions{MaxAttempts: 2})
	for i := 0; i < 2; i++ {
		task, err := q.Claim(time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := q.Fail("doomed", task.LeaseToken, 0, fmt.Sprintf("fail %d", i)); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := q.Get("doomed")
	if got.Status != StatusDead {
		t.Fatalf("status=%s, want dead", got.Status)
	}
	if got.Attempts != 2 {
		t.Fatalf("attempts=%d", got.Attempts)
	}
	if got.LastError != "fail 1" {
		t.Fatalf("last error %q", got.LastError)
	}
	if _, err := q.Claim(time.Minute); !errors.Is(err, ErrNoTask) {
		t.Fatalf("dead task claimed: %v", err)
	}
	if s := q.Stats(); s.Dead != 1 {
		t.Fatalf("stats=%+v", s)
	}
}

func TestRecoveryAfterCrash(t *testing.T) {
	clock := newFakeClock()
	q, dir := openTestQueue(t, clock)
	q.Enqueue("done", []byte("D"), EnqueueOptions{})
	q.Enqueue("leased", []byte("L"), EnqueueOptions{})
	q.Enqueue("waiting", []byte("W"), EnqueueOptions{})
	done, _ := q.Claim(time.Minute)
	q.Ack("done", done.LeaseToken)
	leased, _ := q.Claim(time.Minute)
	_ = leased
	// Simulate kill -9: the queue is abandoned WITHOUT Close(). We keep a
	// reference only so the file handle can be released at test end
	// (Windows cannot delete an open file during TempDir cleanup).
	crashed := q
	t.Cleanup(func() { crashed.Close() })
	q = nil
	clock.Advance(time.Second)
	q2 := reopen(t, dir, clock)
	defer q2.Close()
	s := q2.Stats()
	if s.Total != 3 || s.Completed != 1 || s.Claimed != 1 || s.Pending != 1 {
		t.Fatalf("recovered stats=%+v", s)
	}
	tasks := q2.List()
	if tasks[0].ID != "done" || tasks[1].ID != "leased" || tasks[2].ID != "waiting" {
		t.Fatalf("order after recovery: %v", tasks)
	}
	if string(tasks[1].Payload) != "L" {
		t.Fatalf("payload lost: %q", tasks[1].Payload)
	}
	next, err := q2.Claim(time.Minute)
	if err != nil || next.ID != "waiting" {
		t.Fatalf("claim after recovery: %v %v", next, err)
	}
	clock.Advance(2 * time.Minute)
	back, err := q2.Claim(time.Minute)
	if err != nil || back.ID != "leased" {
		t.Fatalf("lease-expiry reclaim after recovery: %v %v", back, err)
	}
}
