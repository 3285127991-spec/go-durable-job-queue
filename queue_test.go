package durablequeue

import (
	"errors"
	"testing"
	"time"
)

func testOptions(t *testing.T) Options {
	t.Helper()
	return Options{
		Dir:           t.TempDir(),
		LeaseDuration: 100 * time.Millisecond,
		BaseBackoff:   50 * time.Millisecond,
		MaxBackoff:    200 * time.Millisecond,
	}
}

func mustEnqueue(t *testing.T, q *Queue, id string, maxAttempts int, delay time.Duration) {
	t.Helper()
	if _, _, err := q.Enqueue(id, []byte("payload-"+id), maxAttempts, delay); err != nil {
		t.Fatalf("Enqueue(%s): %v", id, err)
	}
}

func TestEnqueueIdempotent(t *testing.T) {
	q, err := Open(testOptions(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()

	_, created, err := q.Enqueue("job-1", []byte("v1"), 3, 0)
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}
	b, created, err := q.Enqueue("job-1", []byte("v2-different"), 5, 0)
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if created {
		t.Fatal("second enqueue with same ID reported created=true")
	}
	if string(b.Payload) != "v1" || b.MaxAttempts != 3 {
		t.Fatalf("idempotent enqueue mutated task: %+v", b)
	}
	if got := len(q.List()); got != 1 {
		t.Fatalf("len(List) = %d, want 1", got)
	}
}

func TestClaimAckFlow(t *testing.T) {
	q, err := Open(testOptions(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()
	mustEnqueue(t, q, "a", 3, 0)

	task, err := q.Claim()
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if task == nil || task.ID != "a" {
		t.Fatalf("claimed %+v, want task a", task)
	}
	if task.Attempts != 1 || task.Token == "" || task.Status != StatusClaimed {
		t.Fatalf("bad claimed task: %+v", task)
	}

	// Same task cannot be claimed again while the lease is valid.
	again, err := q.Claim()
	if err != nil {
		t.Fatalf("Claim again: %v", err)
	}
	if again != nil {
		t.Fatalf("double claim: %+v", again)
	}

	if err := q.Ack("a", "wrong-token"); !errors.Is(err, ErrTokenMismatch) {
		t.Fatalf("Ack wrong token = %v, want ErrTokenMismatch", err)
	}
	if err := q.Ack("a", task.Token); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	got, _ := q.Get("a")
	if got.Status != StatusDone {
		t.Fatalf("status = %s, want done", got.Status)
	}
	// Idempotent ack: repeating with the same token succeeds silently.
	if err := q.Ack("a", task.Token); err != nil {
		t.Fatalf("repeat Ack = %v, want nil", err)
	}
}

func TestLeaseExpiryAndOldToken(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()
	mustEnqueue(t, q, "leased", 3, 0)

	first, err := q.Claim()
	if err != nil || first == nil {
		t.Fatalf("first claim: %v %+v", err, first)
	}
	time.Sleep(2 * opts.LeaseDuration) // let the lease expire

	second, err := q.Claim()
	if err != nil || second == nil {
		t.Fatalf("reclaim after expiry: %v %+v", err, second)
	}
	if second.Token == first.Token {
		t.Fatal("reclaim reused the old token")
	}
	if second.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", second.Attempts)
	}

	// The old token must not operate on the new lease.
	if err := q.Ack("leased", first.Token); !errors.Is(err, ErrTokenMismatch) {
		t.Fatalf("old token Ack = %v, want ErrTokenMismatch", err)
	}
	if err := q.Fail("leased", first.Token); !errors.Is(err, ErrTokenMismatch) {
		t.Fatalf("old token Fail = %v, want ErrTokenMismatch", err)
	}
	if err := q.Ack("leased", second.Token); err != nil {
		t.Fatalf("new token Ack: %v", err)
	}
}

func TestAckWithExpiredLease(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()
	mustEnqueue(t, q, "slow", 3, 0)

	task, _ := q.Claim()
	time.Sleep(2 * opts.LeaseDuration)
	if err := q.Ack("slow", task.Token); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("Ack after lease expiry = %v, want ErrLeaseExpired", err)
	}
}

func TestDelayedRetry(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()
	mustEnqueue(t, q, "flaky", 3, 0)

	task, _ := q.Claim()
	if err := q.Fail("flaky", task.Token); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	got, _ := q.Get("flaky")
	if got.Status != StatusPending {
		t.Fatalf("status = %s, want pending (retry scheduled)", got.Status)
	}
	// Immediately: still backing off, nothing to claim.
	if next, _ := q.Claim(); next != nil {
		t.Fatalf("claimed %v during backoff, want nil", next.ID)
	}
	time.Sleep(2 * opts.BaseBackoff)
	next, err := q.Claim()
	if err != nil || next == nil {
		t.Fatalf("claim after backoff: %v %+v", err, next)
	}
	if next.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", next.Attempts)
	}
}

func TestEnqueueWithDelay(t *testing.T) {
	q, err := Open(testOptions(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()
	mustEnqueue(t, q, "later", 3, 150*time.Millisecond)

	if task, _ := q.Claim(); task != nil {
		t.Fatalf("claimed delayed task early: %+v", task)
	}
	time.Sleep(200 * time.Millisecond)
	if task, _ := q.Claim(); task == nil {
		t.Fatal("delayed task not claimable after its delay")
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	q, err := Open(testOptions(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()
	mustEnqueue(t, q, "doomed", 2, 0)

	for i := 0; i < 2; i++ {
		var task *Task
		deadline := time.Now().Add(2 * time.Second)
		for task == nil && time.Now().Before(deadline) {
			task, _ = q.Claim()
			if task == nil {
				time.Sleep(10 * time.Millisecond)
			}
		}
		if task == nil {
			t.Fatalf("attempt %d: no task to claim", i+1)
		}
		if err := q.Fail("doomed", task.Token); err != nil {
			t.Fatalf("attempt %d Fail: %v", i+1, err)
		}
	}
	got, _ := q.Get("doomed")
	if got.Status != StatusDead {
		t.Fatalf("status = %s, want dead", got.Status)
	}
	if dlq := q.List(StatusDead); len(dlq) != 1 || dlq[0].ID != "doomed" {
		t.Fatalf("DLQ = %+v, want [doomed]", dlq)
	}
	// Dead tasks are never claimed.
	if task, _ := q.Claim(); task != nil {
		t.Fatalf("claimed dead task %+v", task)
	}
	// Requeue brings it back.
	if err := q.Requeue("doomed"); err != nil {
		t.Fatalf("Requeue: %v", err)
	}
	task, _ := q.Claim()
	if task == nil || task.Attempts != 1 {
		t.Fatalf("requeued claim = %+v, want attempts=1", task)
	}
}

func TestDeadLetterWhenWorkerDies(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()
	mustEnqueue(t, q, "crashy", 2, 0)

	// Claim and never ack/fail, twice: the task must be dead-lettered
	// once attempts reach MaxAttempts.
	for i := 0; i < 2; i++ {
		task, _ := q.Claim()
		if task == nil {
			t.Fatalf("claim %d returned nil", i+1)
		}
		time.Sleep(2 * opts.LeaseDuration)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, _ := q.Get("crashy")
		if got.Status == StatusDead {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %s, want dead after %d lost leases", got.Status, 2)
		}
		q.Claim() // triggers lazy dead-lettering
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRecoverAfterReopen(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Closed only at test end so Windows can clean up TempDir; the reopen
	// below happens while this handle is still open, simulating a crash.
	defer q.Close()
	mustEnqueue(t, q, "done-task", 3, 0)
	mustEnqueue(t, q, "pending-task", 3, 0)
	task, _ := q.Claim() // claims done-task (FIFO)
	if err := q.Ack(task.ID, task.Token); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// Simulate a crash: drop the handle WITHOUT Close.
	q2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()

	done, err := q2.Get("done-task")
	if err != nil || done.Status != StatusDone {
		t.Fatalf("done-task after reopen: %+v err=%v", done, err)
	}
	pend, err := q2.Get("pending-task")
	if err != nil || pend.Status != StatusPending {
		t.Fatalf("pending-task after reopen: %+v err=%v", pend, err)
	}
	// Idempotency survives recovery.
	if _, created, _ := q2.Enqueue("done-task", nil, 0, 0); created {
		t.Fatal("duplicate enqueue after reopen reported created=true")
	}
	// Queue keeps working.
	next, _ := q2.Claim()
	if next == nil || next.ID != "pending-task" {
		t.Fatalf("claim after reopen: %+v", next)
	}
}
