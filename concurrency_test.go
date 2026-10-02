package durablequeue

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestConcurrentClaimsNoDuplicates hammers one queue with many workers and
// verifies every task is delivered to exactly one worker.
func TestConcurrentClaimsNoDuplicates(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()

	const total = 200
	for i := 0; i < total; i++ {
		mustEnqueue(t, q, fmt.Sprintf("job-%03d", i), 3, 0)
	}

	var mu sync.Mutex
	claims := map[string]int{}
	acked := map[string]bool{}

	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task, err := q.Claim()
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				if task == nil {
					return
				}
				mu.Lock()
				claims[task.ID]++
				mu.Unlock()
				if err := q.Ack(task.ID, task.Token); err != nil {
					t.Errorf("Ack(%s): %v", task.ID, err)
					return
				}
				mu.Lock()
				acked[task.ID] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(claims) != total {
		t.Fatalf("claimed %d distinct tasks, want %d", len(claims), total)
	}
	for id, n := range claims {
		if n != 1 {
			t.Fatalf("task %s claimed %d times, want exactly 1", id, n)
		}
	}
	if len(acked) != total {
		t.Fatalf("acked %d tasks, want %d", len(acked), total)
	}
	st := q.Stats()
	if st.Done != total || st.Pending != 0 || st.Claimed != 0 {
		t.Fatalf("stats = %+v, want %d done and nothing left", st, total)
	}
}

// TestConcurrentEnqueueSameID verifies idempotent dedup under concurrency:
// exactly one caller observes created=true and only one task exists.
func TestConcurrentEnqueueSameID(t *testing.T) {
	q, err := Open(testOptions(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()

	const writers = 32
	var createdCount int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, created, err := q.Enqueue("same-id", []byte(fmt.Sprintf("v%d", i)), 3, 0)
			if err != nil {
				t.Errorf("Enqueue: %v", err)
				return
			}
			if created {
				mu.Lock()
				createdCount++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if createdCount != 1 {
		t.Fatalf("created=true %d times, want exactly 1", createdCount)
	}
	if got := len(q.List()); got != 1 {
		t.Fatalf("len(List) = %d, want 1", got)
	}
}

// TestConcurrentOpsWithCompaction runs enqueue/claim/ack/fail/compact
// concurrently and verifies no state is lost across a reopen.
func TestConcurrentOpsWithCompaction(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const total = 120
	var producers sync.WaitGroup
	for p := 0; p < 4; p++ {
		producers.Add(1)
		go func(p int) {
			defer producers.Done()
			for i := 0; i < total/4; i++ {
				id := fmt.Sprintf("p%d-%02d", p, i)
				if _, _, err := q.Enqueue(id, []byte(id), 100, 0); err != nil {
					t.Errorf("Enqueue(%s): %v", id, err)
					return
				}
			}
		}(p)
	}

	drained := func() bool {
		st := q.Stats()
		return st.Done+st.Dead == total
	}

	var workers sync.WaitGroup
	for w := 0; w < 8; w++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				task, err := q.Claim()
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				if task == nil {
					if drained() {
						return
					}
					time.Sleep(5 * time.Millisecond)
					continue
				}
				// Tasks whose id ends in '0' fail their first attempt and
				// are retried; everything else is acked immediately.
				var opErr error
				if strings.HasSuffix(task.ID, "0") && task.Attempts < 2 {
					opErr = q.Fail(task.ID, task.Token)
				} else {
					opErr = q.Ack(task.ID, task.Token)
				}
				if opErr != nil {
					t.Errorf("op on %s: %v", task.ID, opErr)
					return
				}
			}
		}()
	}

	stop := make(chan struct{})
	var compactor sync.WaitGroup
	compactor.Add(1)
	go func() {
		defer compactor.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := q.Compact(); err != nil {
					t.Errorf("Compact: %v", err)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	producers.Wait()
	workers.Wait()
	close(stop)
	compactor.Wait()
	if !drained() {
		st := q.Stats()
		t.Fatalf("queue not drained: %+v", st)
	}
	q.Close()

	// Reopen and verify: every task accounted for, none lost, none stuck.
	q2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()
	st := q2.Stats()
	if st.Pending != 0 || st.Claimed != 0 {
		t.Fatalf("after reopen: %+v, want no pending/claimed", st)
	}
	if st.Done+st.Dead != total {
		t.Fatalf("after reopen: done+dead = %d, want %d", st.Done+st.Dead, total)
	}
	for _, task := range q2.List() {
		if task.Status != StatusDone && task.Status != StatusDead {
			t.Fatalf("task %s in state %s after drain", task.ID, task.Status)
		}
	}
}
