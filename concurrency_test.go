package durablequeue

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestConcurrentClaim: many workers race to claim; every task must be
// claimed by exactly one worker.
func TestConcurrentClaim(t *testing.T) {
	q, _ := openTestQueue(t, nil)
	defer q.Close()
	const n = 200
	for i := 0; i < n; i++ {
		if _, _, err := q.Enqueue(fmt.Sprintf("job-%03d", i), nil, EnqueueOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	var (
		mu      sync.Mutex
		claimed = map[string]int{}
		wg      sync.WaitGroup
	)
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task, err := q.Claim(time.Minute)
				if errors.Is(err, ErrNoTask) {
					return
				}
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				mu.Lock()
				claimed[task.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(claimed) != n {
		t.Fatalf("claimed %d distinct tasks, want %d", len(claimed), n)
	}
	for id, c := range claimed {
		if c != 1 {
			t.Fatalf("task %s claimed %d times", id, c)
		}
	}
}

// TestConcurrentEnqueueSameID: idempotent dedup under concurrency.
func TestConcurrentEnqueueSameID(t *testing.T) {
	q, _ := openTestQueue(t, nil)
	defer q.Close()
	const ids = 50
	const writers = 16
	var createdCount int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ids; i++ {
				_, created, err := q.Enqueue(fmt.Sprintf("dup-%02d", i), []byte("x"), EnqueueOptions{})
				if err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
				if created {
					mu.Lock()
					createdCount++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	if createdCount != ids {
		t.Fatalf("created=%d, want %d", createdCount, ids)
	}
	if got := q.Stats().Total; got != ids {
		t.Fatalf("total=%d, want %d", got, ids)
	}
}

// TestConcurrentMixedWorkload runs producers, workers and a compactor
// concurrently and checks: no task is ever leased to two workers at once,
// nothing is lost, and the final state survives a restart.
func TestConcurrentMixedWorkload(t *testing.T) {
	q, dir := openTestQueue(t, nil)
	const n = 300
	for i := 0; i < n; i++ {
		if _, _, err := q.Enqueue(fmt.Sprintf("mix-%03d", i), []byte("payload"), EnqueueOptions{MaxAttempts: 1000}); err != nil {
			t.Fatal(err)
		}
	}

	var (
		inflight = map[string]string{} // taskID -> token currently leased
		mu       sync.Mutex
		done     = make(chan struct{})
		wg       sync.WaitGroup
	)
	lease := func(id, token string) {
		mu.Lock()
		defer mu.Unlock()
		if _, busy := inflight[id]; busy {
			t.Errorf("task %s leased to two workers concurrently", id)
		}
		inflight[id] = token
	}
	release := func(id string) {
		mu.Lock()
		defer mu.Unlock()
		delete(inflight, id)
	}

	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for {
				select {
				case <-done:
					return
				default:
				}
				task, err := q.Claim(100 * time.Millisecond)
				if errors.Is(err, ErrNoTask) {
					time.Sleep(time.Millisecond)
					continue
				}
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				lease(task.ID, task.LeaseToken)
				if rng.Intn(10) < 7 {
					if err := q.Ack(task.ID, task.LeaseToken); err != nil {
						t.Errorf("ack %s: %v", task.ID, err)
					}
				} else {
					if err := q.Fail(task.ID, task.LeaseToken, 0, "transient"); err != nil {
						t.Errorf("fail %s: %v", task.ID, err)
					}
				}
				release(task.ID)
			}
		}(w)
	}

	// Compactor: snapshot+reset the WAL while mutations are in flight.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
			if err := q.Compact(); err != nil {
				t.Errorf("compact: %v", err)
				return
			}
		}
	}()

	deadline := time.After(60 * time.Second)
	for q.Stats().Completed != n {
		select {
		case <-deadline:
			close(done)
			wg.Wait()
			t.Fatalf("timed out; stats=%+v", q.Stats())
		case <-time.After(10 * time.Millisecond):
		}
	}
	close(done)
	wg.Wait()

	if s := q.Stats(); s.Completed != n || s.Total != n {
		t.Fatalf("final stats=%+v", s)
	}
	q.Close()

	// Restart: nothing completed may be lost or resurrected.
	q2 := reopen(t, dir, nil)
	defer q2.Close()
	if s := q2.Stats(); s.Completed != n || s.Total != n || s.Pending != 0 {
		t.Fatalf("stats after restart=%+v", s)
	}
}

// TestConcurrentClaimDuringCompaction focuses on claim exclusivity while
// compaction runs constantly.
func TestConcurrentClaimDuringCompaction(t *testing.T) {
	q, _ := openTestQueue(t, nil)
	defer q.Close()
	const n = 100
	for i := 0; i < n; i++ {
		q.Enqueue(fmt.Sprintf("c-%02d", i), nil, EnqueueOptions{})
	}
	stop := make(chan struct{})
	var cwg sync.WaitGroup
	cwg.Add(1)
	go func() {
		defer cwg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				q.Compact()
			}
		}
	}()
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task, err := q.Claim(time.Minute)
				if errors.Is(err, ErrNoTask) {
					return
				}
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				mu.Lock()
				seen[task.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(stop)
	cwg.Wait()
	if len(seen) != n {
		t.Fatalf("claimed %d distinct, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("task %s claimed %d times", id, c)
		}
	}
}
