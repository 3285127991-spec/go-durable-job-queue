package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	durablequeue "durablequeue"
)

func step(format string, args ...any) {
	fmt.Printf("\n== "+format+"\n", args...)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// runDemo executes a scripted, repeatable end-to-end demonstration. The demo
// directory is wiped first, so repeated runs behave identically.
func runDemo(dir string) error {
	fmt.Printf("demo: resetting data dir %q for a repeatable run\n", dir)
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	q, err := durablequeue.Open(durablequeue.Options{Dir: dir})
	if err != nil {
		return err
	}

	step("1. enqueue three tasks (flaky-api allows only 2 attempts)")
	for _, job := range []struct {
		id       string
		payload  string
		attempts int
	}{
		{"email-welcome", "send welcome email to user@example.com", 0},
		{"report-daily", "generate daily report", 0},
		{"flaky-api", "call unreliable third-party API", 2},
	} {
		task, _, err := q.Enqueue(job.id, []byte(job.payload), durablequeue.EnqueueOptions{MaxAttempts: job.attempts})
		must(err)
		fmt.Printf("   enqueued %-14s max_attempts=%d\n", job.id, task.MaxAttempts)
	}
	_, created, err := q.Enqueue("email-welcome", []byte("different payload"), durablequeue.EnqueueOptions{})
	must(err)
	fmt.Printf("   re-enqueue email-welcome -> created=%v (idempotent dedup by task ID)\n", created)
	printStats(q)

	step("2. worker claims and acks email-welcome")
	task, err := q.Claim(30 * time.Second)
	must(err)
	fmt.Printf("   claimed %s (attempt %d, token %.8s...)\n", task.ID, task.Attempts, task.LeaseToken)
	must(q.Ack(task.ID, task.LeaseToken))
	fmt.Printf("   acked %s\n", task.ID)

	step("3. report-daily fails with a 1s retry delay")
	task, err = q.Claim(30 * time.Second)
	must(err)
	must(q.Fail(task.ID, task.LeaseToken, time.Second, "report service timeout"))
	fmt.Printf("   failed %s, retry delayed by 1s\n", task.ID)
	task, err = q.Claim(30 * time.Second)
	must(err)
	fmt.Printf("   next claim returns %s instead (report-daily still delayed)\n", task.ID)

	step("4. flaky-api exhausts its attempts and lands in the dead-letter queue")
	for {
		if task.ID != "flaky-api" {
			return fmt.Errorf("expected flaky-api claim, got %s", task.ID)
		}
		must(q.Fail(task.ID, task.LeaseToken, 0, "upstream 503"))
		dead, _ := q.Get("flaky-api")
		fmt.Printf("   flaky-api failed (attempt %d/%d) -> status=%s\n", dead.Attempts, dead.MaxAttempts, dead.Status)
		if dead.Status == durablequeue.StatusDead {
			break
		}
		task, err = q.Claim(30 * time.Second)
		must(err)
	}
	printStats(q)

	step("5. after the 1s delay, report-daily becomes claimable again")
	time.Sleep(1100 * time.Millisecond)
	task, err = q.Claim(30 * time.Second)
	must(err)
	fmt.Printf("   claimed %s (attempt %d)\n", task.ID, task.Attempts)
	must(q.Ack(task.ID, task.LeaseToken))
	fmt.Printf("   acked %s\n", task.ID)

	step("6. lease expiry: a stale token cannot touch a reclaimed task")
	_, _, err = q.Enqueue("lease-demo", []byte("worker will crash"), durablequeue.EnqueueOptions{})
	must(err)
	first, err := q.Claim(300 * time.Millisecond)
	must(err)
	fmt.Printf("   worker A claims %s with 300ms lease\n", first.ID)
	time.Sleep(400 * time.Millisecond)
	second, err := q.Claim(30 * time.Second)
	must(err)
	fmt.Printf("   lease expired; worker B reclaims it with a NEW token\n")
	if err := q.Ack(first.ID, first.LeaseToken); errors.Is(err, durablequeue.ErrStaleToken) {
		fmt.Printf("   worker A acks with the OLD token -> rejected: %v\n", err)
	} else {
		return fmt.Errorf("stale token was not rejected: %v", err)
	}
	must(q.Ack(second.ID, second.LeaseToken))
	fmt.Printf("   worker B acks with the new token -> ok\n")

	step("7. compact: snapshot + WAL reset")
	walBefore := fileSize(dir, "queue.wal")
	must(q.Compact())
	walAfter := fileSize(dir, "queue.wal")
	fmt.Printf("   WAL size: %d bytes -> %d bytes (snapshot written)\n", walBefore, walAfter)

	step("8. simulate process kill (no clean shutdown) and restart")
	q = nil // abandoned without Close: every mutation was already fsynced
	q2, err := durablequeue.Open(durablequeue.Options{Dir: dir})
	if err != nil {
		return err
	}
	defer q2.Close()
	printStats(q2)

	step("9. final task list")
	for _, t := range q2.List() {
		fmt.Printf("   %-14s status=%-9s attempts=%d/%d\n", t.ID, t.Status, t.Attempts, t.MaxAttempts)
	}
	fmt.Println("\ndemo finished OK")
	return nil
}

func printStats(q *durablequeue.Queue) {
	s := q.Stats()
	fmt.Printf("   stats: total=%d pending=%d claimed=%d completed=%d dead=%d\n",
		s.Total, s.Pending, s.Claimed, s.Completed, s.Dead)
}

func fileSize(dir, name string) int64 {
	fi, err := os.Stat(dir + string(os.PathSeparator) + name)
	if err != nil {
		return -1
	}
	return fi.Size()
}
