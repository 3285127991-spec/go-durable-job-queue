package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	durablequeue "durablequeue"
)

// cmdDemo runs a scripted, repeatable end-to-end demonstration. It wipes
// its data directory first, so every run starts from a clean slate.
func cmdDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	dir := fs.String("dir", "data-demo", "demo data directory (wiped on every run)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	fmt.Println("== durablequeue demo ==")
	fmt.Printf("data dir: %s (reset for a repeatable run)\n", *dir)
	if err := os.RemoveAll(*dir); err != nil {
		return err
	}

	q, err := durablequeue.Open(durablequeue.Options{
		Dir:           *dir,
		LeaseDuration: 500 * time.Millisecond,
		BaseBackoff:   300 * time.Millisecond,
		MaxBackoff:    2 * time.Second,
	})
	if err != nil {
		return err
	}

	step := func(format string, a ...any) { fmt.Printf("\n--- "+format+"\n", a...) }

	step("1. enqueue tasks (alpha, beta max-attempts=1, gamma delayed 1s)")
	mustEnqueue(q, "alpha", "send-welcome-email", 3, 0)
	mustEnqueue(q, "beta", "charge-card", 1, 0)
	mustEnqueue(q, "gamma", "generate-report", 3, time.Second)

	step("2. idempotent enqueue: 'alpha' again is a no-op")
	_, created, _ := q.Enqueue("alpha", []byte("different-payload"), 9, 0)
	fmt.Printf("created=%v (existing task kept)\n", created)

	step("3. worker claims alpha and acks it")
	task := mustClaim(q)
	fmt.Printf("claimed %s (attempt %d, token %.8s...)\n", task.ID, task.Attempts, task.Token)
	mustDemo(q.Ack(task.ID, task.Token))

	step("4. worker claims beta and fails it -> dead letter (max-attempts=1)")
	task = mustClaim(q)
	mustDemo(q.Fail(task.ID, task.Token))
	show(q, "beta")

	step("5. gamma is delayed: not claimable yet")
	if t, _ := q.Claim(); t == nil {
		fmt.Println("claim returned nothing (gamma still delayed)")
	}
	time.Sleep(1100 * time.Millisecond)
	task = mustClaim(q)
	fmt.Printf("after the delay, claimed %s\n", task.ID)

	step("6. worker dies: lease expires, another worker takes over")
	time.Sleep(600 * time.Millisecond) // lease (500ms) expires
	next := mustClaim(q)
	fmt.Printf("re-claimed %s with a NEW token (old token is now useless)\n", next.ID)
	if err := q.Ack(next.ID, task.Token); err != nil {
		fmt.Printf("old token ack rejected as expected: %v\n", err)
	}
	mustDemo(q.Ack(next.ID, next.Token))

	step("7. compact (snapshot + truncate log), close, and reopen")
	mustDemo(q.Compact())
	mustDemo(q.Close())
	q2, err := durablequeue.Open(durablequeue.Options{Dir: *dir})
	if err != nil {
		return err
	}
	defer q2.Close()
	fmt.Println("reopened after restart; state recovered from disk:")
	for _, t := range q2.List() {
		fmt.Printf("  %-6s %-8s attempts=%d/%d payload=%s\n", t.ID, t.Status, t.Attempts, t.MaxAttempts, t.Payload)
	}
	st := q2.Stats()
	fmt.Printf("\nstats: pending=%d claimed=%d done=%d dead=%d\n", st.Pending, st.Claimed, st.Done, st.Dead)
	fmt.Printf("files: %s\n", filepath.Join(*dir, "queue.wal")+" + "+filepath.Join(*dir, "queue.snap"))
	fmt.Println("\ndemo finished OK")
	return nil
}

func mustDemo(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "demo error:", err)
		os.Exit(1)
	}
}

func mustEnqueue(q *durablequeue.Queue, id, payload string, maxAttempts int, delay time.Duration) {
	_, _, err := q.Enqueue(id, []byte(payload), maxAttempts, delay)
	mustDemo(err)
}

func mustClaim(q *durablequeue.Queue) *durablequeue.Task {
	task, err := q.Claim()
	if err != nil || task == nil {
		fmt.Fprintf(os.Stderr, "demo error: claim failed: %v (%+v)\n", err, task)
		os.Exit(1)
	}
	return task
}

func show(q *durablequeue.Queue, id string) {
	t, err := q.Get(id)
	if err != nil {
		fmt.Fprintln(os.Stderr, "demo error:", err)
		os.Exit(1)
	}
	fmt.Printf("%s: status=%s attempts=%d/%d\n", t.ID, t.Status, t.Attempts, t.MaxAttempts)
}
