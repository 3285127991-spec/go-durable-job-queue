// Command dq is the CLI for the durablequeue library.
//
// Usage:
//
//	dq [--dir PATH] <command> [flags]
//
// Commands: enqueue, claim, ack, fail, get, list, stats, compact, demo.
// All state lives under --dir (default ./data). No external services needed.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	durablequeue "durablequeue"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, durablequeue.ErrNoTask) {
			fmt.Fprintln(os.Stderr, "no claimable task")
			os.Exit(3)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	g := flag.NewFlagSet("dq", flag.ContinueOnError)
	dir := g.String("dir", "data", "data directory (all queue state lives here)")
	if err := g.Parse(args); err != nil {
		return err
	}
	rest := g.Args()
	if len(rest) == 0 {
		usage()
		return errors.New("command required")
	}
	cmd, cmdArgs := rest[0], rest[1:]

	if cmd == "demo" {
		d := flag.NewFlagSet("demo", flag.ContinueOnError)
		if err := d.Parse(cmdArgs); err != nil {
			return err
		}
		return runDemo(*dir)
	}

	q, err := durablequeue.Open(durablequeue.Options{Dir: *dir})
	if err != nil {
		return err
	}
	defer q.Close()

	switch cmd {
	case "enqueue":
		f := flag.NewFlagSet("enqueue", flag.ContinueOnError)
		id := f.String("id", "", "task id (required, idempotency key)")
		payload := f.String("payload", "", "payload string")
		payloadFile := f.String("payload-file", "", "read payload from file")
		maxAttempts := f.Int("max-attempts", 0, "max attempts before dead-letter (0 = default 3)")
		delay := f.Duration("delay", 0, "initial visibility delay, e.g. 5s")
		if err := f.Parse(cmdArgs); err != nil {
			return err
		}
		var body []byte
		if *payloadFile != "" {
			if body, err = os.ReadFile(*payloadFile); err != nil {
				return err
			}
		} else {
			body = []byte(*payload)
		}
		task, created, err := q.Enqueue(*id, body, durablequeue.EnqueueOptions{
			MaxAttempts: *maxAttempts,
			Delay:       *delay,
		})
		if err != nil {
			return err
		}
		if created {
			fmt.Printf("enqueued %s (seq=%d, max_attempts=%d)\n", task.ID, task.Seq, task.MaxAttempts)
		} else {
			fmt.Printf("duplicate: task %s already exists (status=%s), unchanged\n", task.ID, task.Status)
		}
		return nil

	case "claim":
		f := flag.NewFlagSet("claim", flag.ContinueOnError)
		lease := f.Duration("lease", durablequeue.DefaultLease, "lease duration, e.g. 30s")
		if err := f.Parse(cmdArgs); err != nil {
			return err
		}
		task, err := q.Claim(*lease)
		if err != nil {
			return err
		}
		return printJSON(map[string]any{
			"id":           task.ID,
			"token":        task.LeaseToken,
			"payload":      string(task.Payload),
			"attempts":     task.Attempts,
			"max_attempts": task.MaxAttempts,
			"lease_expiry": task.LeaseExpiry.Format(time.RFC3339Nano),
		})

	case "ack":
		f := flag.NewFlagSet("ack", flag.ContinueOnError)
		id := f.String("id", "", "task id")
		token := f.String("token", "", "lease token from claim")
		if err := f.Parse(cmdArgs); err != nil {
			return err
		}
		if err := q.Ack(*id, *token); err != nil {
			return err
		}
		fmt.Printf("acked %s\n", *id)
		return nil

	case "fail":
		f := flag.NewFlagSet("fail", flag.ContinueOnError)
		id := f.String("id", "", "task id")
		token := f.String("token", "", "lease token from claim")
		retryDelay := f.Duration("retry-delay", 0, "delay before the task becomes claimable again")
		cause := f.String("error", "", "failure reason (stored on the task)")
		if err := f.Parse(cmdArgs); err != nil {
			return err
		}
		if err := q.Fail(*id, *token, *retryDelay, *cause); err != nil {
			return err
		}
		task, _ := q.Get(*id)
		if task.Status == durablequeue.StatusDead {
			fmt.Printf("failed %s: max attempts reached, moved to dead-letter queue\n", *id)
		} else {
			fmt.Printf("failed %s: will retry (attempt %d/%d)\n", *id, task.Attempts, task.MaxAttempts)
		}
		return nil

	case "get":
		f := flag.NewFlagSet("get", flag.ContinueOnError)
		id := f.String("id", "", "task id")
		if err := f.Parse(cmdArgs); err != nil {
			return err
		}
		task, err := q.Get(*id)
		if err != nil {
			return err
		}
		return printJSON(taskView(task))

	case "list":
		f := flag.NewFlagSet("list", flag.ContinueOnError)
		status := f.String("status", "", "filter: pending|claimed|completed|dead")
		if err := f.Parse(cmdArgs); err != nil {
			return err
		}
		var tasks []*durablequeue.Task
		if *status == "" {
			tasks = q.List()
		} else {
			tasks = q.List(durablequeue.Status(*status))
		}
		views := make([]map[string]any, 0, len(tasks))
		for _, t := range tasks {
			views = append(views, taskView(t))
		}
		return printJSON(views)

	case "stats":
		return printJSON(q.Stats())

	case "compact":
		if err := q.Compact(); err != nil {
			return err
		}
		fmt.Println("compacted: snapshot written, WAL reset")
		return nil

	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func taskView(t *durablequeue.Task) map[string]any {
	v := map[string]any{
		"id":           t.ID,
		"status":       t.Status,
		"payload":      string(t.Payload),
		"attempts":     t.Attempts,
		"max_attempts": t.MaxAttempts,
		"not_before":   t.NotBefore.Format(time.RFC3339Nano),
		"updated_at":   t.UpdatedAt.Format(time.RFC3339Nano),
	}
	if t.LeaseToken != "" {
		v["lease_token"] = t.LeaseToken
		v["lease_expiry"] = t.LeaseExpiry.Format(time.RFC3339Nano)
	}
	if t.LastError != "" {
		v["last_error"] = t.LastError
	}
	return v
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func usage() {
	fmt.Fprintln(os.Stderr, `dq - durable local job queue

usage: dq [--dir PATH] <command> [flags]

commands:
  enqueue  --id ID [--payload S | --payload-file F] [--max-attempts N] [--delay D]
  claim    [--lease D]                      -> prints task + lease token as JSON
  ack      --id ID --token T
  fail     --id ID --token T [--retry-delay D] [--error MSG]
  get      --id ID
  list     [--status pending|claimed|completed|dead]
  stats
  compact                                 -> write snapshot, reset WAL
  demo                                    -> repeatable end-to-end demo`)
}
