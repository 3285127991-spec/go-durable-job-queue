// Command dq is the CLI for the durablequeue library. All state lives in a
// local data directory (default ./data, override with -dir or DQ_DIR).
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	durablequeue "durablequeue"
)

func usage() {
	fmt.Print(`dq - durable job queue CLI

Usage: dq <command> [flags]

Commands:
  enqueue   -id ID -payload TEXT [-max-attempts N] [-delay D]
  claim     [-lease D]                     lease and print the next task (JSON)
  ack       -id ID -token TOKEN            mark a claimed task done
  fail      -id ID -token TOKEN            fail the attempt (retry or dead-letter)
  get       -id ID                         print one task (JSON)
  list      [-status pending|claimed|done|dead]
  requeue   -id ID                         move a dead task back to pending
  compact                                 write a snapshot and truncate the log
  stats                                    print queue statistics
  demo                                     run a scripted end-to-end demo

Every command accepts -dir DIR (default: env DQ_DIR or "data").
Durations D use Go syntax, e.g. 500ms, 2s, 1m.
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch cmd := os.Args[1]; cmd {
	case "enqueue":
		err = cmdEnqueue(os.Args[2:])
	case "claim":
		err = cmdClaim(os.Args[2:])
	case "ack":
		err = cmdAckFail(os.Args[2:], true)
	case "fail":
		err = cmdAckFail(os.Args[2:], false)
	case "get":
		err = cmdGet(os.Args[2:])
	case "list":
		err = cmdList(os.Args[2:])
	case "requeue":
		err = cmdRequeue(os.Args[2:])
	case "compact":
		err = cmdCompact(os.Args[2:])
	case "stats":
		err = cmdStats(os.Args[2:])
	case "demo":
		err = cmdDemo(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func dataDir(fs *flag.FlagSet) *string {
	def := os.Getenv("DQ_DIR")
	if def == "" {
		def = "data"
	}
	return fs.String("dir", def, "data directory")
}

func openQueue(dir string) (*durablequeue.Queue, error) {
	return durablequeue.Open(durablequeue.Options{Dir: dir})
}

func printJSON(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

func cmdEnqueue(args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ContinueOnError)
	dir := dataDir(fs)
	id := fs.String("id", "", "task id (required, idempotent)")
	payload := fs.String("payload", "", "task payload")
	maxAttempts := fs.Int("max-attempts", durablequeue.DefaultMaxAttempts, "max delivery attempts before dead-letter")
	delay := fs.Duration("delay", 0, "delay before the task becomes claimable")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("-id is required")
	}
	q, err := openQueue(*dir)
	if err != nil {
		return err
	}
	defer q.Close()
	task, created, err := q.Enqueue(*id, []byte(*payload), *maxAttempts, *delay)
	if err != nil {
		return err
	}
	if created {
		fmt.Printf("enqueued %s\n", task.ID)
	} else {
		fmt.Printf("task %s already exists (idempotent, status=%s)\n", task.ID, task.Status)
	}
	return nil
}

func cmdClaim(args []string) error {
	fs := flag.NewFlagSet("claim", flag.ContinueOnError)
	dir := dataDir(fs)
	lease := fs.Duration("lease", 0, "lease duration (default 30s)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	q, err := openQueue(*dir)
	if err != nil {
		return err
	}
	defer q.Close()
	task, err := q.ClaimWithLease(*lease)
	if err != nil {
		return err
	}
	if task == nil {
		fmt.Println("no tasks available")
		return nil
	}
	printJSON(task)
	return nil
}

func cmdAckFail(args []string, ack bool) error {
	name := "ack"
	if !ack {
		name = "fail"
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	dir := dataDir(fs)
	id := fs.String("id", "", "task id (required)")
	token := fs.String("token", "", "lease token from claim (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" || *token == "" {
		return errors.New("-id and -token are required")
	}
	q, err := openQueue(*dir)
	if err != nil {
		return err
	}
	defer q.Close()
	if ack {
		err = q.Ack(*id, *token)
	} else {
		err = q.Fail(*id, *token)
	}
	if err != nil {
		return err
	}
	task, _ := q.Get(*id)
	fmt.Printf("%s ok: %s -> %s (attempts %d/%d)\n", name, task.ID, task.Status, task.Attempts, task.MaxAttempts)
	return nil
}

func cmdGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	dir := dataDir(fs)
	id := fs.String("id", "", "task id (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("-id is required")
	}
	q, err := openQueue(*dir)
	if err != nil {
		return err
	}
	defer q.Close()
	task, err := q.Get(*id)
	if err != nil {
		return err
	}
	printJSON(task)
	return nil
}

func cmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	dir := dataDir(fs)
	status := fs.String("status", "", "filter by status: pending|claimed|done|dead")
	if err := fs.Parse(args); err != nil {
		return err
	}
	q, err := openQueue(*dir)
	if err != nil {
		return err
	}
	defer q.Close()
	var tasks []*durablequeue.Task
	if *status == "" {
		tasks = q.List()
	} else {
		tasks = q.List(durablequeue.Status(*status))
	}
	printJSON(tasks)
	return nil
}

func cmdRequeue(args []string) error {
	fs := flag.NewFlagSet("requeue", flag.ContinueOnError)
	dir := dataDir(fs)
	id := fs.String("id", "", "task id (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("-id is required")
	}
	q, err := openQueue(*dir)
	if err != nil {
		return err
	}
	defer q.Close()
	if err := q.Requeue(*id); err != nil {
		return err
	}
	fmt.Printf("requeued %s\n", *id)
	return nil
}

func cmdCompact(args []string) error {
	fs := flag.NewFlagSet("compact", flag.ContinueOnError)
	dir := dataDir(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	q, err := openQueue(*dir)
	if err != nil {
		return err
	}
	defer q.Close()
	if err := q.Compact(); err != nil {
		return err
	}
	fmt.Println("compacted: snapshot written, log truncated")
	return nil
}

func cmdStats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	dir := dataDir(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	q, err := openQueue(*dir)
	if err != nil {
		return err
	}
	defer q.Close()
	printJSON(q.Stats())
	return nil
}
