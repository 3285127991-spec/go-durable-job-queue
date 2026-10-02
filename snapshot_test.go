package durablequeue

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompactAndRecover(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustEnqueue(t, q, "s1", 3, 0)
	mustEnqueue(t, q, "s2", 3, 0)
	mustEnqueue(t, q, "s3", 3, 0)
	task, _ := q.Claim() // s1
	if err := q.Ack(task.ID, task.Token); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := q.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if size, _ := q.log.Size(); size != 0 {
		t.Fatalf("wal size after compact = %d, want 0", size)
	}
	// Keep working after compaction.
	mustEnqueue(t, q, "s4", 3, 0)
	task, _ = q.Claim() // s2
	if task == nil || task.ID != "s2" {
		t.Fatalf("claim after compact = %+v, want s2", task)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	q2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()
	want := map[string]Status{
		"s1": StatusDone,
		"s2": StatusClaimed,
		"s3": StatusPending,
		"s4": StatusPending,
	}
	for id, st := range want {
		got, err := q2.Get(id)
		if err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if got.Status != st {
			t.Fatalf("%s status = %s, want %s", id, got.Status, st)
		}
	}
	// Idempotency survives snapshot + recovery.
	if _, created, _ := q2.Enqueue("s1", nil, 0, 0); created {
		t.Fatal("duplicate enqueue after compact+reopen reported created=true")
	}
}

func TestSnapshotWithoutTruncateRecovers(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustEnqueue(t, q, "a", 3, 0)
	mustEnqueue(t, q, "b", 3, 0)
	// Simulate a crash between snapshot write and WAL truncate.
	q.mu.Lock()
	if err := q.writeSnapshot(); err != nil {
		q.mu.Unlock()
		t.Fatalf("writeSnapshot: %v", err)
	}
	q.mu.Unlock()
	q.Close()

	q2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()
	if got := len(q2.List()); got != 2 {
		t.Fatalf("tasks after reopen = %d, want 2", got)
	}
	// The queue must not re-apply stale records: enqueueing a duplicate
	// is still idempotent and claims still work.
	if _, created, _ := q2.Enqueue("a", nil, 0, 0); created {
		t.Fatal("stale record re-applied: duplicate reported created=true")
	}
	task, _ := q2.Claim()
	if task == nil || task.ID != "a" || task.Attempts != 1 {
		t.Fatalf("claim = %+v, want a attempts=1", task)
	}
}

func TestCorruptSnapshotFails(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustEnqueue(t, q, "x", 3, 0)
	if err := q.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	q.Close()

	path := filepath.Join(opts.Dir, snapFileName)
	if err := os.WriteFile(path, []byte("{not-json"), 0o666); err != nil {
		t.Fatalf("corrupt snapshot: %v", err)
	}
	_, err = Open(opts)
	if err == nil {
		t.Fatal("Open succeeded with corrupted snapshot, want error")
	}
	if !strings.Contains(err.Error(), "snapshot") {
		t.Fatalf("error = %v, want it to mention the snapshot", err)
	}
}

func TestRecoverFromSnapshotAndLogTail(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// One task fully done before the snapshot.
	mustEnqueue(t, q, "before-snap", 3, 0)
	task, _ := q.Claim()
	if err := q.Ack(task.ID, task.Token); err != nil {
		t.Fatalf("Ack before-snap: %v", err)
	}
	if err := q.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// Records after the snapshot live only in the WAL tail.
	mustEnqueue(t, q, "after-snap", 3, 0)
	task, _ = q.Claim()
	if task == nil || task.ID != "after-snap" {
		t.Fatalf("claim = %+v, want after-snap", task)
	}
	if err := q.Ack(task.ID, task.Token); err != nil {
		t.Fatalf("Ack after-snap: %v", err)
	}
	q.Close()

	q2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()
	before, _ := q2.Get("before-snap")
	after, _ := q2.Get("after-snap")
	if before.Status != StatusDone {
		t.Fatalf("before-snap = %s, want done (from snapshot)", before.Status)
	}
	if after.Status != StatusDone {
		t.Fatalf("after-snap = %s, want done (from wal tail)", after.Status)
	}
}
