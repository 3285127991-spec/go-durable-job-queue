package durablequeue

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func walSize(t *testing.T, dir string) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

func populate(t *testing.T, q *Queue) {
	t.Helper()
	for _, id := range []string{"t1", "t2", "t3", "t4"} {
		if _, _, err := q.Enqueue(id, []byte("payload-"+id), EnqueueOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	c1, err := q.Claim(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(c1.ID, c1.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Claim(time.Minute); err != nil { // t2 stays claimed
		t.Fatal(err)
	}
}

func TestCompactAndRecover(t *testing.T) {
	q, dir := openTestQueue(t, nil)
	populate(t, q)
	if walSize(t, dir) == 0 {
		t.Fatal("expected non-empty WAL before compact")
	}
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := walSize(t, dir); got != 0 {
		t.Fatalf("WAL size after compact = %d, want 0", got)
	}
	if _, err := os.Stat(filepath.Join(dir, snapshotFileName)); err != nil {
		t.Fatalf("snapshot missing: %v", err)
	}
	q.Close()

	q2 := reopen(t, dir, nil)
	defer q2.Close()
	s := q2.Stats()
	if s.Total != 4 || s.Completed != 1 || s.Claimed != 1 || s.Pending != 2 {
		t.Fatalf("stats after compact+recover: %+v", s)
	}
	// Queue keeps working: new records append after the snapshot LSN.
	if _, _, err := q2.Enqueue("t5", nil, EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	q2.Close()
	q3 := reopen(t, dir, nil)
	defer q3.Close()
	if got := q3.Stats().Total; got != 5 {
		t.Fatalf("total=%d, want 5", got)
	}
}

func TestSnapshotOnlyRecovery(t *testing.T) {
	q, dir := openTestQueue(t, nil)
	populate(t, q)
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	q.Close()
	// Disaster: WAL is lost entirely. Snapshot must be enough.
	if err := os.Remove(filepath.Join(dir, walFileName)); err != nil {
		t.Fatal(err)
	}
	q2 := reopen(t, dir, nil)
	defer q2.Close()
	s := q2.Stats()
	if s.Total != 4 || s.Completed != 1 || s.Claimed != 1 || s.Pending != 2 {
		t.Fatalf("stats from snapshot only: %+v", s)
	}
	got, err := q2.Get("t3")
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Payload) != "payload-t3" {
		t.Fatalf("payload %q", got.Payload)
	}
}

// TestCrashBetweenSnapshotAndWALReset simulates a crash in the compaction
// crash window: snapshot already renamed, WAL not yet reset. Recovery must
// skip the already-snapshotted WAL records via LSN (no double apply).
func TestCrashBetweenSnapshotAndWALReset(t *testing.T) {
	q, dir := openTestQueue(t, nil)
	populate(t, q)
	// Write the snapshot but deliberately do NOT reset the WAL.
	q.mu.Lock()
	if err := q.writeSnapshotLocked(); err != nil {
		q.mu.Unlock()
		t.Fatal(err)
	}
	q.mu.Unlock()
	q.Close()

	q2 := reopen(t, dir, nil)
	defer q2.Close()
	s := q2.Stats()
	if s.Total != 4 || s.Completed != 1 || s.Claimed != 1 || s.Pending != 2 {
		t.Fatalf("stats after crash-window recovery: %+v", s)
	}
	// Attempts must not be double-counted by replaying old claim records.
	got, _ := q2.Get("t2")
	if got.Attempts != 1 {
		t.Fatalf("attempts=%d, want 1 (records applied twice?)", got.Attempts)
	}
}

func TestSnapshotCorruptionFails(t *testing.T) {
	q, dir := openTestQueue(t, nil)
	populate(t, q)
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	q.Close()
	if err := os.WriteFile(filepath.Join(dir, snapshotFileName), []byte("{not json"), 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Dir: dir}); err == nil {
		t.Fatal("expected snapshot corruption error, got nil")
	}
}

func TestRepeatedCompactionKeepsState(t *testing.T) {
	clock := newFakeClock()
	q, dir := openTestQueue(t, clock)
	for round := 0; round < 5; round++ {
		id := string(rune('a' + round))
		if _, _, err := q.Enqueue(id, nil, EnqueueOptions{}); err != nil {
			t.Fatal(err)
		}
		task, err := q.Claim(time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := q.Ack(task.ID, task.LeaseToken); err != nil {
			t.Fatal(err)
		}
		if err := q.Compact(); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
	}
	q.Close()
	q2 := reopen(t, dir, clock)
	defer q2.Close()
	s := q2.Stats()
	if s.Total != 5 || s.Completed != 5 {
		t.Fatalf("stats=%+v", s)
	}
}
