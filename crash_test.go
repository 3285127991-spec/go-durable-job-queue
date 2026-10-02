package durablequeue

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durablequeue/wal"
)

// TestHelperProcess is not a real test. It is re-executed as a subprocess
// by TestKillProcessRecovery to simulate a worker that crashes hard
// (os.Exit, no Close, no defers) in the middle of work.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("DQ_HELPER") != "1" {
		return
	}
	dir := os.Getenv("DQ_DIR")
	q, err := Open(Options{Dir: dir, LeaseDuration: 300 * time.Millisecond})
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper open:", err)
		os.Exit(2)
	}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("crash-job-%d", i)
		if _, _, err := q.Enqueue(id, []byte("payload-"+id), 3, 0); err != nil {
			fmt.Fprintln(os.Stderr, "helper enqueue:", err)
			os.Exit(2)
		}
	}
	task, err := q.Claim()
	if err != nil || task == nil {
		fmt.Fprintln(os.Stderr, "helper claim:", err, task)
		os.Exit(2)
	}
	// Record what we claimed so the parent test can verify, then die.
	marker := task.ID + " " + task.Token
	if err := os.WriteFile(filepath.Join(dir, "claimed.txt"), []byte(marker), 0o666); err != nil {
		fmt.Fprintln(os.Stderr, "helper marker:", err)
		os.Exit(2)
	}
	os.Exit(1) // simulated kill -9: no Close, no defers
}

// TestKillProcessRecovery kills a worker process mid-work and verifies the
// queue recovers a consistent state on the next open.
func TestKillProcessRecovery(t *testing.T) {
	dir := t.TempDir()

	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcess", "-test.v")
	cmd.Env = append(os.Environ(), "DQ_HELPER=1", "DQ_DIR="+dir)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("helper should have crashed, output:\n%s", out)
	}

	marker, err := os.ReadFile(filepath.Join(dir, "claimed.txt"))
	if err != nil {
		t.Fatalf("read marker: %v (helper output:\n%s)", err, out)
	}
	parts := strings.Fields(string(marker))
	claimedID, oldToken := parts[0], parts[1]

	// Reopen: all committed work must survive the kill.
	q, err := Open(Options{Dir: dir, LeaseDuration: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("reopen after kill: %v", err)
	}
	defer q.Close()
	if got := len(q.List()); got != 5 {
		t.Fatalf("tasks after crash = %d, want 5", got)
	}
	task, err := q.Get(claimedID)
	if err != nil {
		t.Fatalf("Get(%s): %v", claimedID, err)
	}
	if task.Status != StatusClaimed {
		t.Fatalf("claimed task status = %s, want claimed", task.Status)
	}
	// After the lease expires another worker can take over.
	time.Sleep(400 * time.Millisecond)
	reclaimed, err := q.Claim()
	if err != nil || reclaimed == nil {
		t.Fatalf("reclaim after lease expiry: %v %+v", err, reclaimed)
	}
	if reclaimed.ID != claimedID {
		t.Fatalf("reclaimed %s, want %s (FIFO)", reclaimed.ID, claimedID)
	}
	if reclaimed.Token == oldToken {
		t.Fatal("reclaim reused the dead worker's token")
	}
	// Now the dead worker's token must not operate on the new lease.
	if err := q.Ack(claimedID, oldToken); !errors.Is(err, ErrTokenMismatch) {
		t.Fatalf("Ack with dead worker token = %v, want ErrTokenMismatch", err)
	}
	if err := q.Ack(claimedID, reclaimed.Token); err != nil {
		t.Fatalf("Ack with new token: %v", err)
	}
}

// TestTornTailQueueLevel verifies a torn write at the end of the WAL keeps
// all previously committed queue state intact.
func TestTornTailQueueLevel(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustEnqueue(t, q, "t1", 3, 0)
	mustEnqueue(t, q, "t2", 3, 0)
	mustEnqueue(t, q, "t3", 3, 0)
	task, _ := q.Claim()
	if err := q.Ack(task.ID, task.Token); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	q.Close()

	// Simulate a partial write at the tail (crash mid-append).
	walPath := filepath.Join(opts.Dir, walFileName)
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if _, err := f.Write([]byte{0x57, 0x51, 0x4c, 0x31, 0xAA, 0xBB}); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	f.Close()

	q2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen with torn tail: %v", err)
	}
	defer q2.Close()
	if got := len(q2.List()); got != 3 {
		t.Fatalf("tasks after torn tail = %d, want 3", got)
	}
	done, _ := q2.Get("t1")
	if done.Status != StatusDone {
		t.Fatalf("t1 = %s, want done", done.Status)
	}
	// Queue keeps accepting work.
	mustEnqueue(t, q2, "t4", 3, 0)
	if next, _ := q2.Claim(); next == nil || next.ID != "t2" {
		t.Fatalf("claim after torn tail = %+v, want t2", next)
	}
}

// TestMidLogCorruptionQueueLevel verifies corruption in the middle of the
// WAL is reported as an explicit error instead of silently dropping data.
func TestMidLogCorruptionQueueLevel(t *testing.T) {
	opts := testOptions(t)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 6; i++ {
		mustEnqueue(t, q, fmt.Sprintf("m%d", i), 3, 0)
	}
	q.Close()

	walPath := filepath.Join(opts.Dir, walFileName)
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	// Flip a byte well inside the first record; valid records follow.
	data[40] ^= 0xFF
	if err := os.WriteFile(walPath, data, 0o666); err != nil {
		t.Fatalf("corrupt wal: %v", err)
	}

	_, err = Open(opts)
	if err == nil {
		t.Fatal("Open succeeded on mid-log corruption, want explicit error")
	}
	if !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("error = %v, want wal.ErrCorrupt", err)
	}
}
