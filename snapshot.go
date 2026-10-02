package durablequeue

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// snapshotVersion is bumped when the snapshot format changes.
const snapshotVersion = 1

// snapshot is a point-in-time dump of the whole queue state. It covers all
// WAL records with Seq <= LastSeq.
type snapshot struct {
	Version int    `json:"version"`
	LastSeq uint64 `json:"last_seq"`
	Tasks   []Task `json:"tasks"`
}

// Compact writes a snapshot of the current state and truncates the WAL.
// Crash safety: the snapshot is fully durable (write, fsync, atomic rename)
// before the log is truncated, and log sequence numbers stay monotonic, so
// a crash anywhere in the process recovers to a consistent state.
func (q *Queue) Compact() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkOpen(); err != nil {
		return err
	}
	if err := q.writeSnapshot(); err != nil {
		return err
	}
	return q.log.Truncate()
}

// writeSnapshot dumps the current state to disk durably. Caller holds q.mu.
func (q *Queue) writeSnapshot() error {
	snap := snapshot{
		Version: snapshotVersion,
		LastSeq: q.lastSeq,
		Tasks:   make([]Task, 0, len(q.tasks)),
	}
	for _, id := range q.order {
		snap.Tasks = append(snap.Tasks, *q.tasks[id])
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	final := filepath.Join(q.opts.Dir, snapFileName)
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	syncDirBestEffort(q.opts.Dir)
	return nil
}

// loadSnapshot reads the snapshot file if present and populates the queue
// state. It returns the sequence number the snapshot covers.
func (q *Queue) loadSnapshot() (uint64, error) {
	data, err := os.ReadFile(filepath.Join(q.opts.Dir, snapFileName))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return 0, fmt.Errorf("queue: snapshot file is corrupted: %w", err)
	}
	if snap.Version != snapshotVersion {
		return 0, fmt.Errorf("queue: unsupported snapshot version %d", snap.Version)
	}
	for i := range snap.Tasks {
		t := snap.Tasks[i]
		q.apply(&t)
	}
	return snap.LastSeq, nil
}

// syncDirBestEffort flushes directory metadata so the rename survives a
// crash. Directory fsync is not supported on Windows (NTFS journals
// metadata instead), so errors are ignored.
func syncDirBestEffort(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
