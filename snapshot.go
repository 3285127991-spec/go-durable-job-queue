package durablequeue

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const (
	walFileName      = "queue.wal"
	snapshotFileName = "queue.snapshot.json"
)

// snapshot is a point-in-time copy of the whole queue state. LSN records how
// far the WAL has been incorporated: records with lsn <= LSN are skipped
// during replay, which makes recovery idempotent even if a crash happens
// after the snapshot rename but before the WAL reset.
type snapshot struct {
	LSN   uint64  `json:"lsn"`
	Seq   uint64  `json:"seq"`
	Tasks []*Task `json:"tasks"`
}

// loadSnapshot loads the latest snapshot if one exists. A missing snapshot
// is not an error; an undecodable one is (explicit corruption report).
func (q *Queue) loadSnapshot() error {
	data, err := os.ReadFile(filepath.Join(q.dir, snapshotFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("snapshot: undecodable: %w", err)
	}
	for _, t := range snap.Tasks {
		if t == nil || t.ID == "" {
			return errors.New("snapshot: task without id")
		}
		q.tasks[t.ID] = t.clone()
	}
	q.lsn = snap.LSN
	q.seq = snap.Seq
	return nil
}

// writeSnapshotLocked serializes the current state to a temp file, fsyncs
// it, and atomically renames it over the previous snapshot. Caller holds mu.
func (q *Queue) writeSnapshotLocked() error {
	snap := snapshot{
		LSN:   q.lsn,
		Seq:   q.seq,
		Tasks: make([]*Task, 0, len(q.tasks)),
	}
	for _, t := range q.tasks {
		snap.Tasks = append(snap.Tasks, t.clone())
	}
	sort.Slice(snap.Tasks, func(i, j int) bool { return snap.Tasks[i].Seq < snap.Tasks[j].Seq })
	data, err := json.Marshal(&snap)
	if err != nil {
		return err
	}
	tmp := filepath.Join(q.dir, snapshotFileName+".tmp")
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
	if err := os.Rename(tmp, filepath.Join(q.dir, snapshotFileName)); err != nil {
		return err
	}
	syncDir(q.dir)
	return nil
}

// Compact writes a full snapshot of the current state and then empties the
// WAL. Crash safety: if the process dies after the snapshot rename but
// before the WAL reset, recovery loads the snapshot and skips the old WAL
// records via LSN, so nothing is applied twice and nothing is lost.
func (q *Queue) Compact() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	if err := q.writeSnapshotLocked(); err != nil {
		return err
	}
	return q.wal.reset()
}

// syncDir fsyncs a directory best-effort (unsupported on some platforms).
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
