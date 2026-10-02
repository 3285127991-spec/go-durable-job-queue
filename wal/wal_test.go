package wal

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openTemp(t *testing.T, startSeq uint64) (*WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	w, _, err := Open(path, startSeq, true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return w, path
}

func appendAll(t *testing.T, w *WAL, payloads ...string) {
	t.Helper()
	for _, p := range payloads {
		if _, err := w.Append([]byte(p)); err != nil {
			t.Fatalf("Append(%q): %v", p, err)
		}
	}
}

func reopen(t *testing.T, path string, startSeq uint64) (*WAL, []Record) {
	t.Helper()
	w, recs, err := Open(path, startSeq, true)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return w, recs
}

func fileBytes(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return b
}

func appendRaw(t *testing.T, path string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		t.Fatalf("Write raw: %v", err)
	}
}

func TestAppendAndRecover(t *testing.T) {
	w, path := openTemp(t, 1)
	var seqs []uint64
	for _, p := range []string{"alpha", "beta", "gamma"} {
		seq, err := w.Append([]byte(p))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		seqs = append(seqs, seq)
	}
	for i, s := range seqs {
		if s != uint64(i+1) {
			t.Fatalf("seq[%d] = %d, want %d", i, s, i+1)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, recs := reopen(t, path, 1)
	defer w2.Close()
	if len(recs) != 3 {
		t.Fatalf("recovered = %d, want 3", len(recs))
	}
	for i, want := range []string{"alpha", "beta", "gamma"} {
		if string(recs[i].Payload) != want {
			t.Fatalf("recs[%d] = %q, want %q", i, recs[i].Payload, want)
		}
		if recs[i].Seq != uint64(i+1) {
			t.Fatalf("recs[%d].Seq = %d, want %d", i, recs[i].Seq, i+1)
		}
	}
	seq, err := w2.Append([]byte("delta"))
	if err != nil {
		t.Fatalf("Append after recover: %v", err)
	}
	if seq != 4 {
		t.Fatalf("seq after recover = %d, want 4", seq)
	}
}

func TestTornTailPartialHeader(t *testing.T) {
	w, path := openTemp(t, 1)
	appendAll(t, w, "one", "two", "three")
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	good := fileBytes(t, path)

	appendRaw(t, path, []byte{0x57, 0x51, 0x4c, 0x31, 0x05})

	w2, recs := reopen(t, path, 1)
	defer w2.Close()
	if len(recs) != 3 {
		t.Fatalf("recovered %d records, want 3", len(recs))
	}
	if got := fileBytes(t, path); len(got) != len(good) {
		t.Fatalf("file size = %d, want %d (torn tail truncated)", len(got), len(good))
	}
	if _, err := w2.Append([]byte("four")); err != nil {
		t.Fatalf("Append after truncation: %v", err)
	}
}

func TestTornTailPartialPayload(t *testing.T) {
	w, path := openTemp(t, 1)
	appendAll(t, w, "aa", "bb")
	w.Close()
	good := fileBytes(t, path)

	// A valid-looking header claiming 100 payload bytes, then a fragment.
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], magicFrame)
	binary.LittleEndian.PutUint64(header[4:12], 3)
	binary.LittleEndian.PutUint32(header[12:16], 100)
	binary.LittleEndian.PutUint32(header[16:20], 12345)
	appendRaw(t, path, append(header, []byte("short")...))

	w2, recs := reopen(t, path, 1)
	defer w2.Close()
	if len(recs) != 2 {
		t.Fatalf("recovered %d records, want 2", len(recs))
	}
	if got := fileBytes(t, path); len(got) != len(good) {
		t.Fatalf("file size = %d, want %d", len(got), len(good))
	}
}

func TestTornTailBadCRCLastRecord(t *testing.T) {
	w, path := openTemp(t, 1)
	appendAll(t, w, "x", "y", "z")
	w.Close()

	b := fileBytes(t, path)
	// Corrupt one payload byte inside the LAST record frame.
	b[len(b)-1] ^= 0xFF
	if err := os.WriteFile(path, b, 0o666); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	w2, recs := reopen(t, path, 1)
	defer w2.Close()
	if len(recs) != 2 {
		t.Fatalf("recovered %d records, want 2 (bad tail record dropped)", len(recs))
	}
	if string(recs[0].Payload) != "x" || string(recs[1].Payload) != "y" {
		t.Fatalf("recovered payloads = %q,%q; want x,y", recs[0].Payload, recs[1].Payload)
	}
}

func TestMidLogCorruptionFails(t *testing.T) {
	w, path := openTemp(t, 1)
	appendAll(t, w, "first", "second", "third")
	w.Close()

	b := fileBytes(t, path)
	// Corrupt a payload byte in the FIRST record (offset headerSize),
	// leaving valid records after it.
	b[headerSize] ^= 0xFF
	if err := os.WriteFile(path, b, 0o666); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, _, err := Open(path, 1, true)
	if err == nil {
		t.Fatal("Open succeeded on mid-log corruption, want error")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}
}

func TestTruncateKeepsSequenceMonotonic(t *testing.T) {
	w, path := openTemp(t, 1)
	appendAll(t, w, "a", "b", "c")
	if err := w.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if size, _ := w.Size(); size != 0 {
		t.Fatalf("size after truncate = %d, want 0", size)
	}
	seq, err := w.Append([]byte("d"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq != 4 {
		t.Fatalf("seq after truncate = %d, want 4 (monotonic)", seq)
	}
	w.Close()

	w2, recs := reopen(t, path, 1)
	defer w2.Close()
	if len(recs) != 1 || string(recs[0].Payload) != "d" {
		t.Fatalf("recovered %+v, want single record d", recs)
	}
	if recs[0].Seq != 4 {
		t.Fatalf("recovered seq = %d, want 4", recs[0].Seq)
	}
}

func TestStartSeqFromSnapshot(t *testing.T) {
	// Simulates a snapshot taken at seq 41: the WAL continues at 42.
	w, _ := openTemp(t, 42)
	defer w.Close()
	seq, err := w.Append([]byte("x"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq != 42 {
		t.Fatalf("seq = %d, want 42", seq)
	}
}
