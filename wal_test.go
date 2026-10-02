package durablequeue

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func appendRaw(t *testing.T, path string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

func collectWAL(t *testing.T, path string) ([]uint64, [][]byte) {
	t.Helper()
	var lsns []uint64
	var payloads [][]byte
	w, err := openWAL(path, false, func(lsn uint64, payload []byte) error {
		lsns = append(lsns, lsn)
		payloads = append(payloads, append([]byte(nil), payload...))
		return nil
	})
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	w.close()
	return lsns, payloads
}

func TestWALRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.wal")
	w, err := openWAL(path, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 100; i++ {
		if err := w.append(i, []byte{byte(i), byte(i >> 8)}); err != nil {
			t.Fatal(err)
		}
	}
	w.close()

	lsns, payloads := collectWAL(t, path)
	if len(lsns) != 100 {
		t.Fatalf("got %d records, want 100", len(lsns))
	}
	for i, lsn := range lsns {
		if lsn != uint64(i+1) {
			t.Fatalf("record %d has lsn %d", i, lsn)
		}
		if !bytes.Equal(payloads[i], []byte{byte(i + 1), byte((i + 1) >> 8)}) {
			t.Fatalf("record %d payload mismatch", i)
		}
	}
}

func TestWALTornTailTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.wal")
	w, err := openWAL(path, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := w.append(i, []byte("payload-aaaa")); err != nil {
			t.Fatal(err)
		}
	}
	w.close()

	// Simulate a crash mid-write: half of a record header + some garbage.
	appendRaw(t, path, []byte{0x44, 0x51, 0x4c, 0x47, 0x00, 0x00, 0xde, 0xad})

	lsns, _ := collectWAL(t, path)
	if len(lsns) != 3 {
		t.Fatalf("got %d records after torn tail, want 3", len(lsns))
	}
	// The torn bytes must be physically truncated so new appends stay clean.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(3 * (walHeaderSize + len("payload-aaaa")))
	if fi.Size() != want {
		t.Fatalf("file size %d, want %d (torn tail not truncated)", fi.Size(), want)
	}
	// And the WAL keeps accepting appends after recovery.
	w2, err := openWAL(path, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.append(4, []byte("after-crash")); err != nil {
		t.Fatal(err)
	}
	w2.close()
	lsns, _ = collectWAL(t, path)
	if len(lsns) != 4 || lsns[3] != 4 {
		t.Fatalf("append after torn-tail recovery failed: %v", lsns)
	}
}

func TestWALTornTailTruncatedPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.wal")
	w, err := openWAL(path, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.append(1, []byte("ok")); err != nil {
		t.Fatal(err)
	}
	w.close()

	// A full header claiming a large payload, but the payload is missing.
	hdr := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:4], walMagic)
	binary.BigEndian.PutUint64(hdr[4:12], 2)
	binary.BigEndian.PutUint32(hdr[12:16], 4096)
	appendRaw(t, path, hdr)

	lsns, _ := collectWAL(t, path)
	if len(lsns) != 1 {
		t.Fatalf("got %d records, want 1", len(lsns))
	}
}

func TestWALMidCorruptionFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.wal")
	w, err := openWAL(path, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		if err := w.append(i, []byte("record-payload")); err != nil {
			t.Fatal(err)
		}
	}
	w.close()

	// Corrupt one byte inside the *middle* record's payload (record 3 of 5).
	recSize := int64(walHeaderSize + len("record-payload"))
	off := 2*recSize + walHeaderSize + 3
	f, err := os.OpenFile(path, os.O_RDWR, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	b := []byte{0}
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = openWAL(path, true, nil)
	if err == nil {
		t.Fatal("expected corruption error, got nil")
	}
	if !IsCorruption(err) {
		t.Fatalf("expected CorruptionError, got %T: %v", err, err)
	}
}

func TestWALBadMagicFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.wal")
	w, err := openWAL(path, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.append(1, []byte("x")); err != nil {
		t.Fatal(err)
	}
	w.close()
	// Overwrite the magic of the first record.
	f, err := os.OpenFile(path, os.O_RDWR, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0, 0, 0, 0}, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err := openWAL(path, true, nil); !IsCorruption(err) {
		t.Fatalf("expected CorruptionError, got %v", err)
	}
}

func TestWALReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.wal")
	w, err := openWAL(path, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 10; i++ {
		w.append(i, []byte("data"))
	}
	if err := w.reset(); err != nil {
		t.Fatal(err)
	}
	if err := w.append(11, []byte("fresh")); err != nil {
		t.Fatal(err)
	}
	w.close()
	lsns, _ := collectWAL(t, path)
	if len(lsns) != 1 || lsns[0] != 11 {
		t.Fatalf("after reset got %v, want [11]", lsns)
	}
}
