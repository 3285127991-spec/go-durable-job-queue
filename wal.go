package durablequeue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// WAL record framing:
//
//	[magic:4][lsn:8][payloadLen:4][crc32:4][payload]
//
// The CRC covers lsn || payloadLen || payload. A record is appended with a
// single Write call followed by fsync, so a crash can only leave a *prefix*
// of a record at the tail of the file (torn write). Anything else that does
// not decode is genuine corruption and is reported as an error.
const (
	walMagic      uint32 = 0x44514C47 // "DQLG"
	walHeaderSize        = 4 + 8 + 4 + 4
)

// CorruptionError is returned when the WAL contains a record that is fully
// present but invalid (bad magic or CRC mismatch). A torn tail is NOT
// corruption: it is truncated silently and previously written valid records
// are preserved.
type CorruptionError struct {
	Offset int64
	Reason string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("wal: corruption at offset %d: %s", e.Offset, e.Reason)
}

// IsCorruption reports whether err is a WAL corruption error.
func IsCorruption(err error) bool {
	var ce *CorruptionError
	return errors.As(err, &ce)
}

type wal struct {
	f      *os.File
	offset int64 // end of valid data == next append offset
	sync   bool
}

// openWAL opens (or creates) the WAL at path, truncates any torn tail, and
// calls fn for every valid record in order. fn may be nil.
func openWAL(path string, syncWrites bool, fn func(lsn uint64, payload []byte) error) (*wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	validEnd, err := scanWAL(f, fi.Size(), fn)
	if err != nil {
		f.Close()
		return nil, err
	}
	if validEnd < fi.Size() {
		// Torn tail: keep the valid prefix, drop the incomplete record.
		if err := f.Truncate(validEnd); err != nil {
			f.Close()
			return nil, err
		}
	}
	if _, err := f.Seek(validEnd, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return &wal{f: f, offset: validEnd, sync: syncWrites}, nil
}

// scanWAL validates records sequentially and returns the offset just past the
// last valid record. A truncated record at the tail is tolerated (torn
// write); a fully-present but invalid record is a CorruptionError.
func scanWAL(ra io.ReaderAt, size int64, fn func(lsn uint64, payload []byte) error) (int64, error) {
	var off int64
	hdr := make([]byte, walHeaderSize)
	for {
		remain := size - off
		if remain == 0 {
			return off, nil // clean EOF
		}
		if remain < walHeaderSize {
			return off, nil // torn header at tail
		}
		if _, err := ra.ReadAt(hdr, off); err != nil {
			return off, err
		}
		if binary.BigEndian.Uint32(hdr[0:4]) != walMagic {
			return off, &CorruptionError{Offset: off, Reason: "bad magic"}
		}
		lsn := binary.BigEndian.Uint64(hdr[4:12])
		n := binary.BigEndian.Uint32(hdr[12:16])
		recEnd := off + walHeaderSize + int64(n)
		if recEnd > size {
			return off, nil // torn payload at tail
		}
		payload := make([]byte, n)
		if _, err := ra.ReadAt(payload, off+walHeaderSize); err != nil {
			return off, err
		}
		c := crc32.NewIEEE()
		c.Write(hdr[4:16])
		c.Write(payload)
		if c.Sum32() != binary.BigEndian.Uint32(hdr[16:20]) {
			return off, &CorruptionError{Offset: off, Reason: "crc mismatch"}
		}
		if fn != nil {
			if err := fn(lsn, payload); err != nil {
				return off, err
			}
		}
		off = recEnd
	}
}

func (w *wal) append(lsn uint64, payload []byte) error {
	buf := make([]byte, walHeaderSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], walMagic)
	binary.BigEndian.PutUint64(buf[4:12], lsn)
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(payload)))
	c := crc32.NewIEEE()
	c.Write(buf[4:16])
	c.Write(payload)
	binary.BigEndian.PutUint32(buf[16:20], c.Sum32())
	copy(buf[walHeaderSize:], payload)
	if _, err := w.f.Write(buf); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	w.offset += int64(len(buf))
	if w.sync {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("wal sync: %w", err)
		}
	}
	return nil
}

// reset empties the WAL in place (used after a snapshot is safely durable).
func (w *wal) reset() error {
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.offset = 0
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

func (w *wal) close() error { return w.f.Close() }
