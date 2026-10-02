// Package wal implements an append-only write-ahead log with CRC-framed
// records. A torn write at the tail of the log (partial header, partial
// payload, or a bad checksum with nothing valid after it) is discarded so
// all previously committed records stay intact. Corruption in the middle of
// the log, where valid records still exist after the damaged one, is
// reported as an explicit error.
package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Record frame layout (little endian):
//
//	magic   uint32        frame marker
//	seq     uint64        monotonic sequence number
//	length  uint32        payload length in bytes
//	crc     uint32        CRC-32 (Castagnoli) of seq||length||payload
//	payload [length]byte
const (
	magicFrame     uint32 = 0x314C5157 // "WLQ1"
	headerSize            = 4 + 8 + 4 + 4
	maxPayloadSize        = 64 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt is returned when a damaged record is followed by at least one
// valid record, meaning the log is corrupted in the middle and automatic
// recovery is not safe.
var ErrCorrupt = errors.New("wal: log corrupted")

// Record is a single decoded log entry.
type Record struct {
	Seq     uint64
	Payload []byte
}

// WAL is an append-only log file. It is not safe for concurrent use;
// callers must serialize access.
type WAL struct {
	f          *os.File
	nextSeq    uint64
	syncWrites bool
}

// Open opens (or creates) the log at path, recovers all valid records, and
// truncates a torn tail if present. Sequences handed out by Append start at
// startSeq or one past the highest recovered sequence, whichever is larger.
func Open(path string, startSeq uint64, syncWrites bool) (*WAL, []Record, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, nil, err
	}
	w := &WAL{f: f, nextSeq: startSeq, syncWrites: syncWrites}
	records, err := w.recover()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return w, records, nil
}

func (w *WAL) recover() ([]Record, error) {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	var records []Record
	var offset int64
	header := make([]byte, headerSize)
	for {
		n, err := io.ReadFull(w.f, header)
		if err == io.EOF && n == 0 {
			break // clean end of log
		}
		if err != nil {
			// Partial header: torn tail, keep everything before it.
			if terr := w.truncateAt(offset); terr != nil {
				return nil, terr
			}
			break
		}
		magic := binary.LittleEndian.Uint32(header[0:4])
		seq := binary.LittleEndian.Uint64(header[4:12])
		length := binary.LittleEndian.Uint32(header[12:16])
		wantCRC := binary.LittleEndian.Uint32(header[16:20])

		valid := magic == magicFrame && length <= maxPayloadSize
		var payload []byte
		if valid {
			payload = make([]byte, length)
			if _, err := io.ReadFull(w.f, payload); err != nil {
				valid = false // torn payload
			}
		}
		if valid && checksum(header[4:16], payload) != wantCRC {
			valid = false
		}
		if !valid {
			later, serr := w.hasValidRecordAfter(offset)
			if serr != nil {
				return nil, serr
			}
			if later {
				return nil, fmt.Errorf("%w at offset %d: valid records exist after the damaged record", ErrCorrupt, offset)
			}
			// Nothing valid after this point: torn tail, discard it.
			if terr := w.truncateAt(offset); terr != nil {
				return nil, terr
			}
			break
		}
		records = append(records, Record{Seq: seq, Payload: payload})
		if seq >= w.nextSeq {
			w.nextSeq = seq + 1
		}
		offset += int64(headerSize) + int64(length)
	}
	if _, err := w.f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	return records, nil
}

// hasValidRecordAfter scans the bytes after offset for any fully valid
// record frame. Used to distinguish mid-log corruption from a torn tail.
func (w *WAL) hasValidRecordAfter(offset int64) (bool, error) {
	if _, err := w.f.Seek(offset+1, io.SeekStart); err != nil {
		return false, err
	}
	rest, err := io.ReadAll(w.f)
	if err != nil {
		return false, err
	}
	var magicBuf [4]byte
	binary.LittleEndian.PutUint32(magicBuf[:], magicFrame)
	for i := 0; i+headerSize <= len(rest); i++ {
		if !bytes.Equal(rest[i:i+4], magicBuf[:]) {
			continue
		}
		length := binary.LittleEndian.Uint32(rest[i+12 : i+16])
		if length > maxPayloadSize || i+headerSize+int(length) > len(rest) {
			continue
		}
		payload := rest[i+headerSize : i+headerSize+int(length)]
		wantCRC := binary.LittleEndian.Uint32(rest[i+16 : i+20])
		if checksum(rest[i+4:i+16], payload) == wantCRC {
			return true, nil
		}
	}
	return false, nil
}

func (w *WAL) truncateAt(offset int64) error {
	if err := w.f.Truncate(offset); err != nil {
		return err
	}
	if _, err := w.f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	return w.f.Sync()
}

func checksum(header, payload []byte) uint32 {
	h := crc32.New(castagnoli)
	h.Write(header)
	h.Write(payload)
	return h.Sum32()
}

// Append writes one record and returns its sequence number. The record is
// fsynced before returning when the WAL was opened with syncWrites.
func (w *WAL) Append(payload []byte) (uint64, error) {
	if len(payload) > maxPayloadSize {
		return 0, fmt.Errorf("wal: payload too large (%d bytes)", len(payload))
	}
	seq := w.nextSeq
	buf := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], magicFrame)
	binary.LittleEndian.PutUint64(buf[4:12], seq)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[16:20], checksum(buf[4:16], payload))
	copy(buf[headerSize:], payload)
	if _, err := w.f.Write(buf); err != nil {
		return 0, err
	}
	if w.syncWrites {
		if err := w.f.Sync(); err != nil {
			return 0, err
		}
	}
	w.nextSeq++
	return seq, nil
}

// Truncate drops all records. The sequence counter keeps advancing so
// sequence numbers stay monotonic across log compaction.
func (w *WAL) Truncate() error {
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return w.f.Sync()
}

// LastSeq returns the sequence number of the most recently appended record,
// or 0 if no record has been appended through this handle.
func (w *WAL) LastSeq() uint64 {
	if w.nextSeq == 0 {
		return 0
	}
	return w.nextSeq - 1
}

// Size returns the current file size in bytes.
func (w *WAL) Size() (int64, error) {
	st, err := w.f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// Close flushes and closes the log file.
func (w *WAL) Close() error {
	if w.syncWrites {
		if err := w.f.Sync(); err != nil {
			w.f.Close()
			return err
		}
	}
	return w.f.Close()
}
