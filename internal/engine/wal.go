// Package engine implements the dkv storage engine.
//
// WAL Design
//
// A Write-Ahead Log ensures durability: every mutation is appended to a
// sequential log file BEFORE it is applied to the in-memory state. On crash,
// we replay the log to reconstruct state.
//
// Why sequential writes? Disk I/O is dramatically faster for sequential access
// vs random. Even on SSDs, sequential writes avoid write amplification in the
// FTL (Flash Translation Layer).
//
// RECORD FORMAT (binary, length-prefixed):
//
//	[4 bytes: record length (big-endian uint32)]
//	[1 byte:  operation type (PUT=1, DELETE=2)]
//	[2 bytes: key length (big-endian uint16)]
//	[N bytes: key]
//	[M bytes: value]  (only for PUT; length = record_length - 1 - 2 - key_length)
//	[4 bytes: CRC32 checksum of the above]
//
// CRC32 detects bit rot and partial writes. If the checksum does not match on
// recovery, the record is corrupt (truncated write during crash) and replay stops.
// This is safe: the write that produced the corrupt tail record was never ACK'd
// to the client (we fsync before responding).
package engine

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// OpType represents a WAL operation.
type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// walRecord is the in-memory representation of a single WAL entry.
type walRecord struct {
	Op    OpType
	Key   string
	Value []byte // nil for deletes
}

// WAL is an append-only, crash-safe write-ahead log.
//
// The WAL has its own mutex, separate from the engine's RWMutex. The WAL lock
// protects file I/O ordering; the engine lock protects the in-memory map.
// Keeping them separate leaves the door open for group commit (batching
// multiple writes per fsync) without restructuring the read path.
type WAL struct {
	mu          sync.Mutex
	dir         string
	active      *os.File // current segment being written to
	syncOnWrite bool
	maxSegBytes int64
	bytesWritten int64
}

// OpenWAL opens (or creates) a WAL in the given directory.
func OpenWAL(dir string, syncOnWrite bool, maxSegBytes int64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}

	w := &WAL{
		dir:         dir,
		syncOnWrite: syncOnWrite,
		maxSegBytes: maxSegBytes,
	}

	// Open or create the latest segment.
	if err := w.rotateIfNeeded(); err != nil {
		return nil, err
	}

	return w, nil
}

// Append writes a record to the WAL. Thread-safe.
// Lock is held for the entire encode+write+sync sequence. In a high-throughput
// system, writes would be batched (group commit) to amortize fsync cost.
func (w *WAL) Append(rec walRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := encodeRecord(rec)

	if _, err := w.active.Write(data); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	w.bytesWritten += int64(len(data))

	// fsync forces the kernel to flush page cache to disk.
	// Without this, data lives in volatile memory and can be lost on power loss.
	if w.syncOnWrite {
		if err := w.active.Sync(); err != nil {
			return fmt.Errorf("wal: sync: %w", err)
		}
	}

	// Check if we need to rotate to a new segment file.
	if w.maxSegBytes > 0 && w.bytesWritten >= w.maxSegBytes {
		if err := w.rotateIfNeeded(); err != nil {
			return fmt.Errorf("wal: rotate: %w", err)
		}
	}

	return nil
}

// ReadAll replays every record across all segments in order.
// Segments are read in lexicographic order (zero-padded names = chronological).
// Each record is CRC-verified; a corrupt tail record stops replay of that segment.
func (w *WAL) ReadAll() ([]walRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	var records []walRecord
	for _, seg := range segments {
		recs, err := readSegment(seg)
		if err != nil {
			return nil, fmt.Errorf("wal: reading segment %s: %w", seg, err)
		}
		records = append(records, recs...)
	}

	return records, nil
}

// Close syncs and closes the active segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.active != nil {
		if err := w.active.Sync(); err != nil {
			return err
		}
		return w.active.Close()
	}
	return nil
}

// --- Segment management ---

// rotateIfNeeded creates a new segment file. Must be called with mu held.
func (w *WAL) rotateIfNeeded() error {
	if w.active != nil {
		if err := w.active.Sync(); err != nil {
			return err
		}
		if err := w.active.Close(); err != nil {
			return err
		}
	}

	// Name segments with zero-padded sequence numbers so lexicographic sort = chronological order.
	segments, _ := w.listSegments()
	nextSeq := len(segments) + 1
	name := filepath.Join(w.dir, fmt.Sprintf("wal-%010d.log", nextSeq))

	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
	if err != nil {
		return fmt.Errorf("wal: create segment %s: %w", name, err)
	}

	w.active = f
	w.bytesWritten = 0
	return nil
}

func (w *WAL) listSegments() ([]string, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}

	var segs []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".log") {
			segs = append(segs, filepath.Join(w.dir, e.Name()))
		}
	}
	sort.Strings(segs) // lexicographic = chronological due to zero-padding
	return segs, nil
}

// --- Binary encoding/decoding ---
//
// Binary encoding is ~10x more compact and far cheaper than JSON for this use
// case — the WAL is on the hot path of every write. protobuf would also work
// but adds a dependency for a format that never crosses a network boundary and
// has no schema-evolution requirements (it is internal, versioned with the binary).

func encodeRecord(rec walRecord) []byte {
	keyBytes := []byte(rec.Key)

	// Calculate total payload size: op(1) + keyLen(2) + key(N) + value(M)
	payloadLen := 1 + 2 + len(keyBytes) + len(rec.Value)

	// Full record: length(4) + payload(N) + crc(4)
	buf := make([]byte, 4+payloadLen+4)

	// Length prefix (does NOT include the length field itself or CRC)
	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadLen))

	// Payload
	offset := 4
	buf[offset] = byte(rec.Op)
	offset++
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(keyBytes)))
	offset += 2
	copy(buf[offset:], keyBytes)
	offset += len(keyBytes)
	copy(buf[offset:], rec.Value)
	offset += len(rec.Value)

	// CRC32 of the payload (not the length prefix)
	checksum := crc32.ChecksumIEEE(buf[4:offset])
	binary.BigEndian.PutUint32(buf[offset:offset+4], checksum)

	return buf
}

func decodeRecord(r io.Reader) (walRecord, error) {
	// Read length prefix
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return walRecord{}, err // io.EOF here means clean end of segment
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])

	// Sanity check: reject absurdly large records (likely corruption)
	const maxPayload = 10 * 1024 * 1024 // 10 MB
	if payloadLen > maxPayload {
		return walRecord{}, fmt.Errorf("wal: record too large (%d bytes), likely corrupt", payloadLen)
	}

	// Read payload + CRC
	data := make([]byte, payloadLen+4)
	if _, err := io.ReadFull(r, data); err != nil {
		return walRecord{}, fmt.Errorf("wal: truncated record: %w", err)
	}

	// Verify CRC
	payload := data[:payloadLen]
	storedCRC := binary.BigEndian.Uint32(data[payloadLen:])
	actualCRC := crc32.ChecksumIEEE(payload)
	if storedCRC != actualCRC {
		return walRecord{}, fmt.Errorf("wal: CRC mismatch (stored=%x actual=%x), corrupt record", storedCRC, actualCRC)
	}

	// Decode payload
	op := OpType(payload[0])
	keyLen := binary.BigEndian.Uint16(payload[1:3])
	key := string(payload[3 : 3+keyLen])

	var value []byte
	if op == OpPut {
		value = make([]byte, len(payload)-3-int(keyLen))
		copy(value, payload[3+keyLen:])
	}

	return walRecord{Op: op, Key: key, Value: value}, nil
}

// readSegment reads all valid records from a single segment file.
// Stops at EOF or first corrupt record (partial write during crash).
func readSegment(path string) ([]walRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records []walRecord
	for {
		rec, err := decodeRecord(f)
		if err != nil {
			if err == io.EOF {
				break // clean end
			}
			// Corrupt record — stop replaying this segment.
			// This is safe: the write that produced this record never got ACK'd
			// to the client (we fsync BEFORE responding).
			break
		}
		records = append(records, rec)
	}
	return records, nil
}