package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeDecodeRoundtrip(t *testing.T) {
	tests := []struct {
		name string
		rec  walRecord
	}{
		{
			name: "put with value",
			rec:  walRecord{Op: OpPut, Key: "hello", Value: []byte("world")},
		},
		{
			name: "put with empty value",
			rec:  walRecord{Op: OpPut, Key: "empty", Value: []byte{}},
		},
		{
			name: "delete",
			rec:  walRecord{Op: OpDelete, Key: "gone"},
		},
		{
			name: "put with binary value",
			rec:  walRecord{Op: OpPut, Key: "bin", Value: []byte{0x00, 0xFF, 0x80}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := encodeRecord(tt.rec)
			r := bytes.NewReader(data)

			got, err := decodeRecord(r)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			if got.Op != tt.rec.Op {
				t.Errorf("op: got %d, want %d", got.Op, tt.rec.Op)
			}
			if got.Key != tt.rec.Key {
				t.Errorf("key: got %q, want %q", got.Key, tt.rec.Key)
			}
			if !bytes.Equal(got.Value, tt.rec.Value) {
				t.Errorf("value: got %v, want %v", got.Value, tt.rec.Value)
			}
		})
	}
}

func TestWAL_AppendAndReadAll(t *testing.T) {
	dir := t.TempDir()

	wal, err := OpenWAL(dir, true, 0) // no segment rotation
	if err != nil {
		t.Fatal(err)
	}

	recs := []walRecord{
		{Op: OpPut, Key: "k1", Value: []byte("v1")},
		{Op: OpPut, Key: "k2", Value: []byte("v2")},
		{Op: OpDelete, Key: "k1"},
	}

	for _, r := range recs {
		if err := wal.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	wal.Close()

	// Re-open and read back
	wal2, err := OpenWAL(dir, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	got, err := wal2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != len(recs) {
		t.Fatalf("got %d records, want %d", len(got), len(recs))
	}

	for i, r := range got {
		if r.Op != recs[i].Op || r.Key != recs[i].Key {
			t.Errorf("record %d: got {%d %s}, want {%d %s}",
				i, r.Op, r.Key, recs[i].Op, recs[i].Key)
		}
	}
}

// TestWAL_CorruptTailIsSkipped simulates a crash mid-write by appending
// garbage bytes to the end of a segment and verifies that only the corrupt
// tail record is dropped while prior records survive intact.
func TestWAL_CorruptTailIsSkipped(t *testing.T) {
	dir := t.TempDir()

	wal, err := OpenWAL(dir, true, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write one good record
	if err := wal.Append(walRecord{Op: OpPut, Key: "good", Value: []byte("data")}); err != nil {
		t.Fatal(err)
	}
	wal.Close()

	// Simulate crash: append garbage to the segment
	segments, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	f, err := os.OpenFile(segments[0], os.O_WRONLY|os.O_APPEND, 0640)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x05})
	f.Close()

	// Re-open — should recover the good record and skip the garbage
	wal2, err := OpenWAL(dir, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	got, err := wal2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 record after corrupt tail, got %d", len(got))
	}
	if got[0].Key != "good" {
		t.Errorf("expected key 'good', got %q", got[0].Key)
	}
}

func TestWAL_SegmentRotation(t *testing.T) {
	dir := t.TempDir()

	// Tiny segment size to force rotation
	wal, err := OpenWAL(dir, true, 50) // 50 bytes per segment
	if err != nil {
		t.Fatal(err)
	}

	// Write enough data to trigger at least one rotation
	for i := 0; i < 10; i++ {
		err := wal.Append(walRecord{
			Op:    OpPut,
			Key:   "key-that-forces-rotation",
			Value: []byte("value-that-is-not-tiny"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	wal.Close()

	// Should have multiple segment files
	segments, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(segments) < 2 {
		t.Errorf("expected multiple segments, got %d", len(segments))
	}

	// All records should still be readable across segments
	wal2, err := OpenWAL(dir, true, 50)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	got, err := wal2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("expected 10 records across segments, got %d", len(got))
	}
}