package engine

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

func testLogger() zerolog.Logger {
	return zerolog.New(os.Stderr).Level(zerolog.Disabled)
}

func openTestEngine(t *testing.T) *Engine {
	t.Helper()
	dir := t.TempDir()
	e, err := Open(dir, true, 0, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func TestEngine_PutGet(t *testing.T) {
	e := openTestEngine(t)

	if err := e.Put("name", []byte("mufasa")); err != nil {
		t.Fatal(err)
	}

	val, err := e.Get("name")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "mufasa" {
		t.Errorf("got %q, want %q", val, "mufasa")
	}
}

func TestEngine_Delete(t *testing.T) {
	e := openTestEngine(t)

	e.Put("k", []byte("v"))
	e.Delete("k")

	_, err := e.Get("k")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestEngine_Overwrite(t *testing.T) {
	e := openTestEngine(t)

	e.Put("k", []byte("v1"))
	e.Put("k", []byte("v2"))

	val, _ := e.Get("k")
	if string(val) != "v2" {
		t.Errorf("got %q, want %q", val, "v2")
	}
}

func TestEngine_EmptyKey(t *testing.T) {
	e := openTestEngine(t)

	if err := e.Put("", []byte("val")); !errors.Is(err, ErrKeyEmpty) {
		t.Errorf("expected ErrKeyEmpty, got %v", err)
	}

	if _, err := e.Get(""); !errors.Is(err, ErrKeyEmpty) {
		t.Errorf("expected ErrKeyEmpty, got %v", err)
	}
}

// TestEngine_CrashRecovery proves the WAL durability guarantee end-to-end:
// write data, close the engine, reopen from the same WAL directory, verify
// every acked key survived and deleted keys stayed deleted.
func TestEngine_CrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Write data and close
	e1, err := Open(dir, true, 0, testLogger())
	if err != nil {
		t.Fatal(err)
	}

	e1.Put("survived-1", []byte("yes"))
	e1.Put("survived-2", []byte("also-yes"))
	e1.Put("deleted", []byte("should-not-survive"))
	e1.Delete("deleted")
	e1.Close()

	// Phase 2: Re-open — should recover from WAL
	e2, err := Open(dir, true, 0, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	val, err := e2.Get("survived-1")
	if err != nil || string(val) != "yes" {
		t.Errorf("survived-1: got %q/%v, want 'yes'/nil", val, err)
	}

	val, err = e2.Get("survived-2")
	if err != nil || string(val) != "also-yes" {
		t.Errorf("survived-2: got %q/%v, want 'also-yes'/nil", val, err)
	}

	_, err = e2.Get("deleted")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("deleted key should not survive recovery, got %v", err)
	}
}

// TestEngine_ConcurrentAccess stress-tests the RWMutex with parallel readers
// and writers. Run with -race to surface any data races.
func TestEngine_ConcurrentAccess(t *testing.T) {
	e := openTestEngine(t)

	const writers = 10
	const readers = 20
	const opsPerGoroutine = 100

	var wg sync.WaitGroup

	// Writers
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("w%d-k%d", id, i)
				e.Put(key, []byte(fmt.Sprintf("val-%d", i)))
			}
		}(w)
	}

	// Readers (will sometimes get ErrKeyNotFound, which is fine)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("w%d-k%d", id%writers, i)
				_, _ = e.Get(key) // best-effort read
			}
		}(r)
	}

	wg.Wait()

	// Verify at least one write landed
	if e.Len() == 0 {
		t.Error("expected at least some keys to be written")
	}
}

func TestEngine_CloseThenOp(t *testing.T) {
	e := openTestEngine(t)
	e.Close()

	if err := e.Put("k", []byte("v")); !errors.Is(err, ErrClosed) {
		t.Errorf("expected ErrClosed on Put, got %v", err)
	}
	if _, err := e.Get("k"); !errors.Is(err, ErrClosed) {
		t.Errorf("expected ErrClosed on Get, got %v", err)
	}
}

// TestEngine_DefensiveCopy verifies that callers cannot corrupt stored data
// by mutating the returned slice.
func TestEngine_DefensiveCopy(t *testing.T) {
	e := openTestEngine(t)

	original := []byte("original")
	e.Put("key", original)

	// Mutate the input slice — should NOT affect stored data
	original[0] = 'X'

	val, _ := e.Get("key")
	if string(val) != "original" {
		t.Errorf("write-side defensive copy failed: got %q", val)
	}

	// Mutate the returned slice — should NOT affect stored data
	val[0] = 'Z'
	val2, _ := e.Get("key")
	if string(val2) != "original" {
		t.Errorf("read-side defensive copy failed: got %q", val2)
	}
}
