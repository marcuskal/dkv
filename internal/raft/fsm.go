// Package quollraft implements the Raft consensus layer for quoll.
//
// The FSM adapter decouples consensus from storage:
//
//	Raft Log ──Apply()──▶ FSM ──Put/Delete──▶ Engine
//
// The Engine's contract is Put(key, value) error — it is unaware of replication.
package quollraft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"

	"github.com/marcuskal/quoll/internal/engine"
	"github.com/marcuskal/quoll/internal/observability"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// CommandType identifies the mutation being applied.
type CommandType uint8

const (
	CmdPut CommandType = iota
	CmdDelete
	CmdBatch  // atomic multi-key write
	CmdLock   // distributed lock acquire
	CmdUnlock // distributed lock release
)

// Command is the payload serialized into each Raft log entry.
type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value []byte      `json:"value,omitempty"`

	Batch      []BatchEntry `json:"batch,omitempty"`
	LockID     string       `json:"lock_id,omitempty"`
	Owner      string       `json:"owner,omitempty"`
	TTLSecs    int          `json:"ttl_secs,omitempty"`
	FenceToken uint64       `json:"fence_token,omitempty"`
}

// BatchEntry is a single operation in an atomic batch.
type BatchEntry struct {
	Op    CommandType `json:"op"`
	Key   string      `json:"key"`
	Value []byte      `json:"value,omitempty"`
}

// FSM implements raft.FSM by delegating to the Engine.
type FSM struct {
	engine  *engine.Engine
	log     zerolog.Logger
	metrics *observability.Metrics
	tracer  trace.Tracer

	// lock state is in-memory, rebuilt from the Raft log on recovery
	locks        map[string]*lockEntry
	fenceCounter uint64
}

type lockEntry struct {
	Owner      string
	FenceToken uint64
	ExpiresAt  time.Time
}

func NewFSM(eng *engine.Engine, log zerolog.Logger, metrics *observability.Metrics, tracer trace.Tracer) *FSM {
	if tracer == nil {
		// Use a no-op tracer so Apply never panics on a nil tracer when
		// observability isn't wired up (e.g., in K8s mode without an OTLP
		// collector configured).
		tracer = noop.NewTracerProvider().Tracer("quoll")
	}
	return &FSM{
		engine:  eng,
		log:     log.With().Str("component", "raft-fsm").Logger(),
		metrics: metrics,
		tracer:  tracer,
		locks:   make(map[string]*lockEntry),
	}
}

// Apply is called by Raft after a log entry is committed by a quorum.
// Must be deterministic — the same command must produce the same state on every node.
func (f *FSM) Apply(l *raft.Log) interface{} {
	start := time.Now()

	// Raft's Apply interface doesn't carry a context, so this span is a new
	// trace root rather than a child of the originating gRPC span.
	_, span := f.tracer.Start(
		newContextFromRaftLog(l),
		"raft.fsm.Apply",
		trace.WithAttributes(
			attribute.Int64("raft.log_index", int64(l.Index)),
			attribute.Int64("raft.log_term", int64(l.Term)),
		),
	)
	defer func() {
		span.End()
		if f.metrics != nil {
			f.metrics.RaftApplyDuration.Observe(time.Since(start).Seconds())
		}
	}()

	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		f.log.Error().Err(err).Uint64("index", l.Index).Msg("failed to unmarshal command")
		return fmt.Errorf("unmarshal command: %w", err)
	}

	span.SetAttributes(attribute.Int("cmd.type", int(cmd.Type)))

	switch cmd.Type {
	case CmdPut:
		span.SetAttributes(attribute.String("cmd.key", cmd.Key))
		err := f.engine.Put(cmd.Key, cmd.Value)
		if err != nil {
			f.log.Error().Err(err).Str("key", cmd.Key).Msg("FSM apply put failed")
		}
		return err

	case CmdDelete:
		span.SetAttributes(attribute.String("cmd.key", cmd.Key))
		err := f.engine.Delete(cmd.Key)
		if err != nil {
			f.log.Error().Err(err).Str("key", cmd.Key).Msg("FSM apply delete failed")
		}
		return err

	case CmdBatch:
		span.SetAttributes(attribute.Int("cmd.batch_size", len(cmd.Batch)))
		for _, entry := range cmd.Batch {
			var err error
			switch entry.Op {
			case CmdPut:
				err = f.engine.Put(entry.Key, entry.Value)
			case CmdDelete:
				err = f.engine.Delete(entry.Key)
			}
			if err != nil {
				f.log.Error().Err(err).Str("key", entry.Key).Msg("FSM batch op failed")
				return err
			}
		}
		return nil

	case CmdLock:
		span.SetAttributes(attribute.String("cmd.lock_id", cmd.LockID))
		return f.applyLock(cmd)

	case CmdUnlock:
		span.SetAttributes(attribute.String("cmd.lock_id", cmd.LockID))
		return f.applyUnlock(cmd)

	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

func (f *FSM) applyLock(cmd Command) interface{} {
	existing, held := f.locks[cmd.LockID]
	if held && time.Now().Before(existing.ExpiresAt) && existing.Owner != cmd.Owner {
		return fmt.Errorf("lock %s held by %s", cmd.LockID, existing.Owner)
	}

	f.fenceCounter++
	f.locks[cmd.LockID] = &lockEntry{
		Owner:      cmd.Owner,
		FenceToken: f.fenceCounter,
		ExpiresAt:  time.Now().Add(time.Duration(cmd.TTLSecs) * time.Second),
	}
	return f.fenceCounter
}

func (f *FSM) applyUnlock(cmd Command) interface{} {
	existing, held := f.locks[cmd.LockID]
	if !held {
		return nil // idempotent
	}
	if existing.Owner != cmd.Owner {
		return fmt.Errorf("lock %s not owned by %s", cmd.LockID, cmd.Owner)
	}
	delete(f.locks, cmd.LockID)
	return nil
}

// Snapshot returns a snapshot that captures the current Engine state.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	data, err := f.engine.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("engine snapshot: %w", err)
	}

	f.log.Info().Int("keys", len(data)).Msg("FSM snapshot created")
	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the Engine's entire state from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var data map[string][]byte
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	if err := f.engine.Restore(data); err != nil {
		return fmt.Errorf("engine restore: %w", err)
	}

	f.log.Info().Int("keys", len(data)).Msg("FSM restored from snapshot")
	return nil
}

type fsmSnapshot struct {
	data map[string][]byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(s.data)
		if err != nil {
			return fmt.Errorf("marshal snapshot: %w", err)
		}
		if _, err := sink.Write(b); err != nil {
			return fmt.Errorf("write snapshot: %w", err)
		}
		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// newContextFromRaftLog creates a context for tracing Raft log application.
// Since Raft's FSM interface doesn't propagate context, we create a fresh one.
// In production, you'd embed trace context in the Command payload itself
// to create parent-child relationships between the gRPC span and the FSM span.
func newContextFromRaftLog(l *raft.Log) context.Context {
	return context.Background()
}
