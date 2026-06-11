# dkv

[![CI](https://github.com/marcuskal/dkv/actions/workflows/ci.yml/badge.svg)](https://github.com/marcuskal/dkv/actions/workflows/ci.yml)

A distributed key-value store written in Go. Raft-replicated writes, a custom write-ahead log for durability, gossip-based membership, consistent-hash request routing, distributed locks with fencing tokens, and full observability (Prometheus, OpenTelemetry tracing, Grafana).

I built this to understand how systems like etcd and Consul actually work, by building one and breaking it on purpose. Every layer below the Raft library is hand-written: the storage engine, the WAL binary format, the gRPC service plumbing, the cluster coordinator, the smart client.

**This is a learning system, not a production database.** The interesting part is the engineering inside it, and the failure modes it survives. Both are documented below.

## Architecture

```
                        ┌─────────────────────────────────────────────┐
                        │                  dkv node                   │
                        │                                             │
   client ──gRPC──────▶ │  transport (interceptors: panic recovery,   │
   (retries, circuit    │   metrics+tracing+logging, timeouts, mTLS)  │
    breaker, ring-      │        │                                    │
    aware routing)      │        ▼                                    │
                        │  router ── not my key? ──▶ forward to owner │
                        │        │                                    │
                        │        ▼                                    │
                        │  raft (hashicorp/raft) ── replicate ──────▶ │──▶ peers
                        │        │ committed                          │
                        │        ▼                                    │
                        │  FSM ──▶ engine ──▶ WAL (fsync) ──▶ memtable│
                        │                                             │
                        │  serf gossip ──▶ coordinator ──▶ ring +     │
                        │                   raft voter set            │
                        └─────────────────────────────────────────────┘
```

Write path: client → gRPC → Raft leader → quorum replication → FSM apply → WAL append + fsync → in-memory map → ack. A write is never acknowledged before it is durable on disk and committed by a quorum.

Membership: nodes discover each other over Serf gossip. A coordinator translates join/leave/fail events into Raft voter changes (leader only) and hash-ring updates (every node). No static cluster config beyond seed addresses.

## What's inside

- **Storage engine** with a hand-rolled WAL: length-prefixed binary records, CRC32 per record, segment rotation, and crash recovery that replays segments in order and stops at the first corrupt record. A torn write during a crash loses only the un-acked tail entry, never the log.
- **Raft consensus** via hashicorp/raft with BoltDB log/stable stores, file snapshots, leadership transfer on graceful shutdown, and an FSM that applies committed commands (put, delete, atomic batch, lock, unlock) to the engine.
- **Cluster membership** via Serf gossip, decoupled from consensus. The coordinator is the only place that knows "what happens when a node joins."
- **Consistent hashing** with virtual nodes for request routing, plus server-side forwarding so a request landing on the wrong node still succeeds.
- **Distributed locks** with TTLs and monotonic fencing tokens, stored in the Raft log so lock state survives leader failover. Expired locks are reclaimed lazily on the next acquire.
- **A smart client** (`pkg/client`): ring-aware routing, exponential backoff with full jitter, and a per-node circuit breaker. Retries only on retryable codes; `NotFound` returns immediately.
- **Observability as a first-class layer**: RED metrics on every RPC, domain metrics (Raft term/state, FSM apply latency, WAL bytes and fsyncs, keys stored, locks held), OpenTelemetry spans from the gRPC boundary down into the engine, and logs correlated by trace ID. Metrics and health probes (`/healthz`, `/readyz`) run on a separate HTTP port so a scrape surge or an overloaded gRPC server can't blind the monitoring.
- **A load generator** (`cmd/stress`) with configurable concurrency and read/write ratio, used to drive the dashboards and to reproduce failure scenarios under load.

## Running it

Requires Go 1.22+ and Docker (for the observability stack).

```bash
make build

# Terminal 1, 2, 3 — a three-node cluster on localhost
make run-node1
make run-node2
make run-node3

# Terminal 4 — observability stack + load
make obs-up        # Prometheus :9091, Grafana :3000 (admin/admin), Jaeger :16686
make stress-live   # sustained mixed read/write load
```

The Grafana dashboard ships pre-provisioned: p99 latency, error rate by gRPC code, Raft term changes (i.e. elections), FSM apply latency, per-node key counts, in-flight RPCs.

The demo I'd show you: start the load, `kill -9` the leader, and watch the dashboard. You see the election as a term bump, a brief spike of `Unavailable` errors while there's no leader, the client's circuit breaker and jittered retries absorbing it, and throughput recovering in ~4 seconds with zero acknowledged writes lost. Then restart the dead node and watch it catch up from the leader's log.

## Design decisions

**WAL before memory, always.** The engine appends to the WAL and fsyncs before touching the in-memory map. Reversing the order creates phantom writes: the client sees success, the machine dies, the data is gone. Boring rule, and the entire durability story depends on it.

**Corrupt records end replay, they don't crash it.** Recovery CRC-checks every record and stops a segment at the first failure. This is safe by construction: a record can only be corrupt if the crash happened mid-write, and a mid-write crash means the client never got an ack for it. The test suite proves this by injecting garbage into a segment tail and recovering.

**Hand-rolled binary WAL format instead of JSON or protobuf.** The WAL is on the hot path of every write. The binary format is a few fixed-width fields plus a CRC, roughly 10x smaller and far cheaper to encode than JSON, with no schema-evolution needs because it never leaves the process.

**Hand-built gRPC service descriptor.** The service registration (`grpc.ServiceDesc`, message types, client stubs) is written by hand rather than generated by protoc, with a JSON codec registered through gRPC's public codec API. I did this to learn exactly what protoc generates instead of treating it as magic. The cost is real: JSON serialization is several times slower than protobuf, and it's the first thing I'd swap for production use. It's listed in the roadmap.

**Two mutexes in the engine, not one.** The WAL has its own lock, separate from the engine's RWMutex on the map. Today the engine holds them sequentially; keeping them separate leaves the door open for group commit (batching multiple writes per fsync, the way etcd does) without restructuring the read path.

**Locks return fencing tokens, not just success.** A client can hold a lock, pause (GC, network partition), have the lock expire, and resume believing it still holds it. The monotonically increasing fence token lets downstream systems reject the stale holder. A distributed lock without fencing is a race condition with extra steps.

**Per-node Prometheus registries, no globals.** Global metric registration panics on double-register, which makes multi-node integration tests in one process miserable. Each node gets its own registry; tests spin up three nodes side by side without collisions.

## Failure modes exercised

These aren't hypothetical; each has a test or a documented reproduction:

- Crash and restart: write, kill, recover from WAL, verify every acked key survives and deleted keys stay deleted.
- Torn write: garbage appended to a WAL segment is detected by CRC and skipped without losing prior records.
- Writes to a follower are rejected with `Unavailable` plus the leader's address, never silently dropped or locally applied.
- Leader kill under load: election completes, clients ride through with retries, no acked write is lost.
- WAL segment rotation: records remain readable in order across many small segments.
- Concurrent access under `go test -race` with parallel readers and writers hammering the engine.

## Consistency model and honest limitations

- **Writes are linearizable** (single Raft group, leader-only writes, quorum commit). **Reads are not**: they're served from the local node's state and can be stale on followers. Linearizable reads would need leader leases or read-index, which is on the roadmap.
- The full dataset is replicated to every node and lives in memory. There is no sharding of data placement yet; the hash ring distributes request routing, not storage. Memory is the capacity ceiling.
- The engine WAL grows until restart compaction; Raft snapshots bound the Raft log, but engine-level snapshot+truncate is not done.
- Lock expiry is lazy (checked on next acquire), so a dead client's lock can linger until someone wants it.
- Security defaults are dev-friendly: TLS/mTLS is supported but off by default.

I'd rather list these than have you find them. Knowing exactly where the system is weak is most of what I learned building it.

## Testing

```bash
make test        # everything, -race, including 3-node in-process cluster tests
make test-short  # skip the slow multi-node tests
```

The multi-node integration tests boot a real three-node Raft cluster in one process, elect a leader, write through it, and assert replication and error semantics on the followers.

## Layout

```
cmd/dkv          node binary (boot sequence, graceful shutdown)
cmd/stress       load generator
internal/engine  storage engine + WAL
internal/raft    raft node, FSM, command types
internal/membership, internal/coordinator   serf gossip → cluster changes
internal/hashring, internal/router          consistent hashing + forwarding
internal/lock    distributed lock manager (fencing tokens)
internal/observability  metrics, tracing, metrics/health HTTP server
internal/transport/grpc server + interceptor chain
pkg/api          service contract (hand-written ServiceDesc + codec)
pkg/client       ring-aware client with retries and circuit breaking
```

## Roadmap

Read-index or lease-based linearizable reads; protobuf codec; engine snapshot + WAL truncation; group commit for WAL fsync batching; chaos tests with injected partitions (toxiproxy).

## License

MIT
