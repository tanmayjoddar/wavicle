<p align="center">
  <img src="https://img.shields.io/badge/status-phase2_complete-success?style=for-the-badge" alt="Status"/>
  <img src="https://img.shields.io/badge/incremental-7.9x-success?style=for-the-badge" alt="7.9x"/>
  <img src="https://img.shields.io/badge/hot_path-90ns-success?style=for-the-badge" alt="90ns"/>
  <img src="https://img.shields.io/badge/stale_reads-0%25-success?style=for-the-badge" alt="0% stale"/>
</p>

<br/>

# Wavicle — Proof-Based Cache Consistency Engine

> **Eliminate cache invalidation. Every read proves its own freshness. Zero code changes.**
>
> *Phase 2 complete — FrontierCache + PostgreSQL. 32× speedup on warm reuse. 90ns zero-alloc hot path. 0 stale reads. 44M op soak test passed.*

Every cached value carries a **receipt** (version vector) of what it depends on. On every read, the cache checks that receipt against current state. If nothing changed, return immediately. If something changed, re-reduce only the affected sub-expressions. No TTL to tune. No pub/sub to wire. No `INVALIDATE` to forget.

---

## Architecture (Production)

```
App → RESP3 (:6379) → Proof Engine → FrontierCache (O(unique keys)) → PostgreSQL
                              ↓
                  3 fast paths: VV match, dirty propagation, cold compose
```

- **FrontierCache** is the production cache — O(unique keys) memory, no WAL, no compaction.
- **PostgreSQL** is the source of truth. Writes go to PG first, then FrontierCache.
- **Proof Engine** sits between the RESP3 protocol and the cache, verifying freshness on every read.
- **CausalCrystal** (full DAG with WAL) is dev-mode only — used for algorithm validation.

### READ Flow

```
Client GET users:1:name
  → ProofCache lookup (SHA3 hash of path)
    → HIT → ReduceIncremental(proof)
      → FastPath1: VerifyVersionVector (compare every dep hash)
        → ALL MATCH: return cached value in ~1.7μs  ← typical case
        → SOME MISMATCH: find changed paths, propagate dirty up tree,
          re-reduce only dirty subtrees → ~7μs
    → MISS → ComposeProof (full build from FrontierCache) → ~55μs
  → Return value to client
```

### WRITE Flow

```
Client SET users:1:name "Alice"
  → AppendAtom(expr, path) to WriteThroughStore
    → PostgresStore.AppendAtom (UPSERT into PG)
    → FrontierCache.AppendAtom (store latest atom, replace old)
  → Next read detects hash mismatch → incremental re-reduce
```

External writes (DBA, migrations, other services) are handled via PG logical replication.
The PGListener consumes WAL events, writes new atoms into FrontierCache, and the proof
engine detects the change on the next read — no invalidation logic needed.

---

## Benchmarks (5-run averages, 50-field composite, 12th Gen i5-1240P)

| Benchmark | Latency | vs Cold | Allocs | What It Measures |
|-----------|---------|---------|--------|------------------|
| Cold compose (50 fields, from scratch) | **55,101 ns** (55μs) | 1.0× | 213 | First read — build everything |
| Incremental (1/50 field changed) | **6,980 ns** (7.0μs) | **7.9× faster** | 5 | Dirty propagation + re-reduce |
| Warm reuse FP1 (no changes) | **1,689 ns** (1.7μs) | **32.6× faster** | 0 | Version vector match — return cached |
| Single key hot read | **90 ns** | **612× faster** | 0 | Zero-alloc — one hash comparison |
| Merkle composite check | **406 ns** | **135.7× faster** | 0 | xxHash tree-level staleness |

---

## 2-Minute Production Soak (FrontierCache)

| Metric | Result |
|--------|--------|
| Workload | 4 writers + 16 readers, 1,000 keys, random SET/GET |
| Total External Ops | ~44M (4.3M SETs + 39.7M GETs) |
| Cache Hit Rate | **100.00%** (ProofCache for all reader ops after warmup) |
| Frontier Memory | **O(unique keys)** — 1,000 entries, no growth |
| Errors | **0** |

---

## Correctness (All Tests Pass)

| Test | What It Proves |
|------|----------------|
| PoisonWrite (FrontierCache) | 50-field composite, mutate 1 field → **49/50 cache hits, zero stale reads** |
| PoisonWrite (CausalCrystal) | Same scenario on dev backend → same result |
| ReadAfterWrite | SET Alice → SET Bob → GET → **returns Bob** |
| ConsecutiveWrites | 6 writes to same key, read after each → **all return latest** |
| E2E TCP | RESP3 SET/GET over TCP → **PASS** |
| 2-min Soak | 20 workers, 1000 keys → **zero errors** |

---

## Wavicle vs Traditional Cache (Redis)

| Scenario | Redis (TTL=60s) | Wavicle |
|----------|-----------------|---------|
| External write, immediate read | **Returns stale** (TTL hasn't expired) | **Returns latest** — VV detects hash mismatch |
| 1 of 50 fields changes | Full rewrite or manual patch | **7.9× faster** — only dirty subtree re-reduced |
| Invalidation code | Must write + maintain | **Zero** — built into every read |
| Memory growth | O(total writes) if no eviction | **O(unique keys)** |

---

## Quick Start

```bash
# Build
go build -o wavicle .
go build -o wavicle-cli ./cmd/wavicle-cli/

# Run (dev mode — no PG needed)
./wavicle

# In another terminal:
./wavicle-cli SET users:1:name "Alice"
./wavicle-cli GET users:1:name   # → "Alice"
./wavicle-cli DEL users:1:name
./wavicle-cli GET users:1:name   # → (nil)
```

### CLI Commands

| Command | Description | Invalidates? |
|---------|-------------|-------------|
| `SET key value` | Append new atom | ❌ None needed |
| `GET key` | Read with freshness verification | ❌ None needed |
| `MSET key val [key val...]` | Batch write | ❌ None needed |
| `MGET key [key...]` | Batch read | ❌ None needed |
| `HSET key field val` | Hash field write | ❌ None needed |
| `HGET key field` | Hash field read | ❌ None needed |
| `HGETALL key` | All hash fields | ❌ None needed |
| `DEL key` | Tombstone atom | ❌ None needed |
| `EXPIRE key sec` | Set atom TTL | ❌ None needed |
| `TTL key` | Remaining TTL | ❌ None needed |
| `EXISTS key` | Check key existence | ❌ None needed |
| `DBSIZE` | Count live keys | ❌ None needed |
| `PING` | Health check | ❌ None needed |

**Every column says the same thing: no invalidation needed.**

Wire protocol is **RESP3**. Wavicle includes its own `wavicle-cli` tool, but any RESP3-compatible client can connect.

---

## PostgreSQL Setup (Production)

```sql
-- Required PG config
wal_level = logical
max_replication_slots = 5
max_wal_senders = 5

CREATE PUBLICATION wavicle_proofs FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('wavicle_slot', 'pgoutput');
```

---

## Project Structure

```
wavicle/
├── main.go                              # Server entry point
├── internal/
│   ├── core/
│   │   ├── types.go                     # Core types (Hash, Value, CausalAtom, CombinatorExpr)
│   │   └── intern.go                    # AST interning (sync.Pool + 256-shard dedup)
│   ├── engine/                          # ★ THE MOAT — Proof Engine
│   │   ├── proof.go                     # MaterializedProof, ProofNode, VersionVector, dirty propagation
│   │   ├── compose.go                   # Cold proof composition pipeline
│   │   ├── incremental.go               # ReduceIncremental, FastPath1, 3-path reduction
│   │   ├── version_vector.go            # Merkle root computation
│   │   ├── cache.go                     # Sharded LRU proof cache (64 shards)
│   │   └── engine_test.go               # Correctness tests
│   ├── storage/                         # Storage backends
│   │   ├── adapter.go                   # Store interface + WriteThroughStore
│   │   ├── frontier_cache.go            # ★ PRODUCTION BACKEND — O(unique keys), no WAL
│   │   ├── crystal.go                   # Dev-mode CausalCrystal (WAL + compaction)
│   │   ├── postgres.go                  # PG-backed storage adapter
│   │   ├── wal.go                       # Write-ahead log (CausalCrystal)
│   │   └── frontier.go                  # Frontier index helper
│   ├── protocol/resp3/server.go         # RESP3 TCP server (13 commands)
│   ├── replication/                     # DB change stream
│   │   ├── adapter.go                   # ChangeListener interface
│   │   └── postgres.go                  # PG logical replication (pgoutput)
│   └── telemetry/metrics.go             # Prometheus-format metrics
├── benchmarks/
│   ├── proof_bench_test.go              # Core proof engine benchmarks
│   ├── soak_test.go                     # 2-min production soak (build tag: soak)
│   └── load_test.go                     # Concurrent load benchmarks
├── deployments/postgres/init.sql        # PG setup script
└── cmd/wavicle-cli/main.go              # CLI tool
```

---

## Complexity Bounds

| Operation | Time | Space |
|-----------|------|-------|
| Atom append (FrontierCache) | O(1) | O(1) |
| Proof reduction — FastPath1 (version vector match) | O(m) | O(1) |
| Proof reduction — Incremental (k changed paths) | O(k log d) | O(k) |
| Proof composition (cold) | O(n) | O(n) |
| Proof cache lookup | O(1) | O(1) |

m = paths in version vector, d = tree depth, k = changed paths, n = total nodes.

---

## Built With

- **Go 1.25+** — single binary, zero runtime dependencies
- **golang.org/x/crypto** — SHA3-256 for content-addressed hashing
- **Standard library** — net, sync, atomic, crypto/rand

---

## License

MIT

---

<p align="center">
  <sub>Build the first atom. Measure everything. Let reality decide.</sub>
</p>
