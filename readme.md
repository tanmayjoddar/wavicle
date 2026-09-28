<p align="center">
  <img src="https://img.shields.io/badge/status-pilot_ready-success?style=for-the-badge" alt="Status"/>
  <img src="https://img.shields.io/badge/incremental-7.9x-success?style=for-the-badge" alt="7.9x"/>
  <img src="https://img.shields.io/badge/hot_path-90ns-success?style=for-the-badge" alt="90ns"/>
  <img src="https://img.shields.io/badge/fail_closed-capable-success?style=for-the-badge" alt="fail-closed"/>
</p>

<br/>

# Wavicle — Proof-Based Cache Consistency Engine

> **Eliminate cache invalidation. Every read proves its own freshness. Zero code changes.**
>
> *Pilot-ready: sharded store + PostgreSQL CDC, 32× warm-reuse speedup, 90ns zero-alloc hot path, fail-closed reads when CDC is proven behind, 44M-op soak passed. Single node, no HA yet — stated up front.*

Every cached value carries a **receipt** (version vector) of what it depends on. On every read, the cache checks that receipt against the local frontier. If nothing changed, return immediately. If something changed, re-reduce only the affected sub-expressions. No TTL to tune. No pub/sub to wire. No `INVALIDATE` to forget. Freshness against Postgres is bounded by CDC lag (~50ms p99 same-VPC); when the stream is proven behind or blind, reads refuse instead of serving stale (fail-closed mode).

---

## Architecture (Production)

```
App → RESP3 (:6379, +TLS) → AUTH/ACL → Proof Engine → ShardedFrontierCache (32 shards) → PostgreSQL
                                                  ↓
                        3 paths: VV match, dirty propagation, cold compose
```

- **ShardedFrontierCache** is the production cache — 32 xxhash shards, O(unique keys) memory, LRU ceiling, TTL sweep, JSONL snapshots. No WAL, no compaction.
- **PostgreSQL** is the source of truth. Writes go to PG first, then FrontierCache. (MySQL poller available; true binlog tail is future work.)
- **Proof Engine** sits between protocol and cache, verifying freshness on every read.
- **CausalCrystal** (full DAG with WAL) is dev-mode only — used for algorithm validation.
- **Fail-closed reads**: PG slot health (lag ≥ page bytes, inactive/missing slot) or MySQL poll silence refuses reads with `STALE`. Idle-but-healthy always serves. Dev mode stays fail-open.

### READ Flow

```
Client GET users:42:name
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
Client SET users:42:name "Alice"
  → AppendAtom(expr, path) to WriteThroughStore
    → PostgresStore.AppendAtom (UPSERT into PG)
    → ShardedFrontierCache.AppendAtom (store latest atom, replace old;
       identical value + deadline deduped — no phantom invalidation)
  → Next read detects hash mismatch → incremental re-reduce
```

External writes (DBA, migrations, other services) are handled via PG logical replication.
The PGListener consumes WAL events, writes new atoms into FrontierCache, and the proof
engine detects the change on the next read — no invalidation logic needed.

---

## Benchmarks (5-run averages, 50-field composite, 12th Gen i5-1240P — in-process; add 200–500μs network on the wire)

| Benchmark | Latency | vs Cold | Allocs | What It Measures |
|-----------|---------|---------|--------|------------------|
| Cold compose (50 fields, from scratch) | **55,101 ns** (55μs) | 1.0× | 213 | First read — build everything |
| Incremental (1/50 field changed) | **6,980 ns** (7.0μs) | **7.9× faster** | 5 | Dirty propagation + re-reduce |
| Warm reuse FP1 (no changes) | **1,689 ns** (1.7μs) | **32.6× faster** | 0 | Version vector match — return cached |
| Single key hot read | **90 ns** | **612× faster** | 0 | Zero-alloc — one hash comparison |
| Merkle composite check | **406 ns** | **135.7× faster** | 0 | xxHash tree-level staleness |

Sharded vs single-mutex under 16-way load: **2.16×**. ACL check: ~40ns. Full numbers, failover math and billion-key model: `docc/GOAT_PROOF.md`.

---

## 2-Minute Production Soak

| Metric | Result |
|--------|--------|
| Workload | 4 writers + 16 readers, 1,000 keys, random SET/GET |
| Total External Ops | ~44M (4.3M SETs + 39.7M GETs) |
| Cache Hit Rate | **100.00%** (ProofCache for all reader ops after warmup) |
| Frontier Memory | **O(unique keys)** — 1,000 entries, no growth |
| Errors | **0** |

Longest run so far is 2 minutes — the 24h–7d cloud soak is tracked work, not claimed work (`docc/PILOT_CHECKLIST.md` Tier-1).

---

## Correctness (All Tests Pass — `go test ./...`)

| Test | What It Proves |
|------|----------------|
| PoisonWrite (sharded + crystal) | 50-field composite, mutate 1 field → **49/50 cache hits, latest value** |
| Same-value dedup | Identical SET mints no new hash (**no phantom invalidation**); EXPIRE still mints |
| Stale guard (PG slot + MySQL poll) | Blind/behind CDC fails reads; idle-healthy serves; boot grace serves |
| ReadAfterWrite / ConsecutiveWrites | SET Alice → SET Bob → GET → **returns Bob**, every time |
| Randomized dual-writer checker | Validity + per-key monotonic reads + stall recovery, fixed seed |
| Wire compat (HELLO/pipeline/parallel) | Raw RESP interop for redis-py/ioredis/go-redis/Jedis |
| Chaos (stall/snapshot/dir) | Fail-closed→recover, clean snapshot failure, serving survives |
| E2E TCP + PG integration | RESP3 SET/GET over TCP; CDC end-to-end where a DB is present |
| 2-min Soak | 20 workers, 1000 keys → **zero errors** |

---

## Wavicle vs Traditional Cache (Redis)

| Scenario | Redis (TTL=60s) | Wavicle |
|----------|-----------------|---------|
| External write, immediate read | **Returns stale** (TTL hasn't expired) | **Returns latest** once CDC delivers (~50ms p99 same-VPC); refuses if proven behind |
| 1 of 50 fields changes | Full rewrite or manual patch | **7.9× faster** — only dirty subtree re-reduced |
| Invalidation code | Must write + maintain | **Zero** — built into every read |
| Memory growth | O(total writes) if no eviction | **O(unique keys)** |

Versus CDC-cache systems (ReadySet, Redis Data Integration, Debezium+Kafka consumers):
same stream insight — watch the database log instead of hand-invalidating, no novelty
claimed there. What differs is per-read dependency-hash verification plus incremental
subtree re-reduce instead of whole-result recompute. No head-to-head benchmark against
them exists in this repo — only cold-recompute and TTL-window numbers are claimed.
| Head-to-head vs CDC caches | — | Not run against ReadySet-class systems; only cold-recompute and TTL windows claimed |

---

## Quick Start

```bash
# Build
go build -o wavicle .
go build -o wavicle-cli ./cmd/wavicle-cli/

# Run (dev mode — no PG needed; dev backend is for learning, NOT for benchmarks)
./wavicle

# In another terminal:
./wavicle-cli SET users:42:name "Alice"
./wavicle-cli GET users:42:name   # → "Alice"
./wavicle-cli DEL users:42:name
./wavicle-cli GET users:42:name   # → (nil)
```

> Pilot/production runs `WAVICLE_DB_TYPE=postgres` (see `docker-compose.yml`).
> Bare `./wavicle` is the WAL-backed dev backend (measured ~58ms SETs) — never quote it.

### CLI Commands

KV + batch: `SET GET MSET MGET GETSET SETNX SETEX PSETEX DEL EXISTS DBSIZE`
Counters/strings: `INCR DECR INCRBY DECRBY APPEND STRLEN`
Lists: `LPUSH RPUSH LPOP RPOP LLEN LRANGE LINDEX`
Sets: `SADD SREM SMEMBERS SCARD SISMEMBER`
Sorted sets: `ZADD ZSCORE ZCARD ZREM ZRANGE[WITHSCORES]`
Hashes: `HSET HGET HGETALL HMSET HMGET HDEL HLEN HEXISTS HKEYS HVALS`
Streams: `XADD XLEN XRANGE`
Expiry: `EXPIRE PEXPIRE TTL PTTL PERSIST`
Tx + misc: `MULTI EXEC DISCARD PING AUTH HELLO INFO KEYS SCAN TYPE FLUSHDB`

**No `INVALIDATE` anywhere — and no Lua (`EVAL` returns NOSCRIPT, by design).**
Key rule for PG-backed data: flat `table:id:column` keys via SET (see `docc/LIMITS.md` §2 for the two-hash trap).

Wire protocol is **RESP3**. Wavicle includes its own `wavicle-cli` tool, but any RESP3-compatible client can connect.

---

## PostgreSQL Setup (Production)

```sql
-- Required PG config
wal_level = logical
max_replication_slots = 5
max_wal_senders = 5

CREATE PUBLICATION wavicle_proofs FOR ALL TABLES;
-- tables need REPLICA IDENTITY FULL for UPDATE/DELETE decoding
SELECT pg_create_logical_replication_slot('wavicle_slot', 'pgoutput');
```

The slot watchdog (`WARN ≥1GB, PAGE ≥10GB` retained WAL) pages before a dead consumer
fills PG's disk. It never auto-drops the slot — that's a human decision (runbook procedure).

---

## Project Structure (60 files, ~11.5k lines Go)

```
wavicle/
├── main.go                              # Server entry: backends, CDC, TLS/ACL, snapshots, slot watchdog
├── internal/
│   ├── core/                            # Hash, Value types, interned CombinatorExpr algebra
│   ├── engine/                          # ★ THE MOAT — compose, incremental 3-path reduction, 64-shard proof cache
│   ├── storage/                         # ShardedFrontierCache (prod) · CausalCrystal (dev) · PostgresStore
│   ├── protocol/resp3/                  # RESP3/TLS server, extended commands, MULTI/EXEC, type index, stale guard
│   ├── auth/                            # Multi-user ACL + Redis-style ACL file parser
│   ├── replication/                     # PG pgoutput consumer, MySQL poller, slot watchdog
│   ├── cluster/                         # 128-vnode consistent-hash routing (sharding, not replication)
│   ├── config/                          # All knobs + WAVICLE_* env
│   └── telemetry/                       # Prometheus metrics (bounded label cardinality)
├── cmd/wavicle-cli/                     # CLI + REPL
├── cmd/wavicle-migrate/                 # Shadow prover: diffs vs Redis, exit 2 on drift
├── cmd/wavicle-bench/                   # Raw-RESP load generator (p50/p99, visibility scenario)
├── tests/{consistency,chaos,compat,integration}/  # Checkers, drills, wire tests, PG suites
├── benchmarks/                          # Proof, GOAT, billion-capacity, soak (CSV), load benches
├── configs/wavicle.yaml                 # Production config template (fail-closed armed)
├── deployments/acl/users.acl            # Example ACL file · deployments/postgres/init.sql
└── docc/                                # Local docs (git-ignored by policy) — see hub below
```

---

## Docs Hub (start here, by audience)

| You are | Read |
|---|---|
| A company evaluating us | `docc/PITCH.md` → `docc/PILOT_ONBOARDING.md` → `docc/LIMITS.md` |
| Integrating engineer | `docc/LIMITS.md` → `docc/RUNBOOK.md` → `configs/wavicle.yaml` |
| Evaluating the tech | `docc/PROJECT_BRIEF.md` (one-file total overview) → `docc/GOAT_PROOF.md` |
| Interviewing me | `INTERVIEW_PLAYBOOK.md` (never sent to companies — it contains our weak spots) |
| Operating staging | `docc/PILOT_CHECKLIST.md` → `docc/CHAOS_AND_PG_REALITY.md` |
| Learning hands-on | `docc/HANDS_ON_DEMO.md` · history: `docc/MAKING_HISTORY.md` |

Security policy: `SECURITY.md`. License: MIT (`LICENSE`).

---

## Complexity Bounds

| Operation | Time | Space |
|-----------|------|-------|
| Atom append (sharded FrontierCache) | O(1) | O(1) |
| Proof reduction — FastPath1 (version vector match) | O(m) | O(1) |
| Proof reduction — Incremental (k changed paths) | O(k log d) | O(k) |
| Proof composition (cold) | O(n) | O(n) |
| Proof cache lookup | O(1) | O(1) |

m = paths in version vector, d = tree depth, k = changed paths, n = total nodes.

---

## Standing (no spin)

Single node, no HA, no quorum. Longest soak 2 minutes. Numbers measured on a laptop
(see caveats above). MySQL is a poller, not binlog tail. Fail-closed armed in pilot
config; dev boots fail-open. What remains, in order: Raft quorum → binlog tail →
7-day soak + chaos → cloud benchmarks → design partner. Full accounting:
`docc/PROJECT_BRIEF.md` §2, §11.

---

## Built With

- **Go 1.25+** — single binary, zero runtime dependencies
- **golang.org/x/crypto** — SHA3-256 for content-addressed hashing
- **Standard library** — net, sync, atomic, crypto/rand, crypto/tls

---

## License

MIT — see `LICENSE`.

---

<p align="center">
  <sub>Build the first atom. Measure everything. Let reality decide.</sub>
</p>
