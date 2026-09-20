# Wavicle — In-Depth Architectural Specification & Systems Design

> **Document Status:** Authoritative Technical Specification  
> **Target Audience:** Systems Architects, Principal Engineers, Technical Due Diligence  
> **Source Code:** `wavicle/internal/...`

---

## 1. System Vision & The Core Invalidation Dilemma

### 1.1 The Fundamental Flaw of Reactive Invalidation
In modern web applications, the primary performance bottleneck is data assembly from relational databases. To mitigate this, developers place an in-memory key-value cache (e.g., Redis) between the application layer and the persistent store (e.g., PostgreSQL). 

However, cache invalidation in traditional architectures is **reactive and decoupled**:
1. **Heuristic Time-To-Live (TTL):** A blind timer is attached to a cached key. The cache serves stale data until the timer expires, or evicts fresh data prematurely if the underlying database row remained untouched.
2. **Imperative Invalidation (`DEL` on Write):** The application code updating the database must explicitly issue a cache deletion. In multi-service microservice topologies or distributed codebases, omitting an invalidation call across any write pathway causes **silent, permanent stale reads**.
3. **Coarse-Grained Cache Busting:** When a single field of a composite object (e.g., a dashboard aggregation or user profile spanning 50 attributes) changes, applications generally dump and recompute the entire object from scratch via a heavy SQL query.
4. **Out-of-Band Write Blindness:** Batch ETL scripts, database migrations, DBA maintenance, and external services writing directly to PostgreSQL bypass application-level cache eviction entirely.

### 1.2 The Proactive Verification Hypothesis
Wavicle eliminates reactive invalidation by making cached objects **self-verifying**:
- Every cached query or composite record carries a cryptographic **receipt**—a version vector mapping every dependency path to the hash of its underlying data atom.
- On every read, the cache **proactively proves its own freshness** against an in-memory live frontier.
- If the receipt matches, the value is returned in **~1.59 μs** with **0 allocations**.
- If one or more sub-fields mutated, dirty propagation marks only the affected branches of the proof tree, re-evaluating **only the dirty sub-expressions in ~9.66 μs** (**20.7× faster** than a full recompute), guaranteeing **0% stale reads**.

---

## 2. Complete Layer-by-Layer Architectural Decomposition

```
+=============================================================================+
|                           CLIENT APPLICATIONS                               |
|        (Connects via standard Redis client libraries or raw TCP)            |
+=============================================================================+
                                      |
                                      | TCP Socket (:6379)
                                      v
+=============================================================================+
|                      1. PROTOCOL & CONNECTION LAYER                         |
|                         (internal/protocol/resp3)                           |
|                                                                             |
|  * ListenAndServe(addr string): Accepts TCP connections, tracks activeConns |
|  * readCommand(r *bufio.Reader): Parses RESP3 arrays (*), bulk strings ($), |
|    and inline plaintext commands into argument vectors.                     |
|  * MaxConns guard (default 10,000) with atomic connection tracking.        |
|  * Supported commands: GET, SET, MGET, MSET, HGET, HSET, HGETALL, DEL,      |
|    EXPIRE, TTL, EXISTS, DBSIZE, PING, AUTH.                                 |
+=============================================================================+
                                      |
                                      v
+=============================================================================+
|                         2. PROOF ENGINE & AST CORE                          |
|                       (internal/engine, internal/core)                      |
|                                                                             |
|  * ProofCache: 64-shard LRU cache with xxHash key distribution.             |
|  * AST Hierarchy (CombinatorExpr):                                          |
|      - EConst (Level 0): Literal scalar/record value.                       |
|      - EFieldAccess (Level 1): Dynamic field projection from a record.      |
|      - ECompose (Level 2): Composite object assembling child atom hashes.   |
|      - EApply (Level 2): SKI combinator calculus application.               |
|  * InternTable: 256-shard AST deduplication with monotonic ExprHeader IDs.  |
|  * Three Read Paths:                                                        |
|      1. FastPath 1 (VV Match): In-memory hash comparison (~1.59μs, 0 alloc)|
|      2. Incremental Path: AST dirty propagation & sub-reduction (~9.66μs)   |
|      3. Cold Path: Causal closure construction & tree compilation (~199μs)  |
+=============================================================================+
                                      |
                                      v
+=============================================================================+
|                        3. IN-MEMORY STORAGE LAYER                           |
|                            (internal/storage)                               |
|                                                                             |
|  * FrontierCache (Production Engine):                                       |
|      - sync.RWMutex protecting entries (map[string]*core.CausalAtom).        |
|      - hashToPath index for reverse atom resolution.                        |
|      - Strictly bounded to O(unique keys) RAM. Zero WAL, zero ancestry.     |
|      - sweepExpired(): 60-second ticker reclaiming expired TTL entries.     |
|  * CausalCrystal (Development & Algorithmic Validation Engine):             |
|      - Full causal DAG with persistent Append-Only WAL & JSON serialization.|
|      - Active Frontier Index, Parent Index, and Merkle tree (SHA3-256).     |
|      - Periodic background compaction with double-buffering (.tmp / .old).  |
+=============================================================================+
                    |                                         ^
         Write-Through (Sync)                                 | CDC Stream (Async)
                    v                                         |
+=============================================================================+
|                       4. DURABLE PERSISTENCE & CDC                          |
|                      (internal/storage/postgres.go,                         |
|                       internal/replication/postgres.go)                     |
|                                                                             |
|  * WriteThroughStore: Synchronous write to PG, synchronous append to local. |
|  * PostgresStore: SQL identifier validation, parameter binding, UPSERTs.   |
|  * PGListener: pglogrepl logical replication client consuming PostgreSQL   |
|    WAL events via pgoutput plugin.                                          |
|  * Echo Suppression Guard: Compares commit timestamps and values to         |
|    prevent self-generated write-through updates from cycling through CDC.   |
+=============================================================================+
```

---

## 3. The AST Algebra & Combinator Core

Wavicle represents data and computation uniformly through an interned, typed combinator algebra in [`internal/core`](file:///d:/wavicle/internal/core).

### 3.1 Expression Types (`CombinatorExpr`)
Every node implements `CombinatorExpr`:
- `EConst`: Encapsulates a terminal `Value` (e.g., `VString`, `VInt`, `VRecord`, `VNull`).
- `EFieldAccess`: Encapsulates accessing a named field from an upstream record.
- `ECompose`: Encapsulates a composite object constructed from an array of atom hashes (`[]core.Hash`). This is the foundation of cached multi-field queries.
- `EApply`: Implements classical SKI combinator calculus ($I x = x$, $K x y = x$, $S x y z = (x z)(y z)$), enabling functional evaluation over dynamic graphs without compiling to machine bytecode.

### 3.2 Global Interning & Structural Hashing
To prevent GC thrashing from repeated AST allocation:
1. Every expression embeds a 24-byte `ExprHeader` containing an `ExprType`, tree depth, and atomic xxHash cache.
2. The `InternTable` uses 256 independent shards (`internShard`).
3. Calling `InternExpr(e)` calculates the structural hash. If an equivalent expression already exists in the shard, the pointer to the existing expression is returned. If not, a monotonic 64-bit ID is assigned and stored.

---

## 4. The Storage Engine Evolution: Crystal vs. FrontierCache

One of Wavicle's most critical engineering decisions was recognizing when an algorithmically pure data structure was inappropriate for production memory limits.

### 4.1 CausalCrystal (Dev & Algorithmic Validation)
Originally, Wavicle stored a complete cryptographic DAG in `CausalCrystal`:
- Every write appended a `CausalAtom` containing `CausalPast []Hash` (parent references) and `CausalDepth`.
- Atoms were committed to a disk WAL (`wal.go`) using synchronous `file.Sync()`.
- A 32-byte SHA3-256 Merkle tree updated incrementally with every atom insertion.

**The Production Flaw:**  
Under continuous mutation (e.g., 1,000 updates/sec to the same 1,000 keys), storing historical parents meant memory grew strictly as $O(\text{writes})$. Evicting old atoms required complex offline WAL rewriting and compaction cycles. If a recovery crashed mid-compaction, WAL state had to be restored from `.old` snapshots.

### 4.2 FrontierCache (Production Engine)
To solve this, `FrontierCache` was designed with a single constraint: **memory must scale with active data set size, not mutation history ($O(\text{unique keys})$).**
- Durability is delegated to PostgreSQL.
- Only the latest `CausalAtom` per path is retained in RAM.
- No WAL, no ancestry links, and no compaction routines exist in the production hot path.
- In our `benchstat` testing, eliminating multi-index locking and historical pointer chasing dropped warm reuse latency from **2.43 μs down to 1.59 μs** (a **34.4% reduction**, $p = 0.002$).

---

## 5. Mathematical Walkthrough of the Read Pipeline

### Step 1: ProofCache Lookup
When `GET key` is executed, the key path string and observation mode are hashed via SHA3-256 to form a 32-byte `queryHash`. The engine queries the 64-shard `ProofCache`.

### Step 2: FastPath 1 — Version Vector Exact Match
If a `MaterializedProof` is found, the engine executes:
```go
if local.VerifyVersionVector(proof.VersionVector.Entries) {
    proof.AccessCount++
    proof.LastVerifiedAt = time.Now().UnixNano()
    return proof.Value, nil
}
```
`VerifyVersionVector` iterates over the proof's version vector (`map[string]Hash`). For each path, it looks up the current atom hash in `FrontierCache`.
- If all hashes match and no atom has passed its `ExpiresAt` deadline, the cached `Value` is returned immediately.
- **Complexity:** $O(m)$ where $m$ is the number of dependencies.
- **Measured Latency:** **~1.59 μs**, **0 allocations**, **0 B/op**.

### Step 3: Incremental Path — Dirty Tree Propagation
If any hash mismatches:
1. `local.FindChangedPaths` identifies the dirty paths using a pre-allocated stack buffer `[64]string` to prevent heap escapes.
2. The engine calls `node.PropagateDirtyUp()` for each modified path:
   ```go
   func (n *ProofNode) PropagateDirtyUp() {
       if n.Dirty { return }
       n.Dirty = true
       n.MerkleValid = false
       if n.Parent != nil {
           n.Parent.PropagateDirtyUp()
       }
   }
   ```
3. `reduceDirty` traverses the tree:
   - Nodes where `Dirty == false` immediately return `node.CachedValue` ($O(1)$ pointer return).
   - Nodes where `Dirty == true` fetch the new atom expression from `FrontierCache` and recompute only that sub-expression.
4. The proof's version vector entries are updated in-place with the latest hashes.
5. **Complexity:** $O(k \log d)$ where $k$ is the number of changed leaves and $d$ is the tree depth.
6. **Measured Latency:** **~9.66 μs** (vs. 199.4 μs cold compose), achieving a **20.7× speedup**.

---

## 6. PostgreSQL Change Data Capture & Replication Pipeline

To capture writes originating outside of Wavicle (e.g., DBA updates, batch jobs, direct SQL mutations):

1. **Replication Protocol:** `PGListener` initiates a streaming connection to PostgreSQL using `github.com/jackc/pglogrepl` with the standard `pgoutput` plugin.
2. **LSN Checkpointing & Crash Recovery:**
   - The consumer tracks `lastLSN` and regularly checkpoints it to disk (`replication_checkpoint.lsn`).
   - Every status update and keepalive response transmits `WALWritePosition`, `WALFlushPosition`, and `WALApplyPosition` back to PostgreSQL.
   - This advances `confirmed_flush_lsn` in PostgreSQL, allowing PostgreSQL to safely recycle WAL logs on disk.
   - Upon process crash or restart, Wavicle reads the disk checkpoint and resumes replication from the exact byte offset without re-processing already-applied records or missing offline backlog mutations.
3. **Reconnection with Exponential Backoff:** If PostgreSQL restarts or primary failover occurs, `PGListener` enters an exponential backoff reconnect loop (1s to 30s) and increments `wavicle_db_reconnects_total`.
4. **Tuple Decoding:** Relation metadata messages (`*pglogrepl.RelationMessage`) are cached in memory. Inbound insert/update/delete tuple messages decode column values according to configured `TableMappings`.
5. **Echo Suppression Guard:** When Wavicle issues a write-through `SET`, it writes to PostgreSQL and appends the atom locally. Seconds later, PostgreSQL streams the same event over CDC. To prevent invalidating the proof cache unnecessarily, the CDC listener checks:
   - Is `CommitTime <= currentAtom.PhysicalTime`? If so, discard.
   - Does the decoded column string equal `currentAtom.Expr`? If so, discard.

---

## 7. Memory Safety & Configurable LRU Eviction

To prevent out-of-memory (OOM) fatal crashes during prolonged high-throughput ingestion:

1. **Configurable Ceiling:** The memory ceiling is configured via environment variable `WAVICLE_MAX_MEMORY` (e.g., `500MB`, `2GB`, `4GB`, default: 4GB).
2. **Byte-Accurate Accounting:** Every entry in `FrontierCache` tracks its estimated byte weight (atom struct + map bucket + LRU element + payload string/bytes).
3. **High-Watermark LRU Eviction:**
   - Eviction is triggered when memory usage crosses **90%** of `maxMemoryBytes`.
   - The eviction loop purges least-recently-used entries from the tail of an intrusive doubly-linked list (`container/list`) until memory drops to **<= 80%**.
   - Draining to 80% prevents per-write eviction churn ("ping-ponging").
   - Evictions increment `wavicle_memory_evictions_total` and update `wavicle_memory_bytes`.
4. **Transparent Read-Through Fallback:**
   - Evicted keys are NOT permanently lost. Because `WriteThroughStore` wraps PostgreSQL as the source of truth, an evicted key queried via `GET` incurs a single PostgreSQL query fallback, seamlessly re-populating `FrontierCache`.

---

## 8. Concrete Operational Boundaries & Production Realities

1. **Network Latency Model (Loopback vs. Real Network):**
   - On `localhost`, CDC replication lag registers at **~12ms** because OS page cache and loopback sockets eliminate transport delay.
   - In cross-AZ cloud VPCs (e.g., AWS EC2 to RDS), expected end-to-end CDC propagation lag is **25ms – 80ms** (due to network RTT, EBS disk flush, WAL sender serialization, and queueing). **Loopback metrics must never be quoted as production WAN latencies.**
2. **Replication Slot Hygiene:**
   - PostgreSQL does not recycle WAL for inactive replication slots. If a consumer drops offline permanently, PostgreSQL disk usage will grow until disk exhaustion.
   - In test and production, all ephemeral slots must be cleanly destroyed via `SELECT pg_drop_replication_slot()`.
3. **Protocol Scope:** Supports Key-Value and Hash commands (`GET`, `SET`, `MGET`, `MSET`, `HGET`, `HSET`, `HGETALL`, `DEL`, `EXPIRE`, `TTL`, `EXISTS`, `DBSIZE`, `PING`, `AUTH`). It does **not** support Redis Lists, Sets, Sorted Sets (`ZSET`), Transactions (`MULTI`/`EXEC`), or Pub/Sub.
4. **SQL Parser Scope:** The built-in SQL parser is a specialized projection parser for `SELECT col FROM table WHERE id = X`. It is not an arbitrary relational query optimizer for multi-table joins.

