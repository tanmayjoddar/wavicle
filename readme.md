# Wavicle

**A Causal Proof Engine** -- validating the thesis that proof-based incremental reduction can eliminate cache invalidation.

## What Wavicle Is

A proof-of-concept storage engine that uses mathematical proof composition and version-vector staleness detection to serve cached data that is provably never stale. It speaks RESP3 (Redis wire protocol) for basic key-value operations. It is not a production Redis replacement -- 6 commands implemented, 300+ to go.

## What Wavicle Is Not (Yet)

- A production Redis replacement (6 commands, not 300+)
- A disk-scalable database (all indexes in RAM)
- A multi-node system (no replication, no sharding)
- A tested product (synthetic benchmarks only, no production workload)

## The Thesis

Traditional systems store effects (values) and manage change through external metadata (invalidation logic, TTL, pub/sub). Wavicle stores causes (generative programs over a Causal Crystal) and manages change through internal structure (proof composition, version vector staleness detection). When a write occurs, every cached proof that depended on the written path automatically detects staleness via hash comparison on the next read. No invalidation code is needed.

This thesis is validated for a specific synthetic workload (50-field composite record, single-field mutation) on a single machine. Generalization to real-world workloads, memory pressure, and production durability requires further work.

## Architecture

### Storage Tier: Causal Crystal

An append-only, content-addressed Merkle DAG. All atom indexes reside in RAM, rebuilt from the WAL on restart. No disk-based lookup (B-tree, LSM) exists yet.

```
CausalCrystal
  |-- WAL (write-ahead log, JSON lines, fsynced)
  |-- Active Frontier Index (RAM, map[string]Hash, O(1))
  |-- Parent Index (RAM, map[Hash][]Hash, O(1))
  |-- Atom Cache (RAM, sync.Map)
  |-- Merkle Tree (RAM, binary SHA3-256 tree)
  |-- Logical Clock (atomic.Uint64)
```

Every write creates an immutable CausalAtom:

```
CausalAtom {
    Hash:         SHA3-256(expr || clock || time || nonce || branch || parents),
    Expr:         CombinatorExpr (stratified typed algebra, levels 0-3),
    CausalPast:   []Hash (parent hashes, causal predecessors),
    CausalDepth:  uint64 (1 + max(parent depths)),
    Vector:       [256]float32 (semantic embedding, currently zero-initialized),
    Domain:       Domain (User/Order/Product/etc.),
    LogicalClock: uint64,
    PhysicalTime: time.Time,
    Nonce:        [16]byte,
    BranchID:     [16]byte,
}
```

### Computation Tier: Proof Engine

Composes combinator expressions from the Crystal and incrementally re-reduces them when paths change. Each MaterializedProof contains:

```
MaterializedProof {
    Value:          core.Value (the computed answer),
    ValueHash:      Hash (SHA3-256 of serialized value),
    VersionVector:  {path -> atom_hash} (staleness receipt),
    MerkleRoot:     Hash (Merkle root over all dependency hashes),
    NodeCache:      map[Hash]Value (sub-expression memoization),
    ProofTree:      CombinatorExpr (the generative program),
}
```

### Protocol Tier: RESP3 Server

TCP server speaking Redis wire protocol (inline commands and RESP arrays). Handles SET, GET, DEL, EXISTS, DBSIZE, PING.

## The Incremental Reduction Algorithm

```
ReduceIncremental(proof, crystal):

  -- FAST PATH 1: Version Vector Exact Match (O(m))
     For each {path, hash} in VersionVector, compare against Active Frontier.
     All match -> return cached value.

  -- FAST PATH 2: Merkle Root Match (O(1))
     Compute Merkle root of current dependency hashes.
     Matches stored root -> return cached value.

  -- INCREMENTAL PATH (k changes detected)
     1. FindChangedPaths(): compare VersionVector against Active Frontier
     2. reduceTree():
        For each atom reference in the ECompose tree:
          a. Resolve current atom via Active Frontier (not the stale reference)
          b. Hash unchanged -> expression unchanged -> node cache HIT (O(1))
          c. Hash changed -> new expression -> node cache MISS -> re-reduce
        ECompose is never cached as a whole. Only leaf expressions are cached.
```

### Frontier Resolution

Proof trees reference atom hashes that become stale after writes. The ECompose reducer does not trust its own references -- it resolves each atom hash against the Active Frontier before reducing:

```
for each atom hash in proof tree:
    origAtom = GetAtomByHash(hash)
    current = GetCurrent(origAtom.Path)
    if current.Hash != hash:
        atomToReduce = current    // write detected, new expression
    else:
        atomToReduce = origAtom   // no change, cache hit
    reduceTree(atomToReduce.Expr)
```

## Mathematical Foundations

### Stratified Combinatory Algebra (Levels 0-3)

Expressions are stratified into 4 levels to guarantee decidability. No level can reference a higher level -- no Y combinator, no general recursion, every expression has a finite reduction tree.

| Level | Combinator | Meaning |
|-------|-----------|---------|
| 0 | EConst | Raw values (string, int, bool, record, null) |
| 1 | EFieldAccess | Field extraction from records |
| 1 | EEmbed | Semantic embedding (placeholder -- vector is zero-initialized) |
| 2 | ECompose | Causal composition of atoms by hash |
| 2 | EApply | SK combinator application |
| 3 | EResonate | Semantic resonance query (placeholder) |

### Content-Addressed Hashing (Merkle-DAG)

```
H(atom) = SHA3-256(
    serialize(expr)
    || logical_clock (uint64 BE)
    || physical_time (uint64 BE)
    || nonce (16 bytes)
    || len(causal_past) (uint32 BE)
    || concat(causal_past)
)
```

Branch Merkle root: binary SHA3-256 tree over atom hashes.

### Holographic Reduced Representations (HRR)

256-dimensional vector operations for the semantic plane:

```
Circular Convolution:  (a otimes b)_k = sum_j a_j * b_{(k-j) mod n}
Circular Correlation:  (a oslash b)_k = sum_j a_j * b_{(k+j) mod n}
Binding:               bind(k, v) = normalize(V(k) otimes V(v))
Unbinding:             unbind(b, k) = normalize(b oslash V(k))
```

Note: Embedding model is not yet implemented. All atom vectors are zero-initialized.

### Temporal Entanglement

```
Expected co-occurrence:  E[f_AB] = (f_A * f_B * delta_t) / T
Lift:                    lift = f_AB / E[f_AB]
Chi-squared (Yates):     chi^2 = N(|ad-bc| - 0.5N)^2 / ((a+b)(a+c)(b+d)(c+d))
```

Thresholds: lift > 10 + p < 0.001 (Hard), lift > 3 + p < 0.05 (Soft), lift < 1.5 (Independent).

## Benchmark Results (Synthetic Workload)

**Hardware:** 12th Gen Intel Core i5-1240P, Windows, Go 1.24
**Workload:** 50-field composite record, single-field mutation
**Caveats:** All data in RAM. No real Redis baseline measured. No production workload. Single machine.

| Benchmark | Result | vs Cold | Cache Hits |
|-----------|--------|---------|------------|
| Wavicle WarmRead (Fast Path 1, nothing changed) | 83 ns | -- | -- |
| FastPath2 MerkleMatch (O(1) hash comparison) | 351 ns | -- | -- |
| Cold ProofReduction (50 fields, first ever) | 72,859 ns | 1.0x baseline | 0/50 |
| **Incremental Warm (1/50 changed)** | **1,516 ns** | **48x faster** | **49/50** |
| WarmReuse FastPath1 (hot cache) | 1,560 ns | 47x faster | -- |
| Concurrency 1000 Clients (90/10 read/write) | 50,308 ns | -- | -- |
| Crystal Append (fsync) | 812,388 ns | -- | -- |

48x speedup for incremental vs cold: 49 of 50 unchanged atoms hit the node cache in O(1). Only the changed field's expression misses and is re-reduced.

## Correctness Tests (Passing)

- **PoisonWrite:** Compose 50-field proof. Mutate 1 field. Verify incremental read returns new value (not stale) with 49/50 cache hits. Passes.
- **ReadAfterWrite:** SET Alice, GET Alice. Overwrite Bob, GET Bob. Zero stale reads. Passes.
- **ConsecutiveWrites:** 6 writes to same key, each immediately readable. Passes.

## When Wavicle Fails (Known Limitations)

| Scenario | Why It Fails | Workaround / Status |
|----------|-------------|---------------------|
| >100M atoms | All indexes in RAM. Heap exhaustion at ~8GB for 10M paths. | Add disk-based index (LSM/B-tree) -- not yet built. |
| Write-heavy (>50% writes) | Fsync bottleneck ~1,200 ops/sec. Each append blocks on disk sync. | Batch writes or async fsync -- trade durability for throughput. |
| Complex Redis commands (streams, sorted sets, Lua, pub/sub) | Unimplemented. Only SET, GET, DEL, EXISTS, DBSIZE, PING. | Use real Redis for these workloads. |
| Multi-key atomic transactions | No cross-path atomic commit. Single-key atomicity only. | Not yet designed. |
| Crash during WAL read | JSON lines format. Partial last line is silently discarded. | Acceptable for prototype. Binary encoding with checksums would fix this. |
| Process restart | Full WAL replay. 1M atoms ~1 second. No incremental recovery. | Acceptable for prototype. Segment compaction would bound replay time. |
| Concurrent write contention | Per-atom lock on frontier/parent/merkle updates. No sharding. | Acceptable for prototype. Domain-based sharding would fix this. |

## Current Limitations

- All atom indexes reside in RAM. No disk-based lookup. Capacity is bounded by available memory.
- No WAL segment compaction. The log grows indefinitely.
- Single node only. No replication, no sharding, no consensus.
- 6 RESP3 commands. Not a full Redis implementation.
- Embedding model is not implemented. Atom vectors are zero-initialized.
- Go 1.24 required. Module declares `go 1.25` (cosmetic -- compiles with 1.24 toolchain).

## Built vs Not Built

### Built (Phase 0 Validation Core)
- Causal Crystal with WAL, fsync durability, crash replay recovery
- Active Frontier Index (O(1) path-to-hash)
- Parent Index (O(1) hash-to-parents)
- Binary Merkle Tree (SHA3-256)
- Sharded LRU Proof Cache
- Incremental Reduction Algorithm (3 fast paths, frontier resolution)
- Cold Proof Composition (causal closure walk + full reduction)
- Version Vector Staleness Detection
- RESP3 Server (SET, GET, DEL, EXISTS, DBSIZE, PING)
- Fidelity Firewall (glob-pattern path matching, mode enforcement)
- Structural Diffraction (Merkle tree value decomposition)
- Temporal Entanglement (lift + chi-squared statistical test)
- HRR Operations (circular convolution, correlation, bind, unbind, superpose)
- Resonance Manifold (cosine similarity query over atom vectors)
- Correctness Tests (poison-write, read-after-write, consecutive-writes)
- Benchmark Instrumentation (cache hits/misses, changed path count)
- WAL Crash Resilience (max line size, partial last-line handling, short-write detection)

### Not Yet Built (Required for Production)
- Atom-level semantic embedding model (currently returns zero vector)
- PostgreSQL wire protocol proxy
- Counterfactual Resonance Engine (shadow transactions on replicas)
- Causal Shadow scheduler and edge decay
- Merkle Root Consensus replication
- WAL segment rotation (64MB) and compaction
- Configuration file (wavicle.yaml)
- Dockerfile and Kubernetes deployment
- Integration / E2E / Chaos tests
- Prometheus metrics and structured logging
- Disk-based atom index (LSM/B-tree for >100M atoms)
- Full RESP3 type system (arrays, maps, sets, pushes, nulls)
- Redis-compatible command set (MGET, MSET, INCR, EXPIRE, TTL, etc.)
- Multi-key transactions and atomic cross-path commits

## Quick Start

```bash
go build -o wavicle .
./wavicle
redis-cli SET mykey myvalue
redis-cli GET mykey
```

```bash
go test -run '^$' -bench . -benchtime=200ms ./benchmarks/
go test -v ./internal/engine/
```

## Complexity Bounds

| Operation | Time | Space |
|-----------|------|-------|
| Atom append (fsync) | O(1) amortized | O(1) |
| Active frontier read | O(1) | O(1) |
| Proof reduction (warm, FP1) | O(m) m=deps | O(1) |
| Proof reduction (warm, FP2) | O(1) | O(1) |
| Proof reduction (incremental, k changes) | O(k + (n-k)*O(1)) | O(k) |
| Proof composition (cold) | O(n) | O(n) |
| Merkle root computation | O(n log n) | O(n) |
| Structural diffraction | O(v) | O(v) |
| Temporal entanglement | O(1) | O(e) |
| Resonance query (brute force) | O(N) | O(k) |

## Dependencies

- Go 1.24+
- golang.org/x/crypto (SHA3-256)
