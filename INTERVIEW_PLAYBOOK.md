# Wavicle Technical Interview Playbook

> **How to Discuss, Defend, and Ace System Design Interviews with Wavicle**  
> *Targeted at Senior/Staff Infrastructure, Distributed Systems, and Backend roles.*

---

## 1. The 60-Second Elevator Pitch

> *"Most distributed applications suffer from cache invalidation bugs because cache eviction is reactive—engineers rely on manual `cache.del()`, TTL guesses, or pub/sub topics. If one code path forgets to invalidate, users read stale data.*
> 
> *I built **Wavicle**, a standalone Go key-value cache engine speaking the Redis RESP3 wire protocol that replaces reactive invalidation with **proactive proof verification**. Every cached composite query carries a version vector receipt of its underlying data hashes. On read, it checks the receipt against the live in-memory frontier: if unchanged, it returns in **1.6μs with zero allocations**. If a field changes, it re-evaluates only the dirty AST subtrees in **9.7μs** (a **20.7× speedup** over cold recompute, statistically verified with `benchstat`). It integrates with PostgreSQL logical replication to consume external database updates in real time."*

---

## 2. Realistic Scope Boundaries (What NOT to Overclaim)

Infrastructure interviewers have deep Redis experience. Overclaiming will hurt your credibility. Use this table to stay 100% defensible:

| Topic | ❌ Do NOT Say | ✅ Do Say (The Senior / Staff Answer) |
| :--- | :--- | :--- |
| **Redis Scope** | *"It's a complete drop-in replacement for Redis."* | *"It implements the **RESP3 wire protocol** for standard Key-Value and Hash commands (`GET`, `SET`, `HGET`, `HSET`, `DEL`, `EXPIRE`), allowing standard Redis client libraries to connect without code changes. It does not yet implement Lists, ZSETs, or Lua."* |
| **SQL Capabilities** | *"It automatically caches any SQL query from PostgreSQL."* | *"It features a prototype parser demonstrating projection caching (`SELECT col FROM table WHERE id=X`). Real-world arbitrary queries with joins would require a full AST query planner like Vitess."* |
| **Soak Test Stats** | *"I ran an unverified 44M-op soak test."* | *"In our live 20-worker concurrent soak test (4 writers + 16 readers), the engine sustained **4.27M operations in 15 seconds** (~285k in-process ops/sec) with a **99.97% cache hit rate** and **zero errors**."* |
| **Complexity Claims** | *"It mathematically guarantees $O(k \log d)$ complexity."* | *"It propagates dirty state strictly up the active parent path of mutated fields rather than recomputing the whole tree, which our `benchstat` runs verified at **9.7μs vs. 199μs** cold compose."* |

---

## 3. High-Frequency Interview Questions & Model Answers

### Q1: "Doesn't checking a version vector on every single read introduce latency?"
**Model Answer:**  
*"In network-bound architectures, a round-trip to Redis over TCP typically takes 200–500μs. In contrast, Wavicle's FastPath 1 runs entirely in-memory: it compares pre-computed 32-byte hashes within an existing in-process map. Our Go `benchstat` measurements over 6 independent runs confirmed this check completes in **1.59μs with 0 allocations**. Freshness verification happens faster than typical network socket frame parsing."*

---

### Q2: "Why did you build FrontierCache instead of keeping the full Causal Crystal DAG?"
**Model Answer:**  
*"This was one of our primary architectural trade-offs. The initial prototype, CausalCrystal, stored the entire causal DAG with a disk WAL and parent index. While mathematically elegant for algorithmic validation, retaining ancestry in production causes memory to grow with total write history ($O(\text{writes})$), leading to compaction pauses and RAM exhaustion.*
*For production, PostgreSQL is already the durable source of truth. Therefore, I built **FrontierCache**, which strictly stores only the latest atom per unique key ($O(\text{unique keys})$). `benchstat` showed this slashed read lock contention and reduced warm-path latency by **34.4%** ($p = 0.002$)."*

---

### Q3: "How do you prevent replication loops between write-through and CDC?"
**Model Answer:**  
*"When an application executes a `SET`, Wavicle writes through to PostgreSQL synchronously and updates the local atom with a generated hash. Moments later, PostgreSQL's logical replication stream emits the WAL event back to Wavicle's `pglogrepl` listener.*
*If treated naively, this echo would generate a new atom hash, triggering an unnecessary proof cache invalidation. To prevent this, the CDC listener implements an **echo suppression guard**: it checks if the replication event's commit timestamp and values match the current frontier. If identical, the echo is safely discarded."*

---

### Q4: "What happens under extreme memory pressure (OOM risk)?"
**Model Answer:**  
*"In the current implementation, expired atoms are reclaimed via a background 60-second ticker sweep. However, if unexpired unique keys exceed physical memory, FrontierCache currently has no automatic LRU eviction policy—it relies on the OS page cache or would risk OOM.*
*The immediate next milestone on the roadmap is introducing a configurable `maxmemory` threshold with an LRU sample-based eviction algorithm, similar to Redis's `volatile-lru` or `allkeys-lru`."*

---

### Q5: "How did you verify benchmark numbers with statistical significance?"
**Model Answer:**  
*"Single-run microbenchmarks in Go frequently suffer from CPU governor throttling and Windows OS timer call skew. To ensure scientific rigor, I eliminated `b.StopTimer()`/`b.StartTimer()` calls inside the hot iteration loop, ran 6 independent trials (`-count=6`) with memory profiling enabled (`-benchmem`), and fed the raw outputs into `benchstat`.*
*The tool reported median latencies, confidence intervals, and confirmed that the warm reuse improvement ($1.59\mu\text{s}$) and incremental speedup ($9.66\mu\text{s}$) were statistically significant at $p = 0.002$ ($n = 6$)."*

---

## 4. Key Performance Reference Sheet (All 100% Tested)

| Scenario | Tested Result | What It Demonstrates |
| :--- | :--- | :--- |
| **Warm Reuse (FastPath 1)** | **1.59 μs (0 allocs)** | Version vector match — return cached composite value |
| **Incremental Re-reduce** | **9.66 μs (7 allocs)** | Mutating 1 of 50 fields, dirty AST path re-evaluated |
| **Cold Proof Composition** | **199.4 μs (316 allocs)** | First read — full AST tree construction from frontier |
| **Speedup Ratio** | **20.7× faster** | Measured speedup of incremental path over cold compose |
| **Single Key Read** | **142 ns (0 allocs)** | Zero-alloc hot path for direct key-value read |
| **Live Soak Test (15s)** | **4.27M ops (99.97% hit)** | 20 concurrent goroutines (4 writers, 16 readers), 0 errors |
