# Wavicle System Architecture & Flow Diagrams

> Visual technical reference for Wavicle's proof-based caching model.

---

## 1. End-to-End System Topology

```mermaid
graph TD
    subgraph Clients["Client Applications"]
        App1["App Instance 1 (Go / redis-go)"]
        App2["App Instance 2 (Node / ioredis)"]
        CLI["wavicle-cli / redis-cli"]
    end

    subgraph Wavicle["Wavicle Consistency Engine"]
        TCP["RESP3 TCP Server (:6379)"]
        Engine["Proof Reduction Engine"]
        PCache[("Sharded LRU ProofCache")]
        FCache[("FrontierCache (O(unique keys))")]
        CDC["PG Logical Replication Listener"]
    end

    subgraph Database["Primary Storage"]
        PG[("PostgreSQL 15+")]
        WAL["Postgres WAL / pglogrepl"]
    end

    App1 -->|TCP / RESP3| TCP
    App2 -->|TCP / RESP3| TCP
    CLI -->|TCP / RESP3| TCP

    TCP --> Engine
    Engine <-->|Check Receipt / Fetch Proof| PCache
    Engine <-->|Verify Hashes| FCache
    
    TCP -->|Write-Through (Sync)| PG
    PG -.->|Logical Changes (Async)| WAL
    WAL -->|Stream CDC Events| CDC
    CDC -->|Update Live Atoms| FCache
```

---

## 2. Read Execution Decision Flowchart

```mermaid
flowchart TD
    Start(["Client GET Request"]) --> LookProof["Lookup Query Hash in ProofCache"]
    
    LookProof -->|Cache Hit| VerifyVV{"FastPath 1: VerifyVersionVector()"}
    LookProof -->|Cache Miss| ColdCompose["Cold Path: ComposeProof()"]
    
    VerifyVV -->|All Atom Hashes Match| ReturnCached["Return Cached Value\n(~1.59μs, 0 allocs)"]
    
    VerifyVV -->|Hash Mismatch Detected| FindChanged["FindChangedPaths() via Frontier"]
    FindChanged --> MarkDirty["Mark Dirty Nodes in AST"]
    MarkDirty --> PropagateUp["Propagate Dirty Flags Upward"]
    PropagateUp --> ReReduce["ReduceDirty: Recompute Dirty Subtrees Only"]
    ReReduce --> UpdateVV["Update Version Vector in Proof"]
    UpdateVV --> ReturnFresh["Return Incremental Value\n(~9.66μs, 20.7x faster)"]
    
    ColdCompose --> BuildAST["Build ProofNode Tree from Frontier"]
    BuildAST --> StoreProof["Store in ProofCache"]
    StoreProof --> ReturnCold["Return Initial Value\n(~199.4μs)"]
```

---

## 3. AST ProofNode Tree Dirty Invalidation

This diagram illustrates how mutating a single field (`field_0`) invalidates only its ancestor path, leaving the remaining 49 fields cached.

```mermaid
graph TD
    Root["Root: ECompose (Dirty = true)"]
    
    Branch1["Field 0: EConst (Dirty = true, MUST REDUCE)"]
    Branch2["Field 1: EConst (Dirty = false, CACHE HIT)"]
    Branch3["Field 2: EConst (Dirty = false, CACHE HIT)"]
    BranchN["Field 49: EConst (Dirty = false, CACHE HIT)"]

    Root --> Branch1
    Root --> Branch2
    Root --> Branch3
    Root --> BranchN

    style Branch1 fill:#ff9999,stroke:#cc0000,stroke-width:2px;
    style Root fill:#ffcc99,stroke:#ff6600,stroke-width:2px;
    style Branch2 fill:#d4edda,stroke:#28a745,stroke-width:1px;
    style Branch3 fill:#d4edda,stroke:#28a745,stroke-width:1px;
    style BranchN fill:#d4edda,stroke:#28a745,stroke-width:1px;
```

---

## 4. Write-Through & CDC Replication Sequence

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Wavicle as Wavicle (RESP3 Engine)
    participant Frontier as FrontierCache (In-Memory)
    participant Postgres as PostgreSQL (Durable Store)
    participant CDC as PGListener (pglogrepl)

    Note over Client,Postgres: Direct Application Write
    Client->>Wavicle: SET users:42:name "Alice"
    Wavicle->>Postgres: UPSERT INTO users (id, name) VALUES ('42', 'Alice')
    Wavicle->>Frontier: Update Atom(users:42:name, hash_1)
    Wavicle-->>Client: +OK

    Note over Postgres,Frontier: Asynchronous Replication Loop
    Postgres->>CDC: Emit WAL logical replication message
    CDC->>CDC: Echo Suppression Guard (hash_1 == current)
    Note over CDC: Discard echo (prevents cache thrashing)

    Note over Postgres,Frontier: External DBA / Batch Job Update
    actor DBA as DBA / Migration
    DBA->>Postgres: UPDATE users SET name = 'Bob' WHERE id = '42'
    Postgres->>CDC: Emit WAL event (UPDATE users id=42 name='Bob')
    CDC->>Frontier: AppendAtom(users:42:name, "Bob", hash_2)
    Note over Frontier: Live atom hash updated in RAM

    Note over Client,Wavicle: Subsequent Read Detects Change Proactively
    Client->>Wavicle: GET users:42:name
    Wavicle->>Wavicle: Compare Version Vector (expected hash_1 != live hash_2)
    Wavicle->>Wavicle: Incremental Re-reduce (returns "Bob")
    Wavicle-->>Client: $3\r\nBob\r\n
```

---

## 5. Memory Footprint Comparison

```mermaid
xychart-beta
    title "Memory Scaling Under Sustained Write Load (1K unique keys)"
    x-axis ["0s", "30s", "60s", "90s", "120s"]
    y-axis "Memory Entries (Atoms in RAM)" 0 --> 50000
    line [1000, 10000, 25000, 40000, 50000]
    line [1000, 1000, 1000, 1000, 1000]
```
*(Blue line: CausalCrystal full DAG $O(\text{writes})$; Orange line: FrontierCache $O(\text{unique keys})$ flat at 1,000 entries).*
