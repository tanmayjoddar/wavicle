# Wavicle Architecture Diagrams & Visual Technical Specifications

> High-fidelity technical diagrams illustrating internal data flow, state machines, and memory layouts.

---

## 1. End-to-End Component Topology & Network Boundaries

```mermaid
flowchart TB
    subgraph Clients["Client Layer (Standard TCP / RESP3)"]
        C1["App Service 1 (Go / redigo)"]
        C2["App Service 2 (Node.js / ioredis)"]
        CLI["Admin CLI (wavicle-cli / redis-cli)"]
    end

    subgraph Wavicle["Wavicle Server Boundary"]
        subgraph NetLayer["Network & Protocol (internal/protocol/resp3)"]
            TCP["TCP Listener (:6379)"]
            Parser["RESP3 / Inline Frame Parser"]
            ConnLimit{"Active Conns < 10,000?"}
        end

        subgraph CoreEngine["Proof Reduction Engine (internal/engine)"]
            PCache[("ProofCache\n64 Shards / LRU")]
            Resolver["Frontier Resolver"]
            ASTEngine["AST Reducer (reduceDirty)"]
        end

        subgraph StorageLayer["In-Memory Storage (internal/storage)"]
            Frontier[("FrontierCache\nO(unique keys) in RAM")]
            TTLSweeper["TTL Sweeper\n(60s Ticker)"]
        end

        subgraph CDCLayer["Replication Consumer (internal/replication)"]
            CDCListener["PGListener (pglogrepl)"]
            EchoFilter{"Echo Suppression\nGuard"}
        end
    end

    subgraph Database["Persistent Database Layer"]
        PGPrimary[("PostgreSQL 15+ Primary")]
        PGSlot["Logical Replication Slot\n(pgoutput plugin)"]
    end

    C1 -->|TCP / Port 6379| TCP
    C2 -->|TCP / Port 6379| TCP
    CLI -->|TCP / Port 6379| TCP

    TCP --> ConnLimit
    ConnLimit -->|Accept| Parser
    Parser -->|Dispatch GET / HGET| CoreEngine
    Parser -->|Dispatch SET / HSET| StorageLayer

    CoreEngine <-->|Lookup / Update Proof| PCache
    CoreEngine <-->|Verify Version Vector| Frontier
    
    StorageLayer -->|Sync Write-Through| PGPrimary
    PGPrimary -.->|Write-Ahead Log (WAL)| PGSlot
    PGSlot -->|Stream Tuple Messages| CDCListener
    CDCListener --> EchoFilter
    EchoFilter -->|Pass External Writes| Frontier
    EchoFilter -.->|Discard Echoes| Drop[("Discard Echo")]
    TTLSweeper -->|Reclaim Expired| Frontier
```

---

## 2. Read Path Decision Flow & Latency Profiles

```mermaid
flowchart TD
    Req(["Client GET Request: 'user:123'"]) --> HashQuery["Compute SHA3-256 Query Hash"]
    HashQuery --> CheckProof{"ProofCache Lookup\n(64 Shards)"}

    %% Fast Path 1
    CheckProof -->|Hit| LoadProof["Load MaterializedProof & VersionVector"]
    LoadProof --> VerifyVV{"FastPath 1:\nVerifyVersionVector()\nCheck Live Frontier Hashes"}
    
    VerifyVV -->|All Atom Hashes Match| ReturnFast["Return Cached Value\n----------------------------\nLatency: ~1.59 μs\nAllocations: 0 B / 0 allocs\nPath: FastPath 1"]
    
    %% Incremental Path
    VerifyVV -->|Mismatch Detected| FindDiff["FindChangedPaths(buf)\nStack Buffer [64]string"]
    FindDiff --> MarkAST["Mark Changed ProofNodes as Dirty"]
    MarkAST --> PropagateDirty["PropagateDirtyUp() to Root Node"]
    PropagateDirty --> ReduceDirty["reduceDirty(RootNode):\n- Clean Subtrees: Return Cached Pointer (0 alloc)\n- Dirty Subtrees: Re-evaluate Mutated Leaves"]
    ReduceDirty --> UpdateVV["Update VersionVector in-place"]
    UpdateVV --> ReturnInc["Return Updated Value\n----------------------------\nLatency: ~9.66 μs\nSpeedup: 20.7x vs Cold\nAllocations: 7 allocs\nPath: Incremental Path"]

    %% Cold Path
    CheckProof -->|Miss| GatherClosure["Gather Causal Closure from Frontier"]
    GatherClosure --> BuildProofNode["Build ProofNode Hierarchy (AST)"]
    BuildProofNode --> EvalInitial["Evaluate Initial Proof & Compute Hashes"]
    EvalInitial --> StorePCache["Insert into ProofCache (LRU)"]
    StorePCache --> ReturnCold["Return Initial Value\n----------------------------\nLatency: ~199.4 μs\nAllocations: 316 allocs\nPath: Cold Path"]
```

---

## 3. AST ProofNode Tree Dirty Propagation

When 1 field out of a 50-field composite object changes, dirty propagation isolates the mutation to the exact ancestor path, leaving the other 49 branches memoized:

```mermaid
graph TD
    Root["ProofNode: Root (ECompose)\nDirty = true\nRecomputed in 9.66μs"]

    subgraph MutatedBranch["Dirty Subtree (Evaluated)"]
        F0["ProofNode: field_0 (EConst)\nSourcePath: 'user:123:field_0'\nDirty = true (MUTATED LEAF)"]
    end

    subgraph CleanBranches["Clean Subtrees (Memoized Pointer Returns)"]
        F1["ProofNode: field_1 (EConst)\nDirty = false\nCACHED HIT (0 alloc)"]
        F2["ProofNode: field_2 (EConst)\nDirty = false\nCACHED HIT (0 alloc)"]
        FDots["... 46 more fields ...\nDirty = false\nCACHED HIT (0 alloc)"]
        F49["ProofNode: field_49 (EConst)\nDirty = false\nCACHED HIT (0 alloc)"]
    end

    Root --> F0
    Root --> F1
    Root --> F2
    Root --> FDots
    Root --> F49

    style F0 fill:#ffb3b3,stroke:#cc0000,stroke-width:2px;
    style Root fill:#ffe0b2,stroke:#f57c00,stroke-width:2px;
    style F1 fill:#c8e6c9,stroke:#388e3c,stroke-width:1px;
    style F2 fill:#c8e6c9,stroke:#388e3c,stroke-width:1px;
    style FDots fill:#c8e6c9,stroke:#388e3c,stroke-width:1px;
    style F49 fill:#c8e6c9,stroke:#388e3c,stroke-width:1px;
```

---

## 4. Write-Through & CDC Replication Sequence

```mermaid
sequenceDiagram
    autonumber
    actor Client as Application Client
    participant Wavicle as Wavicle RESP3 Server
    participant Frontier as FrontierCache (RAM)
    participant PG as PostgreSQL Engine
    participant CDC as PGListener (pglogrepl)

    %% Direct App Write
    Note over Client,PG: 1. Synchronous Write-Through Pipeline
    Client->>Wavicle: SET users:42:name "Alice"
    Wavicle->>PG: UPSERT INTO users (id, name) VALUES ('42', 'Alice')
    PG-->>Wavicle: Query OK (Row Committed to WAL)
    Wavicle->>Frontier: AppendAtom(users:42:name, "Alice")
    Note over Frontier: Assigns Clock, Hash, and Local Timestamp
    Wavicle-->>Client: +OK

    %% Asynchronous CDC Stream
    Note over PG,CDC: 2. Asynchronous Logical Replication
    PG->>CDC: Emit WAL Message (pgoutput: INSERT/UPDATE users id=42 name='Alice')
    CDC->>CDC: Echo Suppression Guard:
    Note over CDC: current.PhysicalTime >= commitTime AND value == "Alice"
    CDC-->>Wavicle: DISCARD ECHO (Prevent spurious cache invalidation)

    %% External Write
    Note over PG,Frontier: 3. External Direct DB Mutation (DBA / Batch Job)
    actor DBA as External DBA / Migration
    DBA->>PG: UPDATE users SET name = 'Bob' WHERE id = '42'
    PG-->>DBA: 1 row updated
    PG->>CDC: Emit WAL Message (UPDATE users id=42 name='Bob')
    CDC->>CDC: Echo Check: New value differs from "Alice"
    CDC->>Frontier: AppendAtom(users:42:name, "Bob")
    Note over Frontier: Live atom hash updated in RAM

    %% Proactive Read
    Note over Client,Frontier: 4. Next Read Detects Mutation Automatically
    Client->>Wavicle: GET users:42:name
    Wavicle->>Frontier: VerifyVersionVector() -> Mismatch detected
    Wavicle->>Wavicle: Incremental re-reduce (fetches 'Bob')
    Wavicle-->>Client: $3\r\nBob\r\n
```

---

## 5. In-Memory Data Structure Memory Map

```mermaid
classDiagram
    class MaterializedProof {
        +core.Hash QueryHash
        +string QueryPath
        +core.Value Value
        +int64 LastVerifiedAt
        +uint64 AccessCount
        +VersionVector* VersionVector
        +ProofNode* RootNode
        +map~string, ProofNode*~ PathToNode
    }

    class VersionVector {
        +map~string, core.Hash~ Entries
        +core.Hash MerkleRoot
        +PathList() []string
    }

    class ProofNode {
        +core.CombinatorExpr Expr
        +ProofNode* Parent
        +ProofNode[]* Children
        +core.Value CachedValue
        +bool Dirty
        +core.Hash SourceHash
        +string SourcePath
        +PropagateDirtyUp()
        +ResetDirty()
    }

    class CausalAtom {
        +core.Hash Hash
        +core.CombinatorExpr Expr
        +uint64 LogicalClock
        +time.Time PhysicalTime
        +time.Time ExpiresAt
        +string Path
        +ComputeHash() core.Hash
    }

    class FrontierCache {
        -sync.RWMutex mu
        -map~string, CausalAtom*~ entries
        -map~core.Hash, string~ hashToPath
        -atomic.Uint64 clock
        +AppendAtom()
        +GetCurrent()
        +VerifyVersionVector() bool
        +FindChangedPaths() []string
    }

    MaterializedProof --> VersionVector : owns
    MaterializedProof --> ProofNode : RootNode
    ProofNode --> ProofNode : Parent / Children
    FrontierCache --> CausalAtom : stores
```
