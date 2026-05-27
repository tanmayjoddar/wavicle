# **WAVICLE PROJECT BLUEPRINT**

## _Version 1.0 — The Causal Proof Engine_

---

## TABLE OF CONTENTS

1. Fundamental Thesis
2. Mathematical Foundations
3. Core Data Structures
4. The Causal Crystal
5. The Proof Engine
6. The Resonance Manifold
7. Causal Autopoiesis Engine
8. Protocol & API Architecture
9. Fidelity Firewall
10. Replication & Consensus
11. Operational Model
12. Performance & Complexity
13. Implementation Phases

---

## 1. FUNDAMENTAL THESIS

### 1.1 The Axiom

> **All existing data systems store effects and manage change through external metadata. Wavicle stores causes and manages change through internal structure.**

**Formal statement:**
Let `S` be a system state space. Traditional systems store `s ∈ S` (effects) and maintain a separate change-log `Δ(s)` requiring external invalidation logic `I: Δ(s) → {keys to evict}`.

Wavicle stores `P` where `reduce(P) = s` and `P` is a combinator expression over a Causal Crystal `C`. Change is handled by extending `C` with a new atom `a` such that the new proof `P' = C(P, a)`. No external invalidation function exists.

### 1.2 The Invariant

**The Causal Consistency Invariant:**
For any query `q` at causal depth `d`, the result `r = observe(q, d)` satisfies:

```
∀ path p in dependency(q): version(p) ≤ d
```

where `version(p)` is the logical timestamp of the latest atom affecting path `p` in the causal past of depth `d`.

---

## 2. MATHEMATICAL FOUNDATIONS

### 2.1 Combinatory Logic (The Algebra of Proofs)

Wavicle uses a **stratified typed combinatory algebra** — a restricted, decidable fragment.

**Primitives:**

```
K : ∀α. α → (β → α)          [Constant]
S : ∀αβγ. (α → β → γ) → (α → β) → (α → γ)  [Substitution]
I : ∀α. α → α                 [Identity, derived as S K K]
F : FieldPath → Record → Value  [Field access]
C : AtomHash → AtomHash → AtomHash  [Causal composition]
V : Value → Vector              [Semantic embedding]
R : Vector → Vector → Vector    [Resonance / interference]
```

**Type Stratification:**
Expressions are stratified into levels to ensure termination:

- Level 0: Values, constants, vectors
- Level 1: Field access `F`, embedding `V`
- Level 2: Composition `C`, application `S`
- Level 3: Resonance `R`

**Reduction Rules:**

```
K x y → x
S f g x → (f x) (g x)
F "name" {name: "Alice"} → "Alice"
C a b → a  (if b is in causal past of a, used for proof ordering)
V "Alice" → [0.12, -0.88, ...]  (via embedding model)
R v1 v2 → v1 ⊗ v2  (circular convolution in HRR space)
```

**Decidability Proof:**
The stratification prevents general recursion. Every expression has a finite reduction tree bounded by `O(|expr| × max_level)`. No Y combinator. No fixed points.

### 2.2 Causal Set Theory (The Geometry of Time)

A **Causal Crystal** is a locally finite poset `(A, ≺)` where:

- `A` is the set of Causal Atoms
- `a ≺ b` means `a` is in the causal past of `b` (happens-before)
- The relation is transitive, irreflexive, and acyclic

**Causal Cut:**
For a query `q` at depth `d`, the causal cut `Cut(q, d)` is the set of maximal elements in the sub-poset `{a ∈ A : depth(a) ≤ d ∧ a ≺ q}`.

**Past Lightcone:**
`Past(a) = {b ∈ A : b ≺ a}` — all atoms that causally affect `a`.

**Future Lightcone:**
`Future(a) = {b ∈ A : a ≺ b}` — all atoms affected by `a`. This is computed lazily via the Causal Shadow.

### 2.3 Merkle-DAG (Content Addressing)

Every atom `a` has a hash:

```
hash(a) = SHA256( serialize(expr(a)) || hash(parent_1) || ... || hash(parent_n) || nonce )
```

The **Merkle root** of a branch `B` is:

```
merkle_root(B) = MerkleRoot( {hash(a) : a ∈ B} )
```

**Verification:**
To prove that value `v` at path `p` is correct at depth `d`, provide:

1. The proof expression `P`
2. The version vector `VV(p, d) = {path → hash(atom)}`
3. The Merkle path from each atom to the branch root

Verification cost: `O(|VV| × log |B|)` hash operations.

### 2.4 Hyperdimensional Computing (The Resonance Algebra)

Wavicle uses **Holographic Reduced Representations (HRR)** with circular convolution `⊗` and approximate inverse `⊘`.

**Binding:**

```
bind(key, value) = V(key) ⊗ V(value)
```

**Unbinding:**

```
unbind(bundle, key) ≈ V(value)  (via circular correlation)
```

**Superposition:**

```
superpose(a, b) = (a + b) / ||a + b||
```

**Resonance query:**
Given query vector `q`, find atom `a` maximizing:

```
resonance(q, a) = cos_sim(q, V(a.expr)) = (q · V(a.expr)) / (||q|| × ||V(a.expr)||)
```

---

## 3. CORE DATA STRUCTURES

### 3.1 Causal Atom (The Unit of Truth)

```rust
struct CausalAtom {
    // Identity
    hash: [u8; 32],

    // Content
    expr: CombinatorExpr,

    // Causal structure
    causal_past: Vec<[u8; 32]>,  // Parent atom hashes
    causal_depth: u64,           // 1 + max(parent depths)

    // Semantic binding
    vector: [f32; 256],          // HRR semantic embedding
    vector_quantization: VectorQ, // f32, int8, or binary

    // Metadata
    domain_tag: Domain,
    logical_clock: u64,          // Lamport timestamp
    physical_time: u64,            // Nanoseconds since epoch
    nonce: [u8; 16],              // Collision resistance

    // Merkle linkage
    branch_id: [u8; 16],
    merkle_sibling_path: Vec<[u8; 32]>, // For fast verification
}

enum CombinatorExpr {
    // Level 0: Values
    Const(Vec<u8>),
    Int(i64),
    Float(f64),
    Bool(bool),

    // Level 1: Access & Embedding
    FieldAccess { field: String, source: Box<<CombinatorExpr> },
    Embed { model_version: u16, vector: [f32; 256] },

    // Level 2: Composition
    Compose { atoms: Vec<[u8; 32]> },  // Causal composition of atoms
    Apply { func: Box<<CombinatorExpr>, arg: Box<<CombinatorExpr> }, // S/K application

    // Level 3: Resonance
    Resonate { queries: Vec<[f32; 256]> },
}

enum Domain {
    User, Order, Product, Session, Config, Analytics, System
}

enum VectorQ {
    F32,
    Int8,
    Binary,  // For extreme compression
}
```

### 3.2 MaterializedProof (The Cache Unit)

```rust
struct MaterializedProof {
    // Query identity
    query_hash: [u8; 32],         // SHA256(query_path + mode + params)
    query_path: String,
    observation_mode: ObservationMode,

    // The program
    proof_tree: ComposedExpr,

    // Materialized view
    materialized_value: Arc<Value>,
    materialized_hash: [u8; 32],  // SHA256(serialize(value))

    // Consistency tracking
    version_vector: VersionVector,  // path → atom_hash for all dependencies
    merkle_root: [u8; 32],         // Root of all dependency atoms
    causal_depth: u64,             // Max depth in version vector

    // Incremental reduction state
    node_cache: HashMap<[u8; 32], Arc<Value>>, // expr_hash → reduced_value
    hot_paths: Vec<String>,        // Most frequently accessed sub-paths

    // Metadata
    created_at: u64,
    last_verified_at: u64,
    access_count: u64,
    confidence: f32,               // For Inductive/Abductive modes
}

struct VersionVector {
    entries: HashMap<String, [u8; 32]>, // path → latest atom hash
    // Optimization: Merkle tree over entries for O(1) comparison
    entries_merkle_root: [u8; 32],
}

enum ObservationMode {
    Deductive,     // Full causal reduction, Merkle proof
    Inductive,     // Statistical inference from similar histories
    Abductive,     // Best-explanation synthesis
    Intuitive,     // Direct resonance from field
}
```

### 3.3 Causal Shadow Edge (The Dependency Graph)

```rust
struct CausalEdge {
    source_path: String,
    target_path: String,

    // Discovery channel evidence
    structural_binding: f32,       // 0.0-1.0 (Merkle containment)
    temporal_lift: f32,            // Observed co-occurrence lift
    counterfactual_proof: f32,     // 0.0 or 1.0 (empirically proven)

    // Unified confidence
    confidence: f32,

    // Metadata
    discovered_by: DiscoveryChannel,
    discovered_at: u64,
    last_validated_at: u64,
    validation_count: u64,
    tombstoned: bool,
}

enum DiscoveryChannel {
    StructuralDiffraction,
    TemporalEntanglement,
    CounterfactualResonance,
    ExplicitAnnotation,
}
```

---

## 4. THE CAUSAL CRYSTAL (STORAGE PLANE)

### 4.1 Architecture

The Causal Crystal is a **log-structured, content-addressed Merkle-DAG** with three indexes:

```
┌─────────────────────────────────────────┐
│         CAUSAL CRYSTAL                  │
│                                         │
│  ┌─────────────┐  ┌─────────────────┐  │
│  │ Append Log  │  │ Active Frontier │  │
│  │ (NVMe SSD)  │  │ Index (RAM)     │  │
│  │             │  │                 │  │
│  │ Atom 0      │  │ path → hash     │  │
│  │ Atom 1      │  │ (DashMap)       │  │
│  │ Atom 2      │  │                 │  │
│  │ ...         │  │ O(1) lookup     │  │
│  └─────────────┘  └─────────────────┘  │
│         │              │                │
│  ┌──────┴──────────────┴──────────┐   │
│  │      Parent Index (RAM)          │   │
│  │      hash → [parent_hashes]      │   │
│  │      For proof validation         │   │
│  └──────────────────────────────────┘   │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │  Merkle Branch Storage          │   │
│  │  branch_id → MerkleRoot         │   │
│  │  For cross-node verification    │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

### 4.2 Write Algorithm

```rust
fn append_atom(crystal: &mut CausalCrystal, expr: CombinatorExpr, causal_past: Vec<[u8; 32]>) -> Result<[u8; 32], Error> {
    // 1. Determine causal depth
    let depth = if causal_past.is_empty() {
        0
    } else {
        causal_past.iter()
            .map(|h| crystal.get_depth(h).unwrap_or(0))
            .max()
            .unwrap() + 1
    };

    // 2. Generate semantic vector
    let vector = embed_expression(&expr);

    // 3. Construct atom
    let atom = CausalAtom {
        hash: [0; 32], // computed below
        expr,
        causal_past: causal_past.clone(),
        causal_depth: depth,
        vector,
        domain_tag: infer_domain(&expr),
        logical_clock: crystal.next_logical_clock(),
        physical_time: now_nanos(),
        nonce: random_nonce(),
        branch_id: crystal.current_branch(),
        merkle_sibling_path: vec![], // computed by Merkle tree
    };

    // 4. Compute content hash
    let hash = compute_atom_hash(&atom);
    let atom = CausalAtom { hash, ..atom };

    // 5. Append to log (fsync)
    crystal.log.append(&serialize(&atom))?;
    crystal.log.fsync()?;

    // 6. Update indexes
    if let Some(path) = extract_primary_path(&atom.expr) {
        crystal.active_frontier.insert(path, hash);
    }
    crystal.parent_index.insert(hash, causal_past);
    crystal.depth_index.insert(hash, depth);

    // 7. Update Merkle tree
    crystal.merkle_tree.insert(hash);
    crystal.branch_merkle_root = crystal.merkle_tree.root();

    Ok(hash)
}
```

**Complexity:**

- Append: O(1) log write + O(1) index updates + O(log N) Merkle update
- Fsync latency: ~0.1-0.3ms on modern NVMe

### 4.3 Read Algorithm (Active Frontier)

```rust
fn get_current_atom(crystal: &CausalCrystal, path: &str) -> Option<&CausalAtom> {
    // O(1) hot lookup
    let hash = crystal.active_frontier.get(path)?;
    crystal.log.get(&hash) // O(1) hash lookup in log
}
```

### 4.4 Historical Read (Causal Cut)

```rust
fn get_atom_at_depth(crystal: &CausalCrystal, path: &str, depth: u64) -> Option<&CausalAtom> {
    // Walk back from active frontier through causal past
    let current = crystal.active_frontier.get(path)?;
    let atom = crystal.log.get(&current)?;

    if atom.causal_depth <= depth {
        return Some(atom);
    }

    // Binary search through causal past for atom at exact depth
    // O(log d) where d = causal depth of path
    binary_search_causal_chain(crystal, &atom.causal_past, depth)
}
```

---

## 5. THE PROOF ENGINE (COMPUTATION PLANE)

### 5.1 Proof Composition

When a query arrives for the first time, the Proof Engine composes a proof from the Causal Crystal.

```rust
fn compose_proof(crystal: &CausalCrystal, query: &Query) -> Result<<MaterializedProof, Error> {
    let query_path = &query.path;
    let mode = query.observation_mode;

    // 1. Find all atoms in the causal past of this query
    let relevant_atoms = match mode {
        ObservationMode::Deductive => {
            // Full causal closure
            gather_causal_closure(crystal, query_path)?
        },
        ObservationMode::Inductive => {
            // Sample recent causal history
            gather_causal_sample(crystal, query_path, sample_size=100)?
        },
        ObservationMode::Abductive => {
            // Partial evidence + resonance
            gather_abductive_evidence(crystal, query)?
        },
        ObservationMode::Intuitive => {
            // Skip crystal, use resonance directly
            return compose_intuitive_proof(crystal, query);
        },
    };

    // 2. Build proof tree from atoms
    let proof_tree = build_proof_tree(crystal, &relevant_atoms, query_path)?;

    // 3. Reduce to value
    let (value, node_cache) = reduce_proof_tree(&proof_tree, crystal)?;

    // 4. Capture version vector
    let vv = build_version_vector(crystal, &relevant_atoms);

    // 5. Compute Merkle root
    let merkle_root = compute_version_merkle_root(&vv);

    Ok(MaterializedProof {
        query_hash: query.hash(),
        query_path: query_path.clone(),
        observation_mode: mode,
        proof_tree,
        materialized_value: Arc::new(value),
        materialized_hash: sha256(&serialize(&value)),
        version_vector: vv,
        merkle_root,
        causal_depth: relevant_atoms.iter().map(|a| a.causal_depth).max().unwrap_or(0),
        node_cache,
        hot_paths: extract_hot_paths(query_path),
        created_at: now_nanos(),
        last_verified_at: now_nanos(),
        access_count: 1,
        confidence: 1.0, // Deductive = certain
    })
}
```

### 5.2 Incremental Reduction (The Critical Algorithm)

```rust
fn reduce_incremental(proof: &mut MaterializedProof, crystal: &CausalCrystal) -> Result<<Arc<Value>, Error> {
    // FAST PATH 1: Version vector exact match
    if crystal.verify_version_vector(&proof.version_vector) {
        proof.access_count += 1;
        proof.last_verified_at = now_nanos();
        return Ok(proof.materialized_value.clone());
    }

    // FAST PATH 2: Merkle root match (atomic snapshot consistency)
    let current_root = crystal.merkle_root_for_paths(&proof.dependency_paths());
    if current_root == proof.merkle_root {
        proof.access_count += 1;
        proof.last_verified_at = now_nanos();
        return Ok(proof.materialized_value.clone());
    }

    // INCREMENTAL PATH: Find changed atoms and re-reduce only affected subtrees
    let changed_paths = crystal.find_changed_paths(&proof.version_vector);

    // Invalidate node cache entries for changed paths
    for path in &changed_paths {
        if let Some(expr_hash) = proof.path_to_expr_hash.get(path) {
            proof.node_cache.remove(expr_hash);
        }
    }

    // Re-reduce proof tree with updated node cache
    let (new_value, new_cache) = reduce_incremental_tree(
        &proof.proof_tree,
        crystal,
        &mut proof.node_cache,
        &changed_paths
    )?;

    // Update materialized state
    proof.materialized_value = Arc::new(new_value);
    proof.materialized_hash = sha256(&serialize(proof.materialized_value.as_ref()));
    proof.version_vector = crystal.capture_versions(&proof.dependency_paths());
    proof.merkle_root = crystal.merkle_root_for_paths(&proof.dependency_paths());
    proof.node_cache = new_cache;
    proof.last_verified_at = now_nanos();
    proof.access_count += 1;

    Ok(proof.materialized_value.clone())
}

fn reduce_incremental_tree(
    expr: &ComposedExpr,
    crystal: &CausalCrystal,
    cache: &mut HashMap<[u8; 32], Arc<Value>>,
    changed_paths: &HashSet<String>
) -> Result<(Value, HashMap<[u8; 32], Arc<Value>>), Error> {
    let expr_hash = sha256_expr(expr);

    // If not changed and in cache, return immediately
    if !expr_is_affected(expr, changed_paths) {
        if let Some(cached) = cache.get(&expr_hash) {
            return Ok((cached.as_ref().clone(), cache.clone()));
        }
    }

    // Otherwise, re-reduce
    let result = match expr {
        ComposedExpr::Atom(atom_hash) => {
            let atom = crystal.get_atom(atom_hash)?;
            reduce_atom_expr(&atom.expr, crystal, cache)?
        },
        ComposedExpr::FieldAccess { field, source } => {
            let (source_val, _) = reduce_incremental_tree(source, crystal, cache, changed_paths)?;
            extract_field(field, &source_val)?
        },
        ComposedExpr::Compose(atoms) => {
            let mut values = Vec::new();
            for atom_hash in atoms {
                let atom = crystal.get_atom(atom_hash)?;
                let (val, _) = reduce_atom_expr(&atom.expr, crystal, cache)?;
                values.push(val);
            }
            merge_values(values)
        },
        ComposedExpr::Apply { func, arg } => {
            let (f_val, _) = reduce_incremental_tree(func, crystal, cache, changed_paths)?;
            let (a_val, _) = reduce_incremental_tree(arg, crystal, cache, changed_paths)?;
            apply_value(f_val, a_val)?
        },
    };

    let shared = Arc::new(result);
    cache.insert(expr_hash, shared.clone());
    Ok((shared.as_ref().clone(), cache.clone()))
}
```

### 5.3 Complexity Analysis

| Operation                                | Best Case  | Typical Case | Worst Case | Notes                      |
| ---------------------------------------- | ---------- | ------------ | ---------- | -------------------------- |
| Proof reduction (cold)                   | O(n)       | O(n)         | O(n)       | n = nodes in proof tree    |
| Proof reduction (warm, no changes)       | O(1)       | O(1)         | O(1)       | Version vector match       |
| Proof reduction (incremental, k changes) | O(k log d) | O(k log d)   | O(n)       | d = tree depth             |
| Version vector verification              | O(m)       | O(m)         | O(m)       | m = number of dependencies |
| Merkle root comparison                   | O(1)       | O(1)         | O(1)       | Single hash comparison     |
| Node cache lookup                        | O(1)       | O(1)         | O(1)       | HashMap                    |

---

## 6. THE RESONANCE MANIFOLD (SEMANTIC PLANE)

### 6.1 Architecture

The Resonance Manifold is an **approximate nearest neighbor (ANN) index** over the vector embeddings of active Causal Atoms, combined with a holographic superposition field.

```
┌─────────────────────────────────────────┐
│      RESONANCE MANIFOLD                 │
│                                         │
│  ┌─────────────┐  ┌─────────────────┐  │
│  │ HNSW Index  │  │ Holographic     │  │
│  │ (ANN)       │  │ Superposition   │  │
│  │             │  │ Buffer          │  │
│  │ For fast    │  │                 │  │
│  │ top-K       │  │ Active atoms    │  │
│  │ retrieval   │  │ superposed in   │  │
│  │             │  │ working memory    │  │
│  └──────┬──────┘  └────────┬────────┘  │
│         │                   │           │
│  ┌──────┴───────────────────┴──────┐   │
│  │      Resonance Engine            │   │
│  │  - Query vector projection       │   │
│  │  - Interference computation      │   │
│  │  - Confidence scoring             │   │
│  └──────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

### 6.2 Holographic Superposition

Active atoms (those in the Active Frontier) are maintained in a **superposition buffer**:

```
superposition_field = Σ (weight_i × normalize(atom_i.vector))
```

where `weight_i = access_frequency_i / recency_decay_i`.

**Query resonance:**

```
query_vector = embed(query_text)
resonance_scores = top_k_approximate_nearest_neighbors(query_vector, hnsw_index, k=100)
candidate_atoms = fetch_atoms(resonance_scores)
interference_result = weighted_superposition(candidate_atoms, resonance_scores)
confidence = max(resonance_scores) / sum(resonance_scores)
```

### 6.3 Observation Mode: Intuitive

```rust
fn intuitive_read(manifold: &ResonanceManifold, query: &Query) -> Result<<CacheResponse, Error> {
    let query_vec = embed_query(query);

    // HNSW search: O(log N) approximate
    let neighbors = manifold.hnsw.nearest(&query_vec, k=50)?;

    // Compute resonance scores
    let mut scores = Vec::new();
    for (id, distance) in neighbors {
        let atom = manifold.get_atom(id)?;
        let score = 1.0 / (1.0 + distance); // Convert distance to similarity
        scores.push((atom, score));
    }

    // Weighted superposition
    let mut result_vector = [0.0f32; 256];
    let mut total_weight = 0.0;
    for (atom, score) in &scores {
        for i in 0..256 {
            result_vector[i] += atom.vector[i] * score;
        }
        total_weight += score;
    }
    for i in 0..256 {
        result_vector[i] /= total_weight;
    }

    // Decode vector back to value (via learned decoder)
    let decoded_value = manifold.decoder.decode(&result_vector)?;

    // Confidence calibration
    let max_score = scores.iter().map(|(_, s)| *s).fold(0.0f32, f32::max);
    let confidence = max_score / total_score_sum;

    Ok(CacheResponse {
        value: decoded_value,
        confidence,
        mode: ObservationMode::Intuitive,
        proof_hash: None, // Intuitive has no proof
    })
}
```

---

## 7. CAUSAL AUTOPOIESIS ENGINE

### 7.1 Structural Diffraction

**Algorithm:** Decompose a value into its constituent paths and compute a Merkle tree.

```rust
fn diffract_value(path: &str, value: &Value) -> DiffractionTree {
    match value {
        Value::Null => DiffractionTree::Leaf {
            path: path.to_string(),
            hash: sha256(b"null"),
            value_hash: sha256(b"null"),
        },
        Value::Bool(b) => DiffractionTree::Leaf {
            path: path.to_string(),
            hash: sha256(&[if *b { 1 } else { 0 }]),
            value_hash: sha256(&serialize(b)),
        },
        Value::Number(n) => DiffractionTree::Leaf {
            path: path.to_string(),
            hash: sha256(&serialize(n)),
            value_hash: sha256(&serialize(n)),
        },
        Value::String(s) => DiffractionTree::Leaf {
            path: path.to_string(),
            hash: sha256(s.as_bytes()),
            value_hash: sha256(s.as_bytes()),
        },
        Value::Array(arr) => {
            let children: Vec<_> = arr.iter().enumerate()
                .map(|(i, v)| diffract_value(&format!("{}[{}]", path, i), v))
                .collect();
            let child_hashes: Vec<_> = children.iter().map(|c| c.hash()).collect();
            DiffractionTree::Node {
                path: path.to_string(),
                hash: merkle_hash(&child_hashes),
                children,
            }
        },
        Value::Object(obj) => {
            let mut children = Vec::new();
            let mut keys: Vec<_> = obj.keys().collect();
            keys.sort(); // Deterministic ordering
            for key in keys {
                let child = diffract_value(&format!("{}.{}", path, key), &obj[key]);
                children.push(child);
            }
            let child_hashes: Vec<_> = children.iter().map(|c| c.hash()).collect();
            DiffractionTree::Node {
                path: path.to_string(),
                hash: merkle_hash(&child_hashes),
                children,
            }
        },
    }
}

fn discover_structural_edges(tree: &DiffractionTree, shadow: &mut CausalShadow) {
    match tree {
        DiffractionTree::Node { path, children, .. } => {
            for child in children {
                shadow.add_edge(StructuralEdge {
                    source: child.path().to_string(),
                    target: path.clone(),
                    confidence: 1.0, // Structural containment is certain
                    channel: DiscoveryChannel::StructuralDiffraction,
                });
                discover_structural_edges(child, shadow);
            }
        },
        DiffractionTree::Leaf { .. } => {}
    }
}
```

### 7.2 Temporal Entanglement

**Statistical Model:**

For a sliding window `W` of `N` requests:

- `f_A` = count of path `A` in `W`
- `f_B` = count of path `B` in `W`
- `f_AB` = count of pairs where `A` and `B` appear within `Δt` of each other

**Expected co-occurrence under independence:**

```
E[f_AB] = (f_A × f_B × Δt) / T
```

where `T` is the total window duration.

**Lift:**

```
lift(A, B) = f_AB / E[f_AB]
```

**Chi-squared test:**

```
χ² = Σ (O - E)² / E

For 2x2 table:
        B present   B absent
A pres   f_AB       f_A - f_AB
A abs    f_B - f_AB  N - f_A - f_B + f_AB
```

**Decision thresholds (empirically derived, not guessed):**

```rust
fn evaluate_entanglement(f_AB: u64, f_A: u64, f_B: u64, N: u64, delta_t_ms: u64, T_ms: u64) -> EntanglementStatus {
    if f_AB < 10 {
        return EntanglementStatus::InsufficientData;
    }

    let expected = (f_A as f64 * f_B as f64 * delta_t_ms as f64) / T_ms as f64;
    if expected < 1.0 {
        return EntanglementStatus::InsufficientData;
    }

    let lift = f_AB as f64 / expected;

    // Chi-squared with Yates' continuity correction
    let a = f_AB as f64;
    let b = (f_A - f_AB) as f64;
    let c = (f_B - f_AB) as f64;
    let d = (N - f_A - f_B + f_AB) as f64;

    let chi_sq = (N as f64 * (a*d - b*c).powi(2)) /
                 ((a+b)*(a+c)*(b+d)*(c+d));

    // Degrees of freedom = 1, p-value from chi-squared CDF
    let p_value = chi_sq_pvalue(chi_sq, 1.0);

    if lift > 10.0 && p_value < 0.001 {
        EntanglementStatus::Hard(lift, p_value)
    } else if lift > 3.0 && p_value < 0.05 {
        EntanglementStatus::Soft(lift, p_value)
    } else if lift < 1.5 {
        EntanglementStatus::Independent
    } else {
        EntanglementStatus::WeakCorrelation
    }
}
```

### 7.3 Counterfactual Resonance

**Protocol:**

```rust
fn run_counterfactual(
    replica: &DatabaseConnection,
    target_path: &str,
    suspected_source: &str,
    shadow: &mut CausalShadow
) -> Result<(), Error> {
    // 1. Capture baseline
    let baseline = replica.query(target_path)?;

    // 2. Run synthetic mutation in rolled-back transaction
    let mutated = replica.in_transaction(|tx| {
        tx.execute(&format!("UPDATE {} SET ...", suspected_source))?;
        tx.query(target_path)
    })?; // Transaction automatically rolls back

    // 3. Compare
    if baseline != mutated {
        // Dependency proven
        shadow.add_edge(CausalEdge {
            source: suspected_source.to_string(),
            target: target_path.to_string(),
            counterfactual_proof: 1.0,
            confidence: 0.95,
            channel: DiscoveryChannel::CounterfactualResonance,
        });
    }

    Ok(())
}
```

**Scheduling:**

- Run only for Soft edges with confidence 0.70-0.90
- Maximum 0.01% of production QPS (throttled)
- Schedule during low-traffic periods
- Use read replica only

---

## 8. PROTOCOL & API ARCHITECTURE

### 8.1 RESP3 Proxy

```rust
struct Resp3Proxy {
    listener: TcpListener,
    crystal: Arc<RwLock<CausalCrystal>>,
    proof_cache: Arc<RwLock<<ProofCache>>,
    fallback: Option<<RedisConnection>,
}

impl Resp3Proxy {
    async fn handle_command(&self, cmd: Resp3Command) -> Result<<Resp3Value, Error> {
        match cmd {
            Resp3Command::Get { key } => {
                let query = Query::from_redis_key(&key, ObservationMode::Deductive);
                self.handle_query(query).await
            },
            Resp3Command::Set { key, value, .. } => {
                // Write to Causal Crystal
                let atom = self.build_atom_from_kv(&key, &value)?;
                let hash = self.crystal.write().await.append_atom(atom)?;

                // Async update of active frontier
                self.crystal.write().await.update_frontier(&key, hash);

                // Async seed proof cache (best effort)
                let _ = self.seed_proof_cache(&key).await;

                Ok(Resp3Value::SimpleString("OK".to_string()))
            },
            // ... other commands
        }
    }

    async fn handle_query(&self, query: Query) -> Result<<Resp3Value, Error> {
        // 1. Check Proof Cache
        let cache = self.proof_cache.read().await;
        if let Some(proof) = cache.get(&query.hash()) {
            match self.reduce_incremental(proof).await {
                Ok(value) => return Ok(value.into()),
                Err(_) => {} // Fall through
            }
        }
        drop(cache);

        // 2. Fallback to legacy Redis if configured
        if let Some(ref fallback) = self.fallback {
            match fallback.get(&query.path).await {
                Ok(value) => {
                    // Seed cache from fallback response
                    self.seed_proof_from_value(&query, &value).await?;
                    return Ok(value);
                },
                Err(_) => {} // Fall through
            }
        }

        // 3. Compose proof from Crystal (slow path)
        let proof = self.compose_proof(&query).await?;
        let value = proof.materialized_value.clone();

        // 4. Cache proof
        self.proof_cache.write().await.insert(query.hash(), proof);

        Ok(value.into())
    }
}
```

### 8.2 PostgreSQL Wire Protocol Proxy

```rust
struct PgProxy {
    // Parse frontend messages, proxy to backend or handle via Crystal
}

impl PgProxy {
    async fn handle_query(&self, sql: &str) -> Result<PgResultSet, Error> {
        // Parse SQL to extract paths
        let paths = sql_parser::extract_paths(sql)?;

        // Check if query is simple enough for proof cache
        if paths.len() == 1 && is_simple_select(sql) {
            let query = Query::from_sql(sql, ObservationMode::Deductive);
            return self.handle_simple_query(query).await;
        }

        // Complex query: proxy to PostgreSQL
        self.fallback.query(sql).await
    }
}
```

### 8.3 Native Causal Query API (HTTP/GraphQL)

```graphql
type Query {
  # Deductive: exact, proven
  user(id: ID!, atDepth: Int): User! @observation(mode: DEDUCTIVE)

  # Intuitive: fast, approximate
  feed(userId: ID!, limit: Int): [Post!]! @observation(mode: INTUITIVE)

  # Time travel
  userAtTime(id: ID!, timestamp: Timestamp): User @observation(mode: DEDUCTIVE)
}
```

---

## 9. FIDELITY FIREWALL

### 9.1 Policy Specification

```yaml
fidelity_policy:
  # Exact paths: Deductive only
  - pattern: "/payment/*/execute"
    mode: DEDUCTIVE
    max_latency_ms: 5
    require_merkle_proof: true

  - pattern: "/auth/*/token"
    mode: DEDUCTIVE
    max_latency_ms: 2

  # User data: Inductive or better
  - pattern: "/user/*/profile"
    mode: INDUCTIVE
    max_latency_ms: 10
    min_confidence: 0.95

  # Feeds: Any mode
  - pattern: "/feed/*"
    mode: INTUITIVE
    max_latency_ms: 1
    min_confidence: 0.70

  # Analytics: Abductive acceptable
  - pattern: "/analytics/*"
    mode: ABDUCTIVE
    max_latency_ms: 50
    min_confidence: 0.80
```

### 9.2 Enforcement Engine

```rust
fn enforce_policy(query: &Query, policy: &FidelityPolicy) -> Result<<ValidatedQuery, PolicyError> {
    let rule = policy.match(&query.path)?;

    if query.observation_mode.strictness() < rule.required_mode.strictness() {
        return Err(PolicyError::ModeTooWeak {
            requested: query.observation_mode,
            required: rule.required_mode,
        });
    }

    Ok(ValidatedQuery {
        query: query.clone(),
        rule: rule.clone(),
        deadline: now_nanos() + rule.max_latency_ms * 1_000_000,
    })
}
```

---

## 10. REPLICATION & CONSENSUS

### 10.1 Merkle Root Consensus

Instead of Raft/Paxos, Wavicle uses **hash consensus** on immutable branches.

**Protocol:**

1. Node A appends atom `a`, updates local Merkle root to `R_A`
2. Node A broadcasts `R_A` to quorum (e.g., 2 of 3 nodes)
3. Nodes B and C validate: does their Crystal produce the same root given the same atoms?
4. If yes, consensus is reached. If no, nodes exchange missing atoms by hash.

**Why this works:**

- The log is immutable. There are no conflicting writes to reconcile, only missing atoms to sync.
- Split-brain is impossible in the same sense as blockchain: divergent branches have different hashes.

### 10.2 Replication Architecture

```
┌─────────┐     ┌─────────┐     ┌─────────┐
│ Node A  │◄───►│ Node B  │◄───►│ Node C  │
│ (Leader)│     │ (Follow)│     │ (Follow)│
└────┬────┘     └────┬────┘     └────┬────┘
     │               │               │
     └───────────────┴───────────────┘
              Merkle Sync

Write Path:
1. Client → Node A
2. Node A appends atom, fsyncs
3. Node A broadcasts Merkle root to B, C
4. B, C acknowledge root (or request missing atoms)
5. Node A acknowledges client

Read Path:
1. Client → any Node
2. Node serves from local Proof Cache
3. If cache miss, compose from local Crystal
4. Eventual consistency: all nodes have all atoms within milliseconds
```

---

## 11. OPERATIONAL MODEL

### 11.1 Deployment Topology

**Single Node (Development / Small Production):**

```
┌─────────────────────────────┐
│ Wavicle Binary              │
│  - RESP3/PSQL/HTTP listeners │
│  - Proof Cache (in-memory)   │
│  - Causal Crystal (NVMe)     │
│  - Resonance Manifold (RAM)  │
└─────────────────────────────┘
```

**Multi-Node (Production):**

```
┌─────────┐  ┌─────────┐  ┌─────────┐
│ Node 1  │  │ Node 2  │  │ Node 3  │
│ Shard A │  │ Shard B │  │ Shard C │
└────┬────┘  └────┬────┘  └────┬────┘
     │            │            │
     └────────────┴────────────┘
           Merkle Sync Mesh

Sharding: By domain prefix (user.*, order.*, product.*)
Cross-shard queries: Federated proof composition
```

### 11.2 Monitoring & Observability

**Critical Metrics:**

```
wavicle_crystal_append_latency_ms
wavicle_crystal_append_throughput_per_sec
wavicle_proof_cache_hit_rate
wavicle_proof_cache_hit_latency_us
wavicle_proof_composition_latency_ms
wavicle_proof_reduction_cold_latency_ms
wavicle_proof_reduction_warm_latency_us
wavicle_incremental_changed_nodes_count
wavicle_fidelity_violations_total
wavicle_fallback_to_legacy_total
wavicle_causal_shadow_edges_total
wavicle_causal_shadow_hard_edges
wavicle_causal_shadow_soft_edges
wavicle_resonance_query_latency_us
wavicle_resonance_confidence_distribution
```

**Alerting:**

- `fallback_to_legacy_total` > 1% of queries → Wavicle is failing, investigate
- `fidelity_violations_total` > 0 → Critical bug, stop writes
- `proof_cache_hit_rate` < 80% after warmup → Proof composition too slow or cache too small

---

## 12. PERFORMANCE & COMPLEXITY MODEL

### 12.1 Theoretical Bounds

| Operation                                | Time Complexity | Space Complexity | Notes                           |
| ---------------------------------------- | --------------- | ---------------- | ------------------------------- |
| Atom append                              | O(1) amortized  | O(1)             | Log append + index update       |
| Active frontier read                     | O(1)            | O(1)             | HashMap lookup                  |
| Proof composition (cold)                 | O(n)            | O(n)             | n = atoms in causal closure     |
| Proof reduction (warm, no change)        | O(1)            | O(1)             | Version vector match            |
| Proof reduction (incremental, k changes) | O(k log d)      | O(k)             | d = tree depth                  |
| Structural diffraction                   | O(v)            | O(v)             | v = value nodes                 |
| Temporal entanglement update             | O(1)            | O(e)             | e = edges tracked (bounded)     |
| Resonance query                          | O(log N)        | O(k)             | N = active atoms, k = neighbors |
| Merkle verification                      | O(m log B)      | O(log B)         | m = deps, B = branch size       |

### 12.2 Expected Latency Model (Single Node, EPYC 9654, NVMe)

| Path                             | Component Latency | Total           |
| -------------------------------- | ----------------- | --------------- |
| Network parse (RESP3)            | 5-10 μs           |                 |
| Fidelity Firewall check          | 1-2 μs            |                 |
| Proof Cache hash lookup          | 0.5-1 μs          |                 |
| Version vector comparison        | 0.1-0.5 μs        |                 |
| **Warm read total**              |                   | **~10-20 μs**   |
|                                  |                   |                 |
| Incremental reduction (1 change) | 50-100 μs         |                 |
| Subtree re-reduce                | 20-50 μs          |                 |
| Value reconstruction             | 10-20 μs          |                 |
| **Incremental read total**       |                   | **~100-200 μs** |
|                                  |                   |                 |
| Cold proof composition           | 500-2000 μs       |                 |
| Causal closure walk              | 200-800 μs        |                 |
| Full tree reduction              | 300-1000 μs       |                 |
| **Cold read total**              |                   | **~1-3 ms**     |
|                                  |                   |                 |
| Atom append (fsync)              | 100-300 μs        |                 |
| Index updates                    | 1-5 μs            |                 |
| Merkle tree update               | 10-50 μs          |                 |
| **Write total**                  |                   | **~0.2-0.5 ms** |

### 12.3 Memory Model

| Structure                 | Per-Unit Size          | Scaling                    |
| ------------------------- | ---------------------- | -------------------------- |
| Causal Atom (log)         | 200-500 bytes          | Linear with history        |
| Active Frontier Index     | 64 bytes per path      | Linear with distinct paths |
| Parent Index              | 32 bytes per atom      | Linear with history        |
| MaterializedProof         | 500-2000 bytes         | Bounded by Proof Cache LRU |
| Node Cache (within proof) | 32 bytes per expr hash | Bounded by proof size      |
| Resonance HNSW Index      | 1-2 KB per active atom | Linear with active atoms   |
| Causal Shadow Edge        | 128 bytes              | Bounded by edge limit      |

**Capacity planning:**

- 10M active paths → ~640MB Active Frontier
- 1M active atoms in Resonance → ~1-2GB HNSW
- 100K cached proofs → ~50-200MB Proof Cache
- **Total RAM for 10M object system: ~4-8GB**

---

## 13. IMPLEMENTATION PHASES

### Phase 0: Validation (Weeks 1-12)

**Goal:** Prove the thesis with a single-node RESP3 proxy.

**Deliverables:**

- Causal Crystal with fsync durability
- Active Frontier Index
- Proof Cache with incremental reduction
- RESP3 proxy with fail-open to Redis
- Benchmark harness
- Go/No-Go decision

### Phase 1: Proxy Product (Months 4-6)

**Goal:** Production-ready proxy for Redis workloads.

**Deliverables:**

- Multi-threaded Rust core
- PostgreSQL wire protocol proxy
- Fidelity Firewall v1
- Structural diffraction
- Temporal entanglement (lift-based)
- Docker / Kubernetes operator
- 5 design partners

### Phase 2: Native Mode (Months 7-12)

**Goal:** Causal Crystal as primary write path.

**Deliverables:**

- Native write API
- Merkle root consensus replication
- Time-travel queries (`atDepth`, `atTime`)
- Causal SQL projection layer
- Counterfactual resonance engine
- First native customer

### Phase 3: Distributed Fabric (Months 13-18)

**Goal:** Multi-node, multi-tenant cloud offering.

**Deliverables:**

- Automatic sharding by domain
- Cross-shard query federation
- Resonance Manifold (HNSW + holographic)
- Cloud marketplace offering
- Enterprise features (SSO, RBAC, audit)

---

## APPENDIX A: THE COMBINATOR ALGEBRA (FORMAL SPEC)

### Syntax

```
expr ::= const
       | field(expr, string)
       | compose(expr, expr)
       | apply(expr, expr)
       | embed(value)
       | resonate(expr, expr)

value ::= bytes | int | float | bool | string | record | array
```

### Typing Rules

```
Γ ⊢ c : τ    (constants)
Γ ⊢ e : Record { f: τ }    Γ ⊢ field(e, "f") : τ
Γ ⊢ e1 : τ1    Γ ⊢ e2 : τ2    Γ ⊢ compose(e1, e2) : τ1 × τ2
Γ ⊢ e1 : τ2 → τ    Γ ⊢ e2 : τ2    Γ ⊢ apply(e1, e2) : τ
Γ ⊢ v : string    Γ ⊢ embed(v) : Vector
```

### Reduction Semantics

```
field(record({f: v, ...}), "f") → v
compose(v1, v2) → pair(v1, v2)
apply(lambda(x, body), v) → body[x/v]
```

---

## APPENDIX B: MERKLE-DAG HASHING

### Atom Hash

```
H(atom) = SHA3-256(
    serialize(atom.expr)
    || atom.logical_clock (uint64 BE)
    || atom.physical_time (uint64 BE)
    || atom.nonce (16 bytes)
    || len(atom.causal_past) (uint32 BE)
    || concat(atom.causal_past)
)
```

### Branch Merkle Root

```
MerkleRoot(S) = MerkleTreeRoot({ H(a) : a ∈ S })
```

Using standard binary Merkle tree with SHA3-256.

---

## APPENDIX C: HRR VECTOR OPERATIONS

### Circular Convolution (⊗)

```
(a ⊗ b)_k = Σ_{j=0}^{n-1} a_j × b_{(k-j) mod n}
```

### Circular Correlation (⊘) — approximate inverse

```
(a ⊘ b)_k = Σ_{j=0}^{n-1} a_j × b_{(k+j) mod n}
```

### Binding

```
bind(key, value) = normalize(V(key) ⊗ V(value))
```

### Unbinding

```
unbind(bundle, key) ≈ normalize(bundle ⊘ V(key))
```

---

This is the complete blueprint. Every structure, every algorithm, every complexity bound, every operational consideration. The only remaining question is execution.

Build the first atom.
