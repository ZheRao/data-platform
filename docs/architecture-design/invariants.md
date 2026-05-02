# System Invariants

These are **necessary constraints** of `data-platform`.

They define what must always be true for the system to remain correct, scalable, and interpretable.

## 1. Structure–Meaning Separation

**Invariant**  
Structure processing and business meaning must remain strictly separated.

**Why**  
Mixing them destroys reusability and prevents system-level scaling.

**Enforced By**
- Bronze/Silver: structure only
- Gold: meaning via contracts

**Violation Result**  
Pipelines become source-specific and non-reusable.

## 2. Configuration-Driven Behavior

**Invariant**  
All system variability must be externalized via configuration.

**Why**  
Hardcoding prevents multi-tenant scaling and adaptability.

**Enforced By**
- contracts
- config-driven mappings

**Violation Result**  
Logic forks across clients → system fragmentation.

## 3. Engine Agnosticism

**Invariant**  
Pipeline logic must not depend on execution engine.

**Why**  
Ensures portability across Pandas, Spark, and future engines.

**Enforced By**
- abstraction layer
- engine-agnostic transformations

**Violation Result**  
System becomes locked to one execution environment.

## 4. Schema Discovery Before Enforcement

**Invariant**  
No schema may be assumed before it is discovered from data.

**Why**  
External sources are inherently inconsistent and evolving.

**Enforced By**
- pre-flatten schema discovery
- global column union

**Violation Result**  
Data loss or incomplete representation.

## 5. Fail-Loud Structural Validation 

**Invariant**  
All external data must pass strict structural validation before being processed.

**Why**  
Silent misinterpretation is more dangerous than failure.

**Enforced By**
- validation gates:
    - presence → KeyError
    - type → TypeError
    - content → ValueError
- exhaustive classification (no fallback paths)

**Violation Result**  
Silent data corruption under schema drift.

## 6. Classification Closure

**Invariant**  
Every structured input must map to exactly one known type.

**Why**  
Partial or ambiguous classification leads to undefined behavior.

**Enforced By**
- closed set of node types
- no default / fallback logic

**Violation Result**  
Hidden logic branches and inconsistent outputs.

## 7. Control-Plane / Data-Plane Separation

**Invariant**  
Control-plane state mutation must be completed before distributed execution begins.  
Distributed workers may consume shared state, but must never mutate it.

**Why**  
Distributed systems (e.g., Spark) execute work in parallel, unordered, and retryable ways.  
State mutation (e.g., auth refresh) is order-sensitive and must be single-writer to remain consistent.

**Enforced By**
- auth refresh as preflight step (driver-only)
- atomic persistence of auth state
- read-only broadcast of per-run auth snapshot
- no mutation logic inside worker execution

**Violation Result**  
Race conditions, inconsistent state, and non-deterministic pipeline behavior.

## 8. Config-as-Contract Validation

**Invariant**  
All external configuration must be validated as an explicit contract before being returned to downstream programs.

**Why**  
Manual correctness over many config files, paths, keys, and nested structures does not scale. As platform scope expands, relying on memory to maintain hundreds of knobs becomes a structural failure mode.

**Enforced By**
- missing-file checks with exact expected location
- recursive required-key validation
- nested structure/type validation
- source-system-level config schema definitions
- fail-loud config boundary before execution

**Violation Result**  
Downstream failures become detached from their true cause, making missing or malformed config extremely difficult to diagnose.

# Missing Invariants (Planned)

These define future system hardening areas.

**Failure Invariants**
- partial ingestion handling
- retry idempotency
- corrupted Bronze recovery

**Time Invariants**
- incremental processing
- late-arriving data
- backfill correctness

**Scale Invariants**
- memory pressure handling
- partition skew
- API rate limiting

**Trust Invariants**
- data validation
- reconciliation
- auditability (“can finance trust this number?”)