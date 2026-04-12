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