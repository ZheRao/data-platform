# System Invariants

This document defines the non-negotiable architectural rules of `data-platform`.

These invariants are derived from real system behavior and must not be violated.

## 1. Structure–Meaning Separation

### Statement
Data structure processing and business interpretation must be strictly separated.

### Scope
All layers

### Rationale
Mixing structure and meaning leads to non-reusable, non-scalable pipelines.

### Allowed
- Flattening nested JSON (Silver)
- Business mapping using contracts (Gold)

### Forbidden
- Business logic in Bronze or Silver
- Source-specific assumptions in Gold

## 2. Bronze Immutability

### Statement
Bronze must remain a lossless, immutable representation of source data.

### Scope
Bronze layer

### Rationale
Ensures reproducibility and protects against upstream inconsistencies.

### Allowed
- Raw data storage
- Append-only writes

### Forbidden
- Modifying raw payloads
- Dropping fields
- Applying transformations

## 3. Configuration-Driven Behavior

### Statement
All system variability must be externalized via configuration.

### Scope
All layers

### Rationale
Hardcoding prevents scalability and multi-tenant reuse.

### Allowed
- JSON-based contracts
- Environment-specific configs

### Forbidden
- Inline business logic
- Hardcoded paths or rules

### Examples
✔ location mapping via config  
✘ if company == "X": logic

## 4. Engine Agnosticism

### Statement
Pipeline logic must be independent of execution engine.

### Scope
Core + transformations

### Rationale
Ensures portability across Pandas and Spark environments.

### Allowed
- Engine abstraction layer
- Config-based engine selection

### Forbidden
- Exposed engine-specific logic in transformations

### Examples
✔ same logic runs on Pandas/Spark  
✘ using Spark-only APIs in core logic

## 5. Schema Discovery

### Statement
Assume no prior knowledge about expected columns in nested raw JSON files.

### Scope
Silver

### Rationale
Different reports from different entities might include different columns/column_names.  
Enforcing predefined columns introduce schema drift or incomplete data capture.

### Allowed
- Schema discovery run based on targeted data files

### Forbidden
- Hardcode expected column names in external config and enforce it in Silver flattening


















# Missing Invariants


## 1. Failure invariants
- partial ingestion failures
- retry idempotency
- corrupted Bronze states
## 2. Time invariants
- incremental processing
- late-arriving data
- backfills vs real-time
## 3. Scale invariants
- memory pressure
- skewed partitions
- API rate limiting at scale
## 4. Trust invariants
- data validation
- reconciliation
- “can finance trust this number?”