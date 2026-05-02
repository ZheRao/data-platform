# Failure Logs — [from detected risks]

This document captures potential system failures detected from early stage and the invariants extracted from them.

Each entry follows:

Risk → Root Cause → System Insight → Invariant → Change

## [2026-04-03] — Silent Misclassification Risk in Nested JSON Traversal

### Context
- Source: QBO API (nested JSON)
- Layer: Bronze → Silver
- Component: Node classification

### Risk

Unseen QBO JSON structures could be misclassified or skipped without error.

### Root Cause

Classification relied on implicit structural assumptions and tolerated unknown patterns.

### System Insight

Silent misinterpretation is more dangerous than failure.  
Unknown structure must be treated as invalid.

### New / Updated Invariant
- All nodes must pass strict structural validation before classification
- Every node must map to exactly one known type (no fallback)
- Missing / invalid structure must raise immediately (fail-loud)

### Implementation Change
- Added validation gates:
    - key → KeyError
    - type → TypeError
    - content → ValueError
- Enforced exhaustive classification (no default path)

### Notes

Prevents silent data corruption under schema drift.

## [2026-04-12] — Concurrent Auth Mutation in Distributed Ingestion

### Context
- Source: QBO API
- Layer: Ingestion (Auth + Extraction)
- Component: Auth handling under Spark execution

### Risk

Multiple Spark partitions could concurrently refresh and write auth state for the same company, leading to race conditions and inconsistent auth files.

### Root Cause

Auth refresh and persistence were embedded inside extraction logic, which is executed in a distributed, unordered, and retryable environment.

### System Insight

Control-plane state mutation is not compatible with distributed execution.  
Auth refresh is inherently single-writer and order-sensitive, while Spark execution is parallel and non-deterministic.

### New / Updated Invariant
- Auth refresh must not occur inside distributed workers
- Shared auth state must not be mutated concurrently
- Distributed tasks may only consume auth state, never modify it

### Implementation Change
- Moved auth refresh into a preflight sequential step (driver-only)
- Constructed a per-run auth snapshot for all companies
- Broadcast auth snapshot to workers as read-only input
- Removed all auth write logic from partition-level execution

### Notes

Prevents race conditions, inconsistent auth state, and non-deterministic pipeline behavior under Spark execution.

## [2026-05-02] — Missing / Invalid Config Assumptions Across Expanding Platform Scope

### Context
- Source: All source systems
- Layer: Configuration / Control Plane
- Component: `read_configs` + config validation layer

### Risk

As the platform expands into new source systems, scenarios, contracts, paths, entities, and execution modes, the system may assume many external JSON config files exist and contain the correct nested keys.

If a config file is missing, incomplete, incorrectly named, or structurally wrong, downstream code may fail far away from the actual cause.

This would make failures difficult to diagnose because the platform depends on many small configuration pieces distributed across the repo.

### Root Cause

Configuration was being treated as an implicit assumption instead of an explicit contract.

The system assumed:
- required config files exist
- expected keys are present
- nested structures are correct
- key names match downstream program expectations

At small scale, these assumptions can be manually remembered.  
At platform scale, this becomes unsafe.

### System Insight

Manual correctness over many configuration knobs does not scale.

As the number of source systems, scenarios, contracts, paths, and config files grows, relying on human memory to keep everything aligned becomes a structural failure mode.

A platform must assume larger future scope and reject incomplete or malformed configuration before execution begins.

### New / Updated Invariant

- External configuration must be treated as a contract, not a convenience file
- Missing config files must fail loudly with the exact expected location and filename
- Config files must be validated before being returned to downstream programs
- Required keys must be checked recursively, including nested JSON structures
- Invalid config shape must fail at the config boundary, not later in business logic
- Manual handling of many configuration knobs is forbidden as a correctness strategy

### Implementation Change

- Updated `read_configs` to check whether the requested config file exists before reading it
- Added contextual error messages showing the exact expected config path
- Added a config validation layer based on source-system-level schema definitions
- Added recursive required-key validation for nested JSON structures
- Changed config loading so that returned configs are guaranteed to satisfy expected structural requirements

### Notes

Prevents hidden missing-config failures, wrong-key failures, nested-structure mismatches, and downstream crashes caused by incomplete control-plane setup.

This turns configuration from scattered assumptions into an explicit validated interface.