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