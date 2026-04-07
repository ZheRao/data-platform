# Failure Logs — [from actual failures]

This document captures real system failures and the invariants extracted from them.

Each entry follows:

Problem → Root Cause → System Insight → New / Updated Invariant

## [2026-03-21] — Partition Overwrite Leakage in Spark

### Context
- Layer: Silver
- Environment: PySpark
- Trigger: Writing partitioned output with overwrite mode

### Problem
New writes unintentionally overwrote historical partitions outside the intended scope.  
Specifically, raw files are stored quarterly, fiscal year starts from November; to process 2 fiscal years' data, 
2 full and 1 partial fiscal year's data will be loaded → overwritting all 3 fiscal year's data → leakage

### Root Cause
Spark `.write.partitionBy(...).mode("overwrite")` overwrites all partitioned dataset unless scoped.  
Partition boundaries were not explicitly controlled.

### System Insight
Distributed writes are not inherently safe.  
Write operations must be explicitly scoped to avoid unintended data loss.

### Implementation Change
- Introduced scoped overwrite logic
- Ensured partition-level isolation during writes

### Notes
This is a silent failure mode — extremely dangerous without detection.

## [2026-03-23] — Schema Drift in QBO PL Reports

### Context
- Source: QBO API (PL reports)
- Layer: Silver
- Environment: PySpark
- Trigger: Multiple reports with inconsistent column sets

### Problem
Flattening logic produced dataframe with incomplete columns.  

### Root Cause
PL reports from QBO do not have a fixed schema.  
Column presence varies depending on report origin (entity).

### System Insight
Semi-structured APIs should not be flattened with assumed schemas.  
Schema must be discovered dynamically before normalization.

### New / Updated Invariant
Silver layer must not assume schema.  
All schemas must be explicitly discovered and unified prior to transformation.

### Implementation Change
- Introduced global column discovery across all input files
- Enforced consistent column ordering before flattening

### Notes
Schema instability is not an edge case — it is a fundamental property of the source.

## [2026-04-06] — Fiscal Year Calculation Failure for QBO GL

### Context
- Source: QBO API (GL reports)
- Layer: Silver
- Environment: PySpark (ANSI mode) & Pandas
- Trigger: Malformed date values caused runtime failure

### Problem
GL reports include pseudo-data rows (e.g., `Beginning Balance`)  
where `date_col` contains non-date strings while other fields remain valid.

### Root Cause
Fiscal year computation assumed all date values were parseable.  
Under Spark ANSI mode, invalid parsing throws instead of returning null.

### System Insight
Structural validity does not imply semantic validity.  
Field values may be unusable even when rows are correctly formed.

### New / Updated Invariant
Derivation logic must not assume input validity.  
- Structural issues → fail loudly  
- Value-level issues → fail gracefully (coerce + continue)

### Implementation Change
- Switched to tolerant date parsing (`try_cast` / coercion to null)
- Computed `fiscal_year` only on valid parsed dates
- Preserved invalid rows without dropping

### Notes
Pseudo-data rows are part of source behavior, not edge cases.  
Avoid blacklist-based filtering; rely on positive validation instead.


