# Column Schema Discovery Design for Drifting Column Names

## Purpose

This document records the architecture decisions behind introducing a **global column superset** for the Spark Profit & Loss flatten pipeline, while preserving the core philosophy of the generic `data-platform` design.

The goal is to solve a real bug:

* different sub-companies can emit different report columns
* the current flatten logic yields Python dictionaries with variable keys
* Spark does **not** keep widening schema as later rows introduce new keys
* therefore, fields not present in the initially inferred schema are silently dropped

This document explains the design decision, trade-offs considered, final architecture, and a demonstration of the intended implementation mechanics.

## Problem Statement

The original flatten pipeline emits leaf records as dictionaries, with keys derived from report-specific metadata.

This works correctly at the Python-generator level, but breaks at the Spark DataFrame boundary:

* Spark requires a fixed schema
* schema inference happens early
* later unseen keys do not become new columns automatically

This creates a silent failure mode:

* if a later file contains a new column
* and that column was not present in the schema inferred from earlier records
* that field is dropped from the DataFrame output

This is not acceptable for a durable bronze flatten layer.

---

## Core Design Philosophy

The flatten layer belongs to the **source-faithful** part of the platform.

That means:

* no business-level normalization in bronze
* no company-specific semantic mapping in bronze
* no hardcoded target schema imposed from outside the source
* preserve source reality as completely as possible

At the same time, Spark imposes a real engine constraint:

* DataFrames are schema-first
* schema cannot remain dynamic row-by-row

Therefore, some degree of schema stabilization must happen before DataFrame creation.

The architecture question becomes:

> How do we stabilize schema for Spark without violating the generic, source-faithful philosophy of the platform?

## Trade-Offs Considered

### Option 1 — Predefine expected schema

Example idea:

* maintain a fixed list of columns externally
* force all records into that schema

#### Upside

* simple implementation
* deterministic schema

#### Downside

* externally imposed assumption
* brittle against source drift
* violates generic bronze-layer philosophy
* introduces maintenance burden and hidden coupling

### Option 2 — Create a separate DataFrame per sub-company

Observation:

* sub-company is the actual source of schema variation
* within a sub-company, columns may be stable across periods

#### Upside

* preserves source-local schema
* avoids premature global stabilization

#### Downside

* more orchestration overhead
* more DataFrames and unions
* more control-flow complexity
* still leaves hidden assumptions around stability within sub-company

### Option 3 — Discover schema from source metadata, then flatten against a global superset

Refined idea:

* add a `_discovery()` phase
* scan all files in processing scope
* extract report column metadata from each file
* build a global superset of all observed leaf columns
* then use that superset as the schema contract for flattening

#### Upside

* schema is source-derived, not externally imposed
* deterministic and Spark-compatible
* preserves generic platform philosophy
* lower overhead than per-company DataFrames
* removes hidden assumption that first processed file is representative

#### Downside

* requires an explicit discovery pass
* still requires explicit handling for structural fields introduced by crawler traversal

This became the selected direction.

## Key Conceptual Refinement

A fully generic system can never have **zero assumptions**.

The correct goal is:

> permit only assumptions that are mechanically necessary and source-faithful

This leads to an important distinction.

### Bad assumptions

* business semantics imposed in bronze
* hardcoded target columns based on expectation rather than observation
* assuming different companies use the same terminology

### Necessary assumptions

* Spark needs a fixed schema
* hierarchical traversal introduces account context columns that must exist in output
* if a field may emit a companion ID value, schema must have somewhere to store it

The final design accepts only the second category.

## Final Architecture Decision

The flatten output schema should be built from three sources:

### 1. Discovered leaf columns from source metadata

These come from report column metadata scanned from all files in scope.

### 2. Structural traversal columns

These are not declared as leaf columns by report metadata, but are introduced by crawler traversal through the hierarchy.

Examples:

* account-level fields
* carried parent context
* `corp`
* any lineage columns added by traversal design

### 3. Companion `*_id` columns

If a value field can carry an accompanying `id`, the schema must support preserving it.

The generic default rule is:

* include `col`
* include `col_id`

for every discovered leaf column, **except** for columns explicitly listed in a small negative config of known non-ID fields.

## Why Full-Scope Discovery Was Chosen

An intermediate idea was to inspect one file per sub-company.

This was later refined to inspecting **all files in processing scope**, because this removes another hidden assumption:

* that schema is stable within sub-company across all periods

By scanning all files for metadata only, the system becomes more bulletproof while keeping the cost low.

This is acceptable because:

* metadata extraction is much cheaper than full flattening
* dozens of files are not expected to create significant overhead
* durability is more important than avoiding a cheap metadata scan

## Bronze Layer Contract

The bronze flatten layer should obey the following contract:

1. It does not normalize business meaning.
2. It does not hardcode company-specific schema.
3. It derives leaf columns from source metadata across the full job scope.
4. It adds structurally required traversal columns explicitly.
5. It provides storage capacity for companion IDs.
6. It creates an explicit Spark schema before DataFrame creation.
7. It preserves source distinction even when two sub-companies use different names for the same conceptual field.

This means semantic harmonization belongs later, in gold or other business-facing layers.

## Handling `*_id` Columns

### Initial generic rule

For every discovered column, include a companion `*_id` column.

This is durable and future-proof, but may create a small number of all-null columns.

### Refined rule

Maintain a small external config of columns that are known never to have a companion ID.

Examples of likely negative cases:

* `date`
* `amount`
* numeric totals
* percentages

Then apply the rule:

* if column is in `no_id_companion_columns`, include only `col`
* otherwise include `col` and `col_id`

This is safer than maintaining a whitelist of columns that *do* support IDs.

#### Why blacklist is better than whitelist

A whitelist would miss newly introduced valid ID-bearing columns.
A blacklist preserves generic behavior while safely pruning known impossible cases.

## Storage Consideration for Sparse `*_id` Columns

A concern was raised that adding `*_id` columns for many fields might waste storage.

This concern is much less severe when using **Parquet** instead of CSV.

Why:

* Parquet is columnar
* all-null or mostly-null columns compress very well
* cost scales much more like compact null encoding plus metadata overhead than like repeated string payloads

Therefore, preserving durable representational capacity in bronze is usually the correct trade-off.

This further confirms that using Parquet for downstream storage is a strong design choice.

## Implementation Mechanics

The key implementation distinction is:

* `columns` = global schema contract for the job
* `col` = file-specific report metadata used to align the current node’s values with actual field names

This prevents schema loss while preserving correct alignment between current file payload and report-specific column definitions.

### Node-Level Extraction Pattern

```python
records = dict.fromkeys(columns, "")
for i, item in enumerate(raw_records):
    column_name = col[i]
    column_value = item.get("value", "")
    records[column_name] = column_value
```

This means:

* every output record conforms to the full global schema
* only the fields present in the current file are populated
* all other fields remain empty
* Spark receives a stable shape for every row

## Explicit Spark Schema

Instead of relying on schema inference, the flatten stage should build the schema explicitly:

```python
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField(col, StringType(), True)
    for col in columns
])
```

This ensures:

* deterministic output schema
* no dependence on first-row inference
* no silent loss of later columns
* alignment with Spark’s execution model

## Demonstration Design Sketch

Below is a high-level example of how the design can be organized.

```python
from pathlib import Path
from typing import Iterator
from pyspark.sql.types import StructType, StructField, StringType


def discover_leaf_columns(records, raw_root: Path, no_id_companion_columns: set[str]) -> list[str]:
    """
    Scan all files in scope and build a global leaf-column superset.
    """
    observed: set[str] = set()

    for record in records:
        company = record["company"]
        start = record["start"]
        path = build_raw_file_path(raw_root=raw_root, company=company, start=start)

        file_cols = extract_report_columns(path)
        for c in file_cols:
            observed.add(c)
            if c not in no_id_companion_columns:
                observed.add(f"{c}_id")

    return sorted(observed)


def build_flatten_schema(
    discovered_leaf_columns: list[str],
    structural_columns: list[str],
) -> list[str]:
    """
    Combine discovered leaf columns with explicit traversal columns.
    """
    return sorted(set(discovered_leaf_columns).union(structural_columns))


def build_spark_schema(columns: list[str]) -> StructType:
    return StructType([
        StructField(col, StringType(), True)
        for col in columns
    ])
```

### Example flatten extraction

```python
def extract_data_node(
    node: dict,
    columns: list[str],
    col: list[str],
    acc_info: dict[str, str],
    company_info: str,
    no_id_companion_columns: set[str],
) -> dict:
    """
    Extract one data node into a record conforming to the global schema.
    """
    records = dict.fromkeys(columns, "")
    raw_records = node.get("ColData", "")

    if raw_records == "":
        raise ValueError("Empty Data node")

    for i, item in enumerate(raw_records):
        column_name = col[i]
        column_value = item.get("value", "")
        records[column_name] = column_value

        if column_name not in no_id_companion_columns:
            column_id = item.get("id", "")
            if column_id:
                records[f"{column_name}_id"] = column_id

    for k, v in acc_info.items():
        records[k] = v

    records["corp"] = company_info
    return records
```

## Why `columns` and `col` Must Be Kept Separate

This distinction is essential.

### `columns`

The job-level superset used for:

* creating empty output record skeletons
* building Spark schema
* guaranteeing deterministic row shape

### `col`

The file-level report metadata used for:

* aligning the current node’s `ColData` values to the correct field names
* preserving report-specific ordering
* avoiding misalignment between current node values and global schema ordering

Without this distinction, the pipeline risks either:

* losing fields not in current file
* or misaligning values to the wrong columns

## Layer Boundary Decision

Different sub-companies may use different column names for the same conceptual field.

This is acceptable in bronze.

Why:

* bronze should preserve source truth
* semantic unification is not the job of flattening
* business meaning should begin in gold or another explicitly configured transformation layer

So if two sub-companies express the same business concept under different names, the bronze output may preserve both columns independently.

This is not a flaw; it is a correct reflection of source reality.

## Recommended Validation Hook

To make the system safer, add validation during flattening.

Examples:

* if a node has more `ColData` entries than its `col` metadata allows, raise error
* if traversal generates a structural column missing from the final schema, raise error
* optionally record which files introduced which discovered columns for auditability

This ensures discovery and execution remain aligned.

## Summary of Final Decision

The selected design is:

* perform a full-scope `_discovery()` pass across all files in the processing scope
* extract and union all observed report leaf columns
* expand them into companion `*_id` columns by default
* exclude only a small set of known impossible ID columns via negative config
* explicitly append structural traversal fields such as account context and `corp`
* build a fixed explicit Spark schema from the final superset
* use `columns` as the global schema contract
* use `col` as the file-specific metadata alignment list
* preserve semantic differences across sub-companies in bronze
* defer normalization and business mapping to later layers

This design preserves genericity, durability, source-faithfulness, and Spark compatibility.

## Final Takeaway

The most important architectural shift here is not merely “using a global column list.”

It is this:

> bronze flatten must separate **source-derived schema discovery** from **Spark schema stabilization**, while keeping business semantics out of the layer.

That is the core design decision.
