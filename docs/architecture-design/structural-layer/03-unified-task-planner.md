# Controlled Task Planner

## Core Idea

The platform should not execute ingestion or transformation logic directly from loosely-coupled loops and nested control flow.

Instead, every unit of work must first be converted into an explicit, deterministic **task object** containing the minimum metadata required for execution.

The system operates in two stages:

```text
Planning
    ↓
Execution
```

This separation is a foundational architectural invariant for the platform.


## Why This Exists

Early ingestion implementations fused many responsibilities together:

* iteration over companies
* iteration over fiscal periods
* HTTP request construction
* pagination
* response parsing
* file path construction
* writes
* flattening logic
* logging

Example anti-pattern:

```python
for company in companies:
    for quarter in quarters:
        response = requests.get(...)
        json.dump(...)
        flatten(...)
```

This tightly couples:

```text
WHAT should be executed
with
HOW it is executed
```

As the platform grows, this creates major problems:

* difficult to parallelize
* difficult to retry safely
* difficult to validate execution scope
* difficult to distribute to Spark workers
* difficult to isolate failures
* difficult to reuse planning logic across Bronze/Silver layers
* difficult to reason about operational state

The Controlled Task Planner separates these concerns.


## Architectural Principle

### The planner defines scope.

### The engine performs execution.

The planner determines:

```text
- what company
- what dataset
- what period
- what source
- what output target
```

The engine only performs work against already-defined tasks.


## Task-Based Architecture

### Phase 1 — Planning

Planning converts abstract workload definitions into explicit task metadata.

Example:

```text
Pull PL reports for:
- Company A
- FY2025
- quarter grain
```

becomes:

```python
{
    "company": "A",
    "dataset": "ProfitAndLossDetail",
    "start": "2024-10-01",
    "end": "2024-12-31",
}
```

Each task represents one independent execution unit.


### Phase 2 — Execution

Execution engines operate only on prepared tasks.

Example:

```python
for task in tasks:
    run_task(task)
```

This allows:

* local Python execution
* multiprocessing
* Spark distribution
* retry systems
* future orchestration frameworks

without changing planning logic.


## Core Invariant

### Every execution unit must be representable as an explicit task before execution begins.

This is one of the most important invariants in the platform.

The system should never rely on hidden runtime iteration state.

Bad:

```python
for year in years:
    for company in companies:
        ...
```

inside business logic.

Good:

```python
tasks = create_tasks(...)
run(tasks)
```

The task list itself becomes the explicit representation of workload scope.


## Separation of Concerns

The planner must not perform execution.

The executor must not decide scope.


### Planner Responsibilities

The planner is responsible for:

* determining execution scope
* period slicing
* task metadata generation
* fiscal calendar awareness
* workload partitioning
* deterministic task identity

The planner does NOT:

* perform HTTP calls
* write files
* parse responses
* flatten JSON
* run Spark transformations


### Executor Responsibilities

The executor is responsible for:

* performing HTTP requests
* reading files
* writing files
* flattening payloads
* Spark transformations
* retries
* logging execution results

The executor does NOT:

* decide fiscal scope
* determine which periods to run
* determine workload boundaries


## Shared Planning Across Layers

One of the key realizations is that Bronze ingestion and Silver flattening share the same execution scope.

Example:

```text
Company A
FY2025
Quarter 1
```

is simultaneously:

* a Bronze ingestion scope
* a Silver flattening scope
* potentially a validation scope
* potentially a reconciliation scope

Therefore:

### period planning should be reusable across layers

instead of duplicated independently.


## Generic Planning Layer

The reusable invariant is not:

```text
"PL ingestion"
```

The reusable invariant is:

```text
"time-scoped workload partitioning"
```

This becomes a generic platform mechanism.


## Example Task Hierarchy

### Base Period Task

```python
{
    "company": "A",
    "dataset": "ProfitAndLossDetail",
    "start": "2024-10-01",
    "end": "2024-12-31",
    "fiscal_year": 2025,
}
```

This represents generic scope only.


### Bronze Ingestion Task

```python
{
    "company": "A",
    "dataset": "ProfitAndLossDetail",
    "start": "2024-10-01",
    "end": "2024-12-31",
    "out_path": "...",
    "source": "qbo",
}
```

Adds execution metadata for ingestion.


### Silver Flatten Task

```python
{
    "company": "A",
    "dataset": "ProfitAndLossDetail",
    "start": "2024-10-01",
    "end": "2024-12-31",
    "in_path": "...",
    "out_path": "...",
}
```

Adds execution metadata for flattening.


## Why This Matters for Spark

Spark requires workload partitionability.

A task-based design naturally supports distribution because:

```text
each task is independent
```

Instead of Spark workers trying to infer workload dynamically, the planner pre-defines execution units explicitly.

This creates:

* deterministic workload boundaries
* safe retries
* easier monitoring
* easier failure isolation
* future scalability


## Controlled Scope

A major purpose of the planner is controlling blast radius.

Example:

Instead of:

```text
reload all historical data every refresh
```

the planner can define:

```text
FY2025 only
Current quarter only
One company only
Specific report only
```

This makes ingestion bounded and predictable.


## Future Evolution

The planner is expected to evolve into more advanced workload definitions.

Examples:

### Current

```text
quarter-based scoped pulls
```

### Future

```text
incremental pulls by MetaData.LastUpdatedTime
```

### Future

```text
backfill tasks
```

### Future

```text
reconciliation tasks
```

### Future

```text
validation-only tasks
```

The architectural invariant remains unchanged:

```text
plan first
execute second
```


## Design Philosophy

The Controlled Task Planner exists because:

```text
explicit systems scale better than implicit iteration
```

The platform should make workload boundaries visible, deterministic, and auditable before execution begins.

This improves:

* scalability
* correctness
* observability
* debuggability
* recoverability
* distribution
* operational coherence

The planner is therefore not just a utility mechanism.

It is a foundational orchestration layer for the platform.
