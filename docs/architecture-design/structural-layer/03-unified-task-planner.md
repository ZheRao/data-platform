# 03 — Controlled Task Planner

## Core Idea

The platform should separate:

```text id="08r3h2"
generic workload scope planning
```

from:

```text id="vhxmpf"
source-specific and operation-specific task enrichment
```

The planner is not merely responsible for:

```text id="86ktmy"
creating tasks before execution
```

The platform already follows that pattern.

The deeper architectural goal is:

```text id="d6p9hs"
plan workload scope once
reuse and enrich many times
```

instead of allowing each layer to independently recreate workload boundaries.


## Problem

Early implementations tend to duplicate planning logic across multiple systems.

Example anti-pattern:

```python id="glv6bo"
## ingestion
for company in companies:
    for quarter in quarters:
        ingest(...)

## flatten
for company in companies:
    for quarter in quarters:
        flatten(...)
```

Both systems independently recreate:

* fiscal period logic
* date boundaries
* company scope
* workload partitioning
* filtering rules

This creates:

* duplicated logic
* inconsistent scope definitions
* harder maintenance
* harder orchestration
* harder scaling
* harder operational reasoning

The problem is not execution.

The problem is duplicated workload planning.


# Architectural Principle

### Workload scope should be planned once at the platform core level.

Then downstream systems enrich the scope with:

* source-specific context
* operation-specific context
* execution-specific metadata


## Main Invariant

```text id="kh65ql"
scope is planned once
context is layered incrementally
```

instead of:

```text id="g3c78j"
every system independently recreates workload boundaries
```


# Planning Layers

The planner architecture evolves in layers.


## Layer 1 — Core Scope Planner

The core planner creates neutral workload scope.

It does NOT know about:

* QBO
* HTTP
* Bronze
* Silver
* ingestion
* flattening
* Spark

It only defines:

* company
* dataset
* start date
* end date
* fiscal year
* period grain

The planner therefore belongs in platform core because it represents:

```text id="y17yvg"
time-scoped workload partitioning
```

which is a platform-level invariant.


## Why Period Grain Must Be Generic

The core planner must NOT assume:

```text id="9a1mnr"
quarter
```

as the only workload partitioning strategy.

Different systems may require:

* daily partitions
* monthly partitions
* quarterly partitions
* yearly partitions
* custom ranges

Therefore:

```text id="djlwm6"
period grain is configuration
not a platform assumption
```

Example:

```python id="gfl7mc"
period_grain="month"
```

or:

```python id="0k1k56"
period_grain="quarter"
```

The source layer or operation layer chooses the appropriate grain.

Example:

```text id="h84l0i"
QBO PL → quarter
raw incremental ingestion → month
daily monitoring → day
```


## Core Scope Contract

The core planner produces a reusable scope contract.

Example:

```python id="8n7oqr"
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class PeriodScopeTask:
    company: str
    dataset: str

    start: str
    end: str

    fiscal_year: int

    period_grain: Literal[
        "day",
        "month",
        "quarter",
        "year",
        "custom",
    ]
```

This represents generic workload scope only.


## Why `dataclass` Instead of `TypedDict`

The planner uses `dataclass` contracts instead of `TypedDict`.

Example:

```python id="pryjlwm"
@dataclass(frozen=True)
class PeriodScopeTask:
    ...
```

instead of:

```python id="e3rprm"
class PeriodScopeTask(TypedDict):
    ...
```

because planning contracts represent:

```text id="97k4rt"
platform-level domain structures
```

not merely transport dictionaries.

Benefits of `dataclass`:

* stronger structural meaning
* immutable contracts (`frozen=True`)
* easier future validation
* better architectural clarity
* clearer system boundaries
* easier contract evolution


## When `TypedDict` Is Still Useful

`TypedDict` remains useful at execution boundaries.

Example:

* Spark task payloads
* JSON serialization
* transport-oriented metadata

Recommended platform pattern:

```text id="6rkhrm"
core planning → dataclass contracts
execution boundary → convert to dictionaries
```

Example:

```python id="bpcn6e"
from dataclasses import asdict

spark_tasks = [
    asdict(task)
    for task in period_tasks
]
```

This preserves:

* strong platform contracts internally
* lightweight execution payloads externally


## Core Scope Planner Example

Example implementation:

```python id="fjh6yb"
from __future__ import annotations

import datetime as dt

from dataclasses import dataclass
from typing import Literal, Optional, Sequence


@dataclass(frozen=True)
class PeriodScopeTask:
    company: str
    dataset: str

    start: str
    end: str

    fiscal_year: int

    period_grain: Literal[
        "day",
        "month",
        "quarter",
        "year",
        "custom",
    ]


_LAST_DAY = {
    3: 31,
    6: 30,
    9: 30,
    12: 31,
}


def infer_fiscal_year(
    date_value: dt.date,
    fiscal_year_start_month: int = 10,
) -> int:

    if date_value.month >= fiscal_year_start_month:
        return date_value.year + 1

    return date_value.year


def create_quarter_scope_tasks(
    *,
    companies: Sequence[str],
    dataset: str,
    fiscal_years: Sequence[int],
    fiscal_year_start_month: int = 10,
) -> list[PeriodScopeTask]:

    tasks: list[PeriodScopeTask] = []

    quarter_start_months = (1, 4, 7, 10)

    for company in companies:

        for year in fiscal_years:

            for month in quarter_start_months:

                start = dt.date(year, month, 1)

                end_month = month + 2

                end = dt.date(
                    year,
                    end_month,
                    _LAST_DAY[end_month],
                )

                tasks.append(
                    PeriodScopeTask(
                        company=company,
                        dataset=dataset,

                        start=start.isoformat(),
                        end=end.isoformat(),

                        fiscal_year=infer_fiscal_year(
                            start,
                            fiscal_year_start_month,
                        ),

                        period_grain="quarter",
                    )
                )

    return tasks
```


## Layer 2 — Source Enrichment

After generic scope is created, the source layer adds source-specific meaning.

Example:

```python id="0l1h9d"
from dataclasses import dataclass


@dataclass(frozen=True)
class QboTask:
    scope: PeriodScopeTask

    source: str
    source_dataset: str

    minor_version: int
```

Example enrichment:

```python id="9krcjc"
def enrich_qbo_context(
    tasks: list[PeriodScopeTask],
    *,
    source_dataset: str,
    minor_version: int = 75,
) -> list[QboTask]:

    return [
        QboTask(
            scope=task,

            source="qbo",

            source_dataset=source_dataset,

            minor_version=minor_version,
        )
        for task in tasks
    ]
```


## Layer 3 — Operation Enrichment

Operations then enrich the source task with execution-specific metadata.

Examples:

* Bronze ingestion
* Silver flattening
* validation
* reconciliation

All operations reuse the same underlying scope.


## Bronze Ingestion Example

```python id="tv3u1w"
from dataclasses import dataclass


@dataclass(frozen=True)
class QboIngestionTask:
    qbo_task: QboTask

    operation: str
    layer: str

    out_path: str
```

Example enrichment:

```python id="q0y4ko"
def enrich_ingestion_context(
    tasks: list[QboTask],
    *,
    bronze_root: str,
) -> list[QboIngestionTask]:

    enriched_tasks: list[QboIngestionTask] = []

    for task in tasks:

        scope = task.scope

        year, month, _ = scope.start.split("-")

        out_path = (
            f"{bronze_root}/"
            f"QBO/"
            f"{scope.dataset}/"
            f"company={scope.company}/"
            f"year={year}/"
            f"month={month}/"
            f"raw.json"
        )

        enriched_tasks.append(
            QboIngestionTask(
                qbo_task=task,

                operation="ingest",
                layer="bronze",

                out_path=out_path,
            )
        )

    return enriched_tasks
```


## Silver Flatten Example

```python id="9b4r1m"
from dataclasses import dataclass


@dataclass(frozen=True)
class QboFlattenTask:
    qbo_task: QboTask

    operation: str
    layer: str

    in_path: str
    out_path: str
```

Example enrichment:

```python id="e4j1f9"
def enrich_flatten_context(
    tasks: list[QboTask],
    *,
    bronze_root: str,
    silver_root: str,
) -> list[QboFlattenTask]:

    enriched_tasks: list[QboFlattenTask] = []

    for task in tasks:

        scope = task.scope

        year, month, _ = scope.start.split("-")

        in_path = (
            f"{bronze_root}/"
            f"QBO/"
            f"{scope.dataset}/"
            f"company={scope.company}/"
            f"year={year}/"
            f"month={month}/"
            f"raw.json"
        )

        out_path = (
            f"{silver_root}/"
            f"QBO/"
            f"{scope.dataset}/"
            f"company={scope.company}/"
            f"fiscal_year={scope.fiscal_year}"
        )

        enriched_tasks.append(
            QboFlattenTask(
                qbo_task=task,

                operation="flatten",
                layer="silver",

                in_path=in_path,
                out_path=out_path,
            )
        )

    return enriched_tasks
```


## Full Planning Flow

The full planning flow becomes:

```python id="mx3rm7"
scope_tasks = create_quarter_scope_tasks(
    companies=companies,
    dataset="ProfitAndLossDetail",
    fiscal_years=[2025, 2026],
)

qbo_tasks = enrich_qbo_context(
    scope_tasks,
    source_dataset="ProfitAndLossDetail",
)

ingestion_tasks = enrich_ingestion_context(
    qbo_tasks,
    bronze_root="Bronze",
)

flatten_tasks = enrich_flatten_context(
    qbo_tasks,
    bronze_root="Bronze",
    silver_root="Silver",
)
```


## Why This Architecture Matters

This architecture creates:

* reusable planning logic
* coherent workload boundaries
* deterministic execution scope
* easier Spark distribution
* easier retries
* easier orchestration
* lower maintenance cost
* cleaner system boundaries

Most importantly:

```text id="9g7sj4"
the platform gains a unified workload language
```

where ingestion, flattening, validation, reconciliation, and future systems all operate on the same foundational scope model.


## Design Philosophy

The Controlled Task Planner exists because:

```text id="0gvjma"
workload scope is a platform invariant
not an operation-specific implementation detail
```

Therefore:

```text id="pv3xkz"
scope planning belongs in platform core
execution semantics belong downstream
```

The platform should therefore:

```text id="k3vjn6"
centralize workload planning
specialize execution later
```

This creates a scalable orchestration foundation for the entire platform.
