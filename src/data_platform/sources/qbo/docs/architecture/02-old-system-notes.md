# QBO Raw Table Flattening — Future Reference

## Purpose

This raw transformation converts QBO JSON exports into flatter CSV tables for downstream reporting.

The main job is:

1. Read raw QBO JSON per company/corp.
2. Flatten nested dictionaries into underscore-separated columns.
3. For fact tables, explode `Line[]` into one row per meaningful line item.
4. Add company prefixes to IDs.
5. Rename columns into the internal reporting schema.
6. Save processed Silver Raw / Dimension CSV files.


## Core Rule: Dicts Flatten, Lists Need `record_path`

`pd.json_normalize(..., sep="_")` flattens nested dictionaries:

```text
VendorRef.value → VendorRef_value
MetaData.CreateTime → MetaData_CreateTime
```

But it does not automatically explode lists.

Lists such as:
```
Line[]
LinkedTxn[]
```
must be explicitly normalized using record_path.

## Stage 1: Header-Level Flattening

Inside `_raw_read_jsons(...)`, each raw JSON file is first read and flattened:
```py
df = pd.json_normalize(df.to_dict(orient="records"), sep="_")
```
This creates one row per QBO transaction/object.

At this stage:
```
Nested dicts → flattened columns
Line[]       → still a list column
LinkedTxn[]  → still a list column
```
## Stage 2: Fact Line-Item Expansion

If `is_fact=True`, the pipeline explodes `Line[]`.

Before exploding, `_raw_get_lineitem(...)` filters out non-financial or summary lines:
```
skip:
- DescriptionOnly
- SubTotalLineDetail

keep:
- lines with Id
- lines whose DetailType is not skipped
```
Then `Line[]` is normalized:
```py
pd.json_normalize(
    df.to_dict(orient="records"),
    record_path=["Line"],
    meta=[c for c in first_cols if c != "Line" and c in df.columns],
    record_prefix="Line_",
    sep="_",
    errors="ignore"
)
```
This means:
```
Line[] becomes rows
nested dicts inside each Line item become columns
parent transaction fields are copied from meta
line-derived columns receive prefix Line_
```
Example:
```
Line.AccountBasedExpenseLineDetail.AccountRef.value
→ Line_AccountBasedExpenseLineDetail_AccountRef_value
```
Important invariant:
```
Do not include "Line" in meta.
```
Otherwise the full original `Line[]` list gets copied onto every exploded row, wasting memory.

## Fact Table Grain

After line expansion, fact tables are line-level.

The internal `TransactionID` is built as:
```py
TransactionID = df_type[0] + "-" + Id + "-" + Line_Id
```
Example:
```
B-123-1
```
Meaning:
```
transaction type prefix - QBO transaction id - QBO line id
```
The original transaction-level ID is renamed to:
```
TransactionID_partial
```
So:
```
TransactionID_partial = source transaction/header ID
TransactionID         = unique line-level ID
```

## LinkedTxn Handling

`LinkedTxn` is not exploded during line-item flattening.

That is because the line expansion uses:
```py
record_path=["Line"]
```
not:
```py
record_path=["LinkedTxn"]
```
So `LinkedTxn[]` is handled separately in `_raw_construct_LinkedTxn_table(...)`.

That function:

1. Keeps:
    ```
    TransactionID
    LinkedTxn
    AccID
    Corp
    ```

2. Strips the line suffix from `TransactionID`:
    ```py
    TransactionID = TransactionID.split("-")[1]
    ```

3. Explodes LinkedTxn[]:
    ```py
    pd.json_normalize(
        ...,
        record_path=["LinkedTxn"],
        meta=["TransactionID", "AccID", "Corp"],
        sep="_"
    )
    ```

4. Creates corp-prefixed linked transaction ID:
    ```py
    TxnId = Corp + TxnId
    ```

5. Saves a mapping CSV under:
    ```
    Silver/QBO/Raw/LinkedTxn
    ```

Key limitation:
```
Old LinkedTxn mapping is transaction-level / account-level, not full line-breakdown-level.
```

If downstream logic uses:
```py
drop_duplicates(subset=["TxnId"], keep="first")
```
then only the first `AccID` survives per linked transaction. This is lossy for expanded banking-report needs.

## ID Prefixing Rule

`concat_cols` controls which ID columns receive the corp prefix.

Example:
```
Id → MFL123
CustomerRef_value → MFL456
```

For `DocNumber`, a hyphen is inserted:
```
DocNumber → MFL-12345
```
Special case:
```py
if 'LinkedTxn' in concat_cols:
    concat_cols.remove('LinkedTxn')
```
because the raw `LinkedTxn` list itself should not be prefixed directly. Only flattened linked transaction IDs should be prefixed.

## Dimension vs Fact Output

`_raw_processing_combined(...)` handles final shaping.

### Fact tables

Fact tables are saved to:
```
Silver/QBO/Raw
```
They are line-level after `process_line=True`.

Examples:
```
Invoice
SalesReceipt
Bill
Purchase
JournalEntry
VendorCredit
Deposit
CreditMemo
```

### Dimension tables

Dimension tables are saved to both:
```
Silver/QBO/Dimension_time
Silver/QBO/Dimension/CSV
```
Examples:
```
Account
Item
Class
Farm
Customer
Vendor
Term
```

## Account Handling

For `Invoice` and `SalesReceipt`, `AccID` is not directly on the line. It is derived through `ItemID`:
```
ItemID → IncomeAccountID → AccID
```
For item-based `Bill` lines:
```
ItemID → ExpenseAccountID → AccID
```
For account-based bill/purchase/vendor-credit/deposit lines, `AccID` usually comes directly from the line detail.

## Entity Handling

If a table has:
```
EntityType
EntityID
```
then `_raw_sep_entity(...)` splits it into one of:
```
VendorID
CustomerID
EmployeeID
```
Then the generic entity columns are dropped.

## Important Current Limitation

The existing pipeline was built as a coupled raw-to-silver transformation.

It works, but several responsibilities are mixed together:
```
reading files
flattening JSON
line filtering
ID prefixing
schema selection
renaming
account derivation
linked transaction extraction
saving outputs
```
This makes modification risky.

For the weekly banking report expansion, prefer adding a downstream mapping/breakdown layer first instead of rewriting ingestion.

## Safe Future Rule

When adding new requirements:
```
1. Check whether the needed columns already exist in Silver Raw.
2. If yes, build downstream derived logic.
3. If no, minimally expose missing columns in ingestion.
4. Avoid changing existing table grain unless explicitly migrating dependents.
5. Keep old LinkedTxn mapping stable if existing reports depend on it.
```

## Mental Model

The pipeline is doing this:
```
Raw QBO JSON
    ↓
Header flattening
    ↓
Optional Line[] explosion
    ↓
Line-level fact table creation
    ↓
Optional LinkedTxn[] mapping extraction
    ↓
Column renaming / ID prefixing / account enrichment
    ↓
Silver CSV outputs
```
Most important distinction:
```
Line[] = transaction accounting breakdown
LinkedTxn[] = relationship to another transaction/payment/check/etc.
```
They are separate child structures and should usually be normalized separately.

# QBO Ingestion Refactor — Separation of Concerns

## Problem

Current ingestion functions fuse several responsibilities:

- HTTP request execution
- QBO-specific URL/header/parameter construction
- report date slicing
- raw table pagination
- response parsing
- output path construction
- JSON writing
- logging

This makes the system harder to test, harder to distribute, and harder to reuse across Pandas/Spark engines.

## Target Design

Separate ingestion into independent mechanisms:

1. **Task planning**
   - Produces stable, explicit ingestion tasks.
   - Examples:
     - report/date slice tasks
     - raw table/page tasks

2. **Request building**
   - Converts a task into URL, headers, and params.
   - QBO-specific logic lives here.

3. **HTTP execution**
   - Receives only stable inputs:
     - URL
     - headers
     - params
     - method
   - Performs request.
   - Returns response payload or response envelope.

4. **Response validation**
   - Checks status code.
   - Checks expected response shape.
   - Handles empty responses.

5. **Bronze writing**
   - Writes raw response payload to the correct path.
   - Uses atomic writes.
   - Does not know QBO API mechanics.

6. **Manifest/logging**
   - Records task metadata:
     - company
     - dataset
     - slice
     - row count if available
     - output path
     - success/failure

# Date Scope for Raw Tables

You’re **partly right**.

For QBO raw/entity tables, you usually *can* add date filters inside the query, but it is not the same as PL/GL report parameters.

For reports, QBO uses params like:

```python
params = {
    "start_date": "...",
    "end_date": "...",
}
```

For raw/entity query API, date slicing would usually be inside the SQL-like query string, for example:

```sql
SELECT * FROM Invoice
WHERE MetaData.LastUpdatedTime >= '2025-01-01'
AND MetaData.LastUpdatedTime < '2025-04-01'
STARTPOSITION 1
MAXRESULTS 100
```

QBO’s query API supports SQL-like queries with `STARTPOSITION` and `MAXRESULTS`, while Intuit also recommends CDC for tracking changed entities over time. ([Intuit Developer][1])

So the shape becomes:

```text
raw table slice = table + date window + page
```

Example:

```text
Invoice | 2025-01-01 to 2025-03-31 | start_position=1
Invoice | 2025-01-01 to 2025-03-31 | start_position=101
```

But the warning is important:

```text
Not every raw table should be sliced the same way.
```

Some tables are “current-state dimension tables”:

```text
Account, Customer, Vendor, Item, Class, Department
```

For those, you may want full snapshots because inactive/deleted/state changes matter.

Some are transaction-like:

```text
Invoice, Bill, Payment, JournalEntry, Purchase, Deposit
```

For those, date slicing or `MetaData.LastUpdatedTime` slicing makes more sense.

Best design:

```text
RawEntityPlan
  - table_name
  - extraction_mode:
      full_snapshot
      updated_since_window
      transaction_date_window
  - date_field:
      MetaData.LastUpdatedTime
      TxnDate
      DueDate
      etc.
  - pagination:
      STARTPOSITION / MAXRESULTS
```

So no, you are not wrong. But don’t globally assume:

```text
all QBO raw tables have start_date/end_date params
```

Better assumption:

```text
some QBO raw tables can be converted into independently planned date-window + page tasks
```

Your architecture should allow that without forcing it everywhere.


## pagination and date details
Yes — exactly.

For fact/transaction tables, you want a config like:

```yaml
Invoice:
  extraction_mode: date_window
  date_field: TxnDate
  pagination: true
  max_results: 100

Bill:
  extraction_mode: date_window
  date_field: TxnDate
  pagination: true
  max_results: 100

Payment:
  extraction_mode: date_window
  date_field: TxnDate
  pagination: true
  max_results: 100

Account:
  extraction_mode: full_snapshot
  pagination: false

Vendor:
  extraction_mode: full_snapshot
  pagination: false
```

But one refinement: you may want **two possible date meanings**:

```text
TxnDate = business/accounting transaction date
MetaData.LastUpdatedTime = system change date
```

Use:

```text
TxnDate
```

when rebuilding historical business activity.

Use:

```text
MetaData.LastUpdatedTime
```

when doing incremental refreshes / “what changed since last run.”

QBO’s query API supports SQL-like `WHERE` clauses, `AND`, `STARTPOSITION`, and `MAXRESULTS`, but not full SQL behavior like joins, group by, or OR clauses. The exact filterable fields can vary by entity, so your config should be explicit per table rather than assuming every entity behaves the same. ([Intuit Developer][1])

So your model should be:

```yaml
Invoice:
  entity_type: fact
  extraction_mode: transaction_date_window
  date_field: TxnDate
  incremental_field: MetaData.LastUpdatedTime
  pagination:
    enabled: true
    max_results: 100
```

Then query builder does:

```python
query = f"""
SELECT * FROM {table}
WHERE {date_field} >= '{start_date}'
AND {date_field} < '{end_date}'
STARTPOSITION {start}
MAXRESULTS {max_results}
"""
```

Storage should also differ:

```text
dimension snapshot:
Bronze/QBO/Account/company=ABC/extracted_at=2026-05-09/account.json

fact slices:
Bronze/QBO/Invoice/company=ABC/year=2025/month=01/page=001.json
```

Best invariant:

```text
The table config decides the extraction semantics.
The engine only executes explicit tasks.
```

That is the correct direction.

## Ingestion Evolution

Yes — that is the right evolution.

Your stages should be:

```text
Stage 1: scoped full pulls
Stage 2: incremental pulls
Stage 3: CDC-style change capture / merge semantics
```

For now, **scoped pulls are enough** and much safer.

Example first-iteration design:

```text
PL / GL:
- pull by quarter or month
- overwrite that slice atomically
- path includes company + report + period

Fact raw tables:
- pull by TxnDate window
- paginate inside each date window
- write each page/slice separately

Dimension raw tables:
- full snapshot
- write snapshot with extracted_at timestamp
```

Then later:

```text
Incremental mode:
- read last successful ingestion timestamp
- query records where MetaData.LastUpdatedTime > last_successful_run
- write incremental raw payload
- merge/deduplicate downstream by entity Id + latest MetaData.LastUpdatedTime
```

The dedup key should usually be:

```text
entity_id + latest MetaData.LastUpdatedTime
```

Conceptually:

```python
window_spec = Window.partitionBy("Id").orderBy(col("MetaData.LastUpdatedTime").desc())

latest = (
    df
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") == 1)
    .drop("rank")
)
```

But one important correction:

```text
Do not deduplicate in Bronze.
```

Bronze should preserve raw truth.

Deduplicate in Silver.

So:

```text
Bronze:
- raw scoped pulls
- raw incremental pulls
- immutable-ish payloads
- ingestion metadata

Silver:
- parse JSON
- flatten structure
- deduplicate by Id + LastUpdatedTime
- enforce schema

Gold:
- business meaning
- reporting tables
```

Your current instinct is excellent:

```text
First iteration = reduce reload blast radius.
Future iteration = true incremental ingestion.
```

I would phrase the roadmap like this:

```md
## Ingestion Evolution Roadmap

### Phase 1 — Scoped Pulls

Replace brute-force full reloads with bounded extraction scopes.

- Reports: pull PL/GL by fiscal period.
- Fact entities: pull by configured transaction date window.
- Dimension entities: pull full snapshots.
- Pagination happens inside each scoped task.
- Atomic writes preserve slice-level consistency.

Goal: reduce refresh cost and isolate failures without introducing complex merge semantics.

### Phase 2 — Incremental Pulls

Add incremental extraction based on `MetaData.LastUpdatedTime`.

- Store last successful ingestion timestamp per company/source/entity.
- Pull records updated after that timestamp.
- Write incremental raw payloads to Bronze.
- Preserve all raw responses.

Goal: avoid re-pulling unchanged records.

### Phase 3 — Silver Deduplication / Merge

Deduplicate parsed records downstream.

- Partition by entity `Id`.
- Order by `MetaData.LastUpdatedTime` descending.
- Keep latest version.
- Preserve historical raw payloads in Bronze.

Goal: maintain clean current-state Silver tables while keeping Bronze auditable.
```

This is the right tradeoff.

Do **not** jump straight to incremental. Scoped pulls give you 70% of the benefit with much less semantic risk.



