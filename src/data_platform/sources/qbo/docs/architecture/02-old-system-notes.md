# # QBO Raw Table Flattening — Future Reference

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

