
# Purpose

This module classifies nodes from QBO nested JSON into explicit structural types to enable deterministic traversal and data extraction.

The classification is **strict and fail-loud by design**:
- Any unexpected structure must raise immediately
- Silent tolerance is explicitly disallowed

# Core Principle

- Every node MUST map to exactly one known type.  
- Unknown or ambiguous structures MUST raise an error.

This prevents:
- silent data loss
- silent misclassification
- hidden schema drift from QBO

# Node Types

The system recognizes the following **closed set of node types**:
- `Data`
- `Summary Only`
- `Category End`
- `Include Data For Parent`
- `Category`
- `Account`
- `Unspecified Account`


# Structural Patterns

### 1. `Data`

Leaf node containing actual transaction data.

Must satisfy:
- `type == "Data"`
- `ColData` exists
- `ColData` is a non-empty `list`

### 2. `Summary Only`

Terminal node with summary but no structure for traversal.

Must satisfy:
- `type == "Section"`
- no `Header`
- no `Rows`
- `Summary` exists

### 3. `Category End`

Marks the end of a category branch.

Must satisfy:
- no `Header`
- `Rows` exists but is empty:
    - either `{}`
    - or `{"Row": []}`

### 4. `Include Data For Parent`

Represents transaction data belonging to the parent account, without its own header.

Must satisfy:
- no `Header`
- `Rows` exists and is non-empty
- `Rows["Row"]` is a non-empty `list`
- `Summary` exists

### 5. `Category`

Structural grouping node.

Must satisfy:
- `Header` exists
- `Header.ColData[0].id` is **missing**
- `Rows["Row"]` is non-empty

### 6. `Unspecified Account`

Node representing a "Not Specified" account group

Must satisfy:
- `Header` exists
- `Header.ColData[0].value` exists and equals `Not Specified`
- `Rows["Row"]` is non-empty

### 7. `Account`

Node representing a specific account.

Must satisfy:
- `Header` exists
- `Header.ColData[0].id` exists
- `Rows["Row"]` is non-empty

# Validation Model (Fail-Loud Ladder)

All nodes are validated using a strict sequence:

### Gate 1 — Presence

Required keys must exist  
→ else `KeyError`

### Gate 2 — Type

Values must match expected types  
→ else `TypeError`

### Gate 3 — Content

Values must be non-empty / valid  
→ else `ValueError`

### Gate 4 — Classification Closure

Node must match exactly one type  
→ else `ValueError`

# Why This Strictness Exists

QBO JSON is:
- externally controlled
- undocumented in detail
- subject to change

Without strict validation:
- schema drift becomes silent
- incorrect results propagate undetected

Therefore:
- This system is designed to **fail early, fail loudly, and fail locally**.





# Appendix: Row Notes about Observed Nested JSON structure

`...["Rows"]["Row"]` contains a list of dictionaries for nested and layered records

scenario 1: category summary contains `['Header', 'Rows', 'Summary', 'type']`
- `Header` contains category such as `{'ColData': [{'value': 'Ordinary Income/Expenses'},...]}`
    - (dictionary with 1 key and list of dictionaries as value)
- `Summary` contains category summary such as `{'ColData': [{'value': 'Net Ordinary Income'},...,{'value': '-36422501.56'},...]}`
    - (dictionary with 1 key and list of dictionaries as value)
    - value appeared in location of `Amount` column
- `type` is 'Section'
    - (string value)
- `...["Rows"]["Row"]` contains next-level records
    - (list of dictionaries)

scenario 2: category summary contins `['Rows', 'Summary', 'type']` with no `Header`
- `Summary` contains category summary such as `{'ColData': [{'value': 'Gross Profit'}, ..., {'value': '79982146'}, ... ]}`
    - (dictionary with 1 key and list of dictionaries as value)
- `type` is 'Section'
    - (string value)
- `...["Rows"]` is just `{}`
    - (empty dictionary)

scenario 6: summary only node with `['Summary', 'type']`

scenario 3: account contains `['Header', 'Rows', 'Summary', 'type']`
- `Header` contains `AccNum` + `AccID` such as `{'ColData': [{'value': '490300 Hay Bale Sales', 'id': '245'},...]}`
    - (dictionary with 1 key and list of dictionaries as value)
- `Summary` contains summary for that account
- `type` is 'Section'

scenario 4: data records contains `['ColData', 'type']`
- `type` is 'Data'
- `ColData` contains a list of dictionaries such as `[{'value': '2025-12-03'},{'value': 'Invoice', 'id': '108186'}, ... ]`

scenario 5: account contains `['Rows', 'Summary', 'type']` with no `Header`
- this situation happens when the account information is inherited from the prior node,  
so the transactions for the parent account and transactions for the subsequent child accounts are embeded into one list

example: a parent account

```python
obj_1["type"] # Section
obj_1["Header"]
```
output:
```text
{'ColData': [{'value': '495200 Inventory Adjustment - Cattle', 'id': '689'},
  {'value': ''},
  {'value': ''},
  {'value': ''},
  {'value': ''},
  {'value': ''},
  {'value': ''},
  {'value': ''},
  {'value': ''},
  {'value': ''}]}
```

The transactions of the parent account doesn't appear directly next in `obj_1["Rows"]["Row"][0]`

Instead, the transactions are in `obj_1["Rows"]["Row"][0]["Rows"]["Row"]` while at that level, there's no header, so `obj_1["Rows"]["Row"][0]["Header"]` doesn't exist because it would be the same
as the `obj_1["Header"]`

And `obj_1["Rows"]["Row"][1]` is the first child account node with `obj_1["Rows"]["Row"][1]["Header"]` and transactions in `obj_1["Rows"]["Row"][1]["Rows"]["Row"]`

scenario 7: account node with the name "Not Specified" and no "id" followed by transactions

