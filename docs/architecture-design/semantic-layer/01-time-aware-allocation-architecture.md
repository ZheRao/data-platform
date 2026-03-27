# Cattle Operation Split — Architectural Invariants & Design

## 1. Problem Context

Cattle operations consist of two distinct modes:

* **Feedlot**
* **Cow/Calf**

However, transactional data from QBO:

* Contains a sparse and unreliable `Class` field
* Does not consistently encode operation mode
* Cannot be directly used to separate profitability by operation

### Objective

Infer **operation-level profitability** by:

* Using explicit labels when available
* Falling back to **inventory-based proportional inference** when labels are missing



## 2. Core Challenge

The system must:

1. Be **location-aware**
   → Only certain locations require split logic

2. Be **time-aware**
   → Inventory data exists only for certain periods

3. Maintain **scalability**
   → Avoid per-row iteration and repeated computation

4. Preserve **data truth**
   → Never overwrite or destroy original transactions



## 3. Naive Approach (and Why It Fails)

### Row-wise Logic

For each transaction:

1. Check location
2. Check class label
3. If unclear → infer using inventory
4. Compute percentages (possibly repeatedly)
5. Split transaction into multiple rows

### Failure Modes

* ❌ **O(N) row iteration** → slow at scale
* ❌ **Recomputing percentages repeatedly**
* ❌ **Implicit time logic** → hard to reason about correctness
* ❌ **Destructive transformation** → original data lost
* ❌ **Unclear system boundaries** → tightly coupled logic



## 4. Architectural Principles (Extracted Invariants)

### Invariant 1 — Separate *Reference Computation* from *Application*

* Inventory → compute once → store as **reference table**
* Transactions → only *consume* reference

> **Insight:** Expensive logic should be lifted out of the main data flow



### Invariant 2 — Replace Row-wise Logic with Set-based Transformations

* Use vectorized operations (`groupby().transform`)
* Use joins instead of lookups

> **Insight:** Treat logic as **data alignment**, not procedural steps



### Invariant 3 — Make Time Explicit via Keys

* Introduce `year_month_key`
* Align all systems on this shared temporal index

> **Insight:** Time-aware systems require **explicit temporal indexing**, not implicit comparisons



### Invariant 4 — Preserve Truth via Non-destructive Augmentation

* Original transaction remains intact
* Derived rows are **additive**, not replacements

> **Insight:** Downstream systems should be **reversible and auditable**



### Invariant 5 — Encode Logic as Data, Not Branching

* Percentages stored as rows
* Split logic becomes a **merge + multiply**

> **Insight:** Systems scale when logic becomes **data-driven**



### Invariant 6 — Bounded Expansion Instead of Conditional Mutation

* Each transaction expands into `(n + 1)` rows:

  * `n` operation splits
  * `1` offset row

> **Insight:** Prefer **structured expansion** over conditional mutation



## 5. Final Architecture



### Step 1 — Inventory Reference Table (Precomputation Layer)

#### Goal

Compute operation split percentages per location and time

#### Method

1. Create:

   * `master_location`
   * `year_month_key`

2. Compute totals using vectorization:

```python
df["total_inventory"] = df.groupby(
    ["master_location", "year_month"]
)["inventory"].transform("sum")
```

3. Compute percentage:

```python
df["perc"] = df["inventory"] / df["total_inventory"]
```

4. Output mapping table:

```
[location, year_month_key, operation_mode, perc]
```

#### Result

* Fully vectorized
* No repeated computation
* Reusable across all transactions



### Step 2 — Transaction Preparation (Filtering Layer)

1. Filter relevant locations
2. Identify labeled transactions:

```python
df["Class"].str.contains(r"(?:\bFeedlot\b|\bcow\/calf\b)", case=False, na=False)
```

3. Create:

```python
year_month_key
```

4. Filter valid time range using:

* min / max from inventory table



### Step 3 — Split Expansion (Core Mechanism)

#### Key Idea

Instead of conditional logic → **expand dataset**

For each transaction:

* Create `(n + 1)` copies:

  * `n` → operation splits
  * `1` → offset row



### Step 4 — Percentage Injection via Join

Merge:

```
transactions ⨝ inventory_mapping
ON [location, year_month_key]
```

* Each copy receives its corresponding percentage
* Offset row receives `perc = -1`



### Step 5 — Amount Transformation

Apply:

```python
df["amount"] = df["amount"] * df["perc"]
```



### Step 6 — Truth Preservation & Lineage

Add metadata columns:

* `record_type`:

  * `original`
  * `synthetic_split`
  * `synthetic_offset`

* `source`:

  * e.g. `"2025-01_cattle_inventory"`

#### Result

* Original data is recoverable
* All transformations are traceable



## 6. System Properties

### Performance

* ✅ Fully vectorized
* ✅ No row iteration
* ✅ No repeated computation



### Scalability

* Linear scaling with dataset size
* No combinatorial branching



### Auditability

* Original transactions preserved
* Synthetic records clearly labeled



### Extensibility

* Additional operation modes → trivial extension
* New time granularity → replace key
* Alternative reference logic → swap mapping table



## 7. Architectural Insight (Why This Matters)

This system is not just a solution—it demonstrates a broader pattern:

> **Transform messy, conditional logic into clean, composable data flows**

Instead of:

* "If this, then that"

You get:

* "Align datasets → apply transformation → preserve lineage"



## 8. Reusable Pattern (Template)

This architecture generalizes to:

* Cost allocation systems
* Revenue attribution
* Probabilistic classification
* Any **partial-information inference problem**



## 9. Final Reflection

This design achieves:

* Separation of concerns (reference vs application)
* Declarative computation (joins instead of logic)
* Temporal correctness (explicit keys)
* Data integrity (non-destructive design)

> **This is not a transformation script — it is a system.**


