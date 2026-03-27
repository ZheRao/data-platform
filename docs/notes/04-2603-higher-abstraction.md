# Truly Engine Agnostic

## Current
for a `flatten` job 
- define `flatten_spark()` & `flatten_pandas()`
- route appropriate engine based on argument

## Improvement
explicitly define
- `engine` class with shared data ops
    - `engine.read()`
    - `engine.write()`

# Proposed idea — for future

Yes — and your second question gets to the heart of it:
> Build the infrastructure first with plain modules/functions.  
> Do not rush into classes for elegance.

That is my recommendation.

You’re at the stage where the bigger risk is **abstracting the wrong boundary**, not “being insufficiently elegant.”

## The difference between your proposal and mine

At first glance they sound similar:

**Your idea**
```python
engine.read()
engine.extract()
engine.flatten()
```
**My suggestion**
```python
engine.read_json()
engine.write_parquet()
engine.explode()
qbo.flatten_pl_reports(engine, ...)
```
They both use an engine, so why do I think one is cleaner?

Because they assign responsibility differently.

## Core distinction: who decides what to do?

### **In your version**

The `engine` is asked to do:
- reading
- extracting
- flattening

That means the `engine` starts owning **workflow meaning**.

But “flatten” is not a neutral compute primitive.

It implies:
- flatten which source?
- flatten what structure?
- flatten according to which rules?

So now the `engine` must know about QBO.

That creates this shape:
```
engine
 ├── read()
 ├── extract()
 ├── flatten_qbo()
 ├── flatten_harvest_profit()
 ├── flatten_xyz_source()
```
So the engine stops being “Pandas vs Spark” and becomes:
> a giant mixed object containing both execution and source knowledge

That is the problem.

---
### **In my version**

The engine only owns things like:
- read file
- write file
- explode rows
- union frames
- scoped overwrite

These are **execution primitives**.

Then the QBO layer says:
> “I need to flatten a QBO PL report. To do that, I will use engine primitives.”

So the ownership is:
- **QBO layer** knows *what flattening means*
- **engine layer** knows *how to execute operations*

That is the real difference.

## A concrete analogy

Think of a kitchen.

**Your design**

You create a tool called `KitchenEngine` with methods:
- `boil_pasta()`
- `make_sushi()`
- `bake_cake()`

Looks convenient, but now the kitchen engine contains the knowledge of every dish.

**Cleaner design**

You have:
- stove
- oven
- knife
- mixer

Then the recipe decides how to combine them.

In this analogy:
- engine = stove/oven/knife
- QBO flatten logic = recipe

You do not want your stove to know how to make sushi.

# Why `flatten()` is dangerous as an engine method

Because `flatten()` sounds generic, but it is not.

There is no single universal flatten.

Examples:
- flatten nested JSON list
- flatten QBO PL rows
- flatten hierarchical report headers
- flatten GraphQL response edges/nodes

All of those are different.

So if you put `flatten()` on `engine`, one of two things happens:

Option 1
- It becomes so generic that it is useless.

Option 2
- It becomes source-aware, and now the engine is polluted.

Neither is good.

## What belongs in the engine?

Only methods that still make sense without knowing QBO exists.

Good engine methods:
```python
engine.read_json(path)
engine.write_parquet(df, path)
engine.explode(df, column)
engine.select(df, cols)
engine.rename(df, mapping)
engine.union_all(dfs)
engine.repartition(df, cols)
engine.scoped_overwrite(df, path, scope)
```
These are backend operations.

A Spark engine and Pandas engine can each implement them differently.

## What belongs in the source layer?

Methods whose meaning depends on the source structure.

Examples:
```python
flatten_qbo_pl_report(engine, raw_df, config)
extract_qbo_entities(auth, config)
normalize_qbo_report_columns(engine, df, schema)
```
These are not generic compute methods.  
These are source workflows.

## Why this matters for your invariants

You already care about:
- engine agnosticism
- source isolation
- structure vs meaning separation

If `engine` starts owning `flatten_qbo_pl_report`, then you violate all three:

### 1. Engine agnosticism gets muddy

Because the engine now includes source-specific behavior.

### 2. Source isolation breaks

Because QBO knowledge leaks into a shared engine abstraction.

### 3. Structure vs meaning gets riskier

Because the wrong layer starts accumulating workflow logic.

## Now your second question: should you use functions first?

My answer: **yes, strongly yes**.

For where you are right now, the best progression is:

### Stage 1 — plain modules + functions

Use simple boundaries and learn the real shapes.

Example:
```python
# core/engine_ops.py
def read_json_pandas(...): ...
def read_json_spark(...): ...

# sources/qbo/transformation/pl.py
def flatten_pl_reports(engine_name, ...): ...
```
or slightly cleaner:
```python
def get_engine(name): ...
engine = get_engine("spark")
flatten_pl_reports(engine, ...)
```
This lets you discover:
- what operations are truly shared
- what is source-specific
- what differs by engine
- what abstractions are fake

That discovery matters more than elegance right now.

### Stage 2 — extract stable interfaces

After repetition, you’ll notice:
- these 6 operations always recur
- these 3 operations are really Spark-only
- these source transforms are reusable

Only then should you formalize.

### Stage 3 — optional classes

If the interface has become stable, then classes can help.

But only after the abstraction has been earned.

## Why classes too early are dangerous

Because classes make bad abstractions feel “official.”

You might create:
```python
class Engine:
    def read(self): ...
    def flatten(self): ...
    def extract(self): ...
```
It looks elegant, but if the boundary is wrong, now your whole repo bends around a mistaken design.

Functions are much cheaper to change.

That’s why early-stage system design often benefits from:
> functional looseness before object-oriented rigidity

## What I would recommend concretely

### Near-term

Use this shape:
```
core/
    engine/
        pandas_ops.py
        spark_ops.py
        factory.py

sources/
    qbo/
        ingestion/
        transformation/
            pl.py
```
Then:
```python
from data_platform.core.engine.factory import get_engine
from data_platform.sources.qbo.transformation.pl import flatten_pl_reports

engine = get_engine(engine_name="spark", ...)
flatten_pl_reports(engine=engine, input_path=..., output_path=..., config=...)
```
Where `engine` is just a small adapter object or even a namespace of functions.

### Inside `flatten_pl_reports`

Do things like:
```python
raw = engine.read_json(input_path)
schema = discover_schema(raw, ...)
flat = flatten_rows(engine, raw, schema)
engine.write_parquet(flat, output_path, ...)
```
Notice:
- QBO still owns the workflow
- engine only provides primitives

## A good test for abstraction

Before you add a method to engine, ask:
> “Would this method still make sense for any source system?”

If yes, maybe engine.

If no, source layer.

Examples:
- `read_json` → yes
- `scoped_overwrite` → yes
- `flatten_profit_and_loss_report` → no
- `extract_qbo_api_pages` → no

That test will save you a lot of pain.

## Blunt recommendation

Right now, do not optimize for elegance.

Optimize for:
- correct boundaries
- low-cost refactoring
- learning what repeats

So:
> functions first, classes later

That is the safer and smarter move for `data-platform` at this stage.

## Practical heuristic

Use classes only when one of these becomes clearly true:
1. You need persistent shared state across many operations
2. Multiple functions always travel together as one conceptual unit
3. The interface has stabilized across repeated use

If not, prefer modules + functions.

