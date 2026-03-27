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