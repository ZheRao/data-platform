# Validator — Structural Enforcement Layer


## 1. Purpose

The Validator layer enforces structural correctness of semi-structured data before it is interpreted, transformed, or persisted.

Its role is not to “handle errors”, but to:  
**make invalid structure unrepresentable as valid execution**


## 2. Core Principle

> All external data is untrusted.  
> Every assumption must be enforced explicitly.

No downstream logic is allowed to rely on:
- implicit structure
- optional presence
- “usually correct” formats

## 3. System Invariant

> A pipeline may only proceed on data that has passed full structural validation.

If validation fails:
- execution must stop immediately
- failure must be local and diagnosable

## 4. Fail-Loud Doctrine

The system follows a strict rule:
> It is always better to crash early than to continue incorrectly.

This prevents:
- silent data loss
- silent misclassification
- incorrect aggregations
- downstream corruption

## 5. Validation as a Multi-Gate System

Validation is not a single check — it is a sequence of enforced gates.

### Gate 1 — Presence (Existence)

All required keys/fields must exist.
- Missing required structure → invalid
- No fallback allowed

**Failure** → `KeyError`

### Gate 2 — Shape (Type)

All fields must match expected structural types.

Examples:
- dict vs list
- list vs scalar

**Failure** → `TypeError`

### Gate 3 — Content (Validity)

Values must be meaningful and non-empty where required.

Examples:
- empty lists where data is expected
- missing identifiers

**Failure** → `ValueError`

### Gate 4 — Structural Pattern Matching

Data must match one of the explicitly defined structural patterns.

Examples:
- node classification
- record formats
- nested object shapes

**Failure** → **reject structure**

### Gate 5 — Classification Closure

Every input must map to a known category.
> No “default”, no “fallback”, no “unknown-but-continue”.

**Failure** → **explicit error**

## 6. Boundary of Responsibility

Validation occurs at **system boundaries**, not inside business logic.

**Must validate**:
- external API responses
- raw Bronze ingestion
- schema transitions (Bronze → Silver)
**Must NOT defer validation to**:
- aggregation logic
- transformation layers
- downstream consumers