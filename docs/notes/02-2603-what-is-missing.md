### 1. Observability (currently missing)
You *must* eventually include:
- logging structure
- failure tracking
- data validation checks

Otherwise:
> “Works in dev, unknown in prod”

---

### 2. Data contracts enforcement (not just configs)
Right now:
- contracts exist (good)
- but not *validated* (danger)

Future:
- schema validation
- invariants checking
- failure on violation

---

### 3. Incrementality / idempotency
You touched it (state configs), but:

- can jobs rerun safely?
- can partial failures recover?

This is where systems become **production-grade**
