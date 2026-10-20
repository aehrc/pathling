# 14 — Date range annotation

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-004, FR-021, FR-022, FR-023. Old tasks: T053, T065.

**What to build:** Dates carry the specification's range annotations, reflecting the stated precision, and the engine uses them as a fast path for date comparison where the schema carries them.

**Blocked by:** 11

**Status:** ready-for-agent

- [ ] Tests show the range annotations for each precision, from year to full date-time, with and without a time zone (T053).
- [ ] Date comparison with the annotations gives the same results as without them. With the annotation disabled, the suite passes.
