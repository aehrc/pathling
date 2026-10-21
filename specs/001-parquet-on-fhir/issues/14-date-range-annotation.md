# 14 — Date range annotation

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-004, FR-021, FR-022, FR-023. Old tasks: T053, T065.

**What to build:** Dates carry the specification's range annotations, reflecting the stated precision, and the engine uses them as a fast path for date comparison where the schema carries them.

**A time without a timezone (owner, 2026-10-21).** A `dateTime` or `instant` with a time but no timezone is valid (decision 133's addendum). Its range annotation is the range the engine computes for it today, through `FhirPathDateTime`'s boundaries, so the annotation and the computed path agree. Whether that range should widen by ±14 hours is a follow-up in ticket 26.

**Blocked by:** 11

**Status:** ready-for-agent

- [ ] Tests show the range annotations for each precision, from year to full date-time, with and without a time zone (T053).
- [ ] A time without a timezone carries the range the engine computes for it today, shown by a test.
- [ ] Date comparison with the annotations gives the same results as without them. With the annotation disabled, the suite passes.
