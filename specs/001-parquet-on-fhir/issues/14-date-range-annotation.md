# 14 — Date range annotation

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-004, FR-021, FR-022, FR-023. Old tasks: T053, T065.

**What to build:** Dates carry the specification's range annotations, reflecting the stated precision, and the engine uses them as a fast path for date comparison where the schema carries them.

**A time without a timezone (owner, 2026-10-21).** A `dateTime` or `instant` with a time but no timezone is valid (decision 133's addendum). Its range annotation is the range the engine computes for it today, through `FhirPathDateTime`'s boundaries, so the annotation and the computed path agree. Whether that range should widen by ±14 hours is a follow-up in ticket 26.

**Where the kind lives (decision 135).** The range kind is defined once, in the kinds package beside `fhir-schema`'s annotation API. The engine's boundary UDFs (`LowBoundaryForDateTime` and its siblings, registered by `PathlingUdfRegistrar`) move onto it, so `FhirPathDateTime`'s boundaries, or a counterpart `fhir-schema` can reach, are the single computation. It is stored by the annotation step of ticket 11, not by the reader.

**The date grammar (decision 135, ticket 31).** Ticket 31 puts FHIR's date grammar in `fhir-schema` for `toDate()` and `toDateTime()`. The range kind, also in `fhir-schema`, parses dates with that grammar rather than a second one. Whichever ticket lands first puts it in place, and the other uses it.

**Blocked by:** 11

**Status:** ready-for-agent

- [ ] Tests show the range annotations for each precision, from year to full date-time, with and without a time zone (T053).
- [ ] A time without a timezone carries the range the engine computes for it today, shown by a test.
- [ ] Date comparison with the annotations gives the same results as without them. With the annotation disabled, the suite passes.
