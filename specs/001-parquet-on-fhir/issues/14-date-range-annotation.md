# 14 — Date range annotation

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-004, FR-021, FR-022, FR-023. Old tasks: T053, T065.

**What to build:** Dates carry the specification's range annotations, reflecting the stated precision, and the engine uses them as a fast path for date comparison where the schema carries them.

**A time without a timezone (owner, 2026-10-21).** A `dateTime` or `instant` with a time but no timezone is valid (decision 133's addendum). Its range annotation is the range the engine computes for it today, through `FhirPathDateTime`'s boundaries, so the annotation and the computed path agree. Whether that range should widen by ±14 hours is a follow-up in ticket 26.

**Where the kind lives (decision 135).** The range kind is defined once, in the kinds package beside `fhir-schema`'s annotation API. The engine's boundary UDFs (`LowBoundaryForDateTime` and its siblings, registered by `PathlingUdfRegistrar`) move onto it, so `FhirPathDateTime`'s boundaries, or a counterpart `fhir-schema` can reach, are the single computation. It is stored by the annotation step of ticket 11, not by the reader.

**The date grammar (decision 135, ticket 31).** Ticket 31 puts FHIR's date grammar in `fhir-schema` for `toDate()` and `toDateTime()`. The range kind, also in `fhir-schema`, parses dates with that grammar rather than a second one. Whichever ticket lands first puts it in place, and the other uses it.

**Placement (decision 135, part 4).** The range annotations are `_<element>.__start` and `__end`, fields of the date's metadata group, so a repeating date's bounds share the group's alignment with it. The group is stored for them before ticket 17 stores ids and extensions. The engine reads it by pairing the element with `_x`, naming only the annotation fields it needs, so that the read prunes (`evidence/annotation-pruning.md`).

**One computation for both bounds (owner, 2026-10-26).** `__start` and `__end` come from one parse of the date. Ticket 11's annotation step fills each annotation field separately, so as built it would parse every date twice. Change the step so that a kind is computed once per container, and its fields are filled from that one result. Ticket 12 raised this, where its quantity kind had two fields until the owner dropped `__canonical` (decision 137). The engine's fallback, where the schema carries no range, computes both bounds from one parse too.

**The annotations' type against the specification (owner, 2026-10-26).** The Parquet on FHIR specification gives `start` and `end` the physical type `int96` with the logical type `TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS)`, and names `date` and `dateTime` as the types that carry them. Check what Pathling stores against that: Spark's `TimestampType` is adjusted to UTC at microseconds, and whether it is written as `int96` depends on `spark.sql.parquet.outputTimestampType`. Check also whether `instant` carries them, which the specification does not name. Match the specification where Spark can, and report each difference to the owner, to be recorded as a deviation as decision 137 records the quantity's, rather than settling it here. Ticket 30's layout column claims conformance, so a difference matters.

**Blocked by:** 11

**Status:** ready-for-agent

- [ ] Tests show the range annotations for each precision, from year to full date-time, with and without a time zone (T053).
- [ ] A time without a timezone carries the range the engine computes for it today, shown by a test.
- [ ] Date comparison with the annotations gives the same results as without them. With the annotation disabled, the suite passes.
- [ ] The stored type of `__start` and `__end` is checked against the specification's, and each difference is reported to the owner and recorded.
- [ ] The annotation step computes a kind once per container. A plan test shows one parse of a date for both its bounds, at ingest and in the engine's fallback.
