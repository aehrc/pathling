# 16 — DEFERRED: the ingest mechanism, per-value validation and lossless decimals

**Spec:** `specs/001-parquet-on-fhir/spec.md` — OQ-1, FR-002, FR-016, FR-019, FR-049, FR-050, FR-060, FR-063, SC-011. Decisions 133 and 134. Old task: T078e. T124c is likely made moot.

**What to build:** The ingest and egress mechanism ticket 08 measured, chosen and built, so that ingest meets decision 133's per-value scope and removes OQ-1's losses. Decision 134 deferred the choice until the owner has settled the requirements with colleagues: how much of decision 133's per-value scope is needed, and how much code the project is willing to own for it. Until then, ingest runs on the interim of decision 134, and ticket 32 takes XML and bundles off HAPI.

**The evidence to start from:** `evidence/spike-08/README.md`, "Summary of phase 2". It sets A+, A-variant and B, with B both through `Encoders.row` and through a Java UDF, side by side against decision 133's scope, OQ-1's losses, SC-011's bar and A0, and lists what was not measured. The prototypes, harness and batch scripts are under `evidence/spike-08/phase2/`. They were built on the stage-1 code at `514f8f818e`. Once ticket 32 lands, the XML baseline they compare against is no longer HEAD's HAPI route.

**What was not measured, should the choice need it:**

- B's XML encode under `hugeMethodLimit=8000` or through a UDF. B's XML rows go through `Encoders.row` too, so its XML figures may include the same uncompiled serializer that triples its JSON encode.
- B through a UDF on XML, bundles, the messy data and the fixtures.
- A UDF that returns zero or more rows to be exploded, which would need no non-deterministic flag and is the shape bundles need.
- JSON bundles, for any candidate.
- The formal JMH check of SC-011 (`evidence/t136-benchmark.md`).

**Blocked by:** The owner's choice of mechanism (decision 134). Ticket 32, because the rework replaces what it builds. Ticket 31, for the date grammar it puts in `fhir-schema`.

**Status:** needs-info

Criteria that hold whichever mechanism is chosen:

- [ ] The choice is recorded as a decision, and OQ-1's interim answer is replaced: each loss is removed, with its FR text tightened, or kept as a stated exception. FR-063's interim exception is removed or narrowed to match.
- [ ] No ingest or egress route parses into, or builds, a FHIR object per row, HAPI included (FR-061).
- [ ] XML and bundles keep, coerce, report and fail on exactly what JSON does (FR-060).
- [ ] Bundle references resolve to the target entry's relative reference, keeping its version (FR-062, decision 83).
- [ ] The rework carries over what was built on the interim ingest: the primitive metadata group of ticket 17, with its `_x` arrays that have no `x` beside them, and the dense mode of ticket 18. Both pass their own tests on the new mechanism.
- [ ] Encode and decode meet SC-011.
- [ ] Ingest validates dates and decimals with the shared grammar in `fhir-schema`, so ingest and engine agree on what is valid (decision 133, principle 7). Ticket 31 puts the date grammar there; the decimal grammar, and whether ingest validates per value at all, come with the follow-up issue that ticket 26 raises.
- [ ] Ingest's handling of non-conformant data, the modes of "Modes" in `evidence/ingress-strictness/README.md` among it, follows the follow-up issue that ticket 26 raises. #2367's ingest is `trust` (decision 133's addendum of 2026-10-22).
- [ ] If the stored form of any value changes from the interim's, such as a decimal's text, the layout column's version (ticket 29) is bumped, and data written by the interim is read or refused as that rule says.
- [ ] Any thread that outlives this programme, such as the upstream Spark issue T132 describes, is raised as a GitHub issue by this ticket. Ticket 26 closes before #2367 does, which does not wait for this one.

## Comments

### Rewritten as the deferred rework (decision 134, 2026-10-20)

Ticket 08 reported without a choice. This placeholder is now the deferred rework, and the work it held that does
not wait on the choice moved to ticket 32: XML and bundles without HAPI, the reflection workaround, and the XML
file format comment from 2026-10-17.
