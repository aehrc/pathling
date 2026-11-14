# 48 — Decide which annotation decisions belong in an ADR

**Spec:** `specs/001-parquet-on-fhir/evidence/decisions.md` — decisions 140 to 149; `evidence/annotations-decision-record.md`; `docs/adr/`. Raised from the annotations design (owner, 2026-11-05).

**What to decide:** which of the annotation decisions are architectural enough, and lasting enough, to be recorded as ADRs under `docs/adr/`, beside ADR-0003 (annotations are optional) and ADR-0005 (types from definitions only).

The decisions of 2026-11-04 and 2026-11-05 are numbered in `decisions.md`, 140 to 148, and the full record, with the options weighed, is `evidence/annotations-decision-record.md`. Candidates, for the review to confirm or reject:

- **Decision 140:** rely on Spark's optimiser, with no FHIRPath-level analysis of field use (P0).
- **Decision 141:** a primitive's element carries its value and named annotations, with `id` and `extension` behind a switch that is off by default (D2, D14).
- **Decision 143:** pruning comes from the plan's shape, and Pathling's own optimiser rule is a separate experiment (L2, D3b).
- **Decision 147:** a System decimal is a number, and a stored decimal's text reaches views only (D13, 6b).

Decision 149 is not a candidate until the specification's owner has decided D4, D5 and D6 (ticket 46).

**Blocked by:** None.

**Status:** needs-triage

- [ ] Each candidate is either written as an ADR under `docs/adr/`, linking its decision and evidence, or recorded here as staying in `decisions.md`, with the reason.
