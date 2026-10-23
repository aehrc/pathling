# 09 — Decide whether column expressions are bound to a schema

**Spec:** `specs/001-parquet-on-fhir/spec.md` — OQ-2, FR-054, SC-008, SC-010. Replaces T133.

**What to build:** A decision on how a FHIRPath column expression relates to the schema it runs over. There are three options:

- **(a)** Today's schema-agnostic column. It is built without a dataset and resolves presence against whatever schema it meets.
- **(b)** Binding at evaluation time.
- **(c)** An unbound column representation, bound when the dataset is known.

**Criteria:**

- Whether the server and library callers can still combine FHIRPath and search columns with other SQL filters.
- How fragile it is with joins and the analyzer. The root reference needed special handling in join conditions; see the analyzer-gate and join-condition-limit evidence.
- Compatibility with the public API (SC-008).
- What each gives beyond today: bind-time validation, a required-schema manifest, and Pathling-level messages that name the missing element.

**Blocked by:** None — can start immediately

**Status:** done

- [x] Each option is assessed against the criteria, with evidence where a claim rests on Spark behaviour.
- [x] The owner decides, and the decision is recorded (as an ADR if it changes the engine's contract).
- [x] OQ-2 is resolved in the spec. FR-054 and SC-010 are reworded if needed, and ticket 19's parity test is restated to match.

## Comments

**Owner (2026-10-23):** option (a), the current schema-agnostic column, recorded as decision 136. It is to be revisited when extra fields are supported, since an implementation of them may need a schema for fields the definitions do not describe; that is part of the extra-fields follow-up issue that ticket 26 raises. FR-054 and SC-010 stand unchanged, the OQ-2 markers are removed, and ticket 19's expression parity test already takes the form this decision settles.
