# 05 — Remove the legacy-layout normalisation, its test dimension and CI job

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-046, FR-053. Old tasks: T100e and the removal of T100i's job. T049a and T100f are settled by ADR-0001.

**What to build:** The engine reads only the PoF layout. Every branch that converts the legacy layout at traversal goes, along with the machinery that exists only to test it:

- `DecimalNormalisation`, `QuantityNormalisation` and `InstantNormalisation`.
- The `_fid` and root extension-map arms of `TraverseExtension`.
- The decimal arms of `ResolveOrNull`.
- The `*ColumnOrNull` helpers that serve only the legacy layout, and `readField`.
- The `pathling.testLayout` test dimension and every test arm that runs both layouts.
- The SpEL-scoped conformance exclusions that apply only to the legacy layout.
- The `test-fhirpath-previous-layout` CI job, in both `test.yml` and `pre-release.yml`.

Before deleting `DecimalNormalisation`, move `SCALE_SUFFIX` to where `LayoutDetector` can still use it.

**Out of scope:** `CodingSchema._fid` and the tolerance from decision 79 stay. They belong to #2804.

**Blocked by:** 02, 03, 04

**Status:** ready-for-agent

- [ ] None of the listed classes, arms, the property or the CI job remain. `LayoutDetector` still compiles and detects the legacy layout.
- [ ] Ticket 04's loud failure still fires on every route it covers.
- [ ] The conformance baselines gain no entries. Exclusions made obsolete by the removal are taken out (SC-003, per the `pathling-yaml-exclusions` skill).
- [ ] The core build passes. The Python and R suites pass after the Ivy cache is cleared.
