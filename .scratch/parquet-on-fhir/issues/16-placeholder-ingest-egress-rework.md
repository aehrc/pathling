# 16 — PLACEHOLDER: ingest and egress rework per the spike

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-045, FR-060, FR-061, FR-062, FR-016, FR-019, FR-063, SC-001, SC-011. Old task: T078e. T124c is likely made moot.

**What to build:** Ingest and egress rebuilt on the mechanism ticket 08 chooses. This covers:

- XML in and out;
- bundles in both formats;
- JSON as well, if option B is chosen.

To be rewritten with concrete criteria once ticket 08 reports.

**Blocked by:** 08

**Status:** needs-triage

Criteria that hold either way:

- [ ] No ingest or egress route parses into, or builds, a FHIR object per row, HAPI included (FR-061).
- [ ] XML and bundles keep, coerce, report and fail on exactly what JSON does (FR-060).
- [ ] Bundle references resolve to the target entry's relative reference, keeping its version (FR-062, decision 83).
- [ ] An `_x` array with no `x` beside it is kept on every route.
- [ ] Encode and decode meet SC-011.
- [ ] The reflection workaround that finds custom resource types on an executor-side `FhirContext` is gone.
