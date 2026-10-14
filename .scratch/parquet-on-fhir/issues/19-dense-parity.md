# 19 — Dense parity

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-010, FR-057, SC-002, SC-010. Old tasks: T030, T037b, T108b.

**What to build:** Evidence that the dense and fitted modes agree.

- **Transform parity (T030).** Transform the same data in both modes and compare types, cardinality and field order on the branches both carry, after normalising nullability. A fitted structure's field order is a subsequence of the dense one.
- **Suite parity (T037b).** Add `-Dpathling.testSchemaMode=dense` to the test dimension, and run the curated FHIRPath subset, both conformance baselines and the SQL-on-FHIR compliance suite in it.
- **Expression parity (T108b).** Per ticket 09's decision on binding: one column expression applied to a fitted and to a dense dataset holding the same resources gives equal results for every expression in the engine's expression-level test set.

**Blocked by:** 18, 09

**Status:** ready-for-agent

- [ ] The transform parity test passes, including the subsequence check.
- [ ] The dense dimension runs in CI. T038's assertion confirms the active mode is the one requested.
- [ ] The expression parity test passes in the form ticket 09 settled.
