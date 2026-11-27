# 19 — Dense parity

> **Moved after #2367 (owner, 2026-11-02).** This ticket becomes a GitHub issue raised by ticket 26, and is not dispatched within #2367. See this ticket's comments.

> **Raised as #2881 (2026-11-09),** with ticket 18.

> **Superseded by [2881, defined schemas](../../2881-defined-schemas/spec.md) (its FR-026).** With no dense mode there is no second mode to compare. #2881 shows instead that results depend on the data alone (FR-021): both YAML suites run with defined schemas in the regular build, with every case's result equal to the inferred baseline (FR-021b, `evidence/yaml-defined-run.md`), and absence is tested at every kind of position (SC-005). The `dense` test schema mode placeholder was removed. Nothing in this ticket is to be built as written.

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-010, FR-057, SC-002, SC-010. Old tasks: T030, T037b, T108b.

**What to build:** Evidence that the dense and fitted modes agree.

- **Transform parity (T030).** Transform the same data in both modes and compare types, cardinality and field order on the branches both carry, after normalising nullability. A fitted structure's field order is a subsequence of the dense one.
- **Suite parity (T037b).** Add `-Dpathling.testSchemaMode=dense` to the test dimension, and run the curated FHIRPath subset, both conformance baselines and the SQL-on-FHIR compliance suite in it.
- **Expression parity (T108b).** Per ticket 09's decision on binding (decision 136, an unbound column): one column expression applied to a fitted and to a dense dataset holding the same resources gives equal results for every expression in the engine's expression-level test set.

**Blocked by:** 18, 09

**Status:** ready-for-agent

- [ ] The transform parity test passes, including the subsequence check.
- [ ] The dense dimension runs in CI. T038's assertion confirms the active mode is the one requested.
- [ ] The expression parity test passes in the form ticket 09 settled.

## Comments

### Moved after #2367 (owner, 2026-11-02)

The re-plan limits #2367 to a core library that matches the legacy line, with primitive metadata as its one extension. This ticket becomes a GitHub issue, raised by ticket 26 and labelled `new feature` and `PoF_line`, and whether it goes into a release is decided separately.
