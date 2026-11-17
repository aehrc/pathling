# 27 — Investigate the UsCoreBloodPressures planning regression

> **Folded into #2889 (owner, 2026-11-16):** this ticket becomes a bullet of #2889 (Profile and optimise query performance across the engine), with ticket 10 (Benchmark: fix warm planning, add missing views, re-measure), not an issue of its own. This ticket stays as the working record.

**Spec:** `specs/001-parquet-on-fhir/spec.md` — SC-007. Old task: T136a; decision 124.

**What to build:** At the PoF switch, planning `UsCoreBloodPressures` took 1.10–1.14 times the T003 baseline, while every other view was flat or better. The view calls no quantity UDF. Most of its 149 `CASE WHEN` come from the `code.coding.exists(...)` filter, and its plan carries 336 higher-order `filter` lambdas.

Build the baseline revision `4bfadac552`, capture the view's optimised and executed plans there, and compare them with the head's. The head's plans come from ticket 10's cold-planning re-run, after the legacy normalisation removal in ticket 05.

**Blocked by:** 05, 10

**Status:** ready-for-agent

- [ ] Both revisions' plans are saved in the evidence.
- [ ] The cause is recorded, along with whether it is worth reducing. Any fix that outlives the spec is handed to ticket 26.

## Comments

### Moved to stage 5 (2026-10-21)

Owner decision, with ticket 10, whose cold-planning re-run it needs.
