# 10 — Benchmark: fix warm planning, add missing views, re-measure

**Spec:** `specs/001-parquet-on-fhir/spec.md` — SC-007, FR-033. Old tasks: T124g, T136b and the T136 re-runs.

**What to build:** A benchmark that measures what the remaining work needs, re-run against the T003 baseline and the T136 numbers.

- **Fix warm planning (T124g).** The invocation-level setup that prepares view execution also runs before every view-planning invocation. It plans the same query immediately before the measured plan, so planning is measured warm and under-reported. Give the execution preparation a `@State` of its own.
- **Add views (T136b and decision 125):**
  - a quantity comparison, so ticket 12 has a computed-path baseline;
  - a date range, for ticket 14;
  - views that join on references and call `resolve()`, measured on the computed key.
- **Re-measure.** Record encode, decode, planning and execution separately, and save every view's optimised plan beside its timings, for ticket 27.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] Planning is measured cold. The change in the planning numbers is explained in the evidence.
- [ ] The new views are part of the benchmark and run in later re-runs.
- [ ] FR-033 is answered. Either the computed key suffices, or a precomputed key is worth having, in which case a follow-up issue is handed to ticket 26 as an annotation the engine works without.
- [ ] Results, plans and reproduction steps are saved under the spec's `evidence/`.
