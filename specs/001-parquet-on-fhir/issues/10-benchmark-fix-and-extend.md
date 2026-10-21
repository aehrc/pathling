# 10 — Benchmark: fix warm planning, add missing views, re-measure

**Spec:** `specs/001-parquet-on-fhir/spec.md` — SC-007, FR-033. Old tasks: T124g, T136b and the T136 re-runs.

**What to build:** A benchmark that measures what the remaining work needs, re-run against the T003 baseline and the T136 numbers.

- **Fix warm planning (T124g).** The invocation-level setup that prepares view execution also runs before every view-planning invocation. It plans the same query immediately before the measured plan, so planning is measured warm and under-reported. Give the execution preparation a `@State` of its own.
- **Add views (T136b and decision 125):**
  - a quantity comparison, for ticket 12;
  - a date range, for ticket 14;
  - views that join on references and call `resolve()`, measured on the computed key.
- **Re-measure.** Record encode, decode, planning and execution separately, and save every view's optimised plan beside its timings, for ticket 27.
- **Check SC-011 (owner, 2026-10-21).** Run encode and decode for JSON and XML, lines and bundles, against SC-011's bar, and record the result. This is the release's SC-011 check. The formal JMH check stays with ticket 16.
- **Measure the annotations.** Run every view with each annotation kind on and off, using ticket 11's toggles, so that the computed path and the fast path of tickets 12, 13 and 14 are compared at the same revision.

**Blocked by:** 12, 13, 14

**Status:** ready-for-agent

- [ ] Planning is measured cold. The change in the planning numbers is explained in the evidence.
- [ ] The new views are part of the benchmark and run in later re-runs.
- [ ] SC-011 is checked for JSON and XML, lines and bundles, and the result is recorded. A miss is reported to the owner.
- [ ] Each annotation kind's effect is recorded, from runs with it on and off.
- [ ] FR-033 is answered. Either the computed key suffices, or a precomputed key is worth having, in which case a follow-up issue is handed to ticket 26 as an annotation the engine works without.
- [ ] Results, plans and reproduction steps are saved under the spec's `evidence/`.

## Comments

### Moved to stage 5 (2026-10-21)

Owner decision. The benchmark is re-measured once, against the finished layout, rather than before the
annotations land. Tickets 12, 13 and 14 therefore no longer re-run it themselves: this ticket measures each
annotation kind on and off instead, which compares the computed and fast paths at the same revision.
