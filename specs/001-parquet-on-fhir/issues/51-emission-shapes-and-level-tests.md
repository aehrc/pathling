# 51 — Emission shapes, level tests and the by-example tests

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-002, FR-004, FR-022, FR-023. Split from ticket 43 (owner, 2026-11-11). Settled by the annotations design, `design/annotations-in-the-engine.md` §6 question 5 and §8, and decision 143. Examples: `design/annotations-by-example.md`. Evidence: `evidence/annotations-design-rewrites-layer1.md`, `evidence/annotations-design-pairing-shapes.md`, `evidence/annotations-design-project-before-generate.md`.

**What to build:**

- **Emission shapes (decision 143, L2).** Compute first, then generate. Existence tests (`exists()`, `count()` and a multi-column `where`) are rewritten into the checked forms, with the guards the evidence lists, without which the rewrites differ from today's engine. A repeating primitive is paired as `arrays_zip(x AS value, _x.__start AS __start, _x.__end AS __end)`, so its fields match the singular form, with `coalesce(x, array())` guards on extracted arrays and never around `transform` (ticket 43 built the pairing, `ResolveOrNone`).
- **Level tests,** for dates and quantities, and for decimals once ticket 50 lands.
- **The by-example tests:** the examples of `design/annotations-by-example.md` §3 to §5.

**Blocked by:** 50.

**Status:** ready-for-agent

- [ ] Plan-shape tests for the emission shapes assert the plan and equal results, in CI on Spark 4.0.2, including that the zipped fields carry the singular names and that the aliases add no projection that blocks pruning.
- [ ] Engine tests cover each level, over annotated and unannotated data: a date and a quantity at the root, under a repeating parent (`Observation.component.valueQuantity`), inside an extension (an extension's `valueDate` and `valueQuantity`), and a repeating primitive under a repeating parent. Each gives the same results either way, reads the stored annotation where present, and computes nothing where only the value is output.
- [ ] The examples of `design/annotations-by-example.md` §3 to §5 are covered by tests, and the engine suite passes over annotated data and over data written with every annotation disabled (`AnnotationFreeSuiteTest`).
