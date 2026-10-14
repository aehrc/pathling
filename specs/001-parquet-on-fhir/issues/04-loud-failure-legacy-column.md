# 04 — Fail loudly on a legacy-typed column at traversal

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-053.

**What to build:** A legacy-layout column can reach the engine by a route detection does not cover: a source with detection disabled, a dataset the caller supplies, or a dataset built with the legacy encoders. When it does, the engine fails with an error naming the column, rather than answering from it. The check runs once per schema, when the traversal resolves, not per row. Write the tests first.

It must not fire on System values the engine builds itself. Decision 100 is the precedent: after ticket 03, an engine-built System.Quantity no longer looks legacy, and the check must be shown to tell the two apart.

**Blocked by:** 03

**Status:** ready-for-agent

- [ ] Tests come first. A failing test exists for each route: detection disabled, a caller-supplied legacy dataset, and a `FhirEncoders`-built dataset.
- [ ] Each failure names the column and is raised once per schema. A test shows it is not evaluated per row, for example by asserting it is raised at analysis or plan time.
- [ ] Engine-built System.Quantity values, and Coding values while #2804 is open, do not trigger it.
- [ ] The full `fhirpath` suite passes over the PoF layout.
