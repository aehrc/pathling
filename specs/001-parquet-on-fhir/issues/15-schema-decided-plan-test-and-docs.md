# 15 — Test that annotation use is decided from the schema, and document annotations

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-023, FR-047, SC-004. Old task: T090; decision 121.

**What to build:**

- **Plan test.** It proves annotation use is decided from the schema, not per row: the generated plan differs between an annotated and an unannotated input of the same data.
- **Documentation.** Document the annotations and their per-annotation toggles in the API docs (Java, Python, R) and on the library's schema docs page. Revise that page's statement that this release does not write the annotation fields.

Can start once any one annotation kind has landed, and is extended as each further kind lands.

**Blocked by:** any of 12, 13, 14

**Status:** ready-for-agent

- [ ] The plan test exists for each annotation kind that has landed.
- [ ] SC-004: the full engine suite passes over files written with every annotation disabled.
- [ ] The docs describe each kind that has landed, its toggle and its default.
