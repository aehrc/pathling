# 15 — Test that annotation use is decided from the schema, and document annotations

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-023, FR-047, SC-004. Old task: T090; decision 121.

**What to build:**

- **Plan test.** It proves annotation use is decided from the schema, not per row: the generated plan differs between an annotated and an unannotated input of the same data.
- **Documentation.** Document the annotations and their per-annotation toggles in the API docs (Java, Python, R) and on the library's schema docs page. Revise that page's statement that this release does not write the annotation fields.

Can start once any one annotation kind has landed, and is extended as each further kind lands.

**Site documentation (decision 135, amending 121).** Document the annotations, their switches, the *fill*/*recompute* and *drop*/*keep* settings, and the public annotation step in `site/docs/libraries/io/schema.md`, and revise its statement that this release writes no annotations. Ticket 11 documents only the API docs.

**Blocked by:** any of 12, 13, 14. Under the re-plan it is taken last in stage 4, after 42, 43, 17 and 52, so that it describes their final shape (owner, 2026-11-02), and so after 50 and 51, into which 43 was split (owner, 2026-11-11).

**Status:** ready-for-agent

- [ ] The plan test exists for each annotation kind that has landed.
- [ ] SC-004: the full engine suite passes over files written with every annotation disabled.
- [ ] The docs describe each kind that has landed, its toggle and its default.

## Comments

### What the re-plan adds to the documentation (owner, 2026-11-02)

- *Fill* keeps a stored annotation that is null, so a table merged from annotated and unannotated sources needs *recompute*. Deciding *fill* per row moved to a GitHub issue after #2367 (ticket 38).
- Encoding a streaming dataset is refused in this release. A stream is encoded batch by batch, for example in `foreachBatch`. Dense mode and streaming encode moved to a GitHub issue after #2367 (tickets 18 and 19).
- If tickets 17 and 52 land, the primitive metadata group, its ingest option, and its navigation from FHIRPath with the engine's switch.
