# 29 — Identify the PoF layout by a versioned layout column

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-065, FR-066, User Story 7 scenarios 6 and 7. [ADR-0006](../../../docs/adr/0006-layout-identified-by-a-versioned-column.md); decision 132.

**What to build:** Every PoF dataset Pathling writes carries a root column `__layout_v1`, a nullable boolean whose every value is null, and detection identifies the layout by that column before it looks for markers. This makes the first line of defence exact, now that the engine makes no check of its own (decision 132). It has to land before the first release of the PoF layout, so that every PoF dataset a release ever writes carries the column.

- **Writing.** The Parquet, Delta and catalog sinks add the column when they write. Whether `FhirReader` and `encode()` also put it on the datasets they return is an owner decision, recorded below before this ticket is started. If they do, a dataset a user writes with Spark directly keeps it, but the column appears in every dataset the Java, Python and R APIs return.
- **Fitting.** The fitted schema drops structure the data does not use (FR-055), and an all-null column looks exactly like that. The layout column is exempt, whatever the schema mode and the annotation switches.
- **Egress.** Egress throws today on a stored field it does not know (ticket 20). It skips the layout column, so a dataset carrying one is written out as FHIR without error, and it is not reported as an unknown field, before or after ticket 20.
- **Detection.** `LayoutDetector` recognises `__layout_v<n>` by name and type alone. A version this release supports is the PoF layout. A higher version is refused at read time and before an append or merge, with `UnsupportedLayoutException` naming the version found and the version supported. A schema with both a layout column and the legacy root `id_versioned` is refused as mixed. Without a layout column, detection proceeds by markers as today.
- **The refusal message.** User Story 7's first scenario asks for the remedy to name the legacy export. `LayoutDetector`'s message still offers only re-import from source; make it name the legacy export too.
- **Ingest.** A source key named `__layout_v1` is reported as undescribed content, as an annotation's name is.
- **Docs.** The column is user-visible. Describe it in `contracts/storage-layout.md`, `contracts/library-api.md` and the storage-layout page under `site/docs/libraries/io/`.

**Probes first.** The behaviour this rests on was described from memory, not measured. Before building, probe and record in `evidence/layout-column.md`, with a runnable reproduction:

- a column of Spark's `NullType` is refused or dropped by the Parquet and Delta writers (the reason the type is boolean);
- an all-null nullable boolean column survives a Parquet write and read, a Delta overwrite, append and merge (with and without `mergeSchema`) into a table that has it and one that does not, and a catalog table.

If a probe contradicts the design, stop and report to the owner.

**Out of scope:** existing PoF test fixtures and the R package's example data are left without the column unless the owner decides otherwise below. They are still accepted, because no marker contradicts the PoF layout.

**Blocked by:** None. Ticket 07 rewrites the same storage-layout page, so merge 07 before this ticket's docs land.

**Status:** needs-info

**Owner decisions needed before starting:**

1. Is the column added only by the sinks, or also by `FhirReader` and `encode()`?
2. Are the existing PoF test fixtures and the R example data regenerated with it?

- [ ] The probes are recorded in `evidence/layout-column.md`, with a reproduction, and agree with the design.
- [ ] Every sink writes `__layout_v1` in both schema modes and with every annotation switch off. Tests read it back from Parquet, Delta and a catalog table, after an overwrite, an append and a merge.
- [ ] Egress writes a dataset carrying the column as FHIR without error, omits the column, and does not report it.
- [ ] Detection: a supported version reads; `__layout_v2` is refused at read and before an append, naming both versions; a layout column with root `id_versioned` is refused as mixed; a schema with no layout column is classified as before. Each case is tested through the file sources and sinks.
- [ ] The refusal message names the legacy export as a remedy.
- [ ] A source key `__layout_v1` is reported as undescribed content.
- [ ] The storage-layout contract, the library API contract and the site page describe the column.
- [ ] The core build passes. The Python and R suites pass after the Ivy cache is cleared.
