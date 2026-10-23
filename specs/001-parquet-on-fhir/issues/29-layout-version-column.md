# 29 — Identify the PoF layout by a versioned layout column

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-065, FR-066, User Story 7 scenarios 6 and 7. [ADR-0006](../../../docs/adr/0006-layout-identified-by-a-versioned-column.md); decision 132 and its addendum.

**What to build:** Every dataset ingest produces from FHIR JSON or XML carries a root column `__layout_v1`, a nullable boolean whose every value is null. Detection identifies the layout by that column before it looks for markers. Everywhere else the column is ordinary: sinks write it and sources read it unchanged, and nothing adds it on write or drops it on read.

Take the column's name and version rule from the Parquet on FHIR specification amendment (ticket 30) if it has been agreed by the time this is built, and `__layout_v1` otherwise. A later rename before the release is expected to be cheap.

- **Ingest.** `FhirReader` and `encode()` add the column to every dataset they produce, from JSON and XML, single resources and bundles. That covers every schema mode and annotation switch that exists when this lands; dense mode (ticket 18) and the annotation switches (ticket 11) each check the column for themselves. This is the interim ingest of decision 134, with XML and bundles as ticket 32 rebuilds them, so the column goes into that code. If ticket 16 later changes what is stored, it bumps the version.
- **Fitting.** The fitted schema drops structure the data does not use (FR-055), and an all-null column looks exactly like that. The layout column is exempt.
- **Egress.** Egress skips the column, so a dataset carrying it is written out as FHIR without error. It is not reported as an unknown field (the follow-up issue on content the definitions do not describe, raised by ticket 26).
- **Detection.** `LayoutDetector` recognises `__layout_v<n>` by name and type alone. A version this release supports is the PoF layout. A higher version is refused at read time and before an append or merge, with `UnsupportedLayoutException` naming the version found and the version supported. A schema with both a layout column and the legacy root `id_versioned` is refused as mixed. Without a layout column, detection proceeds by markers as today, and unmarked PoF data is accepted (FR-039).
- **The refusal message.** User Story 7's first scenario asks for the remedy to name the legacy export. `LayoutDetector`'s message still offers only re-import from source; make it name the legacy export too.
- **Ingest of a key with that name.** A source key named like the layout column is reported as undescribed content, as an annotation's name is.
- **Docs.** The column is visible in every dataset the API returns. Describe it in `contracts/storage-layout.md`, `contracts/library-api.md` and the storage-layout page under `site/docs/libraries/io/`.

**Probes first.** The behaviour this rests on was described from memory, not measured. Before building, probe and record in `evidence/layout-column.md`, with a runnable reproduction:

- a column of Spark's `NullType` is refused or dropped by the Parquet and Delta writers (the reason the type is boolean);
- an all-null nullable boolean column survives a Parquet write and read, a Delta overwrite, append and merge into a table that has it and into one that does not (with and without `mergeSchema`), and a catalog table.

If a probe contradicts the design, stop and report to the owner.

**Out of scope:** existing PoF test data and the R package's example data are not regenerated. Unmarked PoF data stays accepted, and that data is its regression test.

**Blocked by:** 32

**Status:** ready-for-agent

- [ ] The probes are recorded in `evidence/layout-column.md`, with a reproduction, and agree with the design.
- [ ] Ingest adds the column from JSON and XML, single resources and bundles, in every schema mode and with every annotation switch off that exists when this lands.
- [ ] The column survives a write and read through every sink and source, Parquet, Delta and a catalog table, after an overwrite, an append and a merge, unchanged and without being added or dropped by either.
- [ ] Egress writes a dataset carrying the column as FHIR without error, omits the column, and does not report it.
- [ ] Detection: a supported version reads; `__layout_v2` is refused at read and before an append, naming both versions; a layout column with root `id_versioned` is refused as mixed; a schema with no layout column is classified as before. Each case is tested through the file sources and sinks.
- [ ] The refusal message names the legacy export as a remedy.
- [ ] A source key named like the layout column is reported as undescribed content.
- [ ] The storage-layout contract, the library API contract and the site page describe the column.
- [ ] The core build passes. The Python and R suites pass after the Ivy cache is cleared.

## Comments

### XML data before the bump is already lossless (owner, 2026-10-22)

From ticket 32's review: XML ingest stores a decimal's text unchanged, so XML data written before the layout
version is bumped for ticket 16's decimals is already in the lossless form. Only JSON data of that time passed its
decimals through a double.
