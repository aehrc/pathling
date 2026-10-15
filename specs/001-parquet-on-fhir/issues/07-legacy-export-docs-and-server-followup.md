# 07 — Document the legacy export; raise the server migration issue

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-047, FR-064. Old task: T134b (rewritten); decisions 98, 101, 118.

**What to build:** A user reading the docs about legacy data is pointed at the legacy export, and the server's move to the PoF layout has a tracked owner.

- The storage-layout page's compatibility section no longer offers routing or in-place reading of legacy data. It describes the refusal, the legacy export (ticket 06) as the migration path, and that legacy data bypassing detection is unsupported, with undefined results (decision 132).
- Rewrite T134b as a GitHub issue for the server. The server stays pinned to the last pre-PoF release. It moves by migrating its warehouse with the legacy export from `SchemaMigrator`, and maps `UnsupportedLayoutException` to a 500 with the message for a read of its own warehouse, and to a 400 for `$import` of a client-supplied file. The issue also carries decision 118's docs problem: the server docs link to the library's storage-layout page as the server's Parquet format. Before the library release, either the server has moved, or the legacy layout's description goes into the server docs and those links point at it.

**Blocked by:** 06

**Status:** done

- [x] The storage-layout page and the API docs (Java, Python, R) describe the refusal and the legacy export, with no mention of routing.
- [x] The server issue is raised on GitHub with the content above, and its number is recorded in this ticket's comments.
- [x] The server-docs link problem is either fixed here or stated in that issue as a release blocker.

## Comments

### Implementation (branch `issue/2829-legacy-export-docs`)

**Storage-layout page (`site/docs/libraries/io/schema.md`).** "Data written by earlier releases" keeps
the refusal at read and write, and now:
- names `UnsupportedLayoutException` for Java and Scala, and says why it is not a user-input error
  (decision 98);
- says that legacy data that bypasses detection (detection disabled, a caller's dataset, one built
  with the earlier encoder) is unsupported, with undefined results (decision 132; see below);
- adds "Migrating data in the earlier layout": re-import from source first, because it is lossless,
  and the legacy export where there is no source. This covers the invocation, the arguments, the
  exit codes, the refusals (including a mixed table) and the Java entry point;
- adds "What the legacy export loses", the list from `package-info.java`. The `legacy-export`
  Javadoc is not published on the site, which carries only `library-runtime`'s, so FR-047 needs the
  list on the page.

The page never offered routing, and still does not.

**Other site changes (`io/index.md`).** "Layout detection" points at the export. The opt-out
paragraph and "Datasets" say that legacy data that bypasses detection is unsupported.
The overwrite sentence under "Writing FHIR data" names the export as one source of a re-import.

**API docs.**
- Java: `UnsupportedLayoutException` (the remedy, the export and the unsupported bypass), `DataSourceBuilder`,
  `DatasetSource`, and the detection-flag constructors of `ParquetSource`, `DeltaSource` and
  `CatalogSource`.
- Python: the `parquet`, `delta`, `tables` and `datasets` docstrings.
- R: the matching roxygen blocks.
- No signature changed. Nothing mentions routing.

**Contract.** In `contracts/storage-layout.md`, the "Provisional, pending T049a" paragraph and the
old "version gate and rewrite tool" migration paragraph are replaced, because ADR-0001 settled
T049a. The opt-out sentence no longer says the data "is then read as it is".

**Module docs.** The `--packages` form in `legacy-export`'s `package-info.java` was never run. It is
replaced by the `--jars` form, which was verified. The nesting and extension loss defaults are now
qualified as the Pathling library's, because `FhirEncoders` built directly defaulted to fewer.

**`spark-submit` verification.** This was Spark 4.0.1, with the module's runtime jars and
`delta-spark`/`delta-storage` on `--jars`:
- a legacy Parquet warehouse and the same tables as Delta both exited 0, writing 9 `Patient` and
  200 `Observation` lines;
- a Parquet on FHIR table exited 1 with the "not in the legacy layout" message.

The records are under `.local/work/issue/2829-legacy-export-docs/`
(`spark-submit-verification.md`).

**Server-docs link problem: not fixed here.** It is stated as a release blocker in the drafted server
issue. Decision 118 rejects copying the old page into the server docs now: that "puts server changes
in a core milestone and is wasted if the server moves first". It also makes the fix depend on whether
the server has moved by the release. The draft lists all six sites: `import.md` and `export.md`,
their 2.0.1 snapshots, and the Javadoc of `ImportFormat.PARQUET` and `ExportOutputFormat.PARQUET`.

**Server issue: drafted, awaiting raising.** The draft is
`.local/work/issue/2829-legacy-export-docs/server-issue.md`, and no number exists yet. The draft
covers:
- the pin to `pathling.version` 9.9.0, and its deadline;
- replacing `SchemaMigrator` with export, re-read and overwrite. This is to be confirmed, because
  `SchemaMigrator` diffs against the legacy encoder schema and appends with `mergeSchema`;
- the mapping of `UnsupportedLayoutException`: a 500 with the message for the server's own warehouse
  (today `ErrorHandlingInterceptor` drops the message), and a 400 for a client file in `$import` and
  `$import-pnp`. `ImportExecutor` throws the same type for both, so the mapping must come from the
  call site;
- the docs blocker.

The second and third criteria stay unticked until the issue is raised and its number is recorded
here.

**Doc builds.**
- `bun run build` of the site: no broken links or anchors. Its only warning, from the llms-txt
  plugin, is also in the baseline build.
- `-Pdocs` package of `library-api` and `legacy-export`: Javadoc built.
- Ruff, styler and a roxygen parse are clean.

### Open, for review

- **The runtime refusal message does not name the legacy export.** `LayoutDetector.message` still
  gives re-import from source as the only remedy. It was left alone because its text is asserted by
  tests.
- **No runnable bundle.** The jar is plain, as ticket 06 left it, so users assemble the runtime
  classpath themselves. The docs say so but give no one-step way to do it.
- **Repetition across the API docs.** The export and unsupported-bypass sentences are repeated in each Python and
  R reader and in three Java constructors, following the existing idiom of the remedy sentence.

### FR-053 withdrawn (decision 132)

The owner withdrew FR-053 on 2026-10-15 (decision 132), and ticket 04 is wontfix. The engine makes no
legacy-layout check beyond detection at read time. Data that bypasses detection is unsupported, the
caller owns the consequences, and results are undefined. The first two commits described FR-053's
failure naming the column. That text is replaced, on the site pages, in the Javadoc, the Python
docstrings, the R roxygen, the contract and the drafted server issue, by the statement that such data
is unsupported and its results undefined. No failure is promised. The remedy is still re-import or
the legacy export.

### Server issue raised

Raised as [#2837](https://github.com/aehrc/pathling/issues/2837) on 2026-10-15, from the reviewed
draft, with the `server` and `enhancement` labels. It states the server-docs link problem as a
release blocker (decision 118), which meets the third criterion.
