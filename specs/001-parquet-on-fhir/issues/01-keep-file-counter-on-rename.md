# 01 — Keep the file counter so split partition files do not collide on rename

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-042 (appends through every sink). Old task: T046a follow-up (decisions 129, 130).

**What to build:** A write that Spark splits into several files per partition, which `spark.sql.files.maxRecordsPerFile` causes, completes and keeps every row. Today, two files of one partition that differ only in the file counter (`-c000`, `-c001`) are renamed to the same name. The second rename fails after the rows are written. On the local file system the rename is refused and the table is kept. The renamed file must keep the counter in its qualifier, alongside the partition and write ids, for both the Parquet and the NDJSON sinks. Names written by earlier releases, and names without a counter, must still read as the type.

**Blocked by:** None — can start immediately

**Status:** done

- [x] A test writes one type with `maxRecordsPerFile` small enough to produce several files per partition. It fails before the fix with the rename error and passes after, for Parquet and NDJSON.
- [x] Every row is read back. The existing save-mode tests (`FileSinkSaveModeTest`, `TypeFilesTest`) pass unchanged.
- [x] The file source reads the renamed files as the type, and the save modes find them under a name that is not only letters (decision 130).
- [x] Databricks' name form, `part-<n>-tid-<number>-<writeId>-…`, keeps its partition id, write id and counter. Checking it on a real cluster stays with ticket 28.

## Comments

**2026-10-14, implementation (branch `issue/2367-file-counter`).** `TypeFiles.renameKeepingWriteId` now keeps the file counter: a file Spark numbers `c000`, and a file without a counter, is still named `<name>.<partId>-<writeId>.<extension>`, and every later file of the partition keeps its counter, `<name>.<partId>-<writeId>-c001.<extension>`. The counter is dropped for `c000` because the existing tests pin that name. `TypeFilesTest` renames Spark's `-c000` fixtures and expects `<Type>.00000-<writeId>.ndjson`, and `FileSinkSaveModeTest` and `DataSourcesTest` match names without a counter. Keeping `c000` would have changed every write's file names, but the acceptance criteria require those tests to pass unchanged. A partition that Spark does not split is named as before. The counter is found at the end of whatever follows the write id, so Databricks' task-attempt segments (`-5-1-`) are dropped and its counter is kept.

The qualifier still holds no period, so the file source (`BASE_NAME_WITH_QUALIFIER`) and the save modes (`TypeFiles.list`, including names that are not only letters) find the new names without any change.

Tests, each failing before the fix with "Could not rename the partition file" and passing after:
- `FileSinkSaveModeTest.aPartitionSplitIntoSeveralFilesKeepsEveryRow` (Parquet and NDJSON) writes three rows coalesced into one partition with `maxRecordsPerFile` = 1, then reads all three back through the file source.
- `FileSinkSaveModeTest.overwriteUnderANameNotOnlyLettersReplacesEveryFileOfASplitWrite` (Parquet and NDJSON) covers decision 130.
- `TypeFilesTest.filesOfOnePartitionKeepTheFileCounter` covers `c000`, `c001`, `c010` and `c1000`, for both the Spark and the Databricks name forms.
- `TypeFilesTest.twoAttemptsOfOnePartitionAreRefused` pins the gap left open: two Databricks task attempts of one partition take one name, so the second rename is refused and the table is left in place.

The setting is unset after each split write, so it does not leak into the shared session. Full `library-api`: 322 tests, all passing. The site's I/O page describes the counter names. The Python and R docstrings give only the `c000` example, which is still accurate, so they are unchanged. Checking the Databricks name on a real cluster stays with ticket 28.
