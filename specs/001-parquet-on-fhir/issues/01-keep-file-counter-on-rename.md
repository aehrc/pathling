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
- `TypeFilesTest.twoAttemptsOfOnePartitionAreRefused` pins the gap left open: two Databricks task attempts of one partition take one name, so the write is refused and the table is left in place.

**2026-10-21, review follow-up (branch `issue/2830-type-over-choice`).** A review found two gaps in the rename, both now fixed in `TypeFiles.renameKeepingWriteId`:
- A table is renamed whole or not at all. The targets are computed first, and a write in which two files would take one name, such as two Databricks task attempts, is refused before any file is renamed. Before this, the first file was moved out beside a table that still held the second, so the write's files were split between the two places. If a rename fails part-way, the files already renamed are returned to the table, and the error names any that could not be. `twoAttemptsOfOnePartitionAreRefused` now asserts that both files stay in the table and nothing is renamed. `aRenameFailingPartWayReturnsTheRenamedFilesToTheTable` covers the restoration.
- What this does not change: a refused write leaves its table in place, and the file source still reads that table as the type, as it did for any failed rename before this branch. So where both Databricks attempts are in the table, a read returns the rows of both, and in OVERWRITE mode the replaced files are not deleted, since the write did not complete. Whether a real cluster leaves an uncommitted attempt's file in the output directory at all, rather than only the committed one, stays with ticket 28.
- A counter introduced with a period, `part-<n>-<writeId>.c001.<ext>`, is kept as well as one introduced with a hyphen. Spark 4.0.2's `SingleDirectoryDataWriter` names its files `-c%03d`, and its `BaseDynamicPartitionDataWriter` `.c%03d`, as their format strings show. Before this, the period form was taken as an extension and dropped, so the files of a split partition collided again. `filesOfOnePartitionKeepTheFileCounter` covers the period form.

The setting is unset after each split write, so it does not leak into the shared session. Full `library-api`: 322 tests, all passing. The site's I/O page describes the counter names. The Python and R docstrings give only the `c000` example, which is still accurate, so they are unchanged. Checking the Databricks name on a real cluster stays with ticket 28.
