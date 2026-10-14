# 01 — Keep the file counter so split partition files do not collide on rename

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-042 (appends through every sink). Old task: T046a follow-up (decisions 129, 130).

**What to build:** A write that Spark splits into several files per partition, which `spark.sql.files.maxRecordsPerFile` causes, completes and keeps every row. Today, two files of one partition that differ only in the file counter (`-c000`, `-c001`) are renamed to the same name. The second rename fails after the rows are written. On the local file system the rename is refused and the table is kept. The renamed file must keep the counter in its qualifier, alongside the partition and write ids, for both the Parquet and the NDJSON sinks. Names written by earlier releases, and names without a counter, must still read as the type.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] A test writes one type with `maxRecordsPerFile` small enough to produce several files per partition. It fails before the fix with the rename error and passes after, for Parquet and NDJSON.
- [ ] Every row is read back. The existing save-mode tests (`FileSinkSaveModeTest`, `TypeFilesTest`) pass unchanged.
- [ ] The file source reads the renamed files as the type, and the save modes find them under a name that is not only letters (decision 130).
- [ ] Databricks' name form, `part-<n>-tid-<number>-<writeId>-…`, keeps its partition id, write id and counter. Checking it on a real cluster stays with ticket 28.
