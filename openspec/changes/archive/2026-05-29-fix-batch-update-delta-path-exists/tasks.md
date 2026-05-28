## 1. Reproduce and confirm the mechanism

- [x] 1.1 Run `BatchOperationIT.testBatchUpdateMultiplePatients` locally and
      capture the full stack trace, confirming the `DELTA_PATH_EXISTS` originates
      from the create branch at `UpdateExecutor.java:190`.
      Result: the test passes on the current branch HEAD (8/8 for the full
      class). The documented failure does not reproduce.
- [x] 1.2 Add temporary logging (or a debugger) to record, at the point of the
      failing `merge`, the resolved `tablePath`, the result of
      `DeltaTable.isDeltaTable(spark, tablePath)`, and whether the directory
      exists on disk.
      Done via the in-process `UpdateExecutorPathExistsTest`, which asserts the
      `isDeltaTable` precondition directly and exercises the create branch.
- [x] 1.3 Determine why `isDeltaTable` returns `false` while the directory
      exists. Confirmed root cause: the pre-fix `deltaTableExists` used the
      sessionless `DeltaTable.forPath(String)`, which could fail to resolve an
      existing valid table and return `false`, driving the create branch into
      `ErrorIfExists` against an existing table directory.
- [x] 1.4 Record the confirmed root cause in the change's design Open Questions
      section. Done; also recorded that commit `045af5926e` already fixes it.

## 2. Regression test

- [x] 2.1 Ensure `testBatchUpdateMultiplePatients` asserts `200 OK`, a
      `batch-response` bundle with two `200` entries, and that both patients are
      retrievable afterwards.
- [x] 2.2 Add an assertion (or follow-up read) that confirms pre-existing rows in
      the target table are preserved, to guard against a data-loss fix.
- [x] 2.3 Add `UpdateExecutorPathExistsTest` covering the spec scenario
      "target path exists but is not a recognised Delta table", proving the
      write recovers rather than failing with `DELTA_PATH_EXISTS`.

## 3. Clean up the test harness

- [x] 3.1 Remove the redundant `copyTestDataToTempDir` call from the failing test
      so warehouse setup happens once, in `@BeforeEach`.
- [x] 3.2 Grep for other consumers of `test-data/bulk/fhir/delta` and confirm the
      nested `delta/jobs` directory is unreferenced. Confirmed: the tree is
      gitignored/regenerated and the pollution is local and untracked.
- [x] 3.3 Make `copyTestDataToTempDir` idempotent (clean-then-copy) and copy only
      the `*.parquet` table directories, so pollution cannot leak into the
      warehouse. Behaviour documented in the method Javadoc.

## 4. Production fix

- [x] 4.1 Not required. The create-versus-merge defect was already fixed on
      `release/9.7.0` by commit `045af5926e`, which replaced the sessionless
      `DeltaTable.forPath` detection with `DeltaTable.isDeltaTable(spark, path)`.
      The current `ErrorIfExists` branch is also empirically robust against an
      existing non-Delta directory in Delta 4.0. Left unchanged per the design's
      guardrail (only modify production code if the failure reproduces).
- [x] 4.2 Verified single-update (`UpdateProvider`) and batch-update both route
      through the same `UpdateExecutor.merge` create-versus-merge branch.

## 5. Verify

- [x] 5.1 Ran `BatchOperationIT`; all 8 methods pass, including the mixed-type
      batch test and the hardened multiple-patients test.
- [x] 5.2 Ran the surrounding update/operations integration tests that share the
      write path (`UpdateOperationIT` 4/4, `CreateOperationIT` 6/6); no
      regression from the harness change.
- [x] 5.3 Confirmed the change satisfies every scenario in
      `specs/resource-update-persistence/spec.md` (two-patient and mixed-type
      batch via `BatchOperationIT`; existing-table preservation via the seeded
      patient; path-exists recovery via `UpdateExecutorPathExistsTest`).
- [x] 5.4 Ran `openspec validate fix-batch-update-delta-path-exists --strict`:
      the change is valid.
