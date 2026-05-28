## Why

A FHIR batch bundle that updates two or more resources of the same type returns
`500 Internal Server Error` instead of `200 OK`. The server's
`BatchOperationIT.testBatchUpdateMultiplePatients` integration test has been
failing on the `release/9.7.0` branch for this reason, blocking the release. The
underlying write path is fragile: when the target Delta table directory exists
on disk but is not recognised as a Delta table, the update fails with
`DELTA_PATH_EXISTS` rather than completing the write.

## What Changes

Investigation confirmed the production defect was already fixed on
`release/9.7.0` by commit `045af5926e`, which replaced the fragile sessionless
`DeltaTable.forPath` detection with `DeltaTable.isDeltaTable(spark, path)`. The
`DELTA_PATH_EXISTS` failure could not be reproduced against the current branch
through any path. The change is therefore re-scoped to lock in the fixed
behaviour and remove the test-harness confounds:

- Add regression coverage proving a batch bundle containing multiple updates to
  the same resource type returns `200 OK`, persists every resource via a single
  merge, leaves both resources retrievable, and preserves pre-existing rows.
- Add an in-process regression test (`UpdateExecutorPathExistsTest`) proving the
  write recovers when the target path exists but is not a recognised Delta
  table.
- Remove the redundant, duplicated test-data copy and stop the stray nested
  `delta/jobs` test-data pollution from leaking into the warehouse, so the
  regression tests reflect real server behaviour.

This is a bug fix with no production code change required and no change to the
public FHIR API contract: the batch and update operations already promise
success on valid input, and the implementation already honours that promise on
this branch.

## Capabilities

### New Capabilities

- `resource-update-persistence`: How the server persists resources from FHIR
  update (`PUT`) and batch operations into Delta tables, including grouping
  multiple same-type resources into a single merge and recovering when the
  target table path exists but is not a recognised Delta table.

### Modified Capabilities

<!-- None. No existing spec captures this behaviour. -->

## Impact

- `server/src/test/java/au/csiro/pathling/operations/update/BatchOperationIT.java`
    - hardened the multiple-patients regression test (retrievability and
      pre-existing row preservation) and moved warehouse setup to `@BeforeEach`.
- `server/src/test/java/au/csiro/pathling/operations/update/UpdateExecutorPathExistsTest.java`
    - new in-process regression test for the path-exists recovery requirement.
- `server/src/test/java/au/csiro/pathling/util/TestDataSetup.java`
    - `copyTestDataToTempDir` now copies only the `*.parquet` table directories
      and cleans the destination first, so pollution cannot leak in.
- No production code, dependency, or configuration changes.
