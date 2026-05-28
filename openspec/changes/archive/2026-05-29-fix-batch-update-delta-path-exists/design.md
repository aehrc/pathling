## Context

`UpdateProvider` (single update) and `BatchProvider` (batch) both delegate
persistence to `UpdateExecutor.merge(String resourceCode, List<IBaseResource>)`
(`server/.../update/UpdateExecutor.java:132`). That method:

1. Encodes the resources into a single `Dataset<Row>`.
2. Computes the table path as `<warehouseUrl>/<databaseName>/<Type>.parquet`.
3. Branches on `DeltaTable.isDeltaTable(spark, tablePath)`: - **exists** → optional schema-evolution warmup write (`SaveMode.Append`,
   `mergeSchema=true`) then a Delta `MERGE` on `id`. - **does not exist** → `updates.write().format("delta")
.mode(SaveMode.ErrorIfExists).save(tablePath)`.

The batch path already groups same-type entries correctly: `BatchProvider`
accumulates resources into a `Map<ResourceType, List<...>>` and calls `merge`
once per type (`BatchProvider.java:250-256`). So a two-patient batch is a single
`merge` call with a two-element list - the grouping is not the defect.

The failure observed in `BatchOperationIT.testBatchUpdateMultiplePatients` is a
`DeltaAnalysisException: [DELTA_PATH_EXISTS] Cannot write to already existent
path .../delta/Patient.parquet without setting OVERWRITE = 'true'`, which
originates from the `ErrorIfExists` branch (`UpdateExecutor.java:190`). That
branch only runs when `isDeltaTable` returns `false`, yet the directory clearly
exists - so the create-versus-merge decision and the on-disk state have diverged.

Two contributing factors are visible in the test harness and must be separated
from the production defect during implementation:

- `BatchOperationIT` copies test data in `@BeforeEach` and then again inside the
  test body (`copyTestDataToTempDir` called twice), and the committed test-data
  directory `.../test-data/bulk/fhir/delta/` contains a stray nested `delta/`
  directory. Both are signs of warehouse-setup pollution that can leave the
  `Patient.parquet` path in a state where `isDeltaTable` returns `false` while
  the directory exists.

## Goals / Non-Goals

**Goals:**

- A batch bundle with multiple same-type `PUT` updates returns `200 OK` and
  persists every resource.
- `UpdateExecutor.merge()` never returns a 500 caused by `DELTA_PATH_EXISTS`.
- The regression test reflects real server behaviour with clean warehouse setup.

**Non-Goals:**

- No change to the FHIR API surface, request/response shapes, or supported
  operations.
- No change to schema-evolution (`mergeSchema`) behaviour.
- No broader refactor of `UpdateExecutor` or the provider classes beyond what
  the fix requires.

## Decisions

### Decision: Confirm the mechanism with a local reproduction before changing production code

Per the project's test-driven workflow, the first step is to reproduce the
failure locally and confirm precisely why `isDeltaTable` returns `false` while
the path exists (candidates: duplicated test-data copy leaving a corrupted or
partially written table; `file://` vs `file:/` path-scheme normalisation between
the `isDeltaTable` check and the write; the stray nested `delta/` directory).
Only once the mechanism is confirmed do we choose the minimal production fix.

Rationale: the create-versus-merge branch is shared by single and batch updates;
changing save-mode semantics blind risks masking a test-only problem or
introducing data-loss. Confirming first keeps the fix minimal and correct.

### Decision: Make table creation idempotent rather than relying on `SaveMode.ErrorIfExists`

If the production code is genuinely at fault, replace the fragile
`SaveMode.ErrorIfExists` create branch with a path that tolerates an existing
directory - for example, resolving the table with `DeltaTable.createIfNotExists`
/ `forPath` semantics, or selecting an append/merge path when a directory is
present. `ErrorIfExists` is the wrong contract for an upsert: an update must
succeed regardless of whether the table was created by a prior request.

Alternatives considered:

- **`SaveMode.Overwrite` in the create branch** - rejected: would silently
  destroy existing rows if `isDeltaTable` ever misreports an existing populated
  table, turning a 500 into data loss.
- **Catch `DELTA_PATH_EXISTS` and retry as a merge** - acceptable fallback, but
  exception-driven control flow is less clear than a correct up-front branch.

### Decision: Clean up the test harness regardless of the production fix

Remove the duplicated `copyTestDataToTempDir` call from the failing test, and
remove the committed nested `delta/delta` pollution from the test-data
directory. If the duplicated copy turns out to be the sole cause, this is the
fix; if not, it removes a confound and hardens the regression test.

## Risks / Trade-offs

- **The fix masks a test-only problem and ships dead production code** → confirm
  the mechanism first (Decision 1); only modify production code if the failure
  reproduces against a cleanly set-up warehouse.
- **Changing the create branch causes data loss on an existing table** → never
  use `Overwrite`; preserve existing rows via merge/append semantics and assert
  row counts in the regression test.
- **Removing committed test data breaks other integration tests that rely on it**
  → grep for other consumers of `test-data/bulk/fhir/delta` before deleting the
  nested directory; run the affected suites.

## Migration Plan

No runtime migration. The change is internal to the server module. Deployment is
the normal server release; rollback is reverting the commit. No data format or
configuration changes.

## Open Questions

### Resolved: confirmed root cause and existing production fix

The failure was caused by the pre-fix `deltaTableExists` implementation, which
called the sessionless `DeltaTable.forPath(String)` inside a try/catch. That
overload relies on `SparkSession.active()`; in the server request context it
could fail to resolve and throw, so `deltaTableExists` wrongly returned `false`
for an existing, valid Delta table. The create branch then ran
`SaveMode.ErrorIfExists` against a directory that already held a real Delta
table, producing `DELTA_PATH_EXISTS`.

This was already fixed on `release/9.7.0` by commit `045af5926e` ("fix: Support
custom resource types and fix Delta Lake table detection"), which switched
detection to `DeltaTable.isDeltaTable(spark, tablePath)` with an explicit
`SparkSession`. As a result the production fix described under Decision 2 is not
required: the branch HEAD already passes `BatchOperationIT` (8/8) and the
`DELTA_PATH_EXISTS` state could not be reproduced through any path, including a
target directory that exists with parquet data but no `_delta_log` (Delta 4.0's
`ErrorIfExists` initialises a fresh log alongside the untracked files and
completes the write).

The change is therefore re-scoped to: a regression test guarding the
"target path exists but is not a recognised Delta table" requirement
(`UpdateExecutorPathExistsTest`), strengthened batch-update assertions
(retrievability and pre-existing row preservation), and removal of the
test-harness confounds.

### Resolved: the stray nested `delta/` directory

The entire `server/src/test/resources/test-data` tree is gitignored and
regenerated from NDJSON during the build, so the stray `delta/jobs` directory is
local, untracked bulk-export pollution rather than committed data. It is not
referenced by any test (the only consumers of the read-only delta path are
`TestDataSetup` and `DisabledOperationIT`, neither of which reads `delta/jobs`).
Rather than rely on deleting a local directory, `copyTestDataToTempDir` now
copies only the `*.parquet` table directories, so such pollution can no longer
leak into the warehouse under test.
