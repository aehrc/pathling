## Why

The SQL-on-FHIR benchmark contract advanced to **Wave 2** (submodule `35c3722`),
which mandates **per-case failure isolation**: one failing case MUST NOT abort the
run or void the other cases' recorded results (upstream #8). The report status
taxonomy gains `timeout` and `malformed` and an OPTIONAL per-case `message`
(additive to `benchmark-report.schema.json`). The current `sof-benchmark` runner
has no per-case boundary — both the Java `runCases` and the Python `run_cases`
abort the whole run on the first throwing case — so it does not satisfy the new
contract.

Our Wave-2 implementer feedback (upstream #22) was accepted into the spec
(`35c3722`): `execution_error` is the **always-conformant default** for any
load/prepare/evaluate failure; `timeout`/`malformed` are OPTIONAL best-effort
refinements a runner MAY skip; there is no authored timeout budget; and subset
filtering (`--case`) is optional convenience, not an obligation. This bounds our
work to failure isolation plus an advisory `message`, with no timeout, no
`malformed` classification, no `--case` flag, and no re-bless.

## What Changes

- **Per-case failure isolation** in both runners: each case is measured under its
  own boundary; a load/prepare/evaluate failure is recorded with
  `status: execution_error` and an advisory `message`, and the run CONTINUES to the
  remaining cases. A failing case never aborts the run nor changes another case's
  recorded status; a partial run still emits a schema-valid report.
- **Advisory `message` field**: `CaseResult` (Java) and the Python result dict gain
  an optional `message`; `ReportWriter` emits it only for a non-`ok` case and omits
  it for `ok`.
- **Refresh the vendored report schema** to the Wave-2 version (adds `timeout`,
  `malformed`, and `message`; additive) so report-validation tests exercise an
  `execution_error` + `message` report.
- **Runbook simplification**: drop the manual Synthea-jar download step from
  `sof-benchmark/python/README.md`; upstream #10 now auto-fetches and
  checksum-verifies the pinned jar.
- Submodule pointer bump `be7816c` → `35c3722` accompanies this change.

Non-goals (explicitly out of scope per the accepted #22 clarifications):
`timeout` detection and any wall-clock budget; `malformed` classification;
`--case`/subset filtering; any re-bless of data or the checkfile.

## Capabilities

### New Capabilities
<!-- None. This change modifies the existing sof-benchmark-runner capability. -->

### Modified Capabilities
- `sof-benchmark-runner`: add a per-case failure-isolation requirement
  (record-and-continue with `execution_error` + advisory `message`, partial-report
  validity) and extend the report requirement so the emitted report carries the
  Wave-2 status taxonomy and the optional `message`.

## Impact

- **Code (Java)**: `SofBenchmarkRunner` (per-case boundary in `runCases`),
  `MeasurementHarness` / `CaseResult` (carry `message`), `ReportWriter` (emit
  `message` for non-`ok` cases).
- **Code (Python)**: `python/sof_runner.py` (per-case boundary in `run_cases`,
  `message` in the result dict and report).
- **Tests**: refresh the vendored `benchmark-report.schema.json`; add Java + pytest
  cases for record-and-continue and for an `execution_error` + `message` report.
- **Contract**: consumes the Wave-2 schemas from the `sql-on-fhir` submodule at
  `35c3722` (pointer bump accompanies this change).
- **Docs**: `sof-benchmark/python/README.md` runbook.
- No change to the core Pathling library API; confined to the `sof-benchmark` module.
- No re-bless: data, counts, and the checkfile `sha256` locks are byte-identical.
