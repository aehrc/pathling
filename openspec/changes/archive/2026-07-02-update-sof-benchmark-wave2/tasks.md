## 1. Contract sync and test fixtures

- [x] 1.1 Bump the `sql-on-fhir` submodule pointer `be7816c` → `35c3722` and stage it with this change.
- [x] 1.2 Refresh the vendored `sof-benchmark/src/test/resources/contract-v2/benchmark-report.schema.json` to the Wave-2 version (adds `timeout`, `malformed`, `message`).

## 2. Per-case failure isolation (Java, test-first)

- [x] 2.1 Write a failing test: a run with one throwing case and one succeeding case records the failing case as `execution_error` with a `message`, still measures the other case, and emits a schema-valid report containing both.
- [x] 2.2 Add an optional `message` to `CaseResult` (Nullable, omitted for `ok`); keep the existing count-based status path unchanged.
- [x] 2.3 Wrap the per-case body in `SofBenchmarkRunner.runCases` in a boundary: on a throw, record a failed `CaseResult` (`execution_error` + advisory `message`) and continue the loop.
- [x] 2.4 Emit `message` in `ReportWriter` only for a non-`ok` case; confirm an all-`ok` report omits it.

## 3. Per-case failure isolation (Python, test-first)

- [x] 3.1 Write failing pytest cases mirroring 2.1: record-and-continue with `execution_error` + `message`, partial report validates.
- [x] 3.2 Add a `message` key to the Python result dict and a per-case boundary in `run_cases`; emit `message` in `build_report` only for a non-`ok` case.
- [x] 3.3 Extend the Java/Python parity assertion so a failed-case report differs only by `implementation.binding`, environment and timings.

## 4. Docs and verification

- [x] 4.1 Simplify `sof-benchmark/python/README.md`: drop the manual Synthea-jar download step (upstream #10 auto-fetches and checksum-verifies the pinned jar); note the auto-fetch on-ramp.
- [x] 4.2 Run the Java build/tests (`mvn -pl sof-benchmark -am test`) and the Python runner tests; confirm both emit schema-valid reports including a failed-case run.
- [x] 4.3 `openspec validate update-sof-benchmark-wave2 --strict` passes.
