## Context

The runner loads each distinct subject once and measures every case over its loaded
subject (`SofBenchmarkRunner.runCases` / `sof_runner.run_cases`). Neither loop has a
per-case boundary today: `harness.measure(...)` / `measure_case(...)` is called
directly in the loop, so a throw anywhere in load-or-measure propagates out of
`runCases` and aborts the whole run — voiding every other case's timings. Wave 2
(#8) forbids this for the report path.

The upstream reference runner encodes the pattern with two deliberately asymmetric
paths: `buildReport` runs each case under a boundary (record-and-continue) while
`blessCheckfile` runs all-or-nothing (a bless must never lock a partial run). We
only implement the report path — the checkfile is authored upstream and we never
bless — so we need only the record-and-continue behaviour.

## Goals / Non-Goals

**Goals**
- One failing case is recorded (`execution_error` + advisory `message`) and the run
  continues; the report still validates and contains every case that ran.
- Java and Python stay at parity: same boundary semantics, same `message` wiring.

**Non-Goals** (accepted #22 clarifications)
- No wall-clock budget and no `timeout` detection.
- No `malformed` classification — a lazy, strongly-typed Spark engine cannot cheaply
  distinguish an unparseable input from an engine evaluation error at the catch site,
  and the spec declares best-effort `execution_error` conformant.
- No `--case`/subset filtering flag.
- No re-bless: data, counts, and checkfile sha256 locks are untouched.

## Decisions

### Where the boundary goes

Wrap the per-case body — the subject load (if not already cached) plus
`measure(...)` — in a try/catch inside the case loop. A throw is converted to a
failed `CaseResult` (`execution_error` + `message`) and appended like any other
outcome; the loop proceeds.

The subject-load-once optimisation stays: a subject is loaded lazily on first use
and cached. If a subject's load throws, only cases on that subject fail — but each
fails independently at its own boundary rather than the load throwing once and
aborting. The simplest correct form re-attempts (cheap, cached) or records the load
failure per case; because load is memoised per subject, a load that throws will
throw again for the next case on that subject and each is recorded independently,
which satisfies the "each recorded independently" requirement without special-casing.

### The `message` field

`CaseResult` gains an OPTIONAL `message` (Java) and the Python result dict gains a
`message` key. `ReportWriter` / `build_report` emit it only when the status is not
`ok` (an `ok` case omits it, per the schema's advisory-context intent). Kept free
text; never machine-parsed.

`CorrectnessGuard` is unchanged: it still decides `ok` vs `count_mismatch` for cases
that ran. The failure statuses come from the catch site, not from the guard — status
now has two independent sources (the count guard for cases that completed, the catch
boundary for cases that threw), which is exactly the taxonomy's shape.

### Vendored schema refresh

The test copy `sof-benchmark/src/test/resources/contract-v2/benchmark-report.schema.json`
is refreshed to the Wave-2 version. The change is additive (enum gains `timeout`,
`malformed`; a new optional `message` property), so existing reports still validate;
the refresh is what lets a new test assert that an `execution_error` + `message`
report validates.

## Risks / Trade-offs

- **Re-attempting a failing subject load per case** does a little redundant work when
  a whole subject is unloadable. Acceptable: it keeps the loop uniform (every case
  recorded independently) and these are tiny benchmark inputs; the alternative
  (short-circuit all cases of a broken subject) adds special-case code for a
  degenerate path.
- **`message` free-text** may carry an exception string; it is advisory only and the
  schema forbids machine-parsing it, so no stability contract applies.

## Migration Plan

Additive and backward-compatible for reports that never fail a case: a run with all
cases `ok` emits a byte-comparable report (no `message` keys). No data or checkfile
change, so no re-bless. Submodule pointer bump `be7816c` → `35c3722` ships with the
change.

## Open Questions

None — the #22 clarifications resolved the taxonomy scope, the timeout budget, the
malformed boundary, and the subset-filter question before implementation.
