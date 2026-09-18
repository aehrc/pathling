# Benchmark baseline

The pre-change measurement that driver 1 is judged against (T001). Captured
before any module moved, so that the comparison at the flip (T136, in M4)
measures the programme rather than the machine.

## Conditions

Reproducing this measurement requires all of the following to match, because a
comparison against a differently-scoped or differently-flagged run is not a
comparison.

| | |
| --- | --- |
| Commit | `b5fe0679d6` (documentation only; code-identical to `main`) |
| Machine | Apple M3 Pro, 11 cores, 36 GB, macOS 26.6.2 |
| JVM | OpenJDK 21.0.3+7-LTS-152, HotSpot 64-Bit Server VM |
| Spark | 4.0.2 (Scala 2.13), Delta 4.0.0, Hadoop 3.4.1 |
| Pathling | 9.9.0 |
| JMH | 1.37 |
| Forks | 2 per benchmark (`@Fork(2)`), 28 forks total |
| Warmup | 1 iteration x 5 s |
| Measurement | 3 iterations x 5 s |
| Mode | `SampleTime`, ms/op |
| Spark master | `local[*]` (11 cores) |
| Wall clock | 15 m 03 s, no failures |

`b5fe0679d6` is on `issue/2367` rather than `main`. It is a valid stand-in:

    git diff --name-only main..issue/2367 -- '*.xml' '*.java' '*.scala' '*.py' ':!specs/**'
    → 0 files

The branch differs from `main` only in `specs/`, `openspec/`, `.claude/CLAUDE.md`
and one `/.local/` line in `.gitignore`. None is compiled or on the classpath.
Measuring in the worktree where M4's comparison will also run serves the
same-machine requirement better than measuring in a separate `main` checkout.

## Invocation

    java -Xshare:off -Xmx8g -ea -Duser.timezone=UTC \
      --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
      --add-opens=java.base/java.net=ALL-UNNAMED \
      -jar benchmark/target/benchmark-9.9.0.jar 'PathlingBenchmark' \
      -rf json -rff baseline-b5fe0679d6.json

Two departures from `.github/workflows/benchmark.yml`, both of which the
comparison run must repeat:

- **Scoped to `PathlingBenchmark`.** The shaded jar also carries
  `TerminologyBenchmark`, which the workflow's bare invocation runs and which
  fails in its first warmup iteration without
  `-Dpathling.benchmark.terminology.storagePath`. T001 names `PathlingBenchmark`.
- **`-Xmx8g`** as the workflow uses, not the `-Xmx4g` of the Surefire `argLine`.

The Java 21 `--add-exports`/`--add-opens` flags are required: Spark fails without
them. JMH propagates the launcher's arguments to forked JVMs by default, so they
reach each fork.

## Results

Sample time, milliseconds per operation, lower is better. `n` is the sample count.

| Benchmark | Source | n | Mean (ms/op) | Error (99.9%) |
| --- | --- | ---: | ---: | ---: |
| `conditionFlat` | ndjson | 173 | 177.853 | ± 3.419 |
| `conditionFlat` | delta | 271 | 112.363 | ± 2.170 |
| `encounterFlat` | ndjson | 183 | 166.991 | ± 3.309 |
| `encounterFlat` | delta | 246 | 123.160 | ± 2.396 |
| `patientAddresses` | ndjson | 698 | 43.238 | ± 1.236 |
| `patientAddresses` | delta | 436 | 69.439 | ± 1.978 |
| `patientAndContactAddressUnion` | ndjson | 436 | 69.437 | ± 0.875 |
| `patientAndContactAddressUnion` | delta | 383 | 79.066 | ± 1.273 |
| `patientDemographics` | ndjson | 619 | 48.729 | ± 1.231 |
| `patientDemographics` | delta | 364 | 83.298 | ± 2.022 |
| `questionnaireResponseFlat` | ndjson | 58 | 556.911 | ± 10.985 |
| `questionnaireResponseFlat` | delta | 52 | 612.308 | ± 18.238 |
| `usCoreBloodPressures` | ndjson | 28 | 1168.301 | ± 127.802 |
| `usCoreBloodPressures` | delta | 85 | 364.874 | ± 11.549 |

Full JMH output, including every percentile, is in `baseline-b5fe0679d6.json`.

## What this baseline cannot answer

Each figure times an entire NDJSON-or-Delta-to-view pipeline as one number, so it
cannot separate encode from decode from query execution, nor planning from
execution. Driver 1 is stated in those terms, so **this measurement alone cannot
answer it either way** — which is why T002 splits the benchmark and T003 re-runs
it. The per-phase baseline recorded by T003 is the one T136 compares against; this
table is the whole-pipeline reference point and the proof that the suite was
green before the programme began.

---

# Per-phase baseline

The measurement T136 compares against (T003), taken after T002 split the suite.
Same machine, same session, same JMH settings as the whole-pipeline table above.

**This is not a re-measurement of that table.** It is a different decomposition
of the same work, so the two are not row-comparable and no figure here should be
subtracted from one there.

## Conditions

As above, with these differences:

| | |
| --- | --- |
| Commit | `4bfadac552` |
| Benchmarks | `encode`, `decode`, `viewPlanning`, `viewExecution` |
| Parameters | `resourceType` (5) for encode/decode; `view` (7) x `sourceType=delta` for the query pair |
| Forks | 48 (24 combinations x 2) |
| Wall clock | 23 m 35 s, no failures |

    java -Xshare:off -Xmx8g -ea -Duser.timezone=UTC \
      --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
      --add-opens=java.base/java.net=ALL-UNNAMED \
      -jar benchmark/target/benchmark-9.9.0.jar 'PathlingBenchmark' \
      -rf json -rff baseline-phases-4bfadac552.json

### Two deliberate departures from the whole-pipeline run

**The query benchmarks are delta-only.** A `DatasetSource` holds a *lazy* encode
over raw text, so an "ndjson query" figure is encode plus planning plus
execution — the conflation T002 exists to remove. Encode now has its own
benchmark, which is the honest home for that cost.

**Materialisation is forced by a write, never by `count()`.** The optimiser
prunes the fields of an object serializer that nothing downstream reads, so a
count-forced encode would measure parsing and skip the construction of the very
columns whose width drivers 1 and 4 are about. The `noop` write consumes the
full schema.

The same pruning is why **an encode figure cannot be recovered from the
whole-pipeline table by subtraction**: a view forces only the columns it
selects, so those `ndjson` numbers do not contain a full encode either.

## Results

Sample time, milliseconds per operation, lower is better.

### Encode and decode

| Resource | Encode | Decode | n (enc / dec) |
| --- | ---: | ---: | ---: |
| `Patient` | 83.931 ± 2.115 | 165.723 ± 3.483 | 362 / 185 |
| `Condition` | 178.641 ± 3.966 | 312.629 ± 9.115 | 171 / 99 |
| `Encounter` | 265.577 ± 12.401 | 426.943 ± 20.215 | 116 / 73 |
| `QuestionnaireResponse` | 345.424 ± 9.738 | 824.584 ± 41.351 | 90 / 39 |
| `Observation` | 1004.597 ± 116.286 | 1442.202 ± 142.382 | 34 / 23 |

### Query planning and execution (delta)

| View | Planning | Execution | Plan:exec |
| --- | ---: | ---: | ---: |
| `PatientAddresses` | 50.025 ± 0.988 | 19.612 ± 1.214 | 2.6 |
| `PatientDemographics` | 54.705 ± 0.811 | 20.879 ± 1.477 | 2.6 |
| `PatientAndContactAddressUnion` | 56.807 ± 0.836 | 25.181 ± 2.005 | 2.3 |
| `ConditionFlat` | 62.437 ± 2.646 | 47.544 ± 0.818 | 1.3 |
| `EncounterFlat` | 63.693 ± 1.041 | 65.428 ± 2.398 | 1.0 |
| `UsCoreBloodPressures` | 143.643 ± 2.329 | 205.555 ± 9.128 | 0.7 |
| `QuestionnaireResponseFlat` | 136.004 ± 2.529 | 450.166 ± 19.214 | 0.3 |

Full JMH output is in `baseline-phases-4bfadac552.json`.

## What the split already shows

Three things the whole-pipeline figure could not have told us, all bearing on
driver 1.

**Planning dominates the light views, by more than two to one.** For the three
Patient views, translating the view definition and planning the query costs over
twice what running it does. A change that narrows the schema acts on the number
of columns the analyzer and optimiser handle, so planning is where it should be
expected to show up first — and it is the half that the old single number hid
most completely.

**Planning has a floor near 50 ms** that is close to view-independent. Nothing in
the range 50–64 ms tracks view complexity much; the two expensive views sit
apart at ~140 ms. A per-query fixed cost of that size is worth knowing before
attributing any post-change movement to the layout.

**Decode is consistently the more expensive direction**, running 1.4x to 2.4x
encode for the same resource type, with the ratio widest on
`QuestionnaireResponse`. Both directions are dominated by `Observation`, which is
an order of magnitude above `Patient`.

None of this is gated on: driver 1 is a hypothesis and nothing in the programme
depends on the outcome. It is recorded so that the comparison at the flip
measures the change rather than the machine.
