# Benchmark baseline

The pre-change measurement that driver 1 is judged against (T001). Captured
before any module moved, so that the comparison in M4 (T135) measures the
programme rather than the machine.

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
it. The per-phase baseline recorded by T003 is the one M4 compares against; this
table is the whole-pipeline reference point and the proof that the suite was
green before the programme began.
