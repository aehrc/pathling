# Spark 4.3.0 compatibility of the Pathling core

## Verdict

The current Pathling core does not run on Spark 4.3.0 without build and source changes. Built
against 4.3.0 as it stands (`-Dpathling.sparkVersion=4.3.0`), `terminology` does not compile and
the modules that do compile fail at runtime. The four things required are listed below, each with
its evidence. The fhirpath, SQL on FHIR compliance and YAML conformance results on 4.3.0 remain
unknown, because the Scala errors mask them.

Where the view engine does run, its results are the same: 68 real SQL on FHIR view runs gave
identical output on 4.0.2 and 4.3.0 (step 1). That run used 4.0.2-built Pathling jars, so it shows
the binary-level behaviour of the view engine on 4.3.0, not a build against 4.3.0.

## What is required

1. **Netty pin.** Spark 4.3.0 needs Netty 4.2.x, and the root pom pins `pathling.nettyVersion` to
   4.1.137.Final. Evidence: with the pin, every test that starts a SparkContext fails in encoders
   and io with `NoClassDefFoundError io/netty/channel/kqueue/KQueueIoHandler` (run A, 311 errors in
   encoders, 704 in io). The Spark 4.3.0 classpath carries Netty 4.2.13.Final.
   `-Dpathling.nettyVersion=4.2.13.Final` removes this error (run B). The pin exists for security
   reasons (pom comment), so the replacement version needs a decision.
2. **Source change in `terminology`.** `SnomedRf2Importer.rowCount` (line 472) awaits
   `observation.future()` into a `scala.collection.immutable.Map<String, Object>`. In Spark 4.3.0
   `Observation.future()` returns `Future[Row]` (4.0.2: `Future[Map[String, Any]]`, checked with
   `javap` on the two `spark-sql-api` jars). Evidence: the module fails to compile on 4.3.0 with
   `incompatible types: inference variable T has incompatible bounds` (run A). The 3-line change
   in `specs/001-parquet-on-fhir/evidence/scripts/annotations-design/spark-4-3-results/observation-4-3.patch` compiles; it was applied only in
   the scratchpad copy. Note that the patched code does not compile on 4.0.2, so a source change
   needs a form that works on both, or a version-specific source set.
3. **Scala pin (inferred, NOT confirmed by a rerun).** The root pom pins
   `pathling.scalaFullVersion` to 2.13.16, and Spark 4.3.0 ships Scala 2.13.18 (its classpath has
   `scala-library` 2.13.18). Evidence: after fixing 1 and 2, most tests still fail with
   `NoSuchMethodError scala.util.hashing.MurmurHash3$.caseClassHash$default$3` (about 2,600 in
   fhirpath, 411 in encoders, 698 in io, 39 in terminology, 211 in library-api). The cause is an
   inference from this error and the two Scala versions. I did not rerun with
   `-Dpathling.scalaFullVersion=2.13.18`, so it is not known that the bump fixes it.
4. **Delta.** Delta 4.0.0 is built for Spark 4.0 and fails on 4.3.0: `NoClassDefFoundError` for
   `catalyst.expressions.TimeAdd` and `plans.logical.IgnoreCachedData`, and a
   `CatalogStorageFormat.copy` `NoSuchMethodError` (about 10 terminology and 13 library-api tests
   in run B). No Delta release targets Spark 4.3: `io.delta:delta-spark_2.13` 4.3.0 depends on
   Spark 4.1.0 and 4.4.1 (the latest) on Spark 4.2.0 (POMs read on Maven Central; versions listed
   there: 4.0.0, 4.0.1, 4.1.0, 4.2.0, 4.3.0, 4.3.1, 4.4.0, 4.4.1). No newer Delta was tried.

## What is unknown

The Scala errors (3) hide the fhirpath, SQL on FHIR compliance and YAML conformance results on 4.3.0.
In run B, 4,525 of the fhirpath module's tests error with the `MurmurHash3` `NoSuchMethodError`,
so it cannot be said whether any Spark 4.3.0 behaviour change affects them. The same holds for the
Delta-dependent behaviour. These remain unknown.

## Setup

- Machine: 11 cores, Java 21.0.3, Maven 3.9+, default `pathling.testForkCount`.
- Isolated copy: `rsync -a --exclude target --exclude .local --exclude node_modules` of the
  worktree into `.../scratchpad/pathling-spark43/`. Its `.git` file was replaced by a fresh
  `git init` plus one commit, because the license plugin failed to relativize paths against the
  worktree's git directory. `sql-on-fhir/` was cloned from
  `https://github.com/FHIR/sql-on-fhir.js.git` at `7969489777b72ea87e866c320c461f7a13df9388`.
- No `install` or `deploy` was run, and no tracked file in the worktree was edited. The reactor
  build with `-am` never reads Pathling artifacts from `~/.m2`.
- Baseline: `mvn clean verify -pl library-api -am -fae -Dmaven.test.failure.ignore=true -B`
  (4.0.2 is the default). `verify` was used instead of a separate `mvn test -pl fhirpath -am`
  because it runs the same tests plus step 3 in one pass. No `-PsofComplianceReport`.
- Run A (4.3.0 as specified): the same command plus `-Dpathling.sparkVersion=4.3.0`.
- Run B (4.3.0 with workarounds): the same plus `-Dpathling.nettyVersion=4.2.13.Final
  -Dspotless.check.skip=true`, with the patch applied to the copy's `SnomedRf2Importer.java`.
  The Netty override and the patch were applied in the scratchpad copy only, to see how far the
  build gets; they are not changes to the repository.
- Raw logs: `.local/work/issue/2832_41-42_decimal-and-annotations/spikes/spark-4-3-compat/`
  (`mvn-4.0.2.log`, `mvn-4.3.0-netty41-pinned.log` for run A, `mvn-4.3.0-patched-netty42.log` for
  run B, `surefire-*` XML copies, `summary-*.txt`, and `results/` for step 1). In run A's Surefire
  copy, the fhirpath and library-api directories are stale copies from the baseline build, because
  those modules were skipped.

## Step 1: results across versions

Program: `specs/001-parquet-on-fhir/evidence/scripts/annotations-design/spark-4-3-results/`
(`ResultsDump.java`, `run.sh`, `compare.sh`, `compare.py`). It reuses the view definitions of the
rewrites-layer1 spike, runs the ORIGINAL plan only, and writes each view's output schema and all
sorted rows. Both versions read the same Parquet on FHIR tables (the `data-4.0` engine and edge
tables, plus the singular-meta small set).

Input check (on 4.3.0, data-4.3 against data-4.0): the `engine` and `edge` tables have equal
schemas, 22 and 66 rows, and `except` in both directions is 0. The encoded inputs are identical.

Result: 68 views (32 main set, 4 singular-meta, 32 edge set), 68 equal, 0 different, no errors:
output schema and every row are the same. The per-view list is in
`.local/work/issue/2832_41-42_decimal-and-annotations/spikes/spark-4-3-compat/results/comparison.txt`.

Limit: the run used the 4.0.2-built Pathling jars on the 4.3.0 Spark jars. It shows the binary-level
behaviour of the view engine, not that Pathling builds against 4.3.0.

## Steps 2 and 3: test counts (run / failures / errors / skipped)

| Module | Baseline 4.0.2 | Run A (4.3.0 as specified) | Run B (Netty 4.2 + Observation patch) |
|---|---|---|---|
| utilities | 45 / 0 / 0 / 0 | 45 / 0 / 0 / 0 | 45 / 0 / 0 / 0 |
| fhir-schema | 54 / 0 / 0 / 0 | 54 / 0 / 0 / 0 | 54 / 0 / 0 / 0 |
| encoders | 599 / 0 / 0 / 0 | 421 / 0 / 311 / 0 | 565 / 4 / 411 / 0 |
| io | 788 / 0 / 0 / 1 | 754 / 10 / 704 / 1 | 754 / 10 / 698 / 1 |
| terminology | 538 / 0 / 0 / 0 | compile failure | 413 / 4 / 49 / 0 |
| fhirpath | 7515 / 0 / 0 / 940 | not run | 6663 / 0 / 4525 / 90 |
| library-api | 342 / 0 / 0 / 0 | not run | 299 / 8 / 222 / 0 |

The baseline was BUILD SUCCESS in 13 min 52 s with no failures. Run B reports fewer tests than the
baseline in several modules because classes that fail during set-up are not counted test by test,
so its skipped and run counts are not comparable with the baseline's.

Classification (every failure on 4.3.0 passes on 4.0.2, so none is pre-existing):

| Cause | Class | Where |
|---|---|---|
| Netty 4.1 pin (`KQueueIoHandler`) | (ii) Spark 4.3 change hitting a Pathling build pin | run A, encoders and io |
| `Observation.future()` type change | (ii) Spark 4.3 API change affecting Pathling source | run A, terminology compile error |
| Scala 2.13.16 pin, `MurmurHash3.caseClassHash$default$3` (inferred, not confirmed) | (ii) Pathling build pin against Spark 4.3 | run B, all modules except utilities and fhir-schema |
| `TimeAdd`, `IgnoreCachedData`, `CatalogStorageFormat.copy` | (i) Delta 4.0.0 incompatibility | run B, terminology and library-api |
| Assertion errors "expected X but was NoSuchMethodError/NoClassDefFoundError" | secondary, from the causes above | run B |

## Not covered

Python, R, server, `library-runtime`, Databricks, any Spark version between 4.0.2 and 4.3.0, the
`-PsofComplianceReport` profile (deliberately not used), and any rerun after the Scala or Delta
versions are changed (not done, by decision).

## Reproduce

1. Step 1: `bash specs/001-parquet-on-fhir/evidence/scripts/annotations-design/spark-4-3-results/compare.sh`
   (needs the rewrites-layer1 spike builds and data; it prints a `SUMMARY` line).
2. Copy the worktree as described under Setup, clone the submodule, and `git init` the copy.
3. `mvn clean verify -pl library-api -am -fae -Dmaven.test.failure.ignore=true` with the properties
   of the run above. For run B apply
   `specs/001-parquet-on-fhir/evidence/scripts/annotations-design/spark-4-3-results/observation-4-3.patch`
   to the copy's `terminology/src/main/java/au/csiro/pathling/terminology/store/SnomedRf2Importer.java`
   (the patch was produced with paths relative to the scratchpad; use `patch <file> <patchfile>`).
4. Summaries: `python3 -I .../spark-4-3-results/surefire_summary.py <reports> --failures <baseline>`
   over a directory holding one subdirectory of Surefire XML per module.
