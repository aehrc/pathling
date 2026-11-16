# Spark 4.1: the 10.x and 11 benchmark on Spark 4.0.2 and 4.1.0

The H1 check of ticket 10 for #2879: whether 11 regresses against 10.x, run on
Spark 4.0.2 and again on Spark 4.1.0, without the custom optimiser rule. Both
versions were measured on both Spark versions in one batch on the Petrichor
cluster, with each node running all four cells of its share back to back, so
each comparison is made on one node and does not depend on a baseline from
another machine or another day.

**Result: 11 does not regress against 10.x on Spark 4.1.0, or on Spark 4.0.2.**
On both Spark versions, 11 takes 0.09 to 0.56 of 10.x's time for encode and
decode, and 0.29 to 0.90 for view execution. View planning is the only phase
near parity, at 0.80 to 1.02, and its highest ratio (`UsCoreBloodPressures`,
1.00 to 1.02 on Spark 4.1.0 and 0.99 to 1.00 on 4.0.2) is within the variation
between runs. No regression needs fixing, so no issue is raised and the custom
optimiser rule (#2890) is not needed for this check.

**Moving to Spark 4.1.0 costs about 3% in view planning and little elsewhere,
and it affects 10.x and 11 alike.** Every planning row is 1.00 to 1.09 times its
Spark 4.0.2 time, with a geometric mean of 1.033 for 10.x and 1.030 for 11.
Encode and view execution move by 0.94 to 1.08 with no consistent direction.
Decode is 0.98 to 1.07, and slower on Spark 4.1.0 in all but one of its 20
measurements.

## Conditions

|            | Value                                                                                       |
| ---------- | ------------------------------------------------------------------------------------------- |
| Machine    | Petrichor compute nodes: 2 x AMD EPYC 7543 (64 cores), 503 GB; 32 cores and 64 GB allocated |
| OS         | SUSE Linux Enterprise High Performance Computing 15 SP5                                     |
| JVM        | OpenJDK 21 (Amazon Corretto 21.0.0.35.1)                                                    |
| JMH        | 1.37, `@Fork(2)`, warmup 1 x 5 s, measurement 3 x 5 s, `SampleTime`                         |
| Benchmarks | `encode`, `decode` (5 types); `viewPlanning`, `viewExecution` (7 views, Delta)              |
| Dataset    | `test-data` 1.1.0, `bulk/fhir/*.ndjson`                                                     |
| Batch      | Slurm job 38145073, 2026-11-13 17:36 to 18:12 AEDT, nodes c001, c002, c008, c012, c280      |

The four cells:

| Cell       | Pathling                                   | Spark | Scala   | Delta |
| ---------- | ------------------------------------------ | ----- | ------- | ----- |
| 10 / 4.0.2 | `v10.0.0` (`2e654ed47f`)                   | 4.0.2 | 2.13.16 | 4.0.1 |
| 11 / 4.0.2 | `release/11.0.0` (`56c8e53b1f`)            | 4.0.2 | 2.13.16 | 4.0.1 |
| 10 / 4.1.0 | `v10.0.0` with this branch's build changes | 4.1.0 | 2.13.17 | 4.2.0 |
| 11 / 4.1.0 | this branch (`fefae34f60`)                 | 4.1.0 | 2.13.17 | 4.2.0 |

Every cell runs the same harness. `v10.0.0` has an older benchmark that times
each view as one pipeline over NDJSON, so the 10.x cells use 11's harness
instead: `benchmark/src/main/java` from `release/11.0.0` was copied over
`v10.0.0`'s and compiled against its API without change. The view definitions
and `benchmark/pom.xml` are identical in the two versions apart from the project
version. For the 10 / 4.1.0 cell, this branch's changes to `pom.xml`,
`terminology/pom.xml` and the terminology store were applied to `v10.0.0`; they
apply without conflict, and the benchmark does not exercise the terminology
store. The later fix to the terminology store's row counts on this branch is
likewise outside what the benchmark exercises.

Each cell is a shaded jar built with
`mvn clean package -pl benchmark -am -DskipTests`. Each jar was checked to carry
the intended Spark and Scala versions and to have logback as its only SLF4J
provider, so that every cell logs through the same configuration. The 10.x cells
log about 36 UCUM warnings per run and the 11 cells none, which is too little to
affect the timings.

The batch was a Slurm array of eight allocations of 32 cores and 64 GB each, one
for each of the four phases (`encode`, `decode`, `viewPlanning`,
`viewExecution`) in each of two passes. The allocations shared their nodes with
other users' jobs; exclusive nodes were queued for about 16 hours. Spark ran
with `local[*]`, which sees the 32 allocated cores. Each allocation ran its
phase for the four cells in turn: in the order of the table above in pass 1, and
in reverse in pass 2. The two passes of a phase ran on different nodes, so the
difference between them includes the difference between nodes and between their
neighbours' load. In each pass, two of the four allocations landed on the same
node (pass 1: `viewPlanning` and `viewExecution` on c280; pass 2: `encode` and
`decode` on c012, and `viewPlanning` and `viewExecution` on c002). Each cell was
run as:

    java -Xshare:off -Xmx8g -ea -Duser.timezone=UTC \
      --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
      --add-opens=java.base/java.net=ALL-UNNAMED \
      -jar bench-<cell>.jar 'PathlingBenchmark\.<phase>$' \
      -rf json -rff bench-<cell>-<phase>-<pass>.json

The job script is `scripts/spark-4-1/bench.slurm`. The JMH output is in
`spark-4-1/`, and `scripts/spark-4-1-compare.py` prints the table below from it:

    python3 scripts/spark-4-1-compare.py spark-4-1

## Results

Sample time in milliseconds per operation, lower is better. Each cell is the
mean of the two passes. Each ratio is given as its range over the two passes.

| Phase         | Subject                         | 10 / 4.0.2 | 11 / 4.0.2 | 10 / 4.1.0 | 11 / 4.1.0 | 11:10 on 4.0.2 | 11:10 on 4.1.0 | 4.1:4.0 for 10 | 4.1:4.0 for 11 |
| ------------- | ------------------------------- | ---------: | ---------: | ---------: | ---------: | -------------: | -------------: | -------------: | -------------: |
| encode        | `Condition`                     |      341.1 |      192.6 |      351.1 |      189.9 |           0.56 |           0.54 |      1.02–1.04 |      0.98–0.99 |
| encode        | `Encounter`                     |      434.8 |      199.8 |      417.1 |      207.8 |           0.46 |           0.50 |           0.96 |      1.03–1.05 |
| encode        | `Observation`                   |      973.7 |      292.4 |     1027.9 |      291.8 |           0.30 |      0.28–0.29 |      1.05–1.06 |      0.98–1.02 |
| encode        | `Patient`                       |      170.2 |       86.2 |      176.6 |       89.8 |      0.49–0.52 |      0.50–0.51 |           1.04 |      1.01–1.08 |
| encode        | `QuestionnaireResponse`         |      649.1 |      248.2 |      662.6 |      251.1 |      0.37–0.39 |           0.38 |      1.01–1.03 |      1.00–1.02 |
| decode        | `Condition`                     |      529.3 |      115.5 |      554.1 |      117.7 |      0.21–0.22 |      0.21–0.22 |      1.04–1.05 |      1.01–1.02 |
| decode        | `Encounter`                     |      746.0 |      127.4 |      761.7 |      133.1 |           0.17 |      0.17–0.18 |           1.02 |      1.03–1.06 |
| decode        | `Observation`                   |     1785.7 |      157.7 |     1797.6 |      160.8 |           0.09 |           0.09 |      1.00–1.01 |      0.98–1.06 |
| decode        | `Patient`                       |      288.7 |      103.2 |      300.4 |      107.2 |           0.36 |           0.36 |      1.03–1.05 |           1.04 |
| decode        | `QuestionnaireResponse`         |     1630.7 |      144.5 |     1662.2 |      148.1 |           0.09 |           0.09 |           1.02 |      1.02–1.03 |
| viewPlanning  | `ConditionFlat`                 |       96.9 |       92.3 |      100.4 |       96.9 |      0.95–0.96 |      0.96–0.97 |           1.04 |      1.04–1.06 |
| viewPlanning  | `EncounterFlat`                 |      111.2 |      104.7 |      113.3 |      107.8 |      0.93–0.95 |      0.94–0.97 |      1.01–1.03 |      1.01–1.05 |
| viewPlanning  | `PatientAddresses`              |       73.6 |       73.3 |       78.2 |       73.8 |      0.99–1.00 |      0.92–0.97 |      1.04–1.09 |      1.00–1.01 |
| viewPlanning  | `PatientAndContactAddressUnion` |       89.7 |       86.1 |       91.9 |       88.1 |           0.96 |      0.95–0.97 |      1.01–1.04 |      1.00–1.04 |
| viewPlanning  | `PatientDemographics`           |       87.6 |       86.0 |       89.0 |       87.9 |           0.98 |           0.99 |      1.01–1.02 |      1.02–1.03 |
| viewPlanning  | `QuestionnaireResponseFlat`     |      244.2 |      199.2 |      248.6 |      201.9 |      0.81–0.82 |      0.80–0.82 |      1.00–1.03 |      1.01–1.02 |
| viewPlanning  | `UsCoreBloodPressures`          |      268.5 |      267.6 |      283.3 |      286.0 |      0.99–1.00 |      1.00–1.02 |      1.04–1.07 |      1.06–1.08 |
| viewExecution | `ConditionFlat`                 |       93.3 |       82.2 |       96.4 |       81.3 |      0.88–0.89 |      0.82–0.86 |      1.00–1.07 |           0.99 |
| viewExecution | `EncounterFlat`                 |       92.4 |       63.5 |       91.0 |       64.8 |      0.68–0.69 |      0.71–0.72 |      0.98–0.99 |      1.01–1.03 |
| viewExecution | `PatientAddresses`              |       27.9 |       24.6 |       29.0 |       24.8 |      0.87–0.90 |      0.84–0.87 |      1.01–1.06 |      0.99–1.02 |
| viewExecution | `PatientAndContactAddressUnion` |       35.4 |       26.1 |       36.4 |       26.1 |      0.71–0.76 |      0.69–0.75 |      0.98–1.08 |      0.97–1.04 |
| viewExecution | `PatientDemographics`           |       31.7 |       26.1 |       30.1 |       25.5 |      0.79–0.85 |      0.84–0.85 |      0.94–0.95 |      0.94–1.02 |
| viewExecution | `QuestionnaireResponseFlat`     |      697.4 |      201.5 |      697.8 |      202.7 |           0.29 |           0.29 |      0.99–1.01 |      0.99–1.02 |
| viewExecution | `UsCoreBloodPressures`          |      307.8 |      177.1 |      312.7 |      183.4 |      0.56–0.59 |      0.58–0.60 |      1.01–1.02 |      1.00–1.07 |

All 32 runs completed with no JMH failures.

## What the numbers show

**Variation between runs.** The two passes of a cell differ by a median of 2.7%,
and by under 12.3% in nine rows of ten. The largest differences, 16% to 22%, are
all in `encode`, whose two passes ran on different nodes. The ratios are
steadier than the cells, because each is taken within one pass on one node: no
11:10 ratio differs between the passes by more than 0.07.

**Regression check (H1).** On Spark 4.1.0, the 11:10 ratio is below 1 on every
encode, decode and view execution row, by a wide margin: 0.09 to 0.56 for encode
and decode, and 0.29 to 0.87 for execution (0.29 to 0.90 on 4.0.2). View
planning ranges from 0.80 (`QuestionnaireResponseFlat`) to 1.02
(`UsCoreBloodPressures`). Averaged over the two passes, the ratios on Spark
4.0.2 are the same within 0.06 on every row (within 0.08 for any single pass),
so moving to Spark 4.1 does not change how 11 compares with 10.x.

`UsCoreBloodPressures` planning is the one row at parity. T136 recorded it at
1.10 to 1.14 of the T003 baseline, as 11's one consistent planning regression,
but those were different sessions, and T003 was a single run. Measured here on
one node, back to back, 11 plans it in the same time as 10.x on both Spark
versions. Ticket 27 owns that view's planning; nothing in this run adds to it.

**Spark 4.1.0 against 4.0.2.** Planning is about 3% slower on Spark 4.1.0 for
both versions, consistently across views. That is a cost of the Spark, Scala and
Delta move, not of 11. The other phases change by amounts within the variation
between runs, apart from a small, consistent slowdown in decode.

## Not like for like

- The machine differs from T003 and T136 (Apple M3 Pro), so these figures are
  not comparable with theirs. Only the ratios within this batch are.
- The allocations shared their nodes with other jobs, including, in some cases,
  another allocation of this batch. The pass-to-pass differences above bound the
  effect on the cells; the ratios, taken within one allocation, are less exposed
  to it.
- The 10.x cells run 11's harness, not the one `v10.0.0` shipped. The harness
  only calls the public library API, so it measures the same work, but it is not
  the benchmark a 10.x release would have run.
- View planning is still measured warm, as in T136 (T124g is open in ticket 10),
  in every cell alike.
- The 4.1.0 cells change Scala (2.13.17) and Delta (4.2.0) as well as Spark,
  because Spark 4.1 needs both. The Spark effect cannot be separated from them.
