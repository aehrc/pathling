# T136: the split benchmark at the flip

The comparison of the per-phase benchmark against the T003 baseline
([baseline.md](baseline.md), "Per-phase baseline"), taken after the public API
switched to the new layout (Phase 12) and before any annotation exists. No
outcome is required (SC-007). This is the input that orders Phase 15, and the
task asks for it to be repeated after each annotation kind lands, so this is the
first of several T136 measurements, not the last.

**Result: encode, decode and view execution are faster on every row, and view
planning is within a few per cent of the baseline except for two views.**
Encode takes 0.48 to 0.71 of the baseline time, decode 0.12 to 0.51 and view
execution 0.32 to 0.95. Planning is 0.86 to 1.14 times the baseline. The one
consistent planning regression is `UsCoreBloodPressures`, at 1.10 and 1.14 in two
runs (158.0 and 163.4 ms against 143.6). The one large improvement is
`QuestionnaireResponseFlat`, at 0.86.

**The benchmark cannot answer FR-033.** None of the seven views joins or calls
`resolve()`. `getResourceKey()` and `getReferenceKey()` appear only as projected
columns, and over the new layout they compile to `concat('Observation/', id)` and
`CASE WHEN RLIKE(subject.reference, 'Patient/.+') THEN subject.reference END`. So
this measures computing a key, not joining on one. The engine also built keys this
way before the programme began (T098), so the suite has nothing to compare. To
decide whether joins need a precomputed key (T098, FR-033), a benchmark that joins
or resolves references would have to be added. That is not done here, because it
would change the suite that the baseline was measured with.

## Conditions

The procedure is T003's, repeated as exactly as the machine allowed.

|                        | T003                                                                           | T136                                                       |
| ---------------------- | ------------------------------------------------------------------------------ | ---------------------------------------------------------- |
| Commit                 | `4bfadac552`                                                                   | `26d7332e79` (`issue/2764`)                                |
| Machine                | Apple M3 Pro, 11 cores, 36 GB                                                  | same machine                                               |
| OS                     | macOS 26.6.2                                                                   | **macOS 27.0.1**                                           |
| JVM                    | OpenJDK 21.0.3+7-LTS-152                                                       | same                                                       |
| Spark / Delta / Hadoop | 4.0.2 / 4.0.0 / 3.4.1                                                          | same                                                       |
| JMH                    | 1.37, `@Fork(2)`, warmup 1 x 5 s, measurement 3 x 5 s, `SampleTime`            | same                                                       |
| Benchmarks             | `encode`, `decode` (5 types); `viewPlanning`, `viewExecution` (7 views, delta) | same                                                       |
| Dataset                | `test-data` 1.1.0, `bulk/fhir/*.ndjson`                                        | same release                                               |
| Runs                   | 1                                                                              | **2**, back to back (22 m 08 s and 22 m 06 s, no failures) |

The benchmark's source, its view definitions and its `pom.xml`, including the
`test-data` version, are unchanged since `4bfadac552`
(`git diff --stat 4bfadac552..26d7332e79 -- benchmark/` is empty). The invocation
is the baseline's, with only the output file renamed:

    mvn clean install -pl benchmark -am -DskipTests
    java -Xshare:off -Xmx8g -ea -Duser.timezone=UTC \
      --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
      --add-opens=java.base/java.net=ALL-UNNAMED \
      -jar benchmark/target/benchmark-9.9.0.jar 'PathlingBenchmark' \
      -rf json -rff t136-phases-26d7332e79.json

What is not like for like:

- **The OS has been upgraded** since T003, from macOS 26.6.2 to 27.0.1. This
  cannot be undone.
- **The baseline is one session; T136 is two.** The JMH error is a 99.9%
  interval over two forks of one session, and it understates the difference
  between sessions. The two T136 runs give that difference for the new code. In
  them, most rows agree within 5%, and the largest gap is 14% (`decode Patient`,
  85.2 against 73.6 ms). No equivalent figure exists for the baseline. So a
  movement of under about 10% is not distinguishable from drift between
  sessions, whatever the JMH intervals say.
- **Background load.** Nothing else was building or testing. IntelliJ IDEA, a
  language server and an idle Hive metastore JVM left by another session were
  resident at 0.0 to 0.1% CPU throughout. The one-minute load average was 3.0
  before the first run. What was resident during T003 was not recorded.
- **View planning is measured warm in both runs.** T124g, still open, records
  that the invocation-level setup plans the same query just before each planning
  invocation. Both runs have this bias equally, and it is deliberately left in
  place here, because fixing it would break the comparison.
- **Encode and decode are now a different implementation, not only a different
  layout.** Since T070, T081 and T082, `PathlingContext.encode` and `decode` go
  through the `io` transform and `FhirJsonWriter`. They also write a schema
  fitted to the data rather than the full definition-derived one. So the
  encode and decode figures measure implementation and layout together. Nothing
  here separates the two.

## Results

Sample time in milliseconds per operation, mean ± 99.9% error, lower is better.
The ratio is each T136 run's mean over the baseline mean, as a range across the
two runs. `n` is the sample count for baseline / run 1 / run 2.

| Phase     | Subject                         |           T003 |   T136 run 1 |   T136 run 2 |     Ratio |               n |
| --------- | ------------------------------- | -------------: | -----------: | -----------: | --------: | --------------: |
| encode    | `Patient`                       |     83.9 ± 2.1 |   57.3 ± 1.7 |   54.6 ± 0.9 | 0.65–0.68 | 362 / 526 / 552 |
| encode    | `Condition`                     |    178.6 ± 4.0 |  126.3 ± 2.3 |  120.3 ± 1.5 | 0.67–0.71 | 171 / 241 / 253 |
| encode    | `Encounter`                     |   265.6 ± 12.4 |  147.2 ± 2.3 |  153.2 ± 4.2 | 0.55–0.58 | 116 / 206 / 199 |
| encode    | `QuestionnaireResponse`         |    345.4 ± 9.7 |  169.5 ± 3.1 |  165.6 ± 2.6 | 0.48–0.49 |  90 / 181 / 183 |
| encode    | `Observation`                   | 1004.6 ± 116.3 | 484.3 ± 20.1 | 492.7 ± 20.0 | 0.48–0.49 |    34 / 64 / 64 |
| decode    | `Patient`                       |    165.7 ± 3.5 |   85.2 ± 4.9 |   73.6 ± 1.6 | 0.44–0.51 | 185 / 355 / 410 |
| decode    | `Condition`                     |    312.6 ± 9.1 |   81.5 ± 1.8 |   78.5 ± 1.3 | 0.25–0.26 |  99 / 371 / 385 |
| decode    | `Encounter`                     |   426.9 ± 20.2 |  103.8 ± 6.1 |   97.2 ± 2.3 | 0.23–0.24 |  73 / 291 / 312 |
| decode    | `QuestionnaireResponse`         |   824.6 ± 41.4 |  106.3 ± 2.3 |  106.6 ± 2.6 |      0.13 |  39 / 285 / 285 |
| decode    | `Observation`                   | 1442.2 ± 142.4 |  183.2 ± 6.6 |  168.0 ± 4.9 | 0.12–0.13 |  23 / 166 / 182 |
| planning  | `PatientAddresses`              |     50.0 ± 1.0 |   52.0 ± 1.1 |   51.9 ± 1.2 |      1.04 | 304 / 291 / 291 |
| planning  | `PatientDemographics`           |     54.7 ± 0.8 |   58.6 ± 1.1 |   60.3 ± 2.4 | 1.07–1.10 | 277 / 259 / 250 |
| planning  | `PatientAndContactAddressUnion` |     56.8 ± 0.8 |   57.1 ± 1.5 |   58.8 ± 1.7 | 1.01–1.03 | 266 / 265 / 257 |
| planning  | `ConditionFlat`                 |     62.4 ± 2.6 |   58.4 ± 0.9 |   61.0 ± 1.6 | 0.94–0.98 | 243 / 258 / 251 |
| planning  | `EncounterFlat`                 |     63.7 ± 1.0 |   64.2 ± 1.8 |   64.6 ± 1.8 |      1.01 | 237 / 238 / 235 |
| planning  | `QuestionnaireResponseFlat`     |    136.0 ± 2.5 |  117.4 ± 3.5 |  117.3 ± 3.3 |      0.86 | 112 / 130 / 130 |
| planning  | `UsCoreBloodPressures`          |    143.6 ± 2.3 |  158.0 ± 3.5 |  163.4 ± 5.0 | 1.10–1.14 |   107 / 98 / 94 |
| execution | `PatientAddresses`              |     19.6 ± 1.2 |   15.0 ± 0.3 |   14.7 ± 0.3 | 0.75–0.77 | 417 / 439 / 451 |
| execution | `PatientDemographics`           |     20.9 ± 1.5 |   19.8 ± 1.5 |   17.1 ± 1.1 | 0.82–0.95 | 384 / 357 / 390 |
| execution | `PatientAndContactAddressUnion` |     25.2 ± 2.0 |   18.4 ± 1.7 |   18.9 ± 1.2 | 0.73–0.75 | 353 / 388 / 363 |
| execution | `ConditionFlat`                 |     47.5 ± 0.8 |   42.0 ± 1.2 |   43.6 ± 1.4 | 0.88–0.92 | 275 / 282 / 278 |
| execution | `EncounterFlat`                 |     65.4 ± 2.4 |   44.8 ± 2.2 |   44.3 ± 1.7 | 0.68–0.69 | 226 / 263 / 271 |
| execution | `UsCoreBloodPressures`          |    205.6 ± 9.1 |  146.8 ± 4.7 |  147.6 ± 6.2 | 0.71–0.72 |  85 / 101 / 102 |
| execution | `QuestionnaireResponseFlat`     |   450.2 ± 19.2 |  145.9 ± 1.9 |  150.9 ± 2.9 | 0.32–0.34 |  53 / 117 / 114 |

The full JMH output is in `t136-phases-26d7332e79.json` and
`t136-phases-26d7332e79-run2.json`, beside the baseline's
`baseline-phases-4bfadac552.json`. `scripts/t136-compare.py` prints the
comparison, with the range of per-fork means for every row:

    python3 scripts/t136-compare.py baseline-phases-4bfadac552.json \
      t136-phases-26d7332e79.json t136-phases-26d7332e79-run2.json

## What the numbers support, and what they do not

These checks were made after both runs, at `26d7332e79`, with
`scripts/t136/T136Probe.java`, which uses the benchmark's own data and views:

    java -Xmx8g --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
      --add-opens=java.base/java.net=ALL-UNNAMED \
      -cp benchmark/target/benchmark-9.9.0.jar \
      specs/001-parquet-on-fhir/evidence/scripts/t136/T136Probe.java UsCoreBloodPressures

**The stored schema is far narrower.** It counts the leaf fields of the schema
that `PathlingContext.encode` now writes for the benchmark data, against the
previous layout's encoder schema (`getFhirEncoders().of(type)`, at the same
commit):

| Resource                | New: top-level / leaves | Previous: top-level / leaves |
| ----------------------- | ----------------------: | ---------------------------: |
| `Patient`               |                 16 / 51 |                     26 / 304 |
| `Condition`             |                 12 / 19 |                     33 / 424 |
| `Encounter`             |                 14 / 36 |                     31 / 620 |
| `Observation`           |                 14 / 37 |                     45 / 855 |
| `QuestionnaireResponse` |                  7 / 21 |                   19 / 1,241 |

The decode improvement is largest where the schema shrank most
(`QuestionnaireResponse` and `Observation`, 0.12 to 0.13) and smallest where it
shrank least (`Patient`, 0.44 to 0.51). The same holds for execution
(`QuestionnaireResponseFlat`, 0.32 to 0.34). That fits the narrower schema doing
much of the work. But decode is also a different implementation now, and this
probe does not separate the two causes. It is a correlation, not an attribution.

**The `UsCoreBloodPressures` planning regression is unattributed.** Its optimised
plan at `26d7332e79` is 49,103 characters, with 149 `CASE WHEN`. It holds no
canonical-quantity UDF call, because the view reads only `value`, `unit`,
`system` and `code`, never comparing quantities. So T100o's repeated quantity
decode and T096a's annotation are not in this benchmark. Most of the
`CASE WHEN` (104 of them) are inside the `code.coding.exists(system = ... and
code = ...)` filter lambdas. The four `decimal` casts are the lexical decoding
of `value`. The baseline's plan was not captured, so how the plan grew cannot be
shown without building `4bfadac552`, and this records no cause. The whole plan
is in `.local/work/issue/2764/t136-usbp-plan.txt`.
As an observation only: `QuestionnaireResponseFlat`'s plan holds more decimal
casts (18) than `UsCoreBloodPressures`'s, and its planning improved by a similar
amount.

**Every view's `Generate` runs outside whole-stage code generation, and so does
`UsCoreBloodPressures`'s `Filter`.** In the executed plans, the scan and the
final `Project` carry a `*(n)` prefix, which marks operators inside whole-stage
code generation. The `Generate` that unnests each view's rows never carries one,
in any of the seven views. In `UsCoreBloodPressures`, the `Filter` that holds the
`code.coding.exists(...)` condition does not either. Its optimised plan holds 336
lambda functions (`filter` over `code.coding` and `component`), and
`QuestionnaireResponseFlat`'s holds 75. These are the higher-order array functions
that T100o found to be `CodegenFallback`. So the fallback that T096c describes for
the quantity `!=` is reached here by Coding filtering, which involves no quantity.
The baseline's plans were not captured. So whether these operators were already
outside code generation at `4bfadac552` is not known, and nothing here attributes
a timing to it. No view calls a canonical-quantity UDF, because none uses the
quantity `!=`.

**The planning floor is unchanged.** The three `Patient` views, `ConditionFlat` and
`EncounterFlat` plan in 52 to 65 ms, against 50 to 64 ms in the baseline. The
schema is several times narrower on every one of them, so a per-query fixed cost,
not the width of the schema, dominates planning for light views. The
`PatientDemographics` figure, at 1.07 to 1.10, is just outside the 99.9%
intervals in both runs. But it is inside the drift between sessions described
above, and is not treated as a regression.

## For Phase 15

At the flip, the computed paths that this benchmark reaches are lexical decimal
decoding (`UsCoreBloodPressures`, `QuestionnaireResponseFlat`) and computed
resource keys (every view) and reference keys (every view but the three
`Patient` views). They
show no execution cost that stands out against the baseline: every view runs
faster. The only regression is `UsCoreBloodPressures` planning, at 10 to 14%,
and its cause is not established. Three annotated operations are not exercised
by the suite at all: quantity comparison, joins on reference keys, and date
bounds. So the suite gives Phase 15 no evidence for or against those kinds. The
measurement is to be repeated after each annotation kind lands.

Logs: `.local/work/issue/2764/t136-build.log`, `t136-run1.log`, `t136-run2.log`,
`t136-run{1,2}-env.txt` (processes and load before and after each run),
`t136-compare.txt`, `t136-probe.log`.
