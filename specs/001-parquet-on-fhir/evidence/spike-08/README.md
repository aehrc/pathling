# Spike 08, phase 1: A0 and the shared groundwork

Evidence for ticket 08 (`../../issues/08-spike-ingest-egress-mechanism.md`), gathered 2026-10-19 on the branch
`issue/2831-spike-08`. It measures **A0, the floor**: HEAD at `514f8f818e` as it stands, with the `from_xml` XML
route added from `xml-native.md`. It also records the groundwork the later candidates (B, A+ and A-variant)
share: the four open questions settled by probes, a harness that times the fit pass, encode and decode apart,
the re-measured bar, and the fixture coverage of decision 133. No candidate is chosen here.

| Directory | Holds |
|---|---|
| `probes/` | The four probes, each with its raw output as a `.txt` file of the same name |
| `harness/` | Copies of the phase harness (`phases.py`, `run-phases.sh`), the `from_xml` prototype (`fromxml_route.py`), its correctness gate (`gate_fromxml.py`), and the comparison scripts (`compare_phases.py`, `summary_phases.py`, `pooled_phases.py`) |
| `gate/` | The gate's output at size `s`, and the direct comparison of the two XML decoders (`decoders_equal.py` and its output) |
| `fixtures/` | The added JSON and XML fixtures, the runner and its output |
| `phase2/` | Phase 2: the candidates' prototype code (`proto/`, Java; `harness/`, Python and the extended harness), their gates (`gate/`), the unattended size-`l` batch with its runbook (`batch-l.sh`, `RUNBOOK.md`), and its results (`results/`) |
| `results/` | The result files of the three size-`l` labels with their conditions and notes (`<label>/`), every sample (`comparison.txt`), the per-type tables (`tables.md`), and the pooled ratios (`pooled.txt`) |

**Where the probes live.** They are in this directory, not in `../ingress-strictness/`, so that the spike's
evidence stays together. They follow that directory's style.

## Summary

- **No regression visible against the bar, but 10% is not resolved.** Replicates of identical code ranged
  from 0.59 to 1.51 of each other in this session. Pooled over the ten samples each label has per item, A0 at
  `514f8f8` is 0.87 to 1.10 of the PoF switch (`26d7332`) on every source, type and phase. The highest are JSON
  decode of Encounter (1.10) and Observation (1.07), and XML lines decode of Patient (1.07). See
  [Noise](#noise).
- **Fit and encode separate cleanly on HEAD**, through public API, and identically on every route. Spark infers
  the schema eagerly inside `pc.encode`, so the fit is that call and encode is the write. Every sample of every
  phase ran exactly one Spark job, on every route (88 result entries checked).
- **HEAD's XML routes run the HAPI conversion twice**, once in the fit and once in the encode, because it is
  not cached. Every route parses its input twice, once to infer and once to read; on HEAD's XML routes each
  of those parses is a HAPI parse plus a JSON round trip. On XML lines the fit is about 90% of the encode's
  cost (Observation: 5.6 s against 6.4 s).
- **The `from_xml` route is a viable XML floor.** Generated from the definitions with nothing written per type,
  it encodes the same rows and schema as HEAD's JSON route at size `s`, from lines and from both Bundle shapes.
  At size `l` its round-trip fidelity equals HEAD's. Against HEAD's HAPI route, on the three large types and
  pooled:
  - fit 0.50 to 0.61 for lines;
  - encode 0.67 to 0.89 for lines and collection Bundles, and 0.92 to 0.99 for transaction Bundles;
  - decode 0.50 to 0.65.

  Its weak spot is the Bundle fit. The route infers the whole Bundle schema once per resource type, a constant
  of about 7.3 to 8.4 s that does not shrink for a small type, so the Patient fit is 1.7 to 1.8 times HAPI's.
- **A0 meets none of decision 133's validation scope**, on any route: 0 of 29 invalid JSON values fail. Every
  OQ-1 loss is present on both the JSON route and the `from_xml` route, and so is the per-batch dependence that
  principle 6 forbids.

## Phase definitions (applied identically to every route)

The definitions were fixed before anything was timed, and are in `harness/phases.py`'s docstring.

- **Fit.** It starts at the call that turns the cached raw text into an encoded dataset, and ends when that
  call returns. The call is `pc.encode` or `pc.encode_bundle` on HEAD, and `XmlRoute.fit_lines` or
  `fit_bundles` on `from_xml`. Spark's `DataFrameReader.json(Dataset<String>)`, and `.xml(Dataset<String>)`
  for `from_xml`, infers the schema eagerly: one full scan of the input. The transform is then built on the
  driver from the inferred schema. The phase ends with the fitted schema (FR-011) on the driver and a lazy
  dataset whose schema is fixed. Building the plan falls inside the fit on every route.
- **Encode.** A `noop` write of that dataset. Every row and every column is materialised, with no disk I/O.
  A `count()` was not used, because Spark can prune columns under a count and skip most of the transform.
- **Decode.** The encoded dataset of the last encode iteration is written to Parquet once, untimed, and read
  back. **Decode starts from that Parquet read**, not from a cache: an in-memory cache of a million wide
  Observations under a 16g driver would spill. Decode is a `noop` write of the decoded text, from
  `pc._jpc.decode` on HEAD and `XmlRoute.decode` (`to_xml`) on `from_xml`. A control, the `noop` write of the
  Parquet read alone, gives the scan's share (`parquetScanControlMs`). It is 0.2 to 0.6 s for the large types.
- Each phase runs in a Spark job group of its own, and its number of jobs is recorded. Every sample of every
  phase of every route ran exactly one job, so no work was found in an unexpected phase.

**Fusion.** HEAD does not fuse fit and encode where public API can see it. The fit's scan does, however, include
the HAPI XML-to-JSON conversion on the XML routes. Because that conversion is not cached, encode runs it again.
That is HEAD's real cost, and it was measured as it is.

**What the bundle decodes measure.** Decode depends only on the stored layout, so for a given type the three XML
sources decode the same data. The bundle figures nonetheless run 1.7 to 2.6 times the lines figure on HEAD
(Observation: 15.0 s from bundles against 5.8 s from lines), and on `from_xml` too (7.5 s against 3.1 s).
The only difference is that 2.8 to 3.0 GB of raw Bundles stay cached in the driver while a bundle source runs.
The bundle decodes are therefore repeats under memory pressure, and the lines figure is the one to read for
decode.

## The bar

**The bar is `pof-switch-26d7332` re-measured on this harness, as the owner decided on 2026-10-19. That differs
from SC-011's wording**, which names the JMH benchmark in `../t136-benchmark.md`. The formal JMH check moves to
ticket 16 and is not part of the spike. JMH was not run.

## Results at size `l`

The full per-type tables, with min and mean of five for every source, type, phase and extensions setting, are
in `results/tables.md`. Every sample is in `results/comparison.txt`, and the pooled ratios are in
`results/pooled.txt`. The raw result files are in `results/<label>/`.

**A0 (`514f8f8`) against the bar (`26d7332`)**, as the range over the four types of the ratio of pooled
minimums (ten samples each):

| Source | Fit | Encode | Decode |
|---|---|---|---|
| JSON lines | 0.90 - 0.98 | 0.94 - 0.97 | 0.99 - 1.10 |
| XML lines (HAPI) | 0.93 - 0.99 | 0.97 - 0.99 | 0.95 - 1.07 |
| XML collection Bundles (HAPI) | 0.99 - 1.01 | 0.97 - 1.02 | 0.93 - 1.00 |
| XML transaction Bundles (HAPI) | 0.87 - 0.94 | 0.87 - 0.88 | 0.89 - 0.97 |

Absolute values for Observation, the largest type, in seconds, as min (mean) of five with extensions off:

| Source | Phase | Bar | A0 | A0 `from_xml` |
|---|---|---|---|---|
| JSON lines | fit / encode / decode | 1.07 (1.16) / 1.92 (1.98) / 0.75 (1.12) | 1.20 (1.22) / 2.01 (2.06) / 0.86 (0.93) | - |
| XML lines | fit / encode / decode | 5.69 (5.77) / 6.46 (6.51) / 5.91 (5.96) | 5.64 (5.69) / 6.36 (6.40) / 5.84 (5.90) | 3.44 (3.55) / 4.93 (5.00) / 5.18 (5.23) |
| XML collection Bundles | fit / encode / decode | 9.01 (9.21) / 9.88 (10.18) / 15.23 (15.55) | 8.89 (8.94) / 9.54 (9.68) / 15.01 (15.07) | 7.36 (7.40) / 8.33 (8.50) / 7.71 (7.74) |
| XML transaction Bundles | fit / encode / decode | 11.13 (11.71) / 11.94 (12.75) / 15.50 (15.60) | 9.52 (10.21) / 10.16 (10.94) / 15.01 (15.16) | 8.79 (9.27) / 10.33 (10.90) / 7.56 (7.61) |

**The `from_xml` route against HEAD's HAPI route**, both on the A0 jar, pooled. The second set of columns
subtracts the route's own py4j plan-building time, which is recorded per sample. It is a prototype artefact:
the route builds thousands of column expressions in Python, one py4j call each, where HEAD builds them in the
JVM. It costs 0.09 to 0.41 s per fit and 0.13 to 0.43 s per decode, and it accounts for the route's deficit on
Patient lines. It does not account for the Patient Bundle fit.

| Source | Fit | Encode | Decode | Fit, py4j subtracted | Decode, py4j subtracted |
|---|---|---|---|---|---|
| XML lines | 0.50 - 1.40 | 0.67 - 1.01 | 0.52 - 1.78 | 0.46 - 0.64 | 0.48 - 0.75 |
| XML collection Bundles | 0.83 - 1.79 | 0.78 - 1.05 | 0.50 - 1.38 | 0.81 - 1.74 | 0.48 - 0.78 |
| XML transaction Bundles | 0.88 - 1.74 | 0.92 - 1.16 | 0.50 - 1.29 | 0.86 - 1.67 | 0.49 - 0.73 |

The top of each range is Patient, the smallest type at 11,853 rows, where fixed costs dominate. Excluding
Patient, encode is 0.67 to 0.99 and decode 0.50 to 0.65. Against the bar (`results/pooled.txt`), `from_xml` is 0.48 to
0.95 on Condition, Encounter and Observation in every phase and source, except the Condition Bundle fits
(1.16 and 1.09). Patient is above the bar in fit and decode: on lines 1.31 and 1.91, mostly py4j, and on Bundles
1.62 to 1.81 for the fit.

**Fidelity**, decoded against the input on the first 500 rows per type. It is identical for all three labels
on every source:

- 500 of 500 exact for every JSON type;
- 500 of 500 exact for XML Patient, Condition and Encounter;
- 421 of 500 for XML Observation.

The XML Observation losses are OQ-1's decimal: `0` becomes `0.0`. The JSON check compares numbers by value and
cannot see that loss. The `from_xml` route decodes byte for byte what HEAD's decoder decodes. At size `s`, both
decoders were run over the same stored data, and their outputs are string-equal in all 24,424 documents
(`gate/decoders_equal.py`, output `gate/decoders_equal.txt`).

**Correctness gate (`gate/gate-s.txt`, size `s`).** The datasets the `from_xml` route encodes from XML lines,
collection Bundles and transaction Bundles are equal, in schema and in every row, to the ones HEAD encodes from
the JSON source, for all four types (24,424 resources). The transaction Bundles' `urn:uuid` references are
resolved to `Type/id` through the generated `Reference` columns. The gate's first run found a quadratic plan:
the optimiser inlined the per-Bundle map of `urn:` URLs into the per-entry lambda, rebuilding it for every
reference (7.1 s against 0.36 s for HAPI on Observation at size `s`). The map is now materialised once per
Bundle, behind an `explode` of a one-element array, and the gate was rerun before anything was timed at `l`.

**What the benchmark data does not exercise.** The `clinical-wide` data has no primitive extensions and no
`contained` resources, and only Patient carries a narrative. The `from_xml` route's metadata groups (`_x`),
its narrative alignment across Bundle entries other than Patient, and its handling of undescribed content were
exercised only by the fixtures, and none of them was timed.

## Noise

One run per label, one machine. Neither commit's reader or writer consults `enable_extensions`; it configures
only the legacy `FhirEncoders`. So each label's extensions-off and extensions-on runs are two replicates of the
same code. Between those replicates, the ratio of minimums ranged from 0.59 to 1.51 for the same build. For
example:

- the bar's collection Bundles ran 1.20 to 1.27 times slower with extensions on;
- A0's JSON ran 0.80 to 0.96 times;
- `from_xml`'s lines decode ran 0.59 to 0.94 times.

The per-setting ratios in `results/tables.md` swing accordingly. A0 against the bar on JSON is 1.02 to 1.27
with extensions off and 0.66 to 1.01 with them on. Pooling the replicates gives the steadier 0.87 to 1.10
above. That says no regression is visible. A difference of 10%, which is SC-011's tolerance, is not resolved
by these runs.

## Conditions

| | |
|---|---|
| Commits | Bar `26d7332e79`, built as `9.9.0-pofswitch-26d7332` (POM version only; `pathlingDirty: true`). A0 `514f8f818e`, clean. Evidence and prototype on `issue/2831-spike-08` from `9ec700424c` |
| Jars | `library-runtime-9.9.0-pofswitch-26d7332.jar` SHA-256 `5180983e6e7efa7c09fe3f3b78344966a6789ef8b13a338f24a7d202d619b0c8`; `library-runtime-10.0.0-SNAPSHOT.jar` (514f8f8) `3eb0a768fa6c27052d6eaa66f8499c30558aeab9b14d125277fd7571d54f2320`. Each matched the jar resolved into its label's private Ivy home. Neither was rebuilt: both matched the 2026-10-17 builds |
| Prototype | `harness/fromxml_route.py` SHA-256 `eda25324c20f5854c82a09bbccbdd9a24d8d3eec4f2928b6a5fc94cd8f7d2749`, byte-identical to the harness copy that ran |
| Versions | Java HotSpot 21.0.3. Python 3.12.7. PySpark 4.0.4 in the harness venvs; the project builds on 4.0.2, and probes 1 to 3 ran on 4.0.2 from `lib/python/.venv`. macOS 27.0.1 |
| Machine | Apple M3 Pro, 11 cores, 36 GB. 47 to 49 GiB free throughout |
| Data | `clinical-wide` size `l` from `sql-on-fhir.js` `794e8a7f`: Patient 11,853, Condition 459,617, Encounter 598,548, Observation 1,004,979. XML and Bundles from the harness's `data/` (2026-10-16) |
| Settings | One warmup plus five iterations in a warm JVM, raw input cached untimed, `max_nesting_level` 3, `--driver-memory 16g`, private Ivy home per label |
| Schedule | One batch, 2026-10-19: bar 13:11 to 14:14, A0 14:14 to 15:14, `from_xml` 15:14 to 16:00. Nothing else of mine ran during it |
| Other processes | Two `jdtls` language servers (idle) throughout. IntelliJ with SonarLint was resident at 12:31 and had exited by 13:10. At 13:10 macOS agents (AmbientDisplayAgent, WallpaperAgent, coreaudiod) used about four cores for under a minute, and the batch started after they went idle. Jamf Protect and Microsoft Defender ran at a few percent throughout. During the runs the load average reached 80 to 90. **That it came from the benchmark JVM's own threads is an inference**: no other process was above 12% CPU when checked at 14:24, but the cause was not established. Swap was in use after the batch (`vm.swapusage`: 2.79 GB of 4 GB used at 16:20), which is a possible lead. The 2026-10-17 runs, on the same harness, saw about 5% noise between labels, against up to 50% between replicates here |

## The four unknowns

### 1. Does `parse_json` reject a duplicate key, a top-level array, and text after the object?

`probes/parse_json_probe.py`, PySpark 4.0.2. **It rejects only the duplicate key.**

| Input | `parse_json` (default) | `try_parse_json` | With `spark.sql.variant.allowDuplicateKeys=true` |
|---|---|---|---|
| Duplicate key, at the top level or nested | **Fails** the job: `VARIANT_DUPLICATE_KEY`, inside `MALFORMED_RECORD_IN_PARSING` | `NULL`, which cannot be told from a malformed document | Accepted, **last value wins** |
| Top-level array | Accepted as an `ARRAY<OBJECT<…>>` variant | Same | Same |
| Text after the object (`{…} xyz`) | **Accepted; the text is ignored** | Same | Same |
| A second object after the first | **Accepted; the second object is dropped** | Same | Same |
| A top-level scalar | Accepted as a `STRING` variant | Same | Same |
| Malformed (unclosed) | Fails (`JsonEOFException`) | `NULL` | Fails |

`spark.sql.variant.allowDuplicateKeys` is `false` by default on 4.0.2. A top-level array and a scalar can be
caught after parsing, with `schema_of_variant` or an `is_object` test. Text after the object cannot: the parser
stops at the end of the first value and never reports the rest. A-variant therefore needs a check of its own
for trailing content. A+'s Jackson pre-pass sees every token anyway, so it can reject all three. B's parser
reads to the end of the line by construction.

### 2. Does inference widen an ancestor to `string` when a disagreement is deep in an array or a struct?

`probes/widening_probe.py`, PySpark 4.0.2. **Only where the disagreement is about shape at that node.**

- **A leaf disagreement widens only the leaf**, at any depth. Examples are string against number, string
  against boolean, number against boolean, string against object, and string against array: `name[].family`,
  `component[].valueQuantity.value` and `code.coding[].code` each become `string`, and every ancestor keeps its
  type.
- **An array element disagreement widens only the element type.** `given` becomes `array<string>`, holding
  `"1"`, `{"x":1}` and `["a","b"]` as JSON text.
- **A shape disagreement at an intermediate node widens that node, and its whole subtree, to `string`, for
  every row of the batch.** This covers an array of structs against a struct (`name`, case c5-scalar-for-array),
  a struct against an array one level down (`code.coding`), a struct against a string (`code`), and an array of
  structs against an array of strings (`name` becomes `array<string>`).
- `{}`, `[]` and `null` never widen anything. `{}` arrives as a structure whose fields are all null, so a
  present-but-empty object looks like any present object with no fields set.

For A+ this means a messy batch is cheap where the mess is in leaves, and costly where it is in shape. One row
with `"name": {…}` turns `name` into a JSON string column for every row of the batch. Each row then needs a
`from_json` of its whole `name` subtree, against the definitions' type, to be resolved.

### 3. Can `Dataset.observe` carry a bounded sample of findings?

`probes/observe_probe.py`, PySpark 4.0.2, 100,000 rows with a noop write as the action. **Yes, through public
API, if the sample is built from fixed-size aggregates.**

| Route | Accepted by `observe` | Bounded |
|---|---|---|
| Counts (`count`, `sum(size(_findings))`, rows affected) | Yes | Yes |
| `slice(collect_list(…), 1, 5)` | Yes | **No**: the list is built in full (22,078 entries here) and sliced only on the driver |
| `min(struct(id, path, value))` per kind | Yes | Yes, one example per kind, deterministic |
| k per kind: one `min(struct)` per `pmod(xxhash64(id), k)` bucket | Yes | Yes, k × kinds structs, deterministic |
| `max_by(struct, key)` | Yes | Yes, one example |
| A non-aggregate expression | No (`INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_ATTRIBUTE`) | - |

Two limits remain. Counts *by path* need one aggregate per path, so the set of paths has to be known when the
plan is built. The definitions give a finite set if array indices are left out of the path. And a true
bounded k-sample in one aggregate needs `functions.udaf(Aggregator)`, which is public API in Java and Scala but
cannot be written in Python.

### 4. What does the engine do with an invalid stored decimal?

`probes/engine_decimal_probe.py`, on the A0 jar (`3eb0a768…`) and PySpark 4.0.4. A valid Observation is encoded
and its stored `valueQuantity.value`, which the layout keeps as text, is overwritten. 16 FHIRPath expressions
are then evaluated, each in a view of its own. This is recorded for ticket 31 and is not fixed here.

- **Unparseable text is silently empty.** `abc`, `1.2.3`, `""`, `1,5`, `NaN`, `Infinity` and `1e400` behave the
  same way. `value` is empty, `exists()` is `false`, `empty()` is `true` and `count()` is `0`. Comparisons,
  arithmetic, `toString()`, `toDecimal()` and `convertsToDecimal()` are all empty. Nothing fails, so this case
  already meets principle 7.
- **Surrounding whitespace is accepted.** `" 1.5"` and `"1.5 "` evaluate as `1.5`, so a value FHIR rejects
  reads as valid.
- **A valid value beyond the engine's precision is empty.** The 40-digit `1234…789.5` evaluates as empty in
  every expression. Yet `valueQuantity > 1 'mg'` is `true` for it, so the quantity path reads the value
  differently from the decimal path.
- `toString()` of an unparseable value is empty, so the stored text cannot be reached from FHIRPath at all.
- `round()`, `sum()` and `iif()` are reported as unsupported by this build, so they were left out.

## The fixtures against A0 (decision 133's scope)

`fixtures/fixtures_run.py` runs `../ingress-strictness/fixtures.tsv` and `fixtures-lexical.tsv`, plus
`fixtures/fixtures-json-extra.tsv`, against HEAD's JSON route. It runs the hand-written XML equivalents in
`fixtures/fixtures-xml.tsv` against the `from_xml` route, and against HEAD's HAPI XML route as a reference. The
raw output is `fixtures/fixtures_run.txt`. A fixture that is about JSON syntax has no XML equivalent and is
left out of the XML set: the token-type cases (C4 except object-for-string), the empty array, the nested array,
a single value given for an array, a missing `resourceType`, and a top-level array. Six fixtures are XML only:
no namespace, a prefixed namespace, surrounding spaces in a value, text content in place of a `value`
attribute, an unknown attribute on the root, and a narrative holding markup.

Each fixture is encoded alone and decoded. The outcome is compared with what decision 133 expects of it:

- **store:** the fixture is conformant, breaks only an element-level constraint, or is a text-only type. It
  must be decoded back equal to the input, with JSON numbers compared by their text and XML compared
  canonically.
- **invalid:** fail by default, naming the resource, path and value.
- **contained:** dropped with an informational finding.
- **left-out:** a document of another type.
- **reject:** not one resource.

| Expectation | JSON (HEAD) | `from_xml` | HAPI XML (HEAD, reference) |
|---|---|---|---|
| store (conformant, element-level, text-only) | 13 of 18 | 16 of 22 | 13 of 22 |
| invalid: fail by default | **0 of 29**: 16 with a column-level finding only, 13 silent | **0 of 23**: 7 with a column-level finding, 16 silent | 5 of 23 fail the whole job; 18 silent |
| contained: dropped with a finding | partial (column-level finding) | partial (column-level finding) | dropped silently |
| left-out | 2 of 2 | 2 of 2 | 1 of 2 (an unknown type fails the job) |
| reject | 2 of 4 | 2 of 5 | 4 of 5 |

**A0 meets none of decision 133's validation scope, as expected.** In detail:

- **No invalid value fails, on any route of A0.** Every invalid date, time and instant is stored as received,
  silently. That covers the bad calendar date, the time without a timezone, a time without seconds, an
  unpadded date, a date with a time, `25:61:00`, and `instant` without a timezone or as a date only. A
  `positiveInt`/`unsignedInt` out of range is stored. `""` is stored. An integer overflow and malformed
  base64 become null with no finding (`try_cast`, `try_to_binary`).
- **What A0 reports is per column, not per value, and only as a log line.** Content the definitions do not
  describe or contradict is found by StrictnessCheck per column path, for example "Patient.gender: the
  definitions declare a singular element and the source carries an array". It names no resource and no
  value, and it reaches no caller as data.
- **HAPI's XML route fails where HAPI's model needs a value**, which is the behaviour 9.9.0 had: a bad date or
  unpadded date, an integer overflow, a bad decimal, bad base64. It also fails where 133 says it must not: a
  code outside a required binding (`gender: "xyz"` or `"Male"`), an extension with both a value and nested
  extensions, and a document of an unknown resource type, which should be left out. And it drops a
  whitespace-only string silently.
- **Document-level rejects.**
  - JSON: malformed JSON fails. A duplicate key fails by accident, as `AMBIGUOUS_REFERENCE` from the
    duplicated inferred column, not as a duplicate key. A top-level array is split into two resources
    silently, and text after the object is ignored silently.
  - `from_xml`: malformed XML and a duplicate attribute fail. **Text after the root is ignored, and a second
    root is dropped, silently.** A document with no namespace is accepted. A document with a prefixed FHIR
    namespace, which is valid FHIR, is **left out silently**, because the route selects documents by the
    root's literal name, as xml-native.md found. HAPI reads it correctly.
- **The `from_xml` route stores every value as text and then types it as the JSON reader would**, so it agrees
  with the JSON route value for value. It stores `" Smith "` with its spaces (`ignoreSurroundingSpaces=false`),
  and it keeps a narrative with markup byte for byte.

### OQ-1's losses on A0

| Loss | JSON route | `from_xml` | HAPI XML |
|---|---|---|---|
| The decimal's lexical form (T124b, T132, T134h) | `1.50` → `1.5`, `0.000000123456789` → `1.23456789E-7`, `12345678901234567890.12345` → `1.2345678901234567E19`, `1.0e2` → `100.0`. An integral decimal is stored as `100` or `100.0` depending on the rest of the batch | Same, through a double | Same |
| The granularity of a contradicting value (T124j) | **Whole column, for every row of the batch.** In one batch of 38 Patients, one `"active": "true"` nulled `active` in every row, and one `name` given as an object removed `name` from every Patient that carried one | Same: one repeated `gender` nulls `gender` for every Patient in the batch, and one repeated `family` nulls every `family` | Not observed: HAPI keeps the first value, per row |
| Numbers and structures stored as text without a finding (T124j) | `"family": 123` alone is nulled with a column finding. In a batch with string families it is **stored as `"123"` silently**. In the same batch, `gender` and `birthDate` given as arrays are stored as the **text `["male","female"]` and `["1980-01-01"]`**: the column widened to string, so the bad row holds a wrong value and the other rows keep theirs | Not applicable (all values are text) | Not applicable |
| Empty structures and null-only arrays on export (T134h) | `maritalStatus: {}` alone disappears. In a batch it is stored as `{coding: null}`, a present structure holding nothing, which egress writes back | Same: `<maritalStatus/>` in a batch is stored as a present, empty structure | - |
| Per-value findings, and values a converter cannot store (T124a, T134g) | None per value. An integer overflow and bad base64 become null silently | Same | Fails the job instead |
| The `_x` array without `x` (T078e) | Element kept (`given: null`), extension lost with a column finding | Same; in a batch, `given: [null]` | Dropped, as HAPI drops it |

Principle 6, the same input giving the same outcome, fails on both routes. In the batch runs, 16 of 38 Patient
rows on the JSON route and 21 of 38 on `from_xml` are stored differently from the same document stored alone.
That is `fixtures_run.txt`, under "batch".

## What this changes for B, A+ and A-variant

- **Every candidate must stay close to A0's numbers, and this harness can only just tell.** A0 sits at the bar,
  pooled 0.87 to 1.10. Within one label, identical code varied by up to ±25% between replicates. A candidate's
  label should therefore be run with at least the two replicates used here (extensions off and on), compared
  on pooled minimums, and preferably run in the same batch as a fresh A0 label.
- **XML: the `from_xml` route is the base to extend, for A+ and as A-variant's XML route.** It needs no FHIR
  parser, each of its two parses (infer, then read) is a single Spark XML parse rather than HAPI plus JSON, and
  it decodes in about half HEAD's time on
  the large types. Every value is attribute text, so the decimal's text is available without a pre-pass: the
  JSON-shaped step casts it to a double only to fit `toLayout`'s current converter. Four gaps must be closed
  by whichever candidate adopts it:
  - It selects documents by the root's literal name, so a prefixed FHIR namespace is left out silently.
  - Text or a second root after the document element is ignored silently.
  - A document with no namespace is accepted.
  - The narrative comes from a regular expression over the raw document, with the cases xml-native.md lists
    (a contained narrative, a comment before `</text>`, CDATA).
- **Bundles: fit once per source, not once per type.** Both routes infer over every Bundle for each requested
  type. For `from_xml` that is a constant of about 7.5 s per type, which is why its Patient and Condition Bundle
  fits are slower than HAPI's. One inference shared by all types would remove it. That is a change to how
  `encode_bundle` is called, which ticket 16 should consider.
- **A-variant.** `parse_json` rejects a duplicate key by default, but it accepts a top-level array and silently
  ignores anything after the first value. The array can be caught after parsing. The trailing text cannot be
  caught by any variant function, so A-variant needs a scan of its own for it, and that scan has to be
  measured as part of A-variant's cost.
- **A+.** A leaf disagreement widens only the leaf, so resolving it per value costs one `from_json` per
  affected leaf. A shape disagreement widens the whole subtree to JSON text for every row of the batch. That
  happens with a single object where an array belongs, an array for a single element, or a string for a
  structure. The benchmark data has no such disagreement, so A+ should also be measured on a deliberately messy
  variant of the data (one shape error per file in, say, `name` and `code`) to show its worst case. The harness
  takes a new data directory without change.
- **Findings delivery.** `observe` can carry counts and a bounded, deterministic sample (`min(struct)` per kind,
  or k hash buckets per kind) in the write's own action. Counts by path need the paths at planning time, which
  the definitions supply if indices are left out. No candidate needs a second pass for the summary.
- **B** is unaffected by the probes. It is the only candidate whose parser would see JSON syntax errors (a
  duplicate key, a top-level array, trailing text) as a matter of course.
- **Ticket 31.** An unparseable stored decimal is already empty and never fails, so principle 7 holds for
  decimals today, unlike dates. Three things contradict principle 7's one grammar: surrounding
  whitespace is accepted, a valid 40-digit decimal reads as empty, and the quantity comparison reads that
  decimal differently from the decimal path.

## Deviations from the brief

1. **The `from_xml` route is Python over py4j, not Java in a new build.** It reaches the definitions
   (`DefinitionCanonicalStructure`) and the package-private `ResourceTransformer.toLayout` from Python, on A0's
   unmodified jar. That keeps a single jar under test, with no new version in `~/.m2`. The cost is the time
   Python spends building the plan, which is recorded per sample and reported both with and without.
2. **Probes are in `spike-08/probes/`**, not in `ingress-strictness/`, so that the spike's evidence stays
   together.
3. **Spark versions.** Probes 1 to 3 ran on PySpark 4.0.2, the project's version, as the existing probes did.
   Probe 4, the gate, the fixtures and every timing ran on PySpark 4.0.4 in the harness venvs, the version of
   all earlier harness results.
4. **Decode starts from Parquet read back**, not from a cache (see the definitions), with a scan-only control.
5. **Bundle decodes are inflated by the cached raw Bundles.** This was found after the runs and left as it is,
   so that every label is measured the same way. The lines decode is the one to read.
6. **Pooled ratios.** The brief asks for min and mean of five, which are reported (`results/tables.md`). Because
   the replicate noise exceeds 10%, the conclusions rest on ratios of the minimum of ten instead. The two
   settings are pooled as replicates, which is justified because neither commit reads the extensions flag on
   these paths.
7. **Fixtures.** Six JSON fixtures were added to show the OQ-1 losses (`fixtures/fixtures-json-extra.tsv`).
   The XML set was hand-written, with the JSON-syntax cases marked not applicable. What decision 133 expects of
   each fixture is my reading of it, and is in `fixtures_run.py`'s `EXPECT`. Two are marked unsettled because
   the decision does not say: two choice types at once (c6-choice-two-types) and a document with no
   `resourceType` (c7-no-resourcetype). The XML comment fixture is expected to be stored, since a comment is
   not content. HEAD's HAPI XML route was run beside `from_xml` as a reference.
8. **Smoke test.** `phases.py` was smoke-tested at size `s` for every source and route, on both the bar's and
   A0's builds. `run-phases.sh`, a thin loop around it, ran for the first time at size `l`.
9. **Harness additions.** The following were added beside the existing scripts in the shared harness:
   `run-phases.sh`, `scripts/phases.py`, `scripts/fromxml_route.py`, `scripts/gate_fromxml.py`,
   `compare_phases.py`, `summary_phases.py`, `pooled_phases.py`, and a section appended to its `README.md`.
   No existing script or result was changed.
10. **The A0 and bar jars were not rebuilt.** Both matched their 2026-10-17 builds by SHA-256, so the bench
    worktrees and `~/.m2` were left as they were.

## Reproduction

From the harness directory, `/Users/szu004/dev/pathling/.local/work/issue/2367/pathling-compare`. The
copies in `harness/` are byte-identical to the scripts that ran.

```bash
H=$PWD
WT=/Users/szu004/dev/pathling/.claude/worktrees
# Size-l labels, one after another, nothing else running.
PATHLING_SRC=$WT/bench-26d7332 ./run-phases.sh phases-pof-switch-26d7332 $H/venv-26d7332 pathling l
PATHLING_SRC=$WT/bench-514f8f8 ./run-phases.sh phases-10.0.0-SNAPSHOT-514f8f8 $H/venv-514f8f8 pathling l
PATHLING_SRC=$WT/bench-514f8f8 ./run-phases.sh phases-fromxml-514f8f8 $H/venv-514f8f8 fromxml l
python3 compare_phases.py phases-pof-switch-26d7332 phases-10.0.0-SNAPSHOT-514f8f8 phases-fromxml-514f8f8
python3 pooled_phases.py phases-pof-switch-26d7332 phases-10.0.0-SNAPSHOT-514f8f8
python3 pooled_phases.py phases-10.0.0-SNAPSHOT-514f8f8 phases-fromxml-514f8f8 [--py4j]
```

Each `run-phases.sh` call makes its own Ivy home, `ivy/<label>/` with `cache` and `jars`. The rest share one
private Ivy home and the A0 venv:

```bash
P=$H/venv-514f8f8/bin/python; IVY=<scratch>/ivy; mkdir -p $IVY/cache $IVY/jars
export PYSPARK_PYTHON=$P PYSPARK_DRIVER_PYTHON=$P PYSPARK_SUBMIT_ARGS="--driver-memory 8g --conf spark.jars.ivy=$IVY pyspark-shell"
S=/Users/szu004/dev/sql-on-fhir.js/benchmark/data/synthea-clinical-wide/1/s
$P scripts/gate_fromxml.py $S data/xml/s data/xml-bundles/s data/xml-bundles-txn/s Patient Condition Encounter Observation
E=<pathling>/specs/001-parquet-on-fhir/evidence
(cd $E/spike-08/gate && $P decoders_equal.py $H/scripts $H/data/xml/s Patient Condition Encounter Observation)
(cd $E/spike-08/fixtures && $P fixtures_run.py $H/scripts $E/ingress-strictness .)   # writes fixtures_run.txt
(cd $E/spike-08/probes && $P -I engine_decimal_probe.py)
# Probes 1 to 3 on the project's PySpark 4.0.2:
Q=<pathling>/lib/python/.venv/bin/python
(cd $E/spike-08/probes && for p in parse_json widening observe; do PYSPARK_PYTHON=$Q PYSPARK_DRIVER_PYTHON=$Q $Q -I ${p}_probe.py; done)
```

## Phase 2: B, A+ and A-variant

Phase 2 was approved on 2026-10-19. The owner chose "quiet first, then go". The three candidates are built,
gated at size `s` and run against the fixtures. After a restart of the machine, the whole size-`l` sequence ran
unattended from `phase2/batch-l.sh`, following `phase2/RUNBOOK.md`, from 2026-10-19 19:47 to 2026-10-20
01:36. Its numbers are under [Results at size `l`](#results-at-size-l-1). The smoke figures at size `s` in
`phase2/gate/` are not for the record.

### The candidates as built

Every candidate covers the four benchmark types, ingest and egress. Each is generated from the definitions
(`DefinitionCanonicalStructure`), with nothing written per type. Each builds the stored layout itself, and at
size `s` that layout equals the schema and rows HEAD's `toLayout` produces, apart from decimal text. Each
applies decision 133's datatype checks: dates, times and instants by regex and calendar parse; decimals and
the integer family by regex and range; booleans; base64. The text-only types pass through, and the empty
string is invalid. Undescribed content is found per value; `null`, `{}`, `[]` and `""` are invalid; `contained`
is an informational finding. The policies are `fail`, the default, which stops at the first invalid value and
names the resource, the path and the value, and `drop`, with an opt-in `_findings` column of
`(path, kind, value)`. Timing uses `fail`, so every check is evaluated.

| | Ingest | Egress | Where |
|---|---|---|---|
| **A+** | Then `spark.read.json` with the inferred schema of the marked documents, which is the fit pass, and comparison with the definitions path by path. A complex path that widened to text is resolved per value with `from_json`, typed by a second inference over just its values of the declared shape. Generated SQL makes the checks and builds the stored values and the findings | `to_json`, with each decimal marked and unmarked after (the existing T100j mechanism), so it is written with its text. Like A0, a null structure stays null and an all-null structure or a null-holding array passes through (decision 71) | `Spike08Marker.java`; `spike_routes.py` (`MarkedJson`, `Gen`, `APlusRoute`, egress) |
| **A+, XML** | The `from_xml` route with the gaps phase 1 found closed: a prefixed FHIR namespace is rewritten and read; a missing namespace, text after the root and a second root are refused (`Spike08XmlDocCheck`, a full StAX pass per document); a primitive is parsed in the shape the definitions give it, so text content or a repeated singular element is a finding for that row, not a column lost. The same generated checks follow | `to_xml`, decimal text as stored, empty elements left out | `XmlText`, `APlusRoute.fit_xml`; `Spike08XmlDocCheck.java` |
| **A-variant** | `parse_json`, then `schema_of_variant_agg` as the fit pass. A path whose rows disagree, merged to `VARIANT`, is resolved by a second aggregation over its values of the declared shape. Objects are read as `map<string, variant>`, so extra keys are found; token types come from `schema_of_variant`. `Spike08JsonDocCheck`, a Jackson pass per document, refuses a top-level array and text after the object, which `parse_json` accepts | HEAD's, as ticket 08 says | `Variant`, `AVariantRoute`; `Spike08JsonDocCheck.java` |
| **B** | A streaming parser of our own over Jackson (JSON) and Woodstox StAX (XML), on one definitions-driven core. The fit pass is one job: each partition streams its documents and returns the paths it saw. Encode parses each document straight into a stored row, checking each value as it goes. Bundles take one pass: every entry's `fullUrl` and id are collected, the type's resources are parsed, and their `urn:` references are patched (FR-062). The narrative is copied by the parser's character offsets | A renderer of our own for JSON and XML, a decimal written with its stored text, empty elements left out | `Spike08B`, `Spike08Json`, `Spike08Xml`, `Spike08Layout`, `Spike08Def`, `Spike08Checks`, `Spike08Findings` |

**A+'s pre-pass and A-variant's XML.** A+'s Jackson pre-pass (`Spike08Marker`) marks every number with its own
text, and marks any string that could be mistaken for another token's text once a column widens. It removes
and records `null`, `{}`, `[]` and `""`, and refuses a duplicate key, a top-level array and text after the
object. A-variant has no XML route of its own (Spark 4.0.2 cannot parse XML into a variant), so its XML is
served by A+'s XML route.

**FR-049 and FR-050.** No prototype imports Catalyst. The Java uses `org.apache.spark.sql` public API only:
`Dataset`, `Encoders`, `Row`, `RowFactory`, the `types` package and Java UDFs. B hands its rows to Spark
through `Encoders.row(schema)`, Spark's generic row encoder. A+'s pre-pass and the document checks use
`Encoders.STRING()`, as HEAD's `XmlConversion` does. Neither is an encoder of our own or a FHIR object per row,
but both are, internally, Spark expression encoders. **Whether FR-050's "no expression encoder" admits
Spark's generic row encoder is for the owner to say.** B depends on the answer.

**Built as a separate jar.** The prototype jar is compiled with `javac` against A0's unmodified runtime jar
(`phase2/proto/build.sh`) and added through `--jars`. So there is no Pathling build, no new version, and
nothing written to `~/.m2`. The A+ and A-variant generators are Python over py4j, as the `from_xml` route
was. Their plan-building time is recorded per sample, as in phase 1.

### Correctness at size `s` (`phase2/gate/gate-s.txt`)

For all four types (24,424 resources) and every source each candidate supports, the stored dataset was
compared with HEAD's encoding of the JSON, and the decoded documents with the input:

- **Schema and values: equal for every candidate and source.** The only differences are decimal text: 2,412 values where HEAD stores a
  double's text (`0.0`, `8.823084504462283E-4`) and A+ and B store the source's (`0`,
  `0.0008823084504462283`). A-variant stores the variant's form instead (`1.50` becomes `1.5`).
- **Decode.**
  - A+ and B decode every document **lexically equal** to the JSON input, and **byte-identical** to the XML
    input: 10,085 of 10,085 Observations, against A0's 8,090.
  - A-variant decodes through HEAD, so it keeps A0's losses: Observation is 8,088 of 10,085 lexically equal.
- **Messy data (`gate_messy.py`).** One shape error at an intermediate node per file, under `drop`:
  - A0 loses that element in every row of the file. That is all 125 Patients, 6,406 Conditions, 7,808
    Encounters and 10,085 Observations.
  - All three candidates lose it in the one bad row only, with one per-value finding naming the path and the
    value.
- **Code generation.** As first built, A+'s egress left out every all-null structure and null-only array
  itself (the deferred part of FR-019), and the gate then logged nine "Code grows beyond 64 KB" errors for
  Condition and Encounter, after which Spark fell back from whole-stage code generation. The gate decodes the
  lazy encoded plan, so ingest and egress were compiled as one stage, and which of them overflowed alone was
  not established. Once A+'s egress followed decision 71 as A0's does (deviation 6), the same gate, with
  ingest and egress still in one stage, logged none, and its output was unchanged. Ingest at size `l` has a
  larger fitted schema than at size `s`, so the size-`l` logs were read for these errors as well. They hold
  none (see [Code generation at size `l`](#code-generation-at-size-l)).

### Coverage (`fixtures/fixtures_candidates.py`, output `fixtures/fixtures_candidates.txt`)

The A0 columns are from phase 1. Every failure counted for a candidate is the route's own message (`Invalid
content in <Type>/<id> at <path> ...` or `Invalid <Type> document: <reason>`), or, for four of the reject
fixtures, Spark's malformed-record or duplicate-key error. None is an analysis, cast or code generation
error. The expectations are unchanged, and `c6-choice-two-types` and
`c7-no-resourcetype` stay unsettled. For a candidate, "invalid" is met only where `fail` stops the job
**and** `drop` records a finding that is not informational.

| Expectation | A0 JSON | A0 `from_xml` | HAPI XML | A+ JSON | A+ XML | A-variant | B JSON | B XML |
|---|---|---|---|---|---|---|---|---|
| store | 13 of 18 | 16 of 22 | 13 of 22 | 15 of 18 | 19 of 22 | 13 of 18 | 15 of 18 | 19 of 22 |
| invalid: fails, and recorded per value under `drop` | 0 of 29 | 0 of 23 | 5 of 23 (fails, no findings) | **29 of 29** | **23 of 23** | **29 of 29** | **29 of 29** | **23 of 23** |
| contained: info finding | partial | partial | no | yes | yes | yes | yes | yes |
| left-out | 2 of 2 | 2 of 2 | 1 of 2 | 2 of 2 | 2 of 2 | 2 of 2 | 2 of 2 | 2 of 2 |
| reject | 2 of 4 | 2 of 5 | 4 of 5 | 4 of 4 | 5 of 5 | 4 of 4 | 4 of 4 | 5 of 5 |
| Principle 6: rows of a batch stored differently from alone | 16 of 38 | 21 of 38 | - | 1 of 37 | 1 of 36 | 1 of 37 | **0 of 37** | **0 of 36** |

**The "store" misses.**

- **All three:** the three `c0-prim-ext-*` fixtures fail on every candidate, because the stage-1 layout
  stores no primitive metadata before M5. The element is kept and its extensions are lost. That is the
  layout's limit, not any mechanism's.
- **A-variant** also misses `c0-decimal-text` and `c0-quantity-integral`, the decimal's text.

**The one batch row each for A+ and A-variant** is a structure that every value of which was invalid. It is
stored as an empty structure (`[{}]` or `[]`) in a batch and as absent alone. B stores no empty structure.

### OQ-1's losses, per candidate

| Loss | A+ | A-variant | B |
|---|---|---|---|
| The decimal's lexical form | Removed (JSON and XML) | **Kept**: the variant normalises `1.50` to `1.5` and an exponent to a double | Removed (JSON and XML) |
| The granularity of a contradicting value | Removed: per value, also on the messy data | Removed | Removed |
| Numbers as text without a finding | Removed: a type finding | Removed | Removed |
| Empty structures and null-only arrays on export | Removed by the egress post-pass | Kept (HEAD's egress) | Removed by the renderer |
| Per-value findings, unstorable values | Removed | Removed | Removed |
| The `_x` array without `x` | Element kept, metadata not stored (M5) | Same | Same |

### Code each candidate owns

Rough counts of non-blank, non-comment lines in the prototypes. The Python generators would be Java in a real
implementation, so their counts indicate size, not a final figure. All three also rely on the definitions
(`fhir-schema`) and on Spark.

| Candidate | Components | Lines |
|---|---|---|
| A+ | Pre-pass (`Spike08Marker` 162); XML document check (28); generator core (`Gen` and helpers, about 100); JSON adapter (`MarkedJson`, 105); XML adapter (`XmlText`, 86); route, resolution and Bundle handling (`APlusRoute` and the parse schema, about 175); egress (about 70); checks in SQL (`spike_checks.py`, 85); reused from the `from_xml` route (inference, definitions access, narrative; about 150) | about 960 |
| A-variant | Document check (`Spike08JsonDocCheck`, 23); generator core (shared with A+, about 100); variant adapter (88); type parser (57); route (52); checks in SQL (shared, 85); egress is HEAD's | about 400 |
| B | Definitions graph (`Spike08Def`, 84); checks (97); findings (60); layout and fitted schema (104); JSON parser, presence and renderer (384); XML parser, presence, Bundles and renderer (452); entry points (86) | about 1,270 |

### Prototype limits

- **Resolution below an array.** A+ and A-variant resolve a widened path per value only where no array lies
  above it. That covers `name`, `code.coding` and `type`, the paths the messy data exercises. Below an array,
  the prototypes report the values as findings instead of resolving them.
- **Narratives.** A+'s XML takes the narrative by regular expression, xml-native.md's option 3, with its known
  cases. B takes it by the parser's offsets.
- **JSON Bundles** were not built for B or A+, because the benchmark has none.
- **Primitive metadata** is not stored by any candidate (M5).

### Results at size `l`

The batch ran its 30 steps from 2026-10-19 19:47 to 2026-10-20 01:36 and ended with exit status 0. No step
failed, none was resumed, and the noise gate passed after every slice. Every expected result file exists:
A0, A+ and B on all four sources, A-variant on JSON only, and the four messy labels on the messy JSON.

The results are in `phase2/results/`:

| File | Holds |
|---|---|
| `tables.md` | Every source, type and phase as pooled minimums in seconds: the bar, both A0 labels, and each candidate, with the candidate's ratio to each of them, with and without py4j plan building. It also holds the messy runs. Generated by `tables.py` |
| `pooled.txt` | The output of `pooled_phases.py` for every comparison below, in milliseconds |
| `comparison.txt` | The output of `compare_phases.py`: mean and minimum per label and extensions setting, fidelity, row counts, and Spark jobs per phase |
| `spread.txt` | The output of `p2_spread.py` over A0's two labels, for every source |
| `<label>/` | Each `p2-*` label's result files, `.out` files, notes, conditions and resolved-jar hash, as copied from the harness. The `.err` files stay in the harness, as in phase 1 |
| `batch-p2/` | The batch log, each step's log and the machine's state before it, `preflight.txt`, `after.txt`, `slices.md`, `spread-*.txt` and the prototype jar's hash |

Every ratio is the ratio of pooled minimums, as in phase 1. The bar and each A0 label pool their
extensions-off and extensions-on runs, ten samples. A candidate pools its `r1` and `r2` labels, also ten
samples, under `fail`. Each range below is over the four types. The top of each fit range for A+ and
A-variant, and of most other wide ranges, is Patient, the smallest type, where fixed costs dominate.

**The bar ran in an earlier session.** `phases-pof-switch-26d7332` was measured in phase 1, on 2026-10-19 from
13:11 to 14:14, before the machine was restarted (deviation 7 below). A0 in this batch, which
is the same code as phase 1's A0, ran faster than the bar on most items:

| Source | A0 `r1` against the bar: fit / encode / decode | A0 `r2` against the bar: fit / encode / decode |
|---|---|---|
| JSON lines | 0.77 - 1.00 / 0.84 - 0.97 / 0.96 - 1.01 | 0.87 - 0.99 / 0.82 - 0.95 / 0.94 - 1.01 |
| XML lines | 0.93 - 0.95 / 0.94 / 0.92 - 0.99 | 0.85 - 0.95 / 0.93 - 0.96 / 0.93 - 1.00 |
| XML collection Bundles | 0.97 - 0.99 / 0.96 - 1.00 / 0.97 - 1.00 | 0.97 - 0.98 / 0.96 - 0.97 / 0.94 - 0.99 |
| XML transaction Bundles | 0.85 - 0.91 / 0.86 - 0.88 / 0.78 - 0.95 | 0.85 - 0.90 / 0.86 - 0.87 / 0.79 - 0.97 |

Against phase 1's A0 label (`phases-10.0.0-SNAPSHOT-514f8f8`), the two A0 labels of this batch are 0.78 to
1.06 (`r1`) and 0.87 to 1.01 (`r2`) over every source, type and phase. A candidate's ratio to the bar
therefore includes the difference between the two sessions. Its ratio to an A0 label of this batch does not.

#### Each candidate against the bar and against A0

**A+**, on all four sources. The second set of columns subtracts A+'s py4j plan building, sample by sample.
Encode builds no plan in Python, so its figures are the same in both sets.

| Source | Phase | /bar | /A0 `r1` | /A0 `r2` | py4j removed: /bar | /A0 `r1` | /A0 `r2` |
|---|---|---|---|---|---|---|---|
| JSON lines | fit | 3.01 - 26.69 | 3.26 - 34.70 | 3.28 - 30.15 | 1.12 - 1.43 | 1.38 - 1.49 | 1.27 - 1.51 |
| | encode | 3.35 - 6.12 | 3.52 - 7.25 | 3.70 - 7.43 | 3.35 - 6.12 | 3.52 - 7.25 | 3.70 - 7.43 |
| | decode | 1.32 - 2.28 | 1.37 - 2.26 | 1.39 - 2.26 | 0.86 - 1.35 | 0.85 - 1.35 | 0.85 - 1.44 |
| XML lines | fit | 0.91 - 10.84 | 0.98 - 11.61 | 0.97 - 12.77 | 0.46 - 0.62 | 0.48 - 0.66 | 0.48 - 0.72 |
| | encode | 2.72 - 8.21 | 2.88 - 8.74 | 2.82 - 8.86 | 2.72 - 8.21 | 2.88 - 8.74 | 2.82 - 8.86 |
| | decode | 0.50 - 1.65 | 0.54 - 1.68 | 0.53 - 1.65 | 0.45 - 0.85 | 0.49 - 0.87 | 0.49 - 0.86 |
| XML collection Bundles | fit | 1.11 - 2.51 | 1.15 - 2.54 | 1.14 - 2.58 | 0.87 - 1.79 | 0.90 - 1.81 | 0.89 - 1.84 |
| | encode | 1.29 - 1.99 | 1.35 - 2.07 | 1.35 - 2.07 | 1.29 - 1.99 | 1.35 - 2.07 | 1.35 - 2.07 |
| | decode | 0.40 - 1.04 | 0.40 - 1.08 | 0.40 - 1.11 | 0.39 - 0.63 | 0.39 - 0.65 | 0.39 - 0.67 |
| XML transaction Bundles | fit | 1.01 - 2.29 | 1.16 - 2.50 | 1.16 - 2.54 | 0.81 - 1.68 | 0.93 - 1.83 | 0.93 - 1.86 |
| | encode | 1.24 - 2.00 | 1.45 - 2.28 | 1.45 - 2.30 | 1.24 - 2.00 | 1.45 - 2.28 | 1.45 - 2.30 |
| | decode | 0.38 - 0.86 | 0.40 - 1.11 | 0.40 - 1.09 | 0.37 - 0.51 | 0.39 - 0.66 | 0.38 - 0.65 |

A+'s plan building took 0.98 to 3.17 s per fit and 0.06 to 0.26 s per decode, as the minimum over each label's
five samples. It was largest for Patient.

**B**, on all four sources. B builds no plan in Python.

| Source | Phase | /bar | /A0 `r1` | /A0 `r2` |
|---|---|---|---|---|
| JSON lines | fit | 0.38 - 0.52 | 0.41 - 0.67 | 0.42 - 0.58 |
| | encode | 1.10 - 1.46 | 1.31 - 1.53 | 1.34 - 1.61 |
| | decode | 0.83 - 1.12 | 0.84 - 1.11 | 0.86 - 1.11 |
| XML lines | fit | 0.12 - 0.25 | 0.13 - 0.27 | 0.12 - 0.29 |
| | encode | 0.44 - 0.58 | 0.46 - 0.62 | 0.46 - 0.63 |
| | decode | 0.11 - 0.33 | 0.12 - 0.34 | 0.12 - 0.33 |
| XML collection Bundles | fit | 0.14 - 0.25 | 0.15 - 0.25 | 0.15 - 0.26 |
| | encode | 0.27 - 0.34 | 0.27 - 0.35 | 0.28 - 0.35 |
| | decode | 0.11 - 0.44 | 0.11 - 0.46 | 0.11 - 0.47 |
| XML transaction Bundles | fit | 0.13 - 0.22 | 0.15 - 0.24 | 0.15 - 0.24 |
| | encode | 0.23 - 0.29 | 0.27 - 0.34 | 0.27 - 0.34 |
| | decode | 0.11 - 0.33 | 0.11 - 0.42 | 0.11 - 0.41 |

**A-variant**, on JSON only. Its decode is HEAD's and builds no plan in Python.

| Source | Phase | /bar | /A0 `r1` | /A0 `r2` | py4j removed: /bar | /A0 `r1` | /A0 `r2` |
|---|---|---|---|---|---|---|---|
| JSON lines | fit | 4.65 - 50.19 | 5.02 - 65.24 | 5.06 - 56.70 | 1.13 - 1.38 | 1.34 - 1.49 | 1.27 - 1.50 |
| | encode | 19.95 - 40.31 | 20.98 - 41.48 | 22.05 - 42.50 | 19.95 - 40.31 | 20.98 - 41.48 | 22.05 - 42.50 |
| | decode | 0.92 - 1.07 | 0.91 - 1.11 | 0.92 - 1.12 | 0.92 - 1.07 | 0.91 - 1.11 | 0.92 - 1.12 |

A-variant's plan building took 2.07 to 5.25 s per fit.

**Absolute values for Observation**, the largest type, in seconds, as the pooled minimum of ten. The figure in
brackets has the candidate's py4j plan building removed.

| Source | Phase | Bar | A0 `r1` | A+ | B | A-variant |
|---|---|---|---|---|---|---|
| JSON lines | fit | 1.07 | 0.99 | 3.23 (1.47) | 0.41 | 4.98 (1.48) |
| | encode | 1.92 | 1.72 | 6.89 | 2.53 | 48.28 |
| | decode | 0.75 | 0.75 | 1.13 (1.02) | 0.64 | 0.76 |
| XML lines | fit | 5.69 | 5.26 | 5.20 (3.10) | 0.70 | - |
| | encode | 6.46 | 6.06 | 29.62 | 2.81 | - |
| | decode | 5.91 | 5.55 | 5.03 (4.85) | 0.68 | - |
| XML collection Bundles | fit | 9.01 | 8.70 | 9.98 (7.84) | 1.28 | - |
| | encode | 9.88 | 9.52 | 19.70 | 3.25 | - |
| | decode | 15.23 | 15.16 | 6.09 (5.91) | 1.71 | - |
| XML transaction Bundles | fit | 10.75 | 9.39 | 10.88 (8.73) | 1.37 | - |
| | encode | 11.62 | 10.20 | 23.26 | 3.39 | - |
| | decode | 15.50 | 14.76 | 5.97 (5.79) | 1.67 | - |

Phase 1's caveat about the bundle decodes holds for every route here: the raw Bundles stay cached in the
driver while a bundle source runs, and the lines figure is the one to read for decode.

**Spark jobs per phase.** A0, A+ and B ran one job per phase in every sample on every source, as phase 1
found for A0. A-variant's fit ran two jobs in every sample. On the messy data, A+'s and A-variant's fits ran
four jobs each, and A0's and B's one.

#### The A0 replicate spread

`p2_spread.py` compared A0's `r1` and `r2` on the minimum of ten, item by item, judging only items of at least
1 s (`phase2/results/spread.txt`). **All four sources passed: 33 items judged, worst |ratio − 1| 0.024, median
0.008**, against limits of 0.15 and 0.05. Per source, the judged items were 2 on JSON, 9 on XML lines, 11 on
collection Bundles and 11 on transaction Bundles, and the worst on each was about 0.02.

The JSON verdict rests on two items, Encounter and Observation encode, because every other JSON item is under
1 s. The unjudged JSON items ranged from 0.91 to 1.15. The 1.15 is the Patient fit, 82 ms against 95 ms.

#### The messy data, under `drop`

Each messy label ran once on `data/json-messy/l`, extensions on, five samples per item, so the four compare
with each other directly. Ratios are against A0's messy label (`p2-messy-pathling-514f8f8`), over the four
types:

| Candidate | Fit | Fit, py4j removed | Encode | Decode | Decode, py4j removed |
|---|---|---|---|---|---|
| A+ | 4.42 - 18.87 | 2.97 - 3.47 | 3.41 - 6.27 | 1.83 - 2.72 | 1.12 - 2.00 |
| A-variant | 5.53 - 32.59 | 2.32 - 3.50 | 10.50 - 24.02 | 1.15 - 1.43 | 1.15 - 1.43 |
| B | 0.40 - 0.50 | - | 1.31 - 1.58 | 0.97 - 1.13 | - |

- **A0 stores less on the messy data, so these ratios compare unequal work.** A0 drops the widened element from
  every row, and its stored leaf fields fall from 84, 36, 67 and 61 to 77, 31, 59 and 56 for Patient,
  Condition, Encounter and Observation. Its `.out` logs "Ignoring content that this layout does not store:
  `<path>`: the definitions declare a repeating element and the source carries a single value", six times
  for each type. The candidates store the full schema.
- **Against their own clean runs**, for Observation, A+'s fit with py4j removed was 3.47 s on the messy data and
  1.47 s on the clean, and its encode 6.44 s and 6.89 s. B's figures were 0.40, 2.53 and 0.64 s on the messy
  data and 0.41, 2.53 and 0.64 s on the clean.
- **A-variant's encode was faster on the messy data than on the clean**, for Observation 26.70 s against
  48.28 s, and from 1.76 to 2.03 times across the four types. The clean runs used `fail` and the messy runs `drop`. The
  cause of the difference was not established.

The per-type figures are in `phase2/results/tables.md`, under "Messy JSON".

#### Code generation at size `l`

**No "Code grows beyond 64 KB" error was logged, so none can be attributed to fit, encode or decode.** The search
covered 107 files: the 30 step logs and the batch log in `batch-p2/`, and every `.err` and `.out` file of the
twelve `p2-*` labels in the harness. Progress lines were split at carriage returns, and the patterns "grows
beyond" and "64 KB" (with or without the space) were matched without regard to case. The logs hold no `ERROR`
line. Their only `WARN` lines are Hadoop's `NativeCodeLoader` notice, once per process, and the 24
`ResourceTransformer` lines of A0's messy label described above.

#### Fidelity and row counts

Decoded documents against the input, on the first 500 rows per type, as `compare_phases.py` prints them
(`phase2/results/comparison.txt`). For JSON, "exact" compares numbers by value. The lexical count, which compares
numbers by their text, is taken for the candidates only. For XML, "exact" is string equality and "canonical"
compares after canonicalisation.

| Label | Source | Exact | Lexical (JSON) or canonical (XML) |
|---|---|---|---|
| A0 `r1` and `r2` | JSON | 500 of 500 for every type | Not taken |
| | each XML source | 500 of 500 for Patient, Condition and Encounter; **421 of 500 for Observation** | The same |
| A+ `r1` and `r2` | every source | 500 of 500 for every type | 500 of 500 for every type |
| B `r1` and `r2` | every source | 500 of 500 for every type | 500 of 500 for every type |
| A-variant `r1` and `r2` | JSON | 500 of 500 for every type | **Patient 495 of 500, Observation 421 of 500**; Condition and Encounter 500 of 500 |
| Messy A0 | messy JSON | **0 of 500 for every type** | Not taken |
| Messy A+ and B | messy JSON | 500 of 500 for every type | 500 of 500 for every type |
| Messy A-variant | messy JSON | 500 of 500 for every type | Patient 495 of 500, Observation 421 of 500; Condition and Encounter 500 of 500 |

- **Rows in, encoded and decoded are equal for every label, source, setting and type**: Patient 11,853,
  Condition 459,617, Encounter 598,548 and Observation 1,004,979.
- **A0's XML Observation, 421 of 500**, is the same on every XML source, in both settings and both labels. It
  equals phase 1's figure, and its cause is OQ-1's decimal.
- **A-variant's lexical losses** are the same in both labels and on the messy data. Observation's 421 matches
  the variant's decimal normalisation found at size `s`. **The cause of Patient's 495 was not established.**
- **A0's messy 0 of 500** follows from the element it drops from every row, as above.
- **The messy candidates' 500 of 500 does not cover the bad row.** `make_messy.py` puts the one bad document
  of each file at its middle line, which is outside the first 500 rows. The sample therefore does not show
  how a candidate stored that row.

#### Machine conditions

| | |
|---|---|
| Commits and jars | A0 `514f8f818e`, clean. A0's jar `3eb0a768…`, resolved in all twelve labels' private Ivy homes. The prototype jar `spike08-proto.jar`, built by the batch from `phase2/proto/`, SHA-256 `7a44177b411c22d6baa0a7215b9df532baa190e56e8a8dabc68648947c7c5f52` |
| Versions | As in phase 1: Java HotSpot 21.0.3, Python 3.12.7, PySpark 4.0.4. Apple M3 Pro, 11 cores, 36 GB |
| Start (`preflight.txt`, 2026-10-19 19:47:34) | Uptime 1 h 32 min after the restart. Load average 3.01, 2.69, 3.12. No swap in use. 48 GiB free, the disk 95% full. No Java process. Busiest processes: Microsoft Defender (`wdavdaemon_enterprise`) 79.7% CPU, Jamf Protect 23.7%, `claude` 22.0%, `trustd` 13.5%, `logd` 7.7% |
| Before each step (`*.conditions-before.txt`) | The script waits for the one-minute load to fall under 3, and it was 2.33 to 2.99 at every start. The five-minute load was 2.68 at the first step, then 5.37 to 12.78. Of the five busiest processes in each of the 30 snapshots, Jamf Protect was one in all 30 (8.2 to 35.1% CPU) and Microsoft Defender (`wdavdaemon_enterprise`) in 29 (2.1 to 45.9%), with JamfDaemon up to 10.5% and `trustd` in 29. Two spikes appeared once each: `launchd` 49.4% at 21:52 and `mobileassetd` 77.7% at 01:07. No swap was in use at any step. Free space fell from 48 to 44 GiB, and the disk went from 95% to 96% full |
| End (`after.txt`, 2026-10-20 01:36:22) | Load average 10.90, 10.38, 11.34. No swap in use. 44 GiB free, 96% full. No Java process. Busiest processes: Jamf Protect 70.1%, Microsoft Defender 32.9%, JamfDaemon 15.3%, `wdavdaemon` 13.2%, Defender's `epsext` 12.8% |
| Not recorded | The load during the runs. `after.txt` was taken in the second the last step ended, so its load average covers the last minutes of that step and does not show the load once Java had stopped |

In phase 1, 2.79 GB of swap was in use after the batch. In this batch none was in use at any snapshot.

#### Reproduction

The batch is `phase2/batch-l.sh`, run as `phase2/RUNBOOK.md` says. The outputs above come from these commands,
run from the harness directory, which read only the result files:

```bash
python3 p2_spread.py p2-a0-r1-514f8f8 p2-a0-r2-514f8f8
for c in aplus b avariant; do
  for ref in phases-pof-switch-26d7332 p2-a0-r1-514f8f8 p2-a0-r2-514f8f8; do
    python3 pooled_phases.py $ref "p2-$c-r?-514f8f8"; python3 pooled_phases.py $ref "p2-$c-r?-514f8f8" --py4j
  done
done
for r in aplus avariant b; do python3 pooled_phases.py p2-messy-pathling-514f8f8 p2-messy-$r-514f8f8 [--py4j]; done
python3 compare_phases.py p2-a0-r1-514f8f8 p2-{aplus,b,avariant}-r{1,2}-514f8f8
python3 compare_phases.py p2-messy-pathling-514f8f8 p2-messy-{aplus,avariant,b}-514f8f8
(cd results && python3 <evidence>/spike-08/phase2/results/tables.py $PWD)   # writes tables.md to stdout
```

### The original spike, beside A0 and B (JSON, 2026-10-20)

At the owner's request, the original Parquet on FHIR spike was timed on JSON at size `l`, in a batch of its own
with fresh A0 and B labels (`p3-*`), so that all three come from one session. The original spike is the branch
`spike/parquet-on-fhir`, linked from #2367, whose ingest dates from July 2024. Its reader and writer are
ported into the prototype jar (`proto/src/au/csiro/pathling/spike08/orig/`, entry points `Spike08Orig`), with
only the changes Spark 4 forces (deviation 9).

**It is a reference, not a candidate.** It is Spark's JSON inference followed by a transform that walks HAPI's
definitions over the inferred schema. The transform keeps each decimal's inferred value as text, adds a
`decimal(38,6)` column beside it, casts the integer types, decodes base64 and drops `contained`. It makes none
of decision 133's checks and does not build the stored layout. Its egress casts each decimal to
`decimal(38,6)` and renders the row with `to_json`. Its fidelity follows from that design: exact 0/500 for
Patient and Observation and 2/500 for Encounter, from decimals cut to six places and absent structures written
as `{}`, while Condition is 500/500. A0 and B are 500/500 on every type.

Pooled minimums, as the range over the four types, against this batch's A0 `r1` (`phase2/results/pooled-p3.txt`
holds every row, against the bar and both A0 labels):

| | Fit | Encode | Decode | Fit and encode, summed |
|---|---|---|---|---|
| B | 0.41–0.62 | 1.24–1.58 | 0.86–0.96 | 0.99–1.18 |
| Original spike | 0.39–0.71 | 0.61–0.86 | 0.91–1.05 | 0.53–0.80 |

The last column sums each phase's pooled minimum, an approximation of the minimum of the sum. Against the
bar it is 0.88 to 1.08 for B and 0.49 to 0.70 for the original spike. A0's two labels were within 0.031 of
each other (2 items judged), and no "Code grows beyond 64 KB" error was logged.

**A0's fit costs about 2.5 times the original spike's, though both infer with `spark.read.json`.** The likely
cause, not measured on its own, is A0's selection of the requested type before inference
(`PathlingContext.selectForJson`, a `get_json_object` on `$.resourceType`), which parses every document once
more inside the fit's scan, and again in the encode. The original spike has no such step.

### Profile of B's encode (Observation, JSON, 2026-10-20)

At the owner's request, B's encode of Observation at size `l` (1,004,979 rows) was recorded with Java Flight
Recorder, over five encodes after one warm-up (`phase2/profile/`: `profile_encode.py` records,
`jfr_stages.py` groups each CPU sample by the innermost stage on its stack). This is a diagnosis on the
harness's JVM, not a timing under the batch's conditions.

| Stage | Share of CPU samples |
|---|---|
| `Encoders.row`, `Row` to Spark's internal row (generated code) | 35% |
| Jackson parse | 27% |
| Checks: regular expressions 13%, calendar parse 5%, duplicate keys 4%, other 2% | 24% |
| B's own row building (`Spike08Json`, findings, maps, lists) | 13% |
| Input, Spark's internal row to `String` | under 1% |

**The serializer runs interpreted.** Every sample in Spark's generated `serializefromobject_doConsume_0` was
an interpreted frame. HotSpot does not compile a method over 8,000 bytes of bytecode, and the serializer
generated for Observation's nested schema exceeds that. Encode times, runs 2 to 5 (`profile/encode-times.txt`):

| Setting | A0 | Original spike | B |
|---|---|---|---|
| Default | 1.89 s | 1.25 s | 2.80 s |
| JVM `-XX:-DontCompileHugeMethods` | 1.85 s | 1.15 s | 0.89 s |
| `spark.sql.codegen.hugeMethodLimit=8000` | | | 0.91 s |
| `spark.sql.codegen.wholeStage=false` | | | 0.85 s |
| `spark.sql.codegen.methodSplitThreshold=256` | | | 2.70 s |

The limit costs B about two thirds of its encode and costs A0 and the original spike little. With the
generated code compiled, the samples spread as Jackson 37%, checks 28%, `Encoders.row` 20% and B's row
building 13%. `hugeMethodLimit` is a runtime SQL setting: above it, Spark stops fusing the stage into one
generated method. The size-`l` timings of B elsewhere in this README were taken with the default, and B's XML
encodes, which also hand their rows over through `Encoders.row`, were not profiled.

### Deviations in phase 2

1. **Size `l` deferred.** As instructed on 2026-10-19, nothing was timed at size `l`. The batch and runbook
   are ready, and the batch was dry-run end to end at size `s` (`phase2/RUNBOOK.md`, "Dry run"). An A0 label
   that fails stops the batch. A candidate or messy label that fails is recorded and the batch goes on.
   A grep of the size-`l` JSON found no empty string, `null`, `{}`, `[]` or `dateTime` without a zone, so the
   clean data is not expected to trip `fail`. The size-`l` batch then ran on 2026-10-19 and 2026-10-20, after
   a restart, and no clean label tripped `fail` (see [Results at size `l`](#results-at-size-l-1)).
2. **A separate prototype jar,** not a Pathling build under a new version. A0's jar stays as it is, and
   nothing enters `~/.m2`.
3. **Replicates.** Each candidate runs as two replicate labels (`r1`, `r2`), both with extensions on only,
   and is compared on the two pooled. The owner ruled that the extensions setting matters only to the legacy
   encoders, and that a candidate fits the schema as the data has it. No candidate reads the flag, so phase 1's
   off-and-on pairing would only have relabelled two identical runs. The messy labels run with extensions on
   only too, A0's included. The A0 labels keep both settings as in phase 1, where they are replicates. The
   noise check uses two separate A0 labels, as asked. The owner then chose to run the batch in slices, one
   per source, with every route interleaved within a slice (A0, A+, B and A-variant as `r1`, then as `r2`)
   and the results reported after each slice. So the noise gate runs after the JSON slice rather than before
   any candidate, and `r2` brackets `r1` in every slice in place of a closing A0 label.
4. **The messy data is run on all four routes**, not on A+ alone, so that A+'s worst case has the others
   beside it. Under `drop`.
5. **The harness** gained `scripts/spike_routes.py`, `spike_checks.py`, `gate_candidates.py`, `gate_messy.py`,
   `p2_spread.py` and `tools/make_messy.py`. Two existing scripts were extended:
   - `phases.py` gained the candidate routes and a lexical fidelity count for them;
   - `run-phases.sh` gained the optional `EXTRA_JARS`, `JSON_DIR`, `SOURCES_OVERRIDE` and `SPIKE_POLICY`.

   Both leave the phase 1 routes' behaviour and recorded fields unchanged. The phase 1 versions are in
   `harness/`, and the extended ones in `phase2/harness/`.
6. **A+'s egress follows decision 71, as A0's does.** It leaves a null structure null and passes an all-null
   structure or a null-holding array through as it is, relying on the stored layout to carry neither, as
   A0's egress does. As first built, it left them out itself at every level, which A0 and A-variant (HEAD's
   egress) do not do. That nested a filter in every structure and array of the generated expression, and
   would have timed A+ against a requirement the others do not meet. The owner chose the change on
   2026-10-19, before any size-`l` run. B's renderer still leaves empty elements out as it writes, which costs
   it nothing measurable. The size-`s` gate's output is unchanged.
7. **The bar's label.** The runbook's "Afterwards" commands name `pof-switch-26d7332`. That results directory
   holds the 2026-10-17 runs of the older harness and no phase results, so `pooled_phases.py` prints only its
   header for it. The bar used is `phases-pof-switch-26d7332`, phase 1's re-measurement of the same commit on
   this harness, which is the bar phase 1 reports. It was measured in an earlier session, before the restart.
8. **Results tables.** The per-type tables (`phase2/results/tables.md`) come from a new script,
   `phase2/results/tables.py`, which reads the result files and takes pooled minimums as `pooled_phases.py`
   does. Each candidate was compared with both A0 labels and with the bar, with and without py4j plan
   building, as the runbook lists. The comparisons of A0's two labels with the bar and with phase 1's A0, and
   the messy runs with py4j plan building removed, were added.
9. **The original spike's port.** Four classes of `spike/parquet-on-fhir`'s `io` module (`FhirJsonReader`,
   `FhirJsonWriter`, `DatasetTransformer`, `ColumnDescriptor`) are copied into the package
   `au.csiro.pathling.spike08.orig`. The changes are: the package; `scala.jdk.javaapi.OptionConverters` in
   place of Scala 2.12's `scala.compat.java8`; a column's type taken from the dataset's schema in place of
   `column.expr().dataType()`, which is Catalyst; a `read(Dataset<String>)` for the documents the harness holds
   in memory; and the writer's transform exposed as `transform`, so that decode renders with `to_json` rather
   than writing files, as every other route's decode does. `batch-l.sh` gained `SLICES`, `CANDIDATES` and
   `MESSY`, whose defaults leave the size-`l` batch as it ran; the run was
   `PREFIX=p3 SLICES=json CANDIDATES="b orig" MESSY=0 batch-l.sh`.
