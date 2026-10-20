# Spike 08: the ingest and egress mechanism (pointer)

The evidence for ticket 08 is on the branch
[`issue/2831-spike-08`](https://github.com/aehrc/pathling/tree/issue/2831-spike-08/specs/001-parquet-on-fhir/evidence/spike-08), last at commit
`3e73682d01`. It is kept there rather than here because most of it is throwaway: prototype Java
and Python for each candidate, the timing harness, batch scripts and about 3 MB of raw results. The branch
is not to be merged and should not be deleted while ticket 16 is open.

Where this spec's documents name a file under `evidence/spike-08/`, such as
`evidence/spike-08/harness/fromxml_route.py`, the path is on that branch. The full write-up is its
[`README.md`](https://github.com/aehrc/pathling/blob/issue/2831-spike-08/specs/001-parquet-on-fhir/evidence/spike-08/README.md).

The outcome is decision 134: the choice of mechanism is deferred to ticket 16, and the interim is A0 for JSON
and `from_xml` for XML and bundles (ticket 32).

## Summary of phase 2

This section collects the phase 2 findings in one place, one candidate per column. Each row links to the section
that holds its evidence, on the spike branch. It records facts and chooses nothing. The later JSON batches (`p3` to `p6`, 2026-10-20)
change the reading of B's JSON encode in the main size-`l` tables, and the rows below take them into account.

**Routes.** "B" is B as built, handing its rows to Spark through `Encoders.row`. "B through a UDF" (`budf`) is
the same fit and parser, handing the rows over through a Java UDF. It was built for JSON only.

| | A+ | A-variant | B | B through a UDF |
|---|---|---|---|---|
| Sources built | JSON, XML lines, XML Bundles | JSON (XML through A+'s route) | JSON, XML lines, XML Bundles | JSON |
| Equal to HEAD's stored layout at size `s`, apart from decimal text ([Correctness](https://github.com/aehrc/pathling/blob/issue/2831-spike-08/specs/001-parquet-on-fhir/evidence/spike-08/README.md#correctness-at-size-s-phase2gategate-stxt)) | Yes | Yes | Yes | Yes |
| Invalid fixtures that fail, and are recorded per value under `drop` ([Coverage](https://github.com/aehrc/pathling/blob/issue/2831-spike-08/specs/001-parquet-on-fhir/evidence/spike-08/README.md#coverage-fixturesfixtures_candidatespy-output-fixturesfixtures_candidatestxt)) | 29 of 29 JSON, 23 of 23 XML | 29 of 29 | 29 of 29 JSON, 23 of 23 XML | The parser and checks are B's; the fixtures were not run through this route |
| "Store" fixtures met (JSON) | 15 of 18 | 13 of 18 | 15 of 18 | As B |
| Principle 6: rows stored differently in a batch | 1 of 37 | 1 of 37 | 0 of 37 | As B |
| OQ-1's losses still present ([OQ-1](https://github.com/aehrc/pathling/blob/issue/2831-spike-08/specs/001-parquet-on-fhir/evidence/spike-08/README.md#oq-1s-losses-per-candidate)) | `_x` metadata only (M5) | The decimal's text; empty structures on export; `_x` metadata | `_x` metadata only (M5) | As B |
| Round-trip fidelity at size `l`, lexical ([Fidelity](https://github.com/aehrc/pathling/blob/issue/2831-spike-08/specs/001-parquet-on-fhir/evidence/spike-08/README.md#fidelity-and-row-counts)) | 500 of 500, every source and type | Patient 495, Observation 421 of 500 | 500 of 500, every source and type | Exact (`p6`); lexical not taken |
| Messy data: rows that lose the bad element ([Correctness](https://github.com/aehrc/pathling/blob/issue/2831-spike-08/specs/001-parquet-on-fhir/evidence/spike-08/README.md#correctness-at-size-s-phase2gategate-stxt)) | The bad row only | The bad row only | The bad row only | Not run |
| Code owned, rough lines ([Code](https://github.com/aehrc/pathling/blob/issue/2831-spike-08/specs/001-parquet-on-fhir/evidence/spike-08/README.md#code-each-candidate-owns)) | about 960 (Python generators included) | about 400 | about 1,270 | B's, plus a class of about 40 |
| Catalyst API (FR-049) | None | None | None | None |
| Spark expression encoder (FR-050) | `Encoders.STRING()` in the pre-pass | `Encoders.STRING()` in the document check | `Encoders.row(schema)`; the owner answered on 2026-10-20 that it is allowed | None besides the input's |
| Depends on a Spark setting | No | No | Its encode is about 3 times slower with Spark's default `hugeMethodLimit` ([Profile](https://github.com/aehrc/pathling/blob/issue/2831-spike-08/specs/001-parquet-on-fhir/evidence/spike-08/README.md#profile-of-bs-encode-observation-json-2026-10-20)) | No |

**Timing against the bar** (`phases-pof-switch-26d7332`), as the ratio of pooled minimums. Ingest is fit plus
encode, the two phases' pooled minimums summed. The JSON figures for B through a UDF come from the `p6` batch; the
others come from the main size-`l` batch, where B ran with Spark's default `hugeMethodLimit`. A+'s and
A-variant's figures have their py4j plan building removed.

| Source | Phase | A+ | A-variant | B | B through a UDF |
|---|---|---|---|---|---|
| JSON lines | ingest, Observation | 2.80 | 16.6 | 0.98 | 0.39 |
| | ingest, range over the four types | | | | 0.39 - 0.48 |
| | decode, range | 0.86 - 1.35 | 0.92 - 1.07 | 0.83 - 1.12 | 0.80 - 1.01 |
| XML lines | ingest, Observation | 2.69 | - | 0.29 | - |
| | decode, range | 0.45 - 0.85 | - | 0.11 - 0.33 | - |
| XML collection Bundles | ingest, Observation | 1.46 | - | 0.24 | - |
| XML transaction Bundles | ingest, Observation | 1.43 | - | 0.21 | - |

**JSON against A0 as shipped, in the same batch**, ingest as a range over the four types:

| Route | With decision 133's checks | With no checks (policy `none`) | Batch |
|---|---|---|---|
| Original spike (no checks, no layout) | - | 0.53 - 0.71 | `p4` ([Original spike](https://github.com/aehrc/pathling/blob/issue/2831-spike-08/specs/001-parquet-on-fhir/evidence/spike-08/README.md#the-original-spike-beside-a0-and-b-json-2026-10-20)) |
| A+ | 2.88 - 4.85 | 1.29 - 1.46 | `p4`, `p5` ([Checks](https://github.com/aehrc/pathling/blob/issue/2831-spike-08/specs/001-parquet-on-fhir/evidence/spike-08/README.md#b-and-a-with-and-without-their-checks-json-2026-10-20)) |
| B, `hugeMethodLimit=8000` | 0.43 - 0.81 | 0.34 - 0.72 | `p4`, `p6` |
| B through a UDF, Spark's defaults | 0.43 - 0.51 | 0.34 - 0.47 | `p6` |

In each, the top of the range is Patient, the smallest type, where fixed costs dominate.

**What the checks cost.** About a third of B's JSON encode, and most of A+'s: A+'s encode is 3.6 to 7.1 times
A0's with them and 1.30 to 1.41 without.

**Not measured.**

- B's XML encode under `hugeMethodLimit=8000`. B's XML rows also go through `Encoders.row`, so its XML
  figures above may include the same interpreted serializer.
- B through a UDF on XML, on Bundles, on the messy data and on the fixtures.
- A UDF that returns zero or more rows to be exploded, which would need no non-deterministic flag and is the
  shape Bundles need.
- JSON Bundles, for any candidate.
- The formal JMH check of SC-011, which belongs to ticket 16.
