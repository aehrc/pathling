# Which Spark type the inferred JSON read gives a FHIR decimal

Measured on 2026-11-26 with Spark 4.1.0, the version the build uses, against the `io` module at
commit `43c0e6147e`, using [decimal-read-probe.java](decimal-read-probe.java), run with
`./run-probe.sh decimal-read-probe.java DecimalReadProbe` after building `io` (see
[run-probe.sh](run-probe.sh)). It was run for T003 and research R3.

Each case is one dataset of `Observation` documents differing only in `valueQuantity.value`. For
each, the probe prints the type that Spark's JSON inference gives the value with the options of
`FhirJsonReader.json(spark)` (only `mode` `FAILFAST`), and the text that `FhirReader.json().read`
stores. It then reads the same documents as a defined read would: `from_json` with a schema that
gives the value a fixed type, followed by the same `toLayout`, and compares the stored text. It also
reads the same resources as XML through `FhirReader.xml().read`.

**Result: the inferred type is not one type. It depends on every value of the element in the
dataset read: `double` where any value has a fraction or an exponent, `bigint` where every value is
an integer within the range of a long, and `decimal(p,0)` where every value is an integer and one is
beyond that range. The stored type is `string` in every case, holding the text of the inferred
number.** This is what `PrimitiveConverters.DECIMAL`'s comment already describes.

| Values of the element in the dataset                  | Inferred type   | Stored by the inferred read                            |
| ----------------------------------------------------- | --------------- | ------------------------------------------------------ |
| `1.10`, `100`, `0.1234567890123456789`, `1e2`, `-0.0` | `double`        | `1.1`, `100.0`, `0.12345678901234568`, `100.0`, `-0.0` |
| `1.10`, `2.5`                                         | `double`        | `1.1`, `2.5`                                           |
| `100`, `7`, `-3`                                      | `bigint`        | `100`, `7`, `-3`                                       |
| `12345678901234567890123`, `7`                        | `decimal(23,0)` | `12345678901234567890123`, `7`                         |
| `12345678901234567890123`, `1.5`                      | `double`        | `1.2345678901234568E22`, `1.5`                         |

A defined read gives the element one fixed type whatever the data. No fixed type stores what the
inferred read stores for every dataset:

| Type given to `from_json` | Same text as the inferred read      | Where it differs or fails                                                           |
| ------------------------- | ----------------------------------- | ----------------------------------------------------------------------------------- |
| `double`                  | where inference gives `double`      | integer-only data: `100.0` for `100`; `1.2345678901234568E22` for a big integer     |
| `bigint`                  | integer-only data within a long     | any fraction or out-of-range integer fails the read (`MALFORMED_RECORD_IN_PARSING`) |
| `decimal(38,18)`          | never                               | pads to 18 places (`1.100000000000000000`); fails beyond its precision              |
| `string`                  | fraction-only and integer-only data | mixed data: `100` where inference stores `100.0`, and big integers kept whole       |

`string` does not keep the lexical form either: Spark's JSON parser gives a number token read as
text the text of its double, so `1.10` becomes `1.1` and `1e2` becomes `100.0`, while an integer
token keeps its digits.

The XML read stores the text the XML carries (`1.10`, `1e2`, all 19 digits), so for the same
resources it differs from the JSON read wherever the JSON read went through a double.

## Consequences

- R3's premise that the inferred read gives a decimal one type does not hold. `double` is the type
  it gives whenever a dataset holds a fraction or an exponent, and of the types measured the only
  one that matches it on data mixing integers and fractions. Integer-only datasets infer `bigint` or `decimal(p,0)`, and a defined read with `double`
  stores `100.0` where the inferred read of the same data stores `100`.
- No one fixed type gives the defined read parity with the inferred read of the same data
  (FR-021a) for every dataset. Which type to fix, and what parity then means for a decimal, is the
  owner's choice at gate B.
- JSON and XML of the same resources already store different text for a decimal today (owner,
  2026-10-22). FR-009 requires equal data from JSON and XML loaded with the same schema, and no
  type given to the JSON read makes them equal: even `string` gives `1.1` for `1.10`.

## Output

```text
Spark 4.1.0

--- fractional and integral mixed: [1.10, 100, 0.1234567890123456789, 1e2, -0.0]
    inferred type: double
    stored type: string
    stored by the inferred read: [1.1, 100.0, 0.12345678901234568, 100.0, -0.0]
    stored by the XML read: [1.10, 100, 0.1234567890123456789, 1e2, -0.0] (DIFFERS from JSON)
    given double: stored [1.1, 100.0, 0.12345678901234568, 100.0, -0.0] (same as inferred)
    given bigint: FAILED: SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records are detected in record parsing: [[x],o0,Observation,final,[null]].
    given decimal(38,18): stored [1.100000000000000000, 100.000000000000000000, 0.123456789012345679, 100.000000000000000000, 0.000000000000000000] (DIFFERS from inferred)
    given string: stored [1.1, 100, 0.12345678901234568, 100.0, -0.0] (DIFFERS from inferred)

--- fractional only: [1.10, 2.5]
    inferred type: double
    stored type: string
    stored by the inferred read: [1.1, 2.5]
    stored by the XML read: [1.10, 2.5] (DIFFERS from JSON)
    given double: stored [1.1, 2.5] (same as inferred)
    given bigint: FAILED: SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records are detected in record parsing: [[x],o0,Observation,final,[null]].
    given decimal(38,18): stored [1.100000000000000000, 2.500000000000000000] (DIFFERS from inferred)
    given string: stored [1.1, 2.5] (same as inferred)

--- integral only: [100, 7, -3]
    inferred type: bigint
    stored type: string
    stored by the inferred read: [100, 7, -3]
    stored by the XML read: [100, 7, -3] (same as JSON)
    given double: stored [100.0, 7.0, -3.0] (DIFFERS from inferred)
    given bigint: stored [100, 7, -3] (same as inferred)
    given decimal(38,18): stored [100.000000000000000000, 7.000000000000000000, -3.000000000000000000] (DIFFERS from inferred)
    given string: stored [100, 7, -3] (same as inferred)

--- integral only, one beyond a long: [12345678901234567890123, 7]
    inferred type: decimal(23,0)
    stored type: string
    stored by the inferred read: [12345678901234567890123, 7]
    stored by the XML read: [12345678901234567890123, 7] (same as JSON)
    given double: stored [1.2345678901234568E22, 7.0] (DIFFERS from inferred)
    given bigint: FAILED: SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records are detected in record parsing: [[x],o0,Observation,final,[null]].
    given decimal(38,18): FAILED: SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records are detected in record parsing: [[x],o0,Observation,final,[null]].
    given string: stored [12345678901234567890123, 7] (same as inferred)

--- integral beyond a long, mixed with a fraction: [12345678901234567890123, 1.5]
    inferred type: double
    stored type: string
    stored by the inferred read: [1.2345678901234568E22, 1.5]
    stored by the XML read: [12345678901234567890123, 1.5] (DIFFERS from JSON)
    given double: stored [1.2345678901234568E22, 1.5] (same as inferred)
    given bigint: FAILED: SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records are detected in record parsing: [[x],o0,Observation,final,[null]].
    given decimal(38,18): FAILED: SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records are detected in record parsing: [[x],o0,Observation,final,[null]].
    given string: stored [12345678901234567890123, 1.5] (DIFFERS from inferred)
```
