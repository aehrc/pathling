# Engine tolerance of non-conformant stored data

Evidence for tickets 31 and 33, gathered 2026-10-21 on `issue/2367` at `2c37b63ac1`, against the
`10.0.0-SNAPSHOT` runtime built from it. Nothing here was run against the 9.9.0 engine.

The question is what the engine does when stored data breaks a FHIR rule the PoF layout cannot enforce: a value
whose text is not valid for its type, or a shape FHIR JSON forbids. Parquet guarantees only the physical types,
so data written by another tool, data stored under a future `keep` policy, and data from today's interim ingest
(decision 134) can all carry such content.

**Reproducing.** Each probe takes the Python library source as its first argument and loads the runtime jar
from the local Maven repository. The layout probe also takes a scratch directory for its exports.

```bash
PY=lib/python/.venv/bin/python
export PYSPARK_PYTHON=$PY PYSPARK_DRIVER_PYTHON=$PY
$PY temporal_probe.py lib/python
$PY decimal_probe.py lib/python
$PY layout_probe.py lib/python <scratch-dir>
```

Raw output is beside each probe, as a `.txt` file of the same name, with log lines filtered out.

## Values (`temporal_probe.py`, `decimal_probe.py`)

The temporal probe ingests each value through `encode`, which stored every one unchanged, because the interim
ingest validates no lexical form. The decimal probe replaces the stored text of `valueQuantity.value` directly.

| Stored | Select, `exists()` | Comparison | `toString().toDateTime()` |
|---|---|---|---|
| `birthDate` `2020-13-45`, `2020-02-30` | the text, `true` | **the query fails** | the text, unchanged |
| `birthDate` `garbage` | the text, `true` | **the query fails** | empty |
| `deceasedDateTime` `…T25:00:00Z` | the text, `true` | **the query fails** | the text, unchanged |
| `valueTime` `25:61:00` | the text, `true` | **the query fails** | empty |
| `birthDate` `2020-01-01T10:00:00Z` | the text, `true` | compared as a dateTime, no error | the text |
| `deceasedDateTime` `2020-01-01T10:00:00`, no timezone | the text, `true` | compared, no error | the text |
| decimal `1.2.3`, `abc`, `""`, `1e400` | null, `false` | empty | — |
| decimal `" 1.5"` | `1.5`, `true` (trimmed) | as `1.5` | — |
| decimal `0.0000001` | `0`, `true` | as `0` | — |
| decimal of 29 integer digits | null, `false` | empty, but `> 1 'mg'` on the Quantity is `true` | — |

The failures are `SparkException [FAILED_EXECUTE_UDF]` from `high_boundary_for_date` or `high_boundary_for_time`,
caused by `IllegalArgumentException: Invalid date/dateTime format`, with the value and no resource or path. They
come from `TemporalComparator`, which computes both boundaries of each operand with these UDFs for every
comparison, `=` included.

What this says:

- Dates and decimals disagree. An invalid decimal is absent, so `exists()` is false. An invalid date is present,
  and any comparison on it fails the query.
- There are at least four grammars: the boundary UDFs check the calendar, `toDateTime()` checks only the
  pattern, the decimal cast trims whitespace, and the Quantity comparison decodes a decimal differently from the
  decimal itself.
- Values that are malformed but parse, such as a date carrying a time or a dateTime with no timezone, are
  compared under an assumption rather than refused.
- The last two decimal rows are valid FHIR. FR-035 keeps query-time precision at `DECIMAL(32,6)`, so they are
  that requirement's consequence rather than a tolerance question, but the loss is silent.

## Layout (`layout_probe.py`)

The first half modifies an encoded `Patient` directly, as another writer could store it. The second half sends
the same shapes through `encode`.

Written directly:

| Stored | `exists()` | `count()` | Other answers | Exported as |
|---|---|---|---|---|
| `name` null | false | 0 | | omitted |
| `name` `[]` | false | 0 | | `"name":[]` |
| `name` `[null]` | false | 0 | | `"name":[null]` |
| `name` `[null, Smith]` | true | 1 | | `[null,{…}]` |
| `name` `[{}]`, an all-null struct | true | 1 | `where(family.empty())` is 1; `forEach` gives a row | `[{}]` |
| `name` `[Smith, {}]` | true | 2 | `forEach` gives 2 rows | `[{…},{}]` |
| `maritalStatus` `{}` | true | | | `"maritalStatus":{}` |
| `given` `[]` | false | 0 | | `"given":[]` |
| `given` `[null]` | true | 1 | `first()` empty | `"given":[null]`, no `_given` |
| `given` `[null, Ann]` | true | 2 | `first()` empty | `[null,"Ann"]`, no `_given` |
| `given` `['']` | true | 1 | `''` is a value | `[""]` |

Through ingest:

| JSON | Stored | Answers and export |
|---|---|---|
| `name` `[]`, `[{}]` or `[null]`; `maritalStatus` `{}` or `null` | dropped, no column | absent; exported clean |
| nested `given: []` | kept as `[]` | `exists()` false; exported as `"given":[]` |
| `given: [null]`, no `_given` | kept | `exists()` true, `count()` 1; exported as `[null]` |
| `birthDate: ""` | kept | exported as `""` |
| `_given` only | the metadata is dropped | `given` absent |

What this says:

- Null, `[]` and a null item in a complex array are all absent to the engine, consistently.
- An all-null struct is present. That is deliberate for the placeholders decision 72 keeps for primitive
  metadata the layout does not yet store, and wrong for a writer that did not prune.
- A positional null in a primitive array is counted, and `first()` returns it as empty rather than skipping it.
- Export writes every one of these shapes back as stored. FR-019's pruning is deferred, so nothing makes foreign
  content valid on the way out.
- The interim ingest drops top-level empty values but stores a nested `[]`, an unaligned positional null and
  `""`, so its own output carries some of the shapes too.
