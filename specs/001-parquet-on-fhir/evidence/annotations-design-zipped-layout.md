# Zipped or parallel: what each layout costs a primitive read

A probe of two storage layouts for a FHIR primitive, written from the same logical data and read with
Spark 4.3.0, Spark 4.0.2 and DuckDB 2.0.0.

- **Parallel** (the current Parquet-on-FHIR layout): `x` holds the value, and a sibling `_x:
  struct<id, extension, __start, __end>` holds the metadata. A repeating primitive is `x: array<string>`
  with `_x: array<struct<..>>` aligned by position, and `_x` is null when no element has metadata.
- **Zipped** (the alternative): `x: struct<value, id, extension, __start, __end>`, and
  `x: array<struct<value, id, extension, __start, __end>>` when repeating. Every primitive is a struct,
  with null `id` and `extension` when it has no metadata.

**Verdict: zipped costs a value-only read of a repeating primitive the whole element, in every engine
and shape tried, and the file size is the same to within 4%. The parallel layout is the only one with
a prune-clean value read below a repeating parent.**

- **Two-level value reads (`name[].given[]`, `dosage[].timing.event[]`) do not prune in the zipped
  layout, on either Spark.** Every expression form reads `value`, `id`, `extension` and the
  annotations, as one unit. On M1 (heavy metadata) Spark 4.3.0 reads 47,630,828 bytes for the `given`
  values against 604,859 in the parallel layout (79 times), and 39,630,752 against 1,364,829 for the
  `event` values (29 times). On M0 (no metadata but the date annotations) the ratios are 2.0 and 3.5
  times, and are only the null `id` and `extension` leaves plus the real `__start` and `__end` data.
- **Parallel prunes because its value is a plain array of strings.** `name.given` and
  `dosage.timing.event` (`GetArrayStructFields`) read only the value leaf on both Sparks. The zipped
  equivalents do not exist: `name.given.value` and `dosage.timing.event.value` fail to resolve (the
  element type is `array<array<struct>>`), and the forms that do resolve (`flatten(name.given).value`,
  `transform(name, n -> n.given.value)`, `transform(name, n -> transform(n.given, g -> g.value))`) read
  the whole struct on 4.0.2 and 4.3.0.
- **A singular primitive is fine, and so is one below a repeating parent on 4.3.0.** `birthDate.value`,
  `gender.value` and `transform(name, n -> n.family.value)` read the value leaf alone, with bytes
  identical to the parallel layout. (`birthDate` whole, the control, is 13,557,321 bytes on M1.) On
  Spark 4.0.2 the lambda form `transform(name, n -> n.family.value)` reads all of `name` in both layouts
  (48.1 MB), and only `name.family.value` prunes.
- **Annotation reads on zipped are neither better nor worse than on parallel.** Reading
  `dosage[].timing.event[].__start` on zipped reads the whole `event[]` struct, extensions included
  (39,630,752 bytes on M1). The parallel read of `_event[].__start` reads the whole `_event` (38,265,923
  bytes), which is the finding of
  [annotations-design-pairing-shapes.md](annotations-design-pairing-shapes.md). The two differ by the
  value leaf (1.36 MB).
- **DuckDB prunes neither layout below a list.** Its scan projection is the top-level column (`name`,
  `dosage`) for every lambda and `unnest` form in both layouts, and only a singular struct field
  (`birthDate.value`) is projected to a leaf.
- **File size is a wash.** M1 is 102,644,610 bytes zipped against 102,694,264 parallel (-0.05%). M0 is
  9,466,263 against 9,125,639 (+3.7%), all of it the null `id` and `extension` leaves of the repeating
  `given`, which need a definition level per element when the element struct always exists. The value
  leaf is byte-identical in the two layouts.
- **C10a is confirmed in Spark SQL and DuckDB.** On zipped, `SELECT DISTINCT given`, `GROUP BY given`,
  `array_distinct` and `=` treat equal values with different extensions as different, and
  `array_contains(given, 'Ann')` does not type-check. On parallel the same queries work by value.

## Setup

- Data: 200,000 patients written with pyarrow 26.0.1 (one writer, snappy, 50,000-row row groups, four
  row groups per file, one file per layout and variant), from one logical data set per variant, so the
  two layouts carry the same values. Both layouts have 25 leaf columns in the file footer.
  [`gen.py`](scripts/annotations-design/zipped-layout/gen.py).
  - Fields: singular date `birthDate` (string, with `__start` and `__end` as timestamp micros),
    singular string `gender`, `name[]` of `{family, given[]}` (1 to 3 names, 1 to 3 given each),
    `dosage[]` of `{timing: {event[]}}` (1 or 2 dosages, 1 to 3 events each, dates with `__start` and
    `__end`).
  - M0: no metadata except the date annotations, which every date carries in both variants.
  - M1: `birthDate`, each `given` and each `event` element has, with probability 0.5, an `id` and 3 to 5
    string extensions of 24 random characters (the file is 11 times the size of M0). `gender` and
    `family` carry none.
  - Parallel: `_x` is null when no element has metadata, and in M1 elements without metadata are null
    entries in the aligned list. Zipped: every primitive is a struct.
- Spark: 4.3.0 and 4.0.2, classpath as in the
  [rewrites-layer1 `env.sh`](scripts/annotations-design/zipped-layout/env.sh) (local, AQE off,
  vectorised reader, Pathling's optimiser rules absent). Java 21. The reads are `spark.sql` over the
  Parquet directory, evaluated to a `noop` sink.
- Reads and bytes in Spark: the `ReadSchema` is the `requiredSchema` leaves of the `FileSourceScanExec`,
  and bytes are the compressed column-chunk sizes from the Parquet footer, summed over the leaves read,
  as in the sibling probes. The task input metric was also recorded, but on 4.3.0 it stays at 35-39 KB
  for every query and is not usable, so it is not reported.
- DuckDB 2.0.0 (Python module). Bytes are counted by serving the file through an fsspec filesystem that
  records every read range, with `threads=1`. The projection is the `Projections` of the `READ_PARQUET`
  node in `EXPLAIN (FORMAT JSON)`.
- Polars 2.1.0, `explain()` only.

## (a) Value-only reads

Footer bytes of the leaves read, Spark 4.3.0, M1 (heavy metadata). M0 in brackets.
[`cases.txt`](scripts/annotations-design/zipped-layout/cases.txt).

| Read | Parallel | Zipped |
|---|---|---|
| `birthDate` / `birthDate.value` | 504,317 (504,489) | 504,317 (504,489) |
| `gender` / `gender.value` | 51,249 | 51,249 |
| `transform(name, n -> n.family[.value])` | 202,988 (202,523) | 202,988 (202,523) |
| `name.family[.value]` | 202,988 | 202,988 |
| `transform(name, n -> n.given)` / `transform(name, n -> transform(n.given, g -> g.value))` | 604,859 (603,392) | **47,630,828** (1,209,251) |
| `transform(name, n -> n.given[.value])` | 604,859 | 47,630,828 |
| `flatten(name.given)` / `flatten(name.given).value` | 604,859 | 47,630,828 |
| `name.given` / `name.given.value` | 604,859 | does not resolve |
| `transform(dosage, d -> d.timing.event)` / `.. transform(d.timing.event, e -> e.value)` | 1,364,829 (1,369,333) | **39,630,752** (4,741,956) |
| `flatten(transform(dosage, ..))` of the same | 1,364,829 | 39,630,752 |
| `transform(dosage, d -> d.timing.event[.value])` | 1,364,829 | 39,630,752 |
| `flatten(dosage.timing.event)` / `flatten(dosage.timing.event).value` | 1,364,829 | 39,630,752 |
| `dosage.timing.event` / `dosage.timing.event.value` | 1,364,829 | does not resolve |
| Control: `birthDate` whole | - | 13,557,321 (1,691,807) |

Spark 4.0.2, M1, where it differs from 4.3.0:

| Read | Parallel | Zipped |
|---|---|---|
| `transform(name, n -> n.family[.value])` | 48,111,409 (whole `name`) | 48,061,916 (whole `name`) |
| `name.family[.value]` | 202,988 | 202,988 |
| `transform(name, n -> n.given)` / `.. g -> g.value` | 48,111,409 | 48,061,916 |
| `name.given` / `flatten(name.given).value` | 604,859 | 47,630,828 |
| `dosage.timing.event` / `flatten(dosage.timing.event).value` | 1,364,829 | 39,630,752 |
| `transform(dosage, d -> d.timing.event[.value ..])` | 39,630,752 | 39,630,752 |
| `birthDate`, `gender` | as on 4.3.0 | as on 4.3.0 |

So on 4.0.2 the parallel layout gets the pruned value read only through the `GetArrayStructFields`
chain, which is the form the Pathling expression layer can generate, and zipped has no such form. On
4.3.0 the lambda forms prune a singular primitive below a repeating parent in both layouts, and
neither layout's lambda form prunes a repeating primitive inside a repeating parent. The parallel
`GetArrayStructFields` forms do prune it. The full ReadSchema leaf lists are in `logs/reads-4.3.log` and `logs/reads-4.0.log`.

DuckDB 2.0.0, projection of the Parquet scan (M1 bytes read through fsspec in brackets, parallel then
zipped; see the note below on what the bytes are):

| Read | Parallel | Zipped |
|---|---|---|
| `birthDate` / `birthDate.value` | `birthDate` (635,389) | `birthDate.value` (635,389) |
| `list_transform(name, lambda n: n.family[.value])` | `name` (49,539,239) | `name` (49,838,191) |
| `unnest(name).family[.value]` | `name` (49,539,239) | `name` (49,838,191) |
| `list_transform(name, lambda n: n.given ..)` and the `unnest` form | `name` (49,539,239) | `name` (49,838,191) |
| `list_transform(dosage, lambda d: d.timing.event ..)` and the `unnest` form | `dosage` (40,471,056) | `dosage` (40,470,895) |
| Control: `birthDate` whole | - | `birthDate` (16,502,519) |

DuckDB does not resolve the dot path through a list (`name.family`: "not a struct") and rejects nested
`unnest` calls, so the dot forms were written with a subquery. In both layouts any list read projects
the whole top-level column, `family` included, so there is no difference between the layouts to
measure; the cost is the same large amount in both.

- What DuckDB's tooling shows: `EXPLAIN` gives the scan's `Projections`, which names the leaf only for
  a singular struct field (`birthDate.value`). The JSON profiling output has a query-level
  `total_bytes_read` and no per-column figure. For the zipped `transform(name, n -> n.family.value)` on
  M1 it reported 88,621,221 bytes, which I could not reconcile with the 48 MB of chunk bytes of `name`
  or the fsspec count, so it is not used.
- What it cannot show: bytes per column. The byte figures above come from the fsspec wrapper, which
  counts the ranges DuckDB requests, including the footer and DuckDB's read-ahead. On M1 they are
  within 4% of the chunk sizes for the whole `name` (48.1 MB) and `dosage` (39.6 MB) columns. On M0 they
  overshoot (4,017,536 for a `name` of 1,299,158 chunk bytes) and are an upper bound only, so they are
  in `logs/duck.log` and not used here.

Polars 2.1.0: `explain()` of `scan_parquet(..).select(col("birthDate").struct.field("value"))` shows
`PROJECT 1/5 COLUMNS` for the scan with the `struct.field_by_name` select above it, and the same
`PROJECT 1/5 COLUMNS` for the `name` list of structs. That is a top-level column projection with the
field extraction applied after the scan, so the whole `birthDate` struct is scheduled for reading. This
is the plan only, and I did not measure Polars' I/O.

## (b) File size

[`size.py`](scripts/annotations-design/zipped-layout/size.py), from the Parquet footers.

| Variant | Parallel (bytes) | Zipped (bytes) | Zipped against parallel |
|---|---|---|---|
| M0 | 9,125,639 | 9,466,263 | +340,624 (+3.7%) |
| M1 | 102,694,264 | 102,644,610 | -49,654 (-0.05%) |

Compressed chunk size of the value leaf (the same in both layouts; the uncompressed size is equal too):

| Leaf | M0 | M1 |
|---|---|---|
| `birthDate` / `birthDate.value` | 504,489 | 504,317 |
| `name[].given[]` / `.value` | 603,392 | 604,859 |
| `dosage[].timing.event[]` / `.value` | 1,369,333 | 1,364,829 |
| `gender` / `gender.value` | 51,249 | 51,249 |
| `event[]` `__start` | 1,458,025 | 1,453,704 |

M0 explained: the null `id`, `extension.url` and `extension.valueString` leaves under `name[].given[]`
cost 201,953 bytes each when zipped, because each element struct needs a definition level, and 88,374
each in the parallel layout, where `_given` is null for the whole list. Three leaves at +113,579 is
about the +340,624. The same leaves under `event[]` (152,116 each) and the singular leaves are equal in the two
layouts, because the date annotations make `_event` non-null in the parallel layout too. M1 has real
metadata, which dominates and masks the difference. Top-level column totals on M1: parallel `name`
48,111,409, `dosage` 39,630,752, `_birthDate` 13,053,004; zipped `name` 48,061,916, `dosage`
39,630,752, `birthDate` 13,557,321.

## (c) The two-level annotation read on zipped

Spark 4.3.0, M1 (M0 in brackets), footer bytes of the leaves read.

| Read | Parallel (`_event`) | Zipped (`event`) |
|---|---|---|
| `flatten(transform(dosage, d -> transform(d.timing[._]event, m -> m.__start)))` | 38,265,923 (3,372,623) | 39,630,752 (4,741,956) |
| `transform(dosage, d -> d.timing[._]event.__start)` | 38,265,923 | 39,630,752 |
| `flatten(dosage.timing[._]event).__start` | 38,265,923 | 39,630,752 |
| `dosage.timing[._]event.__start` | does not resolve | does not resolve |
| `where exists(flatten(transform(..)), s -> s > ts)`, selecting `id` | 39,588,248 (4,694,948) | 40,953,077 (6,064,281) |
| `where exists(flatten(dosage.timing[._]event).__start, ..)` | 39,588,248 | 40,953,077 |

- On zipped the read includes `event[].extension` (`url` and `valueString`), `id`, `value`, `__start`
  and `__end`: the whole struct, not `__start` alone. The leaves read are
  `dosage[].timing.event[].{__end, __start, extension[].url, extension[].valueString, id, value}`.
- On parallel the read is the whole `_event` (the same leaves without `value`), extensions included,
  which reproduces the earlier probe. The zipped read is 3.6% larger on M1 because it also takes the
  value leaf.
- Spark 4.0.2 reads the same bytes for all of these. The `GetArrayStructFields` form
  `dosage.timing.event.__start` fails to resolve for the same reason as the value form, an
  `array<array<struct>>`.
- Neither layout gives a prune-clean annotation read below a repeating parent without the lambda-pruning
  rule, so zipped neither fixes nor worsens the problem of the pairing-shapes probe. It does remove the
  need to pair values with metadata, since they are one element.

## (d) The C10a demonstration

Five patients, `given` one element `Ann` unless stated: p1 extension `A`; p2 extension `B`; p3 no
metadata; p4 identical to p1; p5 two elements `Ann`, one with `A` and one with `B`.
[`c10a.sql`](scripts/annotations-design/zipped-layout/c10a.sql) (Spark SQL, identical results on 4.3.0
and 4.0.2) and [`c10a_duck.py`](scripts/annotations-design/zipped-layout/c10a_duck.py) (DuckDB 2.0.0,
identical results).

| Query | Zipped | Parallel |
|---|---|---|
| `SELECT DISTINCT given` | 4 rows: `[Ann{A}]`, `[Ann{B}]`, `[Ann{none}]`, `[Ann{A}, Ann{B}]` | 2 rows: `[Ann]`, `[Ann, Ann]` |
| `GROUP BY given`, count | 1, 1, 2 (p1 with p4), 1 | `[Ann]` 4, `[Ann, Ann]` 1 |
| `array_distinct(given)` / `list_distinct(given)` on p5 | 2 elements | 1 element |
| `array_contains(given, 'Ann')` / `list_contains` | analysis error: `array_contains` needs `ARRAY<STRUCT<..>>` and a struct value, got `STRING` (DuckDB: cannot cast `'Ann'` to the struct) | p1 to p5 |
| `array_contains(transform(given, g -> g.value), 'Ann')`, or `exists(given, g -> g.value = 'Ann')` | p1 to p5 | not needed |
| Self-join on `a.given = b.given` | p1-p4 only | all 6 pairs among p1 to p4 |

On zipped, equality, distinctness and grouping of a primitive or a list of primitives are over the
struct, so the metadata is part of the value, and every use of the value needs `.value` or a transform.
On parallel `given` is a plain `array<string>`, the metadata is elsewhere, and the same queries work by
value. (The parallel self-join has 6 pairs because p1 to p4 are equal by value, and p5 differs.)

## What is not covered

- Time. Only bytes and read schemas were measured, not elapsed time or CPU.
- One-level repeating primitives (a repeating primitive directly under a singular struct, such as
  `timing.event`). By the shape of the Spark rule I would expect 4.3.0 to prune `transform(event, e ->
  e.value)` like it prunes `transform(name, n -> n.family.value)`, but I did not measure it.
- Writing, and write-side cost: both layouts were written with pyarrow, not with Spark or Pathling's
  writer, so row-group and dictionary behaviour may differ from the real writer.
- Writers' encoding choices, such as dictionary fallback for the `extension` strings: the extension
  values are random, so M1 compresses them poorly. Real extensions would compress better and shrink the
  M1 differences in bytes, though not the ratios of leaves read.
- `decimal`, `Quantity` and complex types, choice types, and `meta` or `id` on non-primitive elements.
- Polars' actual I/O and DuckDB bytes per column. Spark's task input metric was unusable on 4.3.0.
- Pathling's own rules (lambda pruning, project-before-generate) were absent, as in the sibling probes.
  The question here is what the layout allows, not what Pathling's optimiser can recover.
- DuckDB CLI 1.5.2 (homebrew) was not used; the DuckDB figures are the 2.0.0 Python module.

## How to reproduce

All scripts are in [scripts/annotations-design/zipped-layout/](scripts/annotations-design/zipped-layout/).
Data and logs go to `.local/work/issue/2832_41-42_decimal-and-annotations/spikes/zipped-layout/`.

- `env.sh` sets the paths. It needs a venv with `pyarrow duckdb polars fsspec` (`uv venv; uv pip
  install pyarrow duckdb polars fsspec`; `PY` points at its python, by default
  `.local/annotations-design/venv`) and the Spark 4.0.2 and 4.3.0 classpaths, which `../common.sh`
  generates on first use.
- `all.sh [rows]` runs everything. Its parts: `gen.sh` (data), `spark.sh 4.3|4.0` (compiles
  `ZipProbe.scala` with the Scala compiler from `~/.m2`, runs the reads of `cases.txt` and the C10a SQL
  of `c10a.sql`), `duck.sh` (`duck.py` reads and `c10a_duck.py`), `size.sh` (`size.py`, also the Polars
  plans), and `tables.sh` to pivot the Spark logs into the tables above.
- Timing: data generation takes about 35 seconds for 200,000 rows, each Spark version about a minute,
  DuckDB a few minutes.
- Logs: `logs/reads-4.3.log`, `logs/reads-4.0.log` (one `ROW` line per case with the ReadSchema leaves),
  `logs/c10a-4.3.log`, `logs/c10a-4.0.log`, `logs/duck.log`, `logs/c10a-duck.log`, `logs/size.log`.
