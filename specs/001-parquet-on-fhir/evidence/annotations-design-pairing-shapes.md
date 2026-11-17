# Can a repeating annotated primitive be paired with its metadata without reading the extensions?

A probe of four emission shapes for pairing `event[i]` with `_event[i].__start` / `__end` on a
repeating primitive (`MedicationRequest.dosageInstruction[].timing.event[]`, a list of dateTime
strings with the parallel metadata list `_event[]`), on Apache Spark 4.0.2 and 4.3.0, with Pathling's
rules absent. It follows case 3a of
[annotations-design-spark-4-3-upstream-pruning.md](annotations-design-spark-4-3-upstream-pruning.md),
which showed that `arrays_zip(flatten(..timing.event), flatten(..timing._event).__start)` reads the
whole `_event`.

**Verdict: one shape avoids the extensions, in one context only.**

- **Directly (a singular `timing` struct above `event`), shape (c) works on both versions:**
  `arrays_zip(event, _event.__start, _event.__end)` reads `event`, `_event[].__start` and
  `_event[].__end` and nothing else (20,775 bytes against 237,320 for the others), with and without
  `inline`, guarded or not. This is `GetArrayStructFields`, which Spark prunes at every depth in the
  top-level column.
- **Spark 4.3.0 only, unguarded: shape (b) also works** (`arrays_zip(event, transform(_event, e ->
  named_struct('__start', .., '__end', ..)))`), but wrapping `event` and `_event` in
  `coalesce(.., array())`, which the null semantics below require for equal results, defeats it. (a)
  and (d) read the whole `_event` on both versions in every variant.
- **Inside a lambda over a repeating `dosage`, no shape avoids the extensions on either version.**
  All four shapes, and an extra shape (e) that zips at the dosage level outside the lambda, read the
  whole of `dosage[].timing._event`. Spark 4.3.0 does prune the value-only control
  (`transform(dosage, d -> d.timing.event)` reads `event` alone), but it cannot prune
  `_event[].__start` out of `_event` through two array levels plus a lambda, even with no
  `arrays_zip` in sight (controls x1 to x3 below). The failure is the depth, not the pairing.
- **A `coalesce` guard is needed for equal results** (a null `_event` makes every shape return NULL
  and lose the values). Results are otherwise identical across all shapes, in both modes, on both
  versions.

So for a repeating primitive below a repeating element the pairing cannot be made prune-clean by
choosing an emission shape. It needs the lambda-pruning rule (or the 4.3.0 upstream pruning extended
to nested lambdas), as for the other annotation reads.

## Setup

- Data: 4,000 rows, one Parquet file, schema
  `id, timing struct<event array<string>, _event array<META>>, dosage array<struct<text,
  timing struct<event, _event>>>` with
  `META = struct<id, extension array<struct<url, valueString>>, __start timestamp, __end timestamp>`.
  Most `_event` elements carry 3 to 5 extensions with 40-character string values, so the extensions
  are 513,612 of the file's 541,373 leaf bytes. Edge rows (every 40th, by `id` number): null
  `_event` with `event` present (p3); null `event` with `_event` present (p6); `_event` shorter than
  `event` (p9); a null element inside `_event` (p12); both empty (p15); empty `_event` (p18); a null
  value in `event` (p21); `event` one element longer than `_event` (p24); both null
  (p27); null `timing` (p33); and null, `[null]` and null-`timing` `dosage` (p30 to p32).
- Context 1 is the top-level `timing` struct, so `event` is one array level down. Context 2 is
  `flatten(transform(dosage, d -> <shape over d.timing.event, d.timing._event>))`.
- Every shape outputs `array<struct<value, start, end>>`, as a plain projection and as
  compute-first `select id, inline(nm) from (select id, <expr> as nm from t)`.
- Shapes: (a) `arrays_zip(event, _event)` then project; (b) `arrays_zip(event, transform(_event, e ->
  named_struct('__start', e.__start, '__end', e.__end)))`; (c) `arrays_zip(event, _event.__start,
  _event.__end)`; (d) `zip_with(event, _event, (v, m) -> named_struct(..))`; (e), context 2 only,
  `arrays_zip(dosage.timing.event, transform(dosage.timing._event, m -> transform(m, x ->
  x.__start)), ..__end)` outside the lambda over `dosage`, then a per-dosage inner `arrays_zip`
  (always guarded). "Guarded" wraps each array input in `coalesce(.., array())`.
- Reads are the `requiredSchema` leaves of the `FileSourceScanExec` in the executed plan (AQE off,
  vectorised reader). Bytes are the compressed column-chunk sizes from the Parquet footer, summed
  over the leaves read, because the scan's own size metric counts whole files.
- Spark 4.0.2 and 4.3.0 on the classpaths of the earlier spikes; the probe is compiled once against
  4.0.2.

## Results, Spark 4.0.2 and 4.3.0

"Reads" abbreviates by what the scan reads of `_event`: **S/E** is `__start` and `__end` only,
**all** is also `extension[].url`, `extension[].valueString` and `id`. `event` is read in every
case. The inline and plain columns read the same leaves in every row, so one column is given.
"Equal" is against shape (a) of the same block; every difference is the null semantics below.

### Context 1, direct (bytes: control 19,159)

| Shape | 4.0.2 unguarded | 4.0.2 guarded | 4.3.0 unguarded | 4.3.0 guarded |
| --- | --- | --- | --- | --- |
| (a) arrays_zip(event, _event) | all, 237,320 | all, 237,320 | all, 237,320 | all, 237,320 |
| (b) zip with a trimmed transform | all, 237,320 | all, 237,320 | **S/E, 20,775** | all, 237,320 |
| (c) arrays_zip of the two field arrays | **S/E, 20,775** | **S/E, 20,775** | **S/E, 20,775** | **S/E, 20,775** |
| (d) zip_with | all, 237,320 | all, 237,320 | all, 237,320 | all, 237,320 |

### Context 2, inside the dosage lambda (bytes: 4.0.2 control 322,054; 4.3.0 control 19,488)

| Shape | 4.0.2 (any variant) | 4.3.0 (any variant) |
| --- | --- | --- |
| 0 control, `d.timing.event` only | whole dosage, all, 322,054 | `event` only, 19,488 |
| (a), (b), (c), (d), (e) | all, 322,054 (also reads `dosage[].text`) | all, 321,789 |

On 4.0.2 the lambda itself defeats pruning, even for the control, so nothing is learned about the
shapes there. On 4.3.0 the control prunes and every shape reads the whole `_event`.

### Extra controls in context 2, plain, unguarded

Did start-only access prune at all inside a dosage lambda?

| Control | 4.0.2 | 4.3.0 |
| --- | --- | --- |
| x1 `flatten(transform(dosage, d -> transform(d.timing._event, x -> x.__start)))` | all, 322,054 | **all**, 320,302 |
| x2 `flatten(transform(dosage, d -> d.timing._event.__start))` | all, 322,054 | **all**, 320,302 |
| x3 `transform(dosage.timing._event, m -> transform(m, x -> x.__start))` | all, 320,302 | **all**, 320,302 |
| x4 `flatten(transform(dosage, d -> arrays_zip(d.timing.event, d.timing._event.__start)))` | all, 322,054 | all, 321,789 |
| x5 as x4 with `transform(d.timing._event, x -> x.__start)` | all, 322,054 | all, 321,789 |

Even a bare start-only access below a repeating `dosage` reads every extension on 4.3.0 (x1 to x3),
including without a lambda over `dosage` (x3, where `dosage.timing._event` is a plain
`GetArrayStructFields` on the root column producing an array of arrays). That is why (e) fails too.

## Null and length semantics

Cross-checks (full sorted rows of `{id, value, start, end}` after `inline`, and the plain list
column), every shape against the guarded (a), in both contexts: **all guarded shapes are equal to
the guarded (a); all unguarded shapes differ from it**, identically on both versions, and unguarded
shapes are equal to one another. The behaviour per edge row, context 1:

| Row | Unguarded (a), (b), (c), (d) | Guarded |
| --- | --- | --- |
| p3 `_event` null, `event` present | NULL, the values are lost | each value with null start and end |
| p6 `event` null, `_event` present | NULL, the annotations are lost | one pair per `_event` element, value null |
| p27 both null, p33 null `timing` | NULL | empty list |
| p9 `_event` shorter | padded: value present, start and end null | same |
| p24 `event` longer | padded the same way | same |
| p12 null element in `_event` | start and end null for that element | same |
| p21 null value in `event` | value null with its own start and end | same |
| p15 both empty | empty list | empty list |
| p18 `_event` empty | each value with null start and end | same |

`arrays_zip` returns NULL if any input is NULL, and `zip_with` does the same, so a guard is needed
whenever either list can be null, which it can: `_event` is absent for a value with no annotation.
After `inline` an unguarded NULL drops the row, so the plain and `inline` forms differ only by that
(the unguarded `inline` loses p3's values; the guarded one keeps them). Padding of the shorter list
is the same for `arrays_zip` and `zip_with`. Shape (c) needs the guard on each of its three inputs
(`coalesce(_event.__start, array())`), since a null `_event` makes both field arrays NULL.

## What is not covered

- Pathling's own rules and engine views: this is plain Spark SQL. Whether the lambda-pruning rule
  rewrites the context 2 shapes into pruned reads is not measured here.
- The row reader (`-rowreader`) and AQE: the probe has the flag, the runs were vectorised, AQE off.
- Wide `dosage` with several repeating primitives, and other depths (`event` two levels below a
  repeating element in a repeating element).
- Timing or CPU cost of the shapes, and a Databricks runtime. Only bytes and read schemas.
- Whether a view over a zipped-then-unnested value pairs with other columns in the same select
  (no second generator was tried).
- Compute-first was written as the nested `select` with `inline`; the optimiser may collapse the
  inner projection, which is the point of the question, and was not varied.

## How to reproduce

Scripts are in
[scripts/annotations-design/pairing-shapes/](scripts/annotations-design/pairing-shapes/):
`PairingProbe.scala` (data, shapes, measurements), `run.sh <4.0|4.3>` (builds against the 4.0.2
classpath, runs on either), `all.sh <dir>` (both, keeping the result lines; the raw log of 4.0.2 is
over a gigabyte of debug output and is not worth keeping). It needs the third-party and Spark 4.3.0
classpaths, which `../common.sh` generates on first use, and `~/.m2` Scala 2.13.16. The filtered
output of the runs reported here is `results-4.0.2.txt` and `results-4.3.0.txt` in that directory.

```bash
bash specs/001-parquet-on-fhir/evidence/scripts/annotations-design/pairing-shapes/all.sh <out dir>
```
