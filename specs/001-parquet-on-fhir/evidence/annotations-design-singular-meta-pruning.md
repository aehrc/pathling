# Does carrying a singular primitive's `_x` as `meta` make Spark read its extensions?

Measured for the design in `design/annotations-in-the-engine.md`, whose design A makes a FHIR
primitive `struct(value, meta)` with `meta` the raw stored `_x` group. The question was whether a
**singular** primitive then reads `_x`, extensions included, where today's engine reads only `x` and
the annotation fields.

**Result: yes, in every case measured, including output only, if `meta` is the raw `_x`.** All
eight FHIRPath cases read the whole `_birthDate` (and `_deceasedDateTime` where used), with their
extension arrays, under A-raw: 182.4 MB for the single-date cases and 295.5 MB for the two-date
cases, against 17.6 MB and 21.0 MB today, which is 10x to 14x more. The cause is not the array
operations that the risk assumed. It is that a view always wraps its result in a `Generate`
(`inline`), and Spark does not simplify `struct(x, _x).value` or push a field extract through it, so
a struct that holds the whole `_x` keeps the whole `_x`. Today's engine avoids this only because its
element carries `named_struct(__start, _x.__start, __end, _x.__end)`, a list of individual field
extracts that Spark can alias below the `Generate`. A-system, which converts to the System form
before the operation, reads exactly what the engine reads, with identical `ReadSchema` and bytes in
every case. So a conversion that copies the named annotation fields out of `meta`
(`resolve_or_none`) at the point where the element is built is needed for I/O, for singular
primitives as well as repeating ones.

## Conditions

- Commit `0c94065ec3` (branch `issue/2832_41-42_decimal-and-annotations`), Spark 4.0.2, `local[2]`,
  adaptive execution off.
- Data: 1,000,000 Patients in the stored layout, 300 MB of Parquet
  (`scripts/annotations-design/singular-meta-pruning/GenData.java`). Every row has `birthDate` and
  `active`; two rows in three have `deceasedDateTime`. Even-numbered rows (half) carry five string
  extensions on `_birthDate`, and on `_deceasedDateTime` where it exists, each with distinct payload
  text. `_x` is `struct<id, extension: array<struct<url, valueString>>, __start, __end>`, with the
  annotations as the encoder wrote them. The encoder does not yet keep extensions in `_x`, so
  `GenData` adds them after encoding.
- Bytes are the sum of `inputMetrics.bytesRead` over the tasks of a no-op write (a `SparkListener`),
  in MiB. No timing was taken.

## What ran through the engine and what was modelled

- **engine**: the real engine, one view column per FHIRPath (`collection: true`) through
  `data.view("Patient").json(...)`, as `EnginePruningProbe` does. The plan was read with
  `queryExecution().executedPlan()`. `iif` is not supported by the engine (`Unsupported function:
  iif`), so that case is replaced by `where(active).birthDate | where(active.not()).deceasedDateTime`,
  which has the same conditional shape.
- **A-raw** and **A-system** are **hand-built SQL models**, not engine code. They mirror the shape of
  the engine's plan: `array(CASE WHEN x IS NOT NULL THEN element END)` filtered for nulls, union as
  `concat` plus an `aggregate` dedupe on the range fields, `where` as `filter` over
  `array(CASE WHEN cond THEN element END)`, comparison as a lambda over `array(element)`, and the
  same `inline(array(struct(expr AS r)))` wrapper that a view adds. A-raw's element is
  `struct(value=x, meta=_x)` and its range is read from `meta.__start`. A-system builds that struct
  and then converts it to `struct(value, ann=struct(meta.__start, meta.__end))` before the
  operation, dropping id and extension.
- A-system is, in effect, a model of today's element, so its agreement with the engine validates the
  mirror rather than saying anything new. The model's comparison is simplified (`start > 1990`
  rather than the engine's range-overlap logic), so the result checksums of the two `where` cases
  differ between the engine and the models. A-raw and A-system have the same checksum in every
  case, and the engine's agree for the five cases without a comparison.

## Results

`ReadSchema` is abbreviated: `{..}` is a struct, `META` stands for
`_x:{id, extension:array<{url,valueString}>, __start, __end}`, and `RANGE` for `_x:{__start,__end}`.

| Case | Arm | ReadSchema | MiB read |
| --- | --- | --- | ---: |
| `birthDate` | engine | `birthDate`, `RANGE` | 17.6 |
| | A-raw | `birthDate`, `META` | **182.4** |
| | A-system | `birthDate`, `RANGE` | 17.6 |
| `birthDate \| deceasedDateTime` | engine | both values, both `RANGE` | 21.0 |
| | A-raw | both values, both `META` | **295.5** |
| | A-system | both values, both `RANGE` | 21.0 |
| `birthDate.combine(deceasedDateTime)` | engine | both values, both `RANGE` | 21.0 |
| | A-raw | both values, both `META` | **295.5** |
| | A-system | both values, both `RANGE` | 21.0 |
| `(birthDate \| deceasedDateTime).first()` | engine | both values, both `RANGE` | 21.0 |
| | A-raw | both values, both `META` | **295.5** |
| | A-system | both values, both `RANGE` | 21.0 |
| conditional (`where(active)` union) | engine | `active`, both values, both `RANGE` | 21.0 |
| | A-raw | `active`, both values, both `META` | **295.5** |
| | A-system | `active`, both values, both `RANGE` | 21.0 |
| `birthDate.where($this > @1990)` | engine | `birthDate`, `RANGE` | 17.6 |
| | A-raw | `birthDate`, `META` | **182.4** |
| | A-system | `birthDate`, `RANGE` | 17.6 |
| `(birthDate \| deceasedDateTime).where($this > @1990).exists()` | engine | both values, both `RANGE` | 21.0 |
| | A-raw | both values, both `META` | **295.5** |
| | A-system | both values, both `RANGE` | 21.0 |
| `where(birthDate > @1990).birthDate` | engine | `birthDate`, `RANGE` | 17.6 |
| | A-raw | `birthDate`, `META` | **182.4** |
| | A-system | `birthDate`, `RANGE` | 17.6 |

Baselines, a plain scan of the stored columns (same data):

| Scan | MiB read |
| --- | ---: |
| `birthDate` | 4.0 |
| `_birthDate.{__start,__end}` | 13.7 |
| whole `_birthDate` | 178.6 |
| both values and both ranges | 21.0 |
| both values and both whole `_x` groups | 295.5 |

So A-raw reads exactly the whole-group baseline, and the extensions are about 90% of it here. The
engine and A-system read exactly the values-plus-ranges baseline.

### Why the wrapper matters

`scripts/annotations-design/singular-meta-pruning/ScalarControl.java` (on the 2,000-row file) builds
`named_struct('value', birthDate, 'meta', _birthDate).value` four ways:

| Shape | Reads |
| --- | --- |
| `select s.value` | `birthDate` only |
| `select array(s.value)` | `birthDate` only |
| `select inline(array(struct(array(s.value))))` | `birthDate` and whole `_birthDate` |
| the full model (`transform(filter(array(s), ...), e -> e.value)`) inside the same `inline` | `birthDate` and whole `_birthDate` |

Outside a `Generate`, Spark simplifies the extract and prunes to the value, even through `array`.
Inside the `Generate` that every view produces, it does not. The singular path therefore does not
prune to the field under A-raw for a view, which is the opposite of the singular case in
`annotation-pruning.md`. That finding is about a column read directly. A higher-order function is
a second, separate way to lose pruning, and every case here also has one (`filter`, `transform`),
but this measurement does not isolate which of the two does the damage in each case, because the
`Generate` alone is enough. The dedupe `aggregate` and the comparison lambda read only range
fields, and under A-system those prune.

Not measured: a FHIRPath evaluated outside a `Generate`-wrapped view (for instance a `$fhirpath`
style evaluation that selects the column directly), where the first two rows of the table above
suggest that an output-only singular primitive would prune. The cases that go through a lambda
would still read whatever the lambda's input holds.

## What this means for the design

- `resolve_or_none` (the copy of named annotation fields out of `meta`) at the point where the
  element is built is needed for I/O, and not only for the semantics of System values. Without it a
  view over an extension-heavy table reads 10x to 14x more bytes for a singular date, including for
  `birthDate` output alone.
- Where A-raw costs extra reads: every case measured, since every view is inside a `Generate`. The
  extra read is the whole `_x` group for each primitive the expression names, which is dominated by
  the extension arrays when they are present.
- See the follow-up section below, which tested this. If A is to keep a `meta` that carries extensions,
  building it from individual field extracts (`meta=named_struct('extension', _x.extension, ...)`)
  might let Spark alias them below the `Generate` as it does for the range, but it would still read
  the extension array whenever that struct is built, so the build would also have to be conditional
  on the expression using `extension()`. This spike did not try either.
- The cost of A-system is that id and extension are dropped from the element, so the System form
  cannot serve `extension()`. The design's split (FHIR element keeps `meta`, System element keeps
  `ann`) therefore has to be paired with `meta` being built from field extracts, or the FHIR form
  pays the read whenever it is built.
- Caveat: the models are hand-built and simplified (see above), and one data shape, one resource and
  two primitives were measured. The ReadSchema result does not depend on row count. The byte figures
  scale with the share of rows that carry extensions: here half of them, five extensions each.

## Reproduction

All in `scripts/annotations-design/singular-meta-pruning/`; `probe.sh` sets the classpath.

```bash
D=<data dir>
probe.sh GenData.java $D/big 1000000       # writes $D/big/Patient.parquet
probe.sh SingularMetaProbe.java $D/big     # the table above
probe.sh ScalarControl.java $D/small       # the wrapper control (any size)
probe.sh EnginePlan.java $D/small          # prints the engine's optimised and executed plans
probe.sh Mechanism.java $D/small           # the plan change log of the follow-up
probe.sh Workarounds.java $D/big           # the follow-up table
```

## Follow-up: why the Generate defeats pruning, and whether it can be avoided

Scripts: `Mechanism.java` (plan change log) and `Workarounds.java` (1M-row file, same conditions
as above, same listener and plan reading).

### Mechanism

For `select inline(array(struct(array(s.value) AS r))) from t`, with `s = named_struct('value',
birthDate, 'meta', _birthDate)`, `spark.sql.planChangeLog` shows four effective optimizer rule
runs. In order: `ColumnPruning` pushes a `Project [birthDate, _birthDate]` under the Generate,
because the generator expression still references the attribute `_birthDate`;
`RemoveNoopOperators` drops the outer `Project [r]`; `SimplifyExtractValueOps` then rewrites
`named_struct(value, birthDate, meta, _birthDate).value` to `birthDate`, so `_birthDate` is no
longer used. Nothing prunes the Generate's child again (the log shows no further effective run), so
the `Project [birthDate, _birthDate]` stays and the scan is asked for the whole `_birthDate`. The
simplification comes too late rather than not at all. My reading of why the second pruning does not
happen is that a Generate's child is pruned from a parent `Project`, which `RemoveNoopOperators` has
by then removed, but I did not confirm that in the rule source. Outside a Generate the extract is
simplified before anything pins the column, which is why a plain select prunes.

### Workarounds

| Shape (1M rows) | ReadSchema | MiB |
| --- | --- | ---: |
| control: `select s.value` (no generator) | `birthDate` | 4.0 |
| base: `inline`, `array(s.value)` | `birthDate`, whole `_birthDate` | 182.4 |
| base: `inline`, output-only model, and `where` model | `birthDate`, whole `_birthDate` | 182.4 |
| 2a: `inline(array(r))` over `(select s.value as r from t)` | `birthDate` | **4.0** |
| 2a: inner Project holds `s`, model above | `birthDate`, whole `_birthDate` | 182.4 |
| 2b: `meta` from field extracts including `extension`, `inline`, output-only and `where` | `birthDate`, whole `_birthDate` | 182.4 |
| 2b: `meta` from extracts without `extension` (`id`, `__start`, `__end`), `inline`, output-only and `where` | `birthDate`, `_birthDate:{id,__start,__end}` | 17.6 |
| 2c: `explode` or `posexplode` instead of `inline`, raw `s`, any model | `birthDate`, whole `_birthDate` | 182.4 |
| 2c: `explode`, extracts with `extension`, output-only and `where` | `birthDate`, whole `_birthDate` | 182.4 |
| 2c: `explode`, extracts without `extension`, `where` | `birthDate`, `_birthDate:{id,__start,__end}` | 17.6 |
| no generator, raw `s`, output-only and `where` model | `birthDate`, whole `_birthDate` | 182.4 |

- **Not specific to `inline`.** `explode` and `posexplode` behave the same.
- **The higher-order function defeats pruning independently.** With no generator at all, the model
  (`transform` and `filter` over `array(s)`) still reads the whole `_birthDate`. So the earlier
  statement that the two causes were not isolated is settled: either one is enough.
- **2a works only when the `.value` is computed in the inner Project.** Then the Generate sees an
  ordinary column and only `birthDate` is read. When the inner Project merely builds `s` and the
  lambdas read it above, the whole `_x` returns. No barrier against `CollapseProject` was tried,
  since the working form is not merged (a Generate is not a Project) and the failing one needs no
  merging to fail.
- **2b prunes the fields it does not name, but naming `extension` costs its read.** A `meta` built
  from extracts reads exactly the named fields (17.6 MiB without `extension`, the same as today).
  With `extension` among them, the whole array is read for every row whenever the struct is built,
  including for output only, because nothing consumes it conditionally.

So a single column can keep pruning if `meta` is built from individual field extracts and
`extension` is left out of it unless the expression calls `extension()` or another consumer of it.
A `meta` that always includes `extension` reads it always.

### Where the view builds its Generate

`fhirpath/src/main/java/au/csiro/pathling/projection/Projection.java`, `execute`: the single line
`filteredResult.select(inline(projectionResult.getResultColumn()))`. A project-before-generate
would go immediately before it, but 2a only helps for a value extracted in that Project; the row
where the Project holds `s` and the lambdas run above did not prune, so a Project that merely
precomputes the result column is not shown to help.
