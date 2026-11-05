# Can the planner prune by extraction chains alone? A sketch of D3a-complex

A plan-level feasibility sketch for option D3a and question 5 of
`design/annotations-in-the-engine.md` (§2, the constraint on unused metadata). D3a is the
competitor to the Catalyst rule of
[annotations-design-lambda-pruning-rule.md](annotations-design-lambda-pruning-rule.md) (D3b). The
limitation it works around is the one in
[annotations-design-singular-meta-pruning.md](annotations-design-singular-meta-pruning.md):
Spark prunes only field extractions applied directly to a column, so anything inside a lambda
reads the whole column.

**Result: D3a-complex is feasible and exact for S4 when the expression does not need element
presence, and only partly feasible for the nested case. It cannot give an exact `count()` or
`exists()` on an intermediate struct without reading that struct whole.**

- **S4 with a leaf predicate.** `location.period.where(start > X)` with `.start`, `.count()` or
  `.exists()` reads `location[].period.start` only (7 KiB with `id`, against 4,346 KiB for the
  engine-shaped baseline), and the results equal the baseline on every row including the edge
  rows. Comparing the stored annotation (`_start.__start`) reads two leaves and is also equal.
  The narrowed period as output reads `location[].period` whole (3,262 KiB), because the output
  needs all of it. That drops `location[].extension` and the sibling fields, nothing more.
- **Presence.** A leaf array from `GetArrayStructFields` has the same length as the array, so
  `size(location.<leaf>)` is the exact element count. It cannot tell a null `period` from a
  `period` whose `start` is null. Exact `count()` or `exists()` on `location.period` needs
  either the struct read whole, or one leaf per child. The per-child plan fails for the
  extension children, because `period.extension[].url` is two array levels below `location`,
  and Spark cannot extract a field across two array levels. The only reachable leaf is the whole
  `extension` array, payload included. Leaves for every child still miss a `period` that is
  stored non-null with every child null (E9). **D3b does not have this problem:** a reader
  schema narrowed to one leaf preserves struct-null versus field-null (measured, both readers),
  so D3b reads one leaf (1 KiB) where D3a reads 3,256 KiB.
- **Nested case.** `dosageInstruction.timing.event`, `.count()` and `.exists()` read
  `dosageInstruction[].timing.event` only (7 KiB with `id`, against 3,315 KiB). `event.where($this
  > @2020)` on `_event.__start` cannot be pruned to `__start`: `_event` is an array inside an
  array element, `dosageInstruction.timing._event.__start` fails analysis, and every workaround
  tried (`transform`, `flatten`, `explode`, indexed access) reads `_event` whole. It reads
  `event` and `_event` (1,630 KiB), halving the baseline but keeping the annotation payload that
  the design wants to avoid. Results are equal.
- **Foreign annotations.** `__foreign` is absent from the D3a read schema for every case that
  reads leaves. It is read whenever D3a reads a struct or an annotation array whole (S4b, S4e,
  S4f, N2). The baseline reads it in every case.

What was measured and what is inferred is separated in "Reading the results".

## Conditions

- Commit `0c94065ec3` plus working-tree changes, Spark 4.0.2 (ANSI mode on, its default), Scala
  2.13.16, Java 21, `local[2]`, adaptive execution off. No Pathling code is involved: the
  probe builds SQL over a local session. Both the vectorized and the row-based Parquet reader were
  run, and the output is identical apart from the reader's name.
- A read is the `requiredSchema` of the `FileSourceScanExec` in the executed plan. Kibibytes are
  the compressed column-chunk sizes in the Parquet footer of the leaves read, summed. They are an
  indication of cost, not a timing. No CPU was measured.
- Results are sorted `Row.toString` lists plus the output schema, compared between the baseline
  and D3a for the same wrapper, bare (`select id, <expr> as r`) or view
  (`select inline(array(named_struct('id', id, 'r', <expr>)))`).
- Data are two Parquet files (`enc`, 1,510 rows; `med`, 1,510 rows) written with an explicit
  schema in the layout of the PoF design:
  - every complex element carries `id`, `extension` (five extensions of 40 random characters)
    and `__foreign: string` (200 random characters);
  - every primitive group is `struct<id, extension, __start, __end, __foreign>`;
  - `enc.location[]` is `struct<id, extension, status, period, __foreign>`, with `period` of
    `id, extension, start, end, _start, _end, __foreign`;
  - `med.dosageInstruction[]` is `struct<id, extension, text, timing>`, with `timing` of
    `id, extension, event[], _event[], __foreign`.
- Edge rows (hand-written rows carry no `__foreign`):
  - `enc`: E1 null location; E2 empty; E3 an element only through its `extension`; E4 `period`
    null; E5 `period` with only `end`; E6 `period` only through its `extension`; E7 a mix of
    four; E8 a null element (hypothetical, see below); E9 `period` stored non-null with every
    child null; E10 `period` whose only content is a `_start` with a value; E11 `period` whose
    only content is `_start.extension`.
  - `med`: M1 null `dosageInstruction`; M2 empty; M3 `timing` null; M4 empty `event`; M5
    `timing` only through its `extension`; M6 two dosages; M7 a null entry in `event` with its
    annotation; M8 `event` with `_event` null; M9 `event` longer than `_event` (hypothetical);
    M10 a dosage only through its `extension`; M11 a null `timing` before a populated one.
- Dates are strings, so `start > @2000-10-10` is a string comparison with `'2000-10-10'`.
- The baseline is the engine's shape: `filter(filter(location, x -> x is not null).period,
  p -> p is not null)` for the traversal, then `filter` for `where`. Counts are wrapped in
  `coalesce(size(..), 0)` on both sides, because `size(null)` is null under ANSI.

## The planner rule, in general

The rule is a function from the demand of an expression to a set of extraction chains plus a
rebuild. It is a sketch, not an implementation.

1. **Demand.** Walk the parsed expression or view, and for each array column compute the set of
   leaf paths used. A leaf path may cross at most one array level below the column, because a
   chain cannot cross two (see "Nested arrays"). Output of a whole element demands every leaf
   under it.
2. **One level.** Each demanded leaf `a.b.c` becomes a chain read outside any lambda,
   `location.period.start`, an array aligned with `location`. With one leaf, the lambda runs
   over that array directly: `filter(location.period.start, s -> s > X)`. With several leaves
   the narrow element is rebuilt, `arrays_zip(location.period.start, location.period._start.__start)`,
   whose fields are named by the leaf names (S4g). Duplicate leaf names (`id` under two groups)
   need `zip_with` with named structs, because `arrays_zip` cannot alias.
3. **Two levels.** The outer array is zipped at the outer level and the inner arrays inside the
   lambda: `zip_with(dosageInstruction.timing.event, dosageInstruction.timing._event,
   (e, m) -> zip_with(e, m, (a, b) -> named_struct('e', a, 's', b.__start)))`. The outer
   `zip_with` over `array<array<..>>` is the plan shape of N2. Each level adds one `zip_with`
   and one narrow `named_struct` per element.
4. **Presence.** For a lambda that needs "the element exists", demand adds the presence leaves
   of that element (next section).

The generated expressions are in `D3aProbe.scala` (`CASES`), and the reads were verified per
case below.

## Cases

Reads in KiB are those of the bare form. The view form read exactly the same leaves in every
case. `__foreign` is the column asked for by the coordinator: whether it is in the read schema,
baseline then D3a. A baseline always reads `location` or `dosageInstruction` whole.

| Case | D3a reads | KiB base / D3a | `__foreign` base / D3a | Results equal (bare, view) |
| --- | --- | --- | --- | --- |
| S4a `location.period.where(start > X).start` | `period.start` | 4346 / 7 | yes / no | yes, yes |
| S4b the narrowed period | `period` whole | 4346 / 3262 | yes / yes | yes, yes |
| S4c `.where(start > X).count()` | `period.start` | 4346 / 7 | yes / no | yes, yes |
| S4d `.where(start > X).exists()` | `period.start` | 4346 / 7 | yes / no | yes, yes |
| S4e `location.period.count()` | `period` whole | 4346 / 3262 | yes / yes | yes, yes |
| S4f `location.period.exists()` | `period` whole | 4346 / 3262 | yes / yes | yes, yes |
| S4g `.where(_start.__start > X).start` | `period.start`, `period._start.__start` | 4346 / 8 | yes / no | yes, yes |
| N1 `dosageInstruction.timing.event` | `timing.event` | 3315 / 7 | yes / no | yes, yes |
| N2 `event.where($this > @2020)` on `_event.__start` | `timing.event`, `timing._event` whole | 3315 / 1630 | yes / yes | yes, yes |
| N3 `.event.count()` | `timing.event` | 3315 / 7 | yes / no | yes, yes |
| N4 `.event.exists()` | `timing.event` | 3315 / 7 | yes / no | yes, yes |

Controls, which are expected to differ (the baseline is `size($PER_B)`):

| Control: `location.period.count()` from | Reads (KiB) | Edge rows that differ |
| --- | --- | --- |
| S4i the non-null `start` leaf | 7 | E5, E6, E7, E9, E10, E11 |
| S4j `size(location.period.start)` (the element count) | 7 | E3, E4, E7, E8 |
| S4h nine leaves, no extension | 12 | E6, E9, E11 |
| S4k nine leaves plus `period.extension` | 605 | E9, E11 |
| S4l every leaf including the three extension arrays | 1791 | E9 |

S4i undercounts where `period` is present with a null `start`, and S4j overcounts where
`period` is null. These are the cases that `where(start > X)` hides: it drops null starts anyway,
so S4a to S4d and S4g are affected by none of it. S4h to S4l show that no leaf set is exact.

Observations from the edge rows, measured:

- For S4c on E7 the result is 1 (only `2005-05-05` passes), and E8 gives 1 on both sides, so
  the null element does not disturb the where-form.
- N2 on M8 (`_event` null) yields no events on both sides, because the predicate reads a null
  `__start`. On M9 (`_event` shorter than `event`) both give `2024-01-01` only. The two
  `zip_with` calls pad the shorter array with nulls, so a misaligned `_event` cannot raise an
  error. It also cannot reveal the misalignment.
- N2 on M7 (null entry in `event`, annotation present) returns `2023-01-01`; the entry with a
  null value is dropped by the final `e is not null`, as in the baseline.
- `exists()` over a nullable predicate is three-valued: `exists(arr, s -> s > X)` returns null
  when no element is true and some are null. The first version of S4d did not match the
  baseline for that reason, and D3a must emit `coalesce(exists(..), false)`.

## Presence

All of this was measured on `enc` unless stated.

**Length.** `size(location.id)`, `size(location.status)` and `size(location.period.start)` equal
`size(location)` on all 1,510 rows. `GetArrayStructFields` preserves the array length, including
for the null element of E8, where `location.id` is `[null, "l"]`. So length answers "how many
elements" exactly, provided no element is null.

**Null elements.** Reading the source, Pathling's reader cannot produce a null element in a
complex array from conformant FHIR JSON, because a JSON array of objects cannot contain null.
The reader's own documentation says that nothing is pruned and that a repeating primitive keeps
positional nulls, which are the only nulls it describes. A null element therefore arises only
from layout data written by another producer. E8 models it: `size()` then counts the null
element and the baseline does not. This is inferred from the documentation in `FhirJsonReader`
and `ResourceTransformer`; I did not run the reader on non-conformant input. If the null element
cannot occur, array length answers per-element presence for `location` and `dosageInstruction`.
A leaf array cannot distinguish the null element from an element whose leaf is null, so D3a
would have to treat the data as trusted, as the reader does.

**An intermediate struct.** The same leaf array cannot tell a null `period` from a `period` whose
`start` is null (S4i). A FHIR element is present if any child is present. The leaves that would
decide it are `id`, `start`, `end`, `extension`, and the groups `_start` and `_end`, each of
which is present through its own `id` or `extension`. Measured:

- Chains reach `location.period.start` and `location.period._start.__start` (one array level).
  They do **not** reach `location.period.extension.url` or `location.extension.url`: Spark
  rejects the second array level at analysis. The only extension read available is
  `location.period.extension` whole, 593 KiB of payload, and the same for `_start` and `_end`
  (1,186 KiB of extension payload between them, 2,661 KiB for both groups with `__foreign`).
  The cheap presence leaf `extension[].url` therefore exists in the file (1 KiB) but cannot be
  asked for by D3a.
- Nine non-extension leaves cost 6 KiB and are wrong on E6, E9 and E11 (S4h). Adding
  `period.extension` is wrong on E9 and E11 (S4k). Adding the extensions of `_start` and `_end`
  is wrong only on E9 (S4l), and costs 1,791 KiB, which is the whole `period` bar the `__foreign` fields.
- E9 is a `period` stored non-null with every child null. It is not hypothetical: the
  reader keeps a structure whose only content was a primitive's id and extensions, stored as a
  null of the declared type, until the metadata is stored (decisions 71 and 72). Today such a
  period is present in FHIR and has only null leaves. After the metadata is stored, `_start.id`
  or `_start.extension` would show it.
- The exact D3a form is therefore `location.period` extracted whole and tested for
  `is not null` (S4e, S4f): 3,256 KiB, against 4,340 for `location`. It skips the
  `location[]` siblings (`extension`, `status`, `__foreign`) and reads everything inside `period`.

**Struct-level extraction.** Test of whether any means other than a lambda lets the reader
return a struct restricted to some children, while keeping struct-null:

- **At the data source it works.** A reader schema narrowed to `location[].period.start`, and
  separately to `period.end`, returned the `period`-is-null flag equal to the full read on
  all 1,510 rows with both readers: E4 true; E5, E6 and E9 false (present, with null leaves).
  A schema narrowed to `location[].status` kept the element-is-null flag for E3 (false) and E8
  (true). So Parquet definition levels carry struct-null through one leaf. That is the property
  D3b relies on, and it holds for the pruned struct.
- **In SQL it does not.** `select location[0].period is null` reads `period` whole (3,256 KiB).
  `size(location.period)` reads `period` whole. A Filter condition on `location[0].period`
  reads `location` whole. `location[0].period.start` reads only that leaf, so a leaf is
  reachable but its parent's nullness is not requestable alone.
- **Index-rooted access inside a lambda does not prune.** The idea was that
  `transform(location.status, (x, i) -> location[i].period is not null)` roots its access at the
  attribute, so the lambda variable is only an index. Measured: `location` whole. The same for
  `location[i].period.start`, and for the two-level form
  `dosageInstruction[i].timing._event[j].__start`, with `[]` and with `try_element_at`
  (`dosageInstruction` whole in all). This refutes the shortcut for both presence and nested
  pruning.

**Which cases are affected.** Presence matters only where the result depends on an element that
has no value in the leaf: `count()` and `exists()` without a leaf predicate (S4e, S4f), `where`
on a non-leaf predicate, and any `forEach` or `select` that keeps empty elements. A `where` on a
leaf (S4a to S4d, S4g) is unaffected, since a null leaf fails the predicate. Value outputs (S4a,
N1) are unaffected, since nulls are dropped. For repeating primitives, an element present
only through `_event[i]` while `event[i]` is null (M7) is dropped by a value-based count on both
sides. If the design counts it as present, `count()` needs a presence leaf of `_event` as well, and
that leaf has the same two-array-level problem.

## Nested arrays

- `dosageInstruction.timing.event` is a single chain and gives `array<array<string>>`, read as
  `timing.event` only (1 KiB). `flatten`, with `coalesce(e, array())` on the inner arrays for
  null timings, gives the flat collection (N1, N3, N4).
- `dosageInstruction.timing._event` gives `array<array<META>>`. A field of `META` cannot be
  taken from it. `.__start` fails analysis with a type mismatch, since the extraction applies to
  the outer array only. Written as `transform(_event, a -> transform(a, m -> m.__start))`,
  as `flatten(...).__start`, or after `explode(dosageInstruction)` it reads `_event` whole
  (1,623 KiB), and `inline(dosageInstruction)` reads `dosageInstruction` whole. The three leaves
  that N2 needs are 1 KiB each in the file.
- **Plan shape for N2.** Two `zip_with`: the outer zips `event` with `_event` per dosage
  (both have the length of `dosageInstruction`, since the leaf arrays are aligned with it), and
  the inner zips each pair of arrays into `named_struct('e', a, 's', b.__start)`, then `filter`,
  `transform`, `coalesce` for null timings, `flatten`, and a final null filter. The baseline
  does the same work over whole structs, so the re-zip adds one struct construction per event, no
  more per-element work than the baseline already does. This is qualitative.
- **Alignment risks.** `zip_with` pads with null, so a `_event` shorter than `event` (M9) or
  null (M8) gives null annotations rather than an error, and the predicate drops those events.
  A positional index lookup would not be safe: under ANSI, `arr[i]` out of range raises, and
  `try_element_at` was not pruned either. The two arrays are aligned by position only by the
  layout's contract that a repeating primitive keeps its positional nulls (spec.md, line 415). A writer that drops a null from one of them
  would misalign every later element silently, and neither D3a nor D3b can detect it.

## Risks and gaps

- **Presence is the structural weakness.** The exact answer needs the struct read whole, so a
  view with `location.period.count()` reads `period` payloads that D3b would not. The
  workaround of reading nine cheap leaves is wrong on E6, E9 and E11, and E9 exists today.
- **Cannot cross two array levels.** Every repeating primitive below a repeating element, the
  `_event` of N2, loses the saving for the annotation fields. The same applies to
  `extension[]` of any complex element under an array, which is what makes the presence leaf
  unreachable.
- **A whole struct read pulls `__foreign` with it.** S4b, S4e, S4f and N2 read it, which is the
  data the constraint in §2 says must cost nothing. Only reads of explicit leaves avoid it.
- **Demand analysis is per expression, and views compose.** A view's `where`, `forEach` and
  `unionAll` columns share one scan, so the demand is a union of chains, and a planner has to see
  the whole view. The rewrite also changes the expression shape for every operator that touches
  a repeating element, which D3b avoids by working after the plan is built.
- **Three-valued logic and null placement.** `exists`, `size(null)` and `flatten` of an array
  containing a null each differ from the engine's present behaviour under ANSI, and each cost a
  failed attempt here. They need a test matrix.
- Not done: no CPU benchmark (as asked); no run through the Pathling engine, so the plans
  are hand-built; no Delta or DataSource V2; one row group per file (`coalesce(1)`), so
  Parquet filter pushdown was not examined; the reader on non-conformant input was not run.

## Reproduction

```bash
specs/001-parquet-on-fhir/evidence/scripts/annotations-design/d3a-complex-sketch/run.sh
specs/001-parquet-on-fhir/evidence/scripts/annotations-design/d3a-complex-sketch/run.sh -rowreader
```

`run.sh` compiles `D3aProbe.scala` with the Scala 2.13 compiler from `~/.m2`, regenerates both
Parquet files under `.local/work/issue/2832_41-42_decimal-and-annotations/spikes/d3a-complex-sketch/`,
runs every case, and writes the report to `output.txt` there (raw log in `raw.txt`). `env.sh`
takes the third-party classpath from `../common.sh`, which generates it on first use; `TP`
overrides it. No Pathling jar is needed. Runtime is about one minute.
