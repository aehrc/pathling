# Do existence rewrites and a lean element shape make compute-first prune every view?

A measurement on the real view engine, extending
[annotations-design-project-before-generate.md](annotations-design-project-before-generate.md).
That spike left eight of 26 views short of what the lambda-pruning rule ("D3b", see
[annotations-design-lambda-pruning-rule.md](annotations-design-lambda-pruning-rule.md)) reads: four
read the `period._start` annotation because of the element shape, and four hold a presence-only
`size(filter(arr, ...))`. This spike adds the two engine changes that were left open, as rewrites of
the analysed plan. The engine is not edited: it builds its plan as today and a probe rewrites the
expressions in it.

**Result: on Spark 4.3.0 with the lambda-pruning rule off, compute-first plus the rewrites reads as
little as the rule on every view. All 26 views are A, the 8 B and C views included. The
rewrites change no result.** The rule and compute-first compose on 4.3.0, but not on 4.0.2 for
the new forms.

- **Classes on 4.3.0, 26 views (rule off unless stated):** compute-first alone A 18, B 4, C 4;
  rewrites alone A 4, C 22 (they need the `Project` under the `Generate`); compute-first plus
  rewrites (P+R) **A 26**; P+R with the rule installed (P+R+D3b) A 26.
- **Classes on 4.0.2, 26 views:** nothing prunes without the rule, so P, R and P+R are A 4, C 22
  (the four are the singular-meta views, which read the same everywhere). P+R+D3b is A 26.
- **All eight B and C views became A:** `name.period.start`, `address.period.start`, forEach
  `name / period.start` and the multi-column view (B), and `name.exists()`, `name.count()` and the
  two `where` views with `exists` (C). The `period._start` reads went with layer 1; the whole-`name`
  reads went with the existence rewrites.
- **One view is now better than the rule:** singular-meta `birthDate` reads `birthDate` alone, where
  the rule (and every earlier plan) reads `birthDate, _birthDate.__start, _birthDate.__end`.
- **Results are equal to the original in every arm of every view, on both versions.** 68 view
  runs per version: the 26 views, 10 extra views on `exists`/`all`/`count`/`empty`, and all 32
  name and address views again over a data set with null elements. Full rows, sorted, `except`
  in both directions, and strict output schemas equal (nullability included).
- **The rewrites need guards to match today's engine.** `exists(c)` and `all(c)` written the obvious
  way differ from it where `name` is null, where an element is null, and where the criteria is
  null, in up to 66 of 66 patients (table below). With the guards listed there they differ in none.
- **Composition fails on 4.0.2 for the new forms.** With the rule installed, 6 of the 10 extra views
  (all those with `array_max`/`array_min` in a `Generate` input) read the whole column, worse than
  the rule alone: the rule bails out on its own output-schema guard (`containsNull` of the
  boolean array changes; debugged for `name.all` only). The result stays correct. The 26 views are unaffected. On 4.3.0, where
  stock Spark prunes, the rule never reads more than P+R.

## Rewrites

All four are applied to the engine's analysed plan by `LayerRewrites.scala`, as expression
substitutions. Each is counted per view in the "Rewrites applied" column.

| Name | Engine builds today | Rewritten to |
| --- | --- | --- |
| E1 (`exists()`, `count()`, `empty()`) | `size(filter(arr, x -> x is not null))`, `isnull(filter(arr, ...))` | `size(filter(arr, x -> x is not null).leaf)`, `isnull(<root array>.leaf)`, where `leaf` is the first atomic field of the element in schema order (`use` for `name`) |
| E2 (`exists(c)`, `where(c).exists()`, `where(c).count()`, `where(c).empty()`) | `size(filter(G, c)) = 0`, `isnull(filter(G, c))`, `size(filter(G, c))` | `not coalesce(array_max(transform(G, x -> coalesce(c(x), false))), false)`; the null test becomes `isnull(transform(G, ...))`; the count becomes `size(filter(transform(G, ...), b -> b))` |
| E3 (`all(c)`) | `count(filter(G, c)) = count(G)` | `coalesce(array_min(transform(G, x -> coalesce(c(x), false))), true)` |
| L1 (layer-1 element shape) | `struct(value, v, range, resolve_fields_or_else(_x, __start, __end, date_range(v)))` | the same with `range` a null of its type, when nothing in the plan reads `range` |

`G` is the engine's own input, `filter(name, x -> x is not null)`, kept as it is.

### What had to be adjusted

1. **No extra leaf for criteria.** E1 alone makes `name.exists(period.start > @1990)` read `use`
   as well as the criteria leaves, because `isnull(filter(G, c))` also took the leaf. For a filter
   with criteria the null test and the count therefore ride on the criteria (E2), not on a leaf. E1
   is used only for the criteria-free filter. This is the "reads only the criteria leaves" of the
   brief.
2. **The coalesce is inside the lambda as well as outside.** `coalesce(c(x), false)` per element
   makes a null criteria count as false, which is what `where` does (it keeps only elements for
   which the criteria is true). Without it `all` differs (below).
3. **Null elements.** `G` already excludes null elements (`x is not null`), and the rewrite keeps `G`.
   The forms that run over `name` directly evaluate the criteria on a null element.
4. **`all(c)` is matched on its shape.** The engine builds
   `CASE WHEN isnull(F) THEN 0 ELSE size(F) END = CASE WHEN isnull(G) THEN 0 ELSE size(G) END`,
   with the two `G` equal only up to lambda-variable ids, so the match is on `semanticEquals`. A
   null `c(x)` counts as false, exactly as today's count of `where(c)`.
5. **L1 is a null of the same type, not a missing field.** The real change would omit `range`; a
   typed null keeps every other expression's types valid, with the same effect on what is read. It
   applies only when no `GetStructField` named `range` exists anywhere in the plan, a conservative
   whole-plan test. `period.start > @1990` reads `range` (comparison uses it), so those views keep
   `period._start`: they were already A.
6. **The element shape in the engine is `struct(value, range)`**, not `struct(value, start, range,
   ...)` as the previous evidence file abbreviates it; `start` is the value.

## Semantic checks

`SemanticProbe.java` compares the engine's `name.exists(c)` and `name.all(c)` per patient with
four SQL forms, over the edge data set (66 patients: the 22 original, plus each with null elements
around and between its names, plus each with a name array of one null element; patient `e1` has a
name present only through its extension and `e2` has no name). Cells are the number of patients
whose value differs from the engine's. The results are identical on 4.3.0 and 4.0.2.

- pure: `coalesce(array_max(transform(name, x -> c)), false)`, or `array_min`/`true` for `all`
- native: `exists(name, x -> c)`, or `forall`
- native + coalesce: the native form with the outer `coalesce`
- final: `coalesce(array_max(transform(filter(name, x -> x is not null), x -> coalesce(c, false))), false)`,
  or `array_min`/`true` for `all`. This is what the rewrite builds.

| Function | Criteria | pure | native | native + coalesce | final |
| --- | --- | --- | --- | --- | --- |
| exists | `use = 'nickname'` | 0/66 | 26/66 | 0/66 | 0/66 |
| all | `use = 'nickname'` | 42/66 | 66/66 | 42/66 | 0/66 |
| exists | `use != 'nickname'` | 0/66 | 66/66 | 0/66 | 0/66 |
| all | `use != 'nickname'` | 2/66 | 26/66 | 2/66 | 0/66 |
| exists | `use.empty()` | 23/66 | 24/66 | 23/66 | 0/66 |
| all | `use.empty()` | 0/66 | 1/66 | 0/66 | 0/66 |
| exists | `family.exists()` | 0/66 | 1/66 | 0/66 | 0/66 |
| all | `family.exists()` | 23/66 | 24/66 | 23/66 | 0/66 |

Examples of the patients that differ, for the pure and coalesced forms:

- all(use = 'nickname'), pure: [p0: engine=false sql=true] [p1: engine=false sql=true] [p2: engine=false sql=true]
- all(use = 'nickname'), native + coalesce: [p0: engine=false sql=true] [p1: engine=false sql=true] [p2: engine=false sql=true]
- all(use != 'nickname'), pure: [e1: engine=false sql=true] [e1-nullelem0: engine=false sql=true]
- all(use != 'nickname'), native + coalesce: [e1: engine=false sql=true] [e1-nullelem0: engine=false sql=true]
- exists(use.empty()), pure: [e2-nullelem0: engine=false sql=true] [p0-nullelem1: engine=false sql=true] [p1-nullelem1: engine=false sql=true]
- exists(use.empty()), native + coalesce: [e2-nullelem0: engine=false sql=true] [p0-nullelem1: engine=false sql=true] [p1-nullelem1: engine=false sql=true]
- all(family.exists()), pure: [e2-nullelem0: engine=true sql=false] [p0-nullelem1: engine=true sql=false] [p1-nullelem1: engine=true sql=false]
- all(family.exists()), native + coalesce: [e2-nullelem0: engine=true sql=false] [p0-nullelem1: engine=true sql=false] [p1-nullelem1: engine=true sql=false]


What this shows:

- **The pure `array_max(transform(...))` form is not the engine's `exists`.** It is equal for
  `use = 'nickname'` and `use != 'nickname'`, but differs for criteria that are true of an empty value
  (`use.empty()`), because it evaluates the criteria on null elements, where the engine's input has
  dropped them (23 of 66 patients). The brief's earlier probe found 2 rows differing where `name` is
  null, with criteria `period.start > '2000'` compared as raw strings; this data set did not
  reproduce those two rows, because for `exists` a null `name` gives `false` in the pure form too.
  The null-`name` difference appears in the native form, below.
- **The native `exists`/`forall` are three-valued.** For a null `name` (patient `e2`) and
  for criteria that are null on every element they return null where the engine returns `false`
  (`exists`) or `true` (`all`), up to 66 of 66 patients.
- **`all` needs the criteria coalesced to false inside the lambda.** The outer `coalesce` alone
  leaves 42 of 66 patients wrong for `all(use = 'nickname')`: an element whose `use` is null makes
  the criteria null, `array_min` ignores nulls, and the form says `true` where the engine counts the
  element as not matching.
- **`final` differs in none.** It is what `LayerRewrites` builds, and the full-row equality of
  all 68 view runs per version confirms it in the engine's own plan, including the extra views with
  `all(given.exists())` (a nested `exists` in the criteria).

## Results

Reads are the leaves Parquet reads (the `ReadSchema` of `FileSourceScanExec`), compact as in the
previous evidence file; a bare name is read entirely. The plans:

- **O**: the engine's plan, rule off.
- **D3b**: the original plan with `LambdaPruning` installed, the reference.
- **P**: compute-first only, the previous spike's arm.
- **P+R**: compute-first plus E1, E2, E3 and L1, rule off.
- **P+R+D3b**: the same with the rule installed.
- R alone (rewrites without compute-first) is counted in the class table but not shown: it reads
  what O reads, `name` whole, except for the singular-meta views, because the rewritten
  expressions are still the `Generate` input.

The class is computed from the leaves read, not judged: A if the plan reads a subset of what D3b
reads (equal or better), C if it reads what O reads, otherwise B. The `P / P+R / P+R+D3b` column
gives the classes. Bytes read were not recorded by the previous spike, so none are recorded here.
"Rewrites applied" is the count per rule in the P+R plan; `none` means the engine's plan holds none
of the patterns. "Results equal" is full-row equality of all five arms with O.

The 22 name and address views are on the 22-patient set; the singular-meta views are on the
2000-row `small` set. The extra views and the edge set are new in this spike. The edge set holds
66 patients: the 22, plus each with `name` and `address` replaced by the array with a null
element at both ends, plus each with a one-element array holding only a null.

### Spark 4.3.0

The views of the previous spike (26 views):

| Plan | A | B | C |
| --- | --- | --- | --- |
| P (compute-first only) | 18 | 4 | 4 |
| R (rewrites only) | 4 | 0 | 22 |
| P+R | 26 | 0 | 0 |
| P+R+D3b | 26 | 0 | 0 |

- P+R+D3b reads no more than either P+R or D3b alone: 26 of 26.
- Results equal to O in every arm: 26 of 26.
- P+R not A: none.

| View | O | D3b | P | P+R | P+R+D3b | P / P+R / P+R+D3b | Rewrites applied | Results equal |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `name.family` | name | name[].family | name[].family | name[].family | name[].family | A / A / A | none | yes (22 rows) |
| `name.given` | name | name[].given | name[].given | name[].given | name[].given | A / A / A | none | yes (22 rows) |
| `name.period.start` | name | name[].period.start | name[].period.start, name[].period._start | name[].period.start | name[].period.start | B / A / A | L1-range=1 | yes (22 rows) |
| `name.where(period.start > @1990).family` | name | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | A / A / A | none | yes (22 rows) |
| `name.where(use = 'nickname').given` | name | name[].use, name[].given | name[].use, name[].given | name[].use, name[].given | name[].use, name[].given | A / A / A | none | yes (22 rows) |
| `name.exists()` | name | name[].use | name | name[].use | name[].use | C / A / A | E1-isnull=2 E1-size=2 | yes (22 rows) |
| `name.count()` | name | name[].use | name | name[].use | name[].use | C / A / A | E1-isnull=1 E1-size=1 | yes (22 rows) |
| `address.period.start` | address | address[].period.start | address[].period | address[].period.start | address[].period.start | B / A / A | L1-range=1 | yes (22 rows) |
| `address.city \| name.family` | name, address | name[].family, address[].city | name[].family, address[].city | name[].family, address[].city | name[].family, address[].city | A / A / A | none | yes (22 rows) |
| forEach name / `family` | name | name[].family | name[].family | name[].family | name[].family | A / A / A | none | yes (41 rows) |
| forEach name / `period.start` | name | name[].period.start | name[].period.start, name[].period._start | name[].period.start | name[].period.start | B / A / A | L1-range=1 | yes (41 rows) |
| `name.first().family` | name | name[].family | name[].family | name[].family | name[].family | A / A / A | none | yes (22 rows) |
| `name.where(family.exists()).given` | name | name[].family, name[].given | name[].family, name[].given | name[].family, name[].given | name[].family, name[].given | A / A / A | none | yes (22 rows) |
| multi-column view with where and unionAll | id, name, birthDate, address | id, name[].use, name[].family, birthDate, address[].city | id, name, birthDate, address[].city | id, name[].use, name[].family, birthDate, address[].city | id, name[].use, name[].family, birthDate, address[].city | B / A / A | L1-range=2 E2-isnull=2 E2-exists=2 | yes (60 rows) |
| nested forEach name / forEach given | name | name[].family, name[].given | name[].family, name[].given | name[].family, name[].given | name[].family, name[].given | A / A / A | none | yes (60 rows) |
| forEachOrNull name / family | name | name[].family | name[].family | name[].family | name[].family | A / A / A | none | yes (42 rows) |
| forEach name / family, where on annotation-free leaf (use) | name | name[].use, name[].family | name | name[].use, name[].family | name[].use, name[].family | C / A / A | E2-isnull=2 E2-exists=2 | yes (40 rows) |
| forEach name / family, where with exists(criteria) | name | name[].use, name[].family | name | name[].use, name[].family | name[].use, name[].family | C / A / A | E2-isnull=2 E2-exists=2 | yes (40 rows) |
| forEach name / family, where first().family = 'F1' (annotation-free leaf) | name | name[].family | name[].family | name[].family | name[].family | A / A / A | none | yes (2 rows) |
| unionAll of two forEach (name family, address city) | name, address | name[].family, address[].city | name[].family, address[].city | name[].family, address[].city | name[].family, address[].city | A / A / A | none | yes (61 rows) |
| forEach name / family, where on period.start (annotated leaf) | name | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | A / A / A | none | yes (12 rows) |
| forEach name / period.start compared in column | name | name[].period.start, name[].period._start | name[].period.start, name[].period._start | name[].period.start, name[].period._start | name[].period.start, name[].period._start | A / A / A | none | yes (41 rows) |
| singular-meta small: `birthDate` | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate | birthDate | A / A / A | L1-range=1 | yes (2000 rows) |
| singular-meta small: `birthDate.where($this > @1990)` | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | A / A / A | none | yes (2000 rows) |
| singular-meta small: `birthDate \| deceasedDateTime` | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | A / A / A | none | yes (2000 rows) |
| singular-meta small: `(birthDate \| deceasedDateTime).where($this > @1990).exists()` | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | A / A / A | E2-isnull=2 E2-exists=2 | yes (2000 rows) |

Extra views, for the existence rewrites (10 views):

| Plan | A | B | C |
| --- | --- | --- | --- |
| P (compute-first only) | 1 | 0 | 9 |
| R (rewrites only) | 0 | 0 | 10 |
| P+R | 10 | 0 | 0 |
| P+R+D3b | 10 | 0 | 0 |

- P+R+D3b reads no more than either P+R or D3b alone: 10 of 10.
- Results equal to O in every arm: 10 of 10.
- P+R not A: none.

| View | O | D3b | P | P+R | P+R+D3b | P / P+R / P+R+D3b | Rewrites applied | Results equal |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| extra: `name.where(use = 'nickname').exists()` | name | name[].use | name | name[].use | name[].use | C / A / A | E2-isnull=2 E2-exists=2 | yes (22 rows) |
| extra: `name.exists(period.start > @1990)` | name | name[].period.start, name[].period._start | name | name[].period.start, name[].period._start | name[].period.start, name[].period._start | C / A / A | E2-isnull=2 E2-exists=2 | yes (22 rows) |
| extra: `name.all(use = 'nickname')` | name | name[].use | name | name[].use | name[].use | C / A / A | E3-all=1 | yes (22 rows) |
| extra: `name.empty()` | name | name[].use | name | name[].use | name[].use | C / A / A | E1-isnull=1 E1-size=1 | yes (22 rows) |
| extra: `name.where(use = 'nickname').count()` | name | name[].use | name | name[].use | name[].use | C / A / A | E2-isnull=1 E2-size=1 | yes (22 rows) |
| extra: `name.where(use = 'nickname').empty()` | name | name[].use | name | name[].use | name[].use | C / A / A | E2-isnull=1 E2-exists=1 | yes (22 rows) |
| extra: `name.given.count()` | name | name[].given | name[].given | name[].given | name[].given | A / A / A | none | yes (22 rows) |
| extra: `address.exists(city = 'C1')` | address | address[].city | address | address[].city | address[].city | C / A / A | E2-isnull=2 E2-exists=2 | yes (22 rows) |
| extra: `name.all(period.start > @1990)` | name | name[].period.start, name[].period._start | name | name[].period.start, name[].period._start | name[].period.start, name[].period._start | C / A / A | E3-all=1 | yes (22 rows) |
| extra: forEach name / family, where all(given.exists()) | name | name[].family, name[].given | name | name[].family, name[].given | name[].family, name[].given | C / A / A | E3-all=1 | yes (40 rows) |

Edge data set (null elements in name and address), all views (32 views):

| Plan | A | B | C |
| --- | --- | --- | --- |
| P (compute-first only) | 15 | 4 | 13 |
| R (rewrites only) | 0 | 0 | 32 |
| P+R | 32 | 0 | 0 |
| P+R+D3b | 32 | 0 | 0 |

- P+R+D3b reads no more than either P+R or D3b alone: 32 of 32.
- Results equal to O in every arm: 32 of 32.
- P+R not A: none.

| View | O | D3b | P | P+R | P+R+D3b | P / P+R / P+R+D3b | Rewrites applied | Results equal |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| edge: `name.family` | name | name[].family | name[].family | name[].family | name[].family | A / A / A | none | yes (66 rows) |
| edge: `name.given` | name | name[].given | name[].given | name[].given | name[].given | A / A / A | none | yes (66 rows) |
| edge: `name.period.start` | name | name[].period.start | name[].period.start, name[].period._start | name[].period.start | name[].period.start | B / A / A | L1-range=1 | yes (66 rows) |
| edge: `name.where(period.start > @1990).family` | name | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | A / A / A | none | yes (66 rows) |
| edge: `name.where(use = 'nickname').given` | name | name[].use, name[].given | name[].use, name[].given | name[].use, name[].given | name[].use, name[].given | A / A / A | none | yes (66 rows) |
| edge: `name.exists()` | name | name[].use | name | name[].use | name[].use | C / A / A | E1-isnull=2 E1-size=2 | yes (66 rows) |
| edge: `name.count()` | name | name[].use | name | name[].use | name[].use | C / A / A | E1-isnull=1 E1-size=1 | yes (66 rows) |
| edge: `address.period.start` | address | address[].period.start | address[].period | address[].period.start | address[].period.start | B / A / A | L1-range=1 | yes (66 rows) |
| edge: `address.city \| name.family` | name, address | name[].family, address[].city | name[].family, address[].city | name[].family, address[].city | name[].family, address[].city | A / A / A | none | yes (66 rows) |
| edge: forEach name / `family` | name | name[].family | name[].family | name[].family | name[].family | A / A / A | none | yes (82 rows) |
| edge: forEach name / `period.start` | name | name[].period.start | name[].period.start, name[].period._start | name[].period.start | name[].period.start | B / A / A | L1-range=1 | yes (82 rows) |
| edge: `name.first().family` | name | name[].family | name[].family | name[].family | name[].family | A / A / A | none | yes (66 rows) |
| edge: `name.where(family.exists()).given` | name | name[].family, name[].given | name[].family, name[].given | name[].family, name[].given | name[].family, name[].given | A / A / A | none | yes (66 rows) |
| edge: multi-column view with where and unionAll | id, name, birthDate, address | id, name[].use, name[].family, birthDate, address[].city | id, name, birthDate, address[].city | id, name[].use, name[].family, birthDate, address[].city | id, name[].use, name[].family, birthDate, address[].city | B / A / A | L1-range=2 E2-isnull=2 E2-exists=2 | yes (120 rows) |
| edge: nested forEach name / forEach given | name | name[].family, name[].given | name[].family, name[].given | name[].family, name[].given | name[].family, name[].given | A / A / A | none | yes (120 rows) |
| edge: forEachOrNull name / family | name | name[].family | name[].family | name[].family | name[].family | A / A / A | none | yes (106 rows) |
| edge: forEach name / family, where on annotation-free leaf (use) | name | name[].use, name[].family | name | name[].use, name[].family | name[].use, name[].family | C / A / A | E2-isnull=2 E2-exists=2 | yes (80 rows) |
| edge: forEach name / family, where with exists(criteria) | name | name[].use, name[].family | name | name[].use, name[].family | name[].use, name[].family | C / A / A | E2-isnull=2 E2-exists=2 | yes (80 rows) |
| edge: forEach name / family, where first().family = 'F1' (annotation-free leaf) | name | name[].family | name[].family | name[].family | name[].family | A / A / A | none | yes (4 rows) |
| edge: unionAll of two forEach (name family, address city) | name, address | name[].family, address[].city | name[].family, address[].city | name[].family, address[].city | name[].family, address[].city | A / A / A | none | yes (122 rows) |
| edge: forEach name / family, where on period.start (annotated leaf) | name | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | A / A / A | none | yes (24 rows) |
| edge: forEach name / period.start compared in column | name | name[].period.start, name[].period._start | name[].period.start, name[].period._start | name[].period.start, name[].period._start | name[].period.start, name[].period._start | A / A / A | none | yes (82 rows) |
| edge: extra: `name.where(use = 'nickname').exists()` | name | name[].use | name | name[].use | name[].use | C / A / A | E2-isnull=2 E2-exists=2 | yes (66 rows) |
| edge: extra: `name.exists(period.start > @1990)` | name | name[].period.start, name[].period._start | name | name[].period.start, name[].period._start | name[].period.start, name[].period._start | C / A / A | E2-isnull=2 E2-exists=2 | yes (66 rows) |
| edge: extra: `name.all(use = 'nickname')` | name | name[].use | name | name[].use | name[].use | C / A / A | E3-all=1 | yes (66 rows) |
| edge: extra: `name.empty()` | name | name[].use | name | name[].use | name[].use | C / A / A | E1-isnull=1 E1-size=1 | yes (66 rows) |
| edge: extra: `name.where(use = 'nickname').count()` | name | name[].use | name | name[].use | name[].use | C / A / A | E2-isnull=1 E2-size=1 | yes (66 rows) |
| edge: extra: `name.where(use = 'nickname').empty()` | name | name[].use | name | name[].use | name[].use | C / A / A | E2-isnull=1 E2-exists=1 | yes (66 rows) |
| edge: extra: `name.given.count()` | name | name[].given | name[].given | name[].given | name[].given | A / A / A | none | yes (66 rows) |
| edge: extra: `address.exists(city = 'C1')` | address | address[].city | address | address[].city | address[].city | C / A / A | E2-isnull=2 E2-exists=2 | yes (66 rows) |
| edge: extra: `name.all(period.start > @1990)` | name | name[].period.start, name[].period._start | name | name[].period.start, name[].period._start | name[].period.start, name[].period._start | C / A / A | E3-all=1 | yes (66 rows) |
| edge: extra: forEach name / family, where all(given.exists()) | name | name[].family, name[].given | name | name[].family, name[].given | name[].family, name[].given | C / A / A | E3-all=1 | yes (80 rows) |

### Spark 4.0.2

The views of the previous spike (26 views):

| Plan | A | B | C |
| --- | --- | --- | --- |
| P (compute-first only) | 4 | 0 | 22 |
| R (rewrites only) | 4 | 0 | 22 |
| P+R | 4 | 0 | 22 |
| P+R+D3b | 26 | 0 | 0 |

- P+R+D3b reads no more than either P+R or D3b alone: 26 of 26.
- Results equal to O in every arm: 26 of 26.
- P+R not A: 22 views.

| View | O | D3b | P | P+R | P+R+D3b | P / P+R / P+R+D3b | Rewrites applied | Results equal |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `name.family` | name | name[].family | name | name | name[].family | C / C / A | none | yes (22 rows) |
| `name.given` | name | name[].given | name | name | name[].given | C / C / A | none | yes (22 rows) |
| `name.period.start` | name | name[].period.start | name | name | name[].period.start | C / C / A | L1-range=1 | yes (22 rows) |
| `name.where(period.start > @1990).family` | name | name[].family, name[].period.start, name[].period._start | name | name | name[].family, name[].period.start, name[].period._start | C / C / A | none | yes (22 rows) |
| `name.where(use = 'nickname').given` | name | name[].use, name[].given | name | name | name[].use, name[].given | C / C / A | none | yes (22 rows) |
| `name.exists()` | name | name[].use | name | name | name[].use | C / C / A | E1-isnull=2 E1-size=2 | yes (22 rows) |
| `name.count()` | name | name[].use | name | name | name[].use | C / C / A | E1-isnull=1 E1-size=1 | yes (22 rows) |
| `address.period.start` | address | address[].period.start | address | address | address[].period.start | C / C / A | L1-range=1 | yes (22 rows) |
| `address.city \| name.family` | name, address | name[].family, address[].city | name, address | name, address | name[].family, address[].city | C / C / A | none | yes (22 rows) |
| forEach name / `family` | name | name[].family | name | name | name[].family | C / C / A | none | yes (41 rows) |
| forEach name / `period.start` | name | name[].period.start | name | name | name[].period.start | C / C / A | L1-range=1 | yes (41 rows) |
| `name.first().family` | name | name[].family | name | name | name[].family | C / C / A | none | yes (22 rows) |
| `name.where(family.exists()).given` | name | name[].family, name[].given | name | name | name[].family, name[].given | C / C / A | none | yes (22 rows) |
| multi-column view with where and unionAll | id, name, birthDate, address | id, name[].use, name[].family, birthDate, address[].city | id, name, birthDate, address | id, name, birthDate, address | id, name[].use, name[].family, birthDate, address[].city | C / C / A | L1-range=2 E2-isnull=2 E2-exists=2 | yes (60 rows) |
| nested forEach name / forEach given | name | name[].family, name[].given | name | name | name[].family, name[].given | C / C / A | none | yes (60 rows) |
| forEachOrNull name / family | name | name[].family | name | name | name[].family | C / C / A | none | yes (42 rows) |
| forEach name / family, where on annotation-free leaf (use) | name | name[].use, name[].family | name | name | name[].use, name[].family | C / C / A | E2-isnull=2 E2-exists=2 | yes (40 rows) |
| forEach name / family, where with exists(criteria) | name | name[].use, name[].family | name | name | name[].use, name[].family | C / C / A | E2-isnull=2 E2-exists=2 | yes (40 rows) |
| forEach name / family, where first().family = 'F1' (annotation-free leaf) | name | name[].family | name | name | name[].family | C / C / A | none | yes (2 rows) |
| unionAll of two forEach (name family, address city) | name, address | name[].family, address[].city | name, address | name, address | name[].family, address[].city | C / C / A | none | yes (61 rows) |
| forEach name / family, where on period.start (annotated leaf) | name | name[].family, name[].period.start, name[].period._start | name | name | name[].family, name[].period.start, name[].period._start | C / C / A | none | yes (12 rows) |
| forEach name / period.start compared in column | name | name[].period.start, name[].period._start | name | name | name[].period.start, name[].period._start | C / C / A | none | yes (41 rows) |
| singular-meta small: `birthDate` | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate | birthDate | A / A / A | L1-range=1 | yes (2000 rows) |
| singular-meta small: `birthDate.where($this > @1990)` | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | A / A / A | none | yes (2000 rows) |
| singular-meta small: `birthDate \| deceasedDateTime` | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | A / A / A | none | yes (2000 rows) |
| singular-meta small: `(birthDate \| deceasedDateTime).where($this > @1990).exists()` | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | A / A / A | E2-isnull=2 E2-exists=2 | yes (2000 rows) |

Extra views, for the existence rewrites (10 views):

| Plan | A | B | C |
| --- | --- | --- | --- |
| P (compute-first only) | 0 | 0 | 10 |
| R (rewrites only) | 0 | 0 | 10 |
| P+R | 0 | 0 | 10 |
| P+R+D3b | 4 | 0 | 6 |

- P+R+D3b reads no more than either P+R or D3b alone: 4 of 10. Not: extra: `name.where(use = 'nickname').exists()`, extra: `name.exists(period.start > @1990)`, extra: `name.all(use = 'nickname')`, extra: `name.where(use = 'nickname').empty()`, extra: `address.exists(city = 'C1')`, extra: `name.all(period.start > @1990)`.
- Results equal to O in every arm: 10 of 10.
- P+R not A: 10 views.

| View | O | D3b | P | P+R | P+R+D3b | P / P+R / P+R+D3b | Rewrites applied | Results equal |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| extra: `name.where(use = 'nickname').exists()` | name | name[].use | name | name | name | C / C / C | E2-isnull=2 E2-exists=2 | yes (22 rows) |
| extra: `name.exists(period.start > @1990)` | name | name[].period.start, name[].period._start | name | name | name | C / C / C | E2-isnull=2 E2-exists=2 | yes (22 rows) |
| extra: `name.all(use = 'nickname')` | name | name[].use | name | name | name | C / C / C | E3-all=1 | yes (22 rows) |
| extra: `name.empty()` | name | name[].use | name | name | name[].use | C / C / A | E1-isnull=1 E1-size=1 | yes (22 rows) |
| extra: `name.where(use = 'nickname').count()` | name | name[].use | name | name | name[].use | C / C / A | E2-isnull=1 E2-size=1 | yes (22 rows) |
| extra: `name.where(use = 'nickname').empty()` | name | name[].use | name | name | name | C / C / C | E2-isnull=1 E2-exists=1 | yes (22 rows) |
| extra: `name.given.count()` | name | name[].given | name | name | name[].given | C / C / A | none | yes (22 rows) |
| extra: `address.exists(city = 'C1')` | address | address[].city | address | address | address | C / C / C | E2-isnull=2 E2-exists=2 | yes (22 rows) |
| extra: `name.all(period.start > @1990)` | name | name[].period.start, name[].period._start | name | name | name | C / C / C | E3-all=1 | yes (22 rows) |
| extra: forEach name / family, where all(given.exists()) | name | name[].family, name[].given | name | name | name[].family, name[].given | C / C / A | E3-all=1 | yes (40 rows) |

Edge data set (null elements in name and address), all views (32 views):

| Plan | A | B | C |
| --- | --- | --- | --- |
| P (compute-first only) | 0 | 0 | 32 |
| R (rewrites only) | 0 | 0 | 32 |
| P+R | 0 | 0 | 32 |
| P+R+D3b | 26 | 0 | 6 |

- P+R+D3b reads no more than either P+R or D3b alone: 26 of 32. Not: edge: extra: `name.where(use = 'nickname').exists()`, edge: extra: `name.exists(period.start > @1990)`, edge: extra: `name.all(use = 'nickname')`, edge: extra: `name.where(use = 'nickname').empty()`, edge: extra: `address.exists(city = 'C1')`, edge: extra: `name.all(period.start > @1990)`.
- Results equal to O in every arm: 32 of 32.
- P+R not A: 32 views.

| View | O | D3b | P | P+R | P+R+D3b | P / P+R / P+R+D3b | Rewrites applied | Results equal |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| edge: `name.family` | name | name[].family | name | name | name[].family | C / C / A | none | yes (66 rows) |
| edge: `name.given` | name | name[].given | name | name | name[].given | C / C / A | none | yes (66 rows) |
| edge: `name.period.start` | name | name[].period.start | name | name | name[].period.start | C / C / A | L1-range=1 | yes (66 rows) |
| edge: `name.where(period.start > @1990).family` | name | name[].family, name[].period.start, name[].period._start | name | name | name[].family, name[].period.start, name[].period._start | C / C / A | none | yes (66 rows) |
| edge: `name.where(use = 'nickname').given` | name | name[].use, name[].given | name | name | name[].use, name[].given | C / C / A | none | yes (66 rows) |
| edge: `name.exists()` | name | name[].use | name | name | name[].use | C / C / A | E1-isnull=2 E1-size=2 | yes (66 rows) |
| edge: `name.count()` | name | name[].use | name | name | name[].use | C / C / A | E1-isnull=1 E1-size=1 | yes (66 rows) |
| edge: `address.period.start` | address | address[].period.start | address | address | address[].period.start | C / C / A | L1-range=1 | yes (66 rows) |
| edge: `address.city \| name.family` | name, address | name[].family, address[].city | name, address | name, address | name[].family, address[].city | C / C / A | none | yes (66 rows) |
| edge: forEach name / `family` | name | name[].family | name | name | name[].family | C / C / A | none | yes (82 rows) |
| edge: forEach name / `period.start` | name | name[].period.start | name | name | name[].period.start | C / C / A | L1-range=1 | yes (82 rows) |
| edge: `name.first().family` | name | name[].family | name | name | name[].family | C / C / A | none | yes (66 rows) |
| edge: `name.where(family.exists()).given` | name | name[].family, name[].given | name | name | name[].family, name[].given | C / C / A | none | yes (66 rows) |
| edge: multi-column view with where and unionAll | id, name, birthDate, address | id, name[].use, name[].family, birthDate, address[].city | id, name, birthDate, address | id, name, birthDate, address | id, name[].use, name[].family, birthDate, address[].city | C / C / A | L1-range=2 E2-isnull=2 E2-exists=2 | yes (120 rows) |
| edge: nested forEach name / forEach given | name | name[].family, name[].given | name | name | name[].family, name[].given | C / C / A | none | yes (120 rows) |
| edge: forEachOrNull name / family | name | name[].family | name | name | name[].family | C / C / A | none | yes (106 rows) |
| edge: forEach name / family, where on annotation-free leaf (use) | name | name[].use, name[].family | name | name | name[].use, name[].family | C / C / A | E2-isnull=2 E2-exists=2 | yes (80 rows) |
| edge: forEach name / family, where with exists(criteria) | name | name[].use, name[].family | name | name | name[].use, name[].family | C / C / A | E2-isnull=2 E2-exists=2 | yes (80 rows) |
| edge: forEach name / family, where first().family = 'F1' (annotation-free leaf) | name | name[].family | name | name | name[].family | C / C / A | none | yes (4 rows) |
| edge: unionAll of two forEach (name family, address city) | name, address | name[].family, address[].city | name, address | name, address | name[].family, address[].city | C / C / A | none | yes (122 rows) |
| edge: forEach name / family, where on period.start (annotated leaf) | name | name[].family, name[].period.start, name[].period._start | name | name | name[].family, name[].period.start, name[].period._start | C / C / A | none | yes (24 rows) |
| edge: forEach name / period.start compared in column | name | name[].period.start, name[].period._start | name | name | name[].period.start, name[].period._start | C / C / A | none | yes (82 rows) |
| edge: extra: `name.where(use = 'nickname').exists()` | name | name[].use | name | name | name | C / C / C | E2-isnull=2 E2-exists=2 | yes (66 rows) |
| edge: extra: `name.exists(period.start > @1990)` | name | name[].period.start, name[].period._start | name | name | name | C / C / C | E2-isnull=2 E2-exists=2 | yes (66 rows) |
| edge: extra: `name.all(use = 'nickname')` | name | name[].use | name | name | name | C / C / C | E3-all=1 | yes (66 rows) |
| edge: extra: `name.empty()` | name | name[].use | name | name | name[].use | C / C / A | E1-isnull=1 E1-size=1 | yes (66 rows) |
| edge: extra: `name.where(use = 'nickname').count()` | name | name[].use | name | name | name[].use | C / C / A | E2-isnull=1 E2-size=1 | yes (66 rows) |
| edge: extra: `name.where(use = 'nickname').empty()` | name | name[].use | name | name | name | C / C / C | E2-isnull=1 E2-exists=1 | yes (66 rows) |
| edge: extra: `name.given.count()` | name | name[].given | name | name | name[].given | C / C / A | none | yes (66 rows) |
| edge: extra: `address.exists(city = 'C1')` | address | address[].city | address | address | address | C / C / C | E2-isnull=2 E2-exists=2 | yes (66 rows) |
| edge: extra: `name.all(period.start > @1990)` | name | name[].period.start, name[].period._start | name | name | name | C / C / C | E3-all=1 | yes (66 rows) |
| edge: extra: forEach name / family, where all(given.exists()) | name | name[].family, name[].given | name | name | name[].family, name[].given | C / C / A | E3-all=1 | yes (80 rows) |


## Composition with the rule

- **4.3.0:** P+R+D3b reads a subset of both P+R and D3b in all 68 view runs, and results are equal. 
- **4.0.2:** the 26 views compose (P+R+D3b is A on all 26, and equals D3b). The new forms do not:
  6 of the 10 extra views (`where(c).exists()`, `exists(c)`, `all(c)` twice, `where(c).empty()`,
  `address.exists(c)`), and the same 6 in the edge set, read the whole column with the rule
  installed, where the rule alone reads the criteria leaves. For `name.all(use = 'x')` the verbose
  rule log shows why: `LambdaPruning bailed out: IllegalStateException: output schema changed`, the
  `containsNull` of the boolean array `r` is `false` in the pruned result and `true` in the plan
  it was given. The rule falls back to the unpruned plan, so results stay correct. The other five
  views were not debugged individually; they have the same pattern (`array_max` or `array_min`
  in a `Generate` input). The rule's guard would need to compare schemas ignoring nullability, or
  the rewrite to produce the nullability the optimiser expects. Neither was tried.

## Where the engine change would go

Three sites, each emulated here by one rewrite:

- `Projection.execute` (`inline`): as in the previous spike, select the result column under an alias,
  then `inline` it.
- `ExistenceFunctions.exists`, `count`, `empty`, `all` and `ColumnRepresentation.count()`/`isEmpty()`:
  build E1, E2 or E3 where the input is an array of structs. E2 and E3 want the criteria as a
  lambda over the filter's input, which `where` already holds.
- The traversal that builds `struct(value, range)` for a dateTime (`resolve_fields_or_else(...)` in
  the plan): build the struct from the value alone unless a downstream operation asks for `range`
  (a comparison does).

## What is not covered

- **The engine was not changed.** The probe rewrites the analysed plan by pattern. A real change
  builds the same shapes for these views, but patterns that the engine builds in other ways (a
  `count()` on a union, an `exists` over a `repeat` result, `exists` in a `where` of a joined
  view) were not exercised, and a pattern the rewrite misses is silently left as it was.
- **L1 is conservative and a null of the same type.** It is skipped for the whole plan if any
  expression reads `range`, which a real per-expression decision would not do; and it builds a typed
  null where the real change would omit the field.
- **No element that is non-null only through a read leaf was constructed.** The edge set has null
  elements and an element present only through its extension (`e1`), which read correctly with the
  one-leaf reads (`use`, or the criteria). A struct whose every *read* leaf is null while the struct
  is present is the case the pruner's null handling depends on; `e1`'s name is that case for
  `use`, `family`, `given` and `period`, and results are equal.
- **No Spark-version difference in results**, but read plans differ as shown: nothing prunes on 4.0.2
  without the rule.
- **Two cores, tiny data:** read schemas are the claim, not timings or bytes. Bytes read and timings
  were not measured.
- **Only the view path.** Other users of `exists`, `count` and `all`, such as FHIRPath evaluated
  outside a view, were not run.
- **The rule's own fix** for the nullability guard (above) was not attempted, and the `D3b` copy
  here is unchanged.
- `last()` is not supported by the engine, so a `where` on a leaf uses `first()`, as before.

## How to reproduce

The scripts are in `scripts/annotations-design/rewrites-layer1/`, copied from
`project-before-generate/` (which is unchanged). New: `LayerRewrites.scala` (the rewrites),
`SemanticProbe.java`, `DebugRule.java` and `debug.sh`, `all.sh`, `plans.sh`, `semantic.sh`,
`make_tables.py`. Changed: `ProbeMain.java` (five arms, extra views, edge data set, leaf-set
classes), `run.sh`, `env.sh` (data directory), `PlanShape.java`.

```bash
cd specs/001-parquet-on-fhir/evidence/scripts/annotations-design/rewrites-layer1
bash all.sh 4.3     # build.sh, run.sh and semantic.sh for Spark 4.3.0; about 6 minutes
bash all.sh 4.0     # the same for 4.0.2
python3 make_tables.py <probe-4.3.log> <probe-4.0.log>                       # the tables
python3 make_tables.py <probe-4.3.log> <probe-4.0.log> <semantic-4.3.log>    # the semantic table
bash debug.sh 4.0 "name.all(use = 'x')"   # the rule's log for one view as P+R+D3b
bash plans.sh 4.3 "name.exists()"         # the analysed plan of one view
```

Logs and data are in `.local/work/issue/2832_41-42_decimal-and-annotations/spikes/rewrites-layer1/`
(`logs/probe-4.3.log`, `logs/probe-4.0.log`, `logs/semantic-4.3.log`, `logs/tables.md`,
`logs/semantic.md`). The probe prints a `DIFF` line with the first differing rows for any arm
that is not equal to O; there are none. Classpaths as `env.sh` of the previous spike (the third-party
classpath file is in a scratchpad of an earlier session, so `env.sh` needs it regenerated if that is
gone).

## Versions

Spark 4.3.0 (Scala 2.13.18 at runtime) and 4.0.2, Java 21.0.3, Pathling 10.0.0-SNAPSHOT jars built
against 4.0.2.
