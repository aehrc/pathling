# Can a Catalyst rule make nested schema pruning cross lambdas and `Generate`?

A feasibility spike for option C and question 5 of `design/annotations-in-the-engine.md`
(§2, the constraint on unused metadata). The limitation is
[finding 13](spark-type-findings.md#13-nested-schema-pruning-does-not-reach-the-engines-array-access),
[annotation-pruning.md](annotation-pruning.md) and
[annotations-design-singular-meta-pruning.md](annotations-design-singular-meta-pruning.md):
Spark's `SchemaPruning` follows only field extractions applied directly to a column. A
higher-order function, `flatten` or a view's `Generate` therefore makes Parquet read the whole
column.

**Result: yes, it is feasible. The prototype prunes every case it was built for, including views
through the real engine, and never changed a result.** A single optimizer rule of about 320 lines
of Scala propagates demand backwards through `filter`, `transform`, `exists`, `forall`,
`aggregate`, `zip_with`, `arrays_zip`, `flatten`, `element_at`, `array`, `named_struct`, `concat`,
`CASE`/`coalesce` and `Generate(inline)`. It then rewrites the Parquet
relation's attributes and `dataSchema` to the pruned struct types, and re-types the plan above,
including lambda variables.

- Parquet reads only the leaves the expression uses in these cases:
  - all 24 SQL cases (cases 1 to 4) except the three controls, 1e, 1f and 1j, which read whole by
    design;
  - all 8 A-raw models of the singular-meta spike, union with the dedupe `aggregate` included;
  - all 18 real engine views (case 5), including a multi-column view with `where`, `unionAll` and
    `forEachOrNull`.
- In every case, the results are identical to those with the rule off.
- For the design, the element carrying the raw `_x` as `meta`, extensions included, reads what
  today's engine reads: `birthDate` plus the annotations used, not `_birthDate.extension`. This
  includes a repeating primitive below a repeating element paired with the whole `_x` by
  `zip_with` or `arrays_zip` (cases 3c to 3f), the shape that is hardest in
  [annotation-pruning.md](annotation-pruning.md).
- **That conclusion rests on hand-built models, not the engine.** The engine does not yet build a
  raw-`meta` element or store primitive extensions in `_x`. The A-raw models and cases 3c to 4c
  build the shapes in SQL, mirroring the engine's plans. Through the real engine, what is shown is
  that the rule prunes the engine's actual plans for complex elements with extensions, and leaves
  its results unchanged.
- For the engine as a whole, it removes finding 13. `name.family` through a view now reads
  `name[].family`, not all of `name`.

The rule can be installed at runtime on an existing session through
`spark.experimental.extraOptimizations`, which is what `PathlingContext.create(spark)` would need.
It also works through `injectOptimizerRule` and `injectPreCBORule`, and on Delta tables, including
Delta tables with column mapping.

The cost is a dependency on Catalyst internals. Spark itself does not do this, so nothing upstream
keeps the rule correct as Spark changes. It also has failure modes that need a production-quality
whitelist and test matrix (see "Correctness risks").

## Conditions

- Commit `0c94065ec3`, branch `issue/2832_41-42_decimal-and-annotations`, with the branch's built
  Pathling jars. Spark 4.0.2, Scala 2.13.16, Java 21, `local[2]`, Delta 4.0.0.
- Adaptive execution was off except in one re-run of cases 1 to 4 with it on, which gave the same
  reads and results.
- What is read is the `requiredSchema` of each `FileSourceScanExec` in the executed plan, shown
  compactly. `x (whole)` means the whole subtree at `x` is read. Results were compared as sorted
  `Row.toString` lists, together with the output schema, between the same query with the rule off
  and on.
- No bytes or timings were measured. The read schemas decide the bytes, as the singular-meta spike
  measured: on its 1M-row file, whole `_birthDate` is 182 MiB and `birthDate` with the range is
  17.6 MiB.

### Data

- Cases 1 to 4 run on 203 rows written from JSON with an explicit schema (`CaseProbe.SCHEMA`):
  - every `_x` group is `struct<id, extension: array<struct<url, valueString>>, __start, __end>`
    with five string extensions;
  - `name[]` carries `family`, `_family`, `given`, `_given[]` and `period` (with `_start` and
    `_end`);
  - `location[].period` has the same shape;
  - `dosage[].timing` carries `event[]` and `_event[]`.
- Three edge rows test the null shapes the rule must keep. They contain:
  - a null element in an array;
  - an array element that is non-null only through its `extension`, so the leaf the rule keeps is
    null;
  - a null `_birthDate`;
  - a non-null `_birthDate` whose `id` and `__start` are null;
  - a null `period` and a null `timing`;
  - a row with nothing but `id`.
- The A-raw models run on the singular-meta spike's 2,000-row `small/Patient.parquet`, with
  extensions in `_birthDate` and `_deceasedDateTime`.
- The engine cases run on 22 Patients from NDJSON, encoded and stored by the library API. Names,
  periods and addresses carry five extensions each. Two edge Patients have an extension-only name
  and no name. The engine does not yet store primitive extensions in `_x` (the singular-meta spike
  notes the same), so here the extensions sit on complex elements (`name[].extension`,
  `address[].extension`).

## How the rule works

`scripts/annotations-design/lambda-pruning-rule/LambdaPruning.scala` contains the rule.

### 1. Demand

The analysis walks the plan top-down, keeping a requirement per `ExprId`. The `ExprId`s cover
attributes and lambda variables. A requirement is one of:

- `*`, for the whole value;
- `{f: req, …}`, for the named struct fields;
- `{}`, for presence only: whether the value is null, and an array's length.

Arrays are transparent: a requirement on an array is a requirement on each of its elements.

The plan's output is seeded `*`. Operators are handled as follows:

- **`Project`.** An alias passes its requirement into its expression. A bare attribute is a
  pass-through: its requirement arrives from above under the same `ExprId`, so it adds nothing.
- **`Filter`.** The condition is `*`.
- **`Generate(Inline | Explode | PosExplode)`.** Each output attribute's requirement goes to the
  element field at the same position.
- **Operators that only pass attributes through** (`Join`, `Sort`, `Limit`). Their own expressions
  are `*`.
- **Anything else** (`Aggregate`, `Union`, `Window`, `Expand`, ...). Everything they read is `*`.

Each expression form passes demand to its inputs as follows:

| Expression | Demand passed to inputs |
| --- | --- |
| `GetStructField`, `GetArrayStructFields` | Add one field level. |
| `named_struct` | Routes each field's requirement to that field's value. Fields nobody reads get nothing. |
| `array`, `concat` (arrays), `flatten`, `element_at`/`[]`, the value branches of `CASE`/`if`/`coalesce`, `KnownNotNull` | Pass the result's requirement through. |
| `isnull`, `isnotnull`, `size` | Presence. |
| `filter(A, x -> p)` | `A` owes the result's requirement **plus** what `p` reads of `x`. |
| `transform(A, x -> b)` | `b` is visited with the result's requirement; `A` owes what that reads of `x`. |
| `exists`, `forall` | `A` owes what the predicate reads of `x`. |
| `zip_with(A, B, (a, b) -> m)` | `A` and `B` owe what `m` reads of `a` and `b`, plus presence, since the result is as long as the longer input. |
| `arrays_zip(A, B)` | Each input owes the requirement on its field of the result, plus presence. |
| `aggregate(A, z, (acc, y) -> m, acc -> f)` | The accumulator owes what `f` reads, plus what `m` reads of `acc`, iterated to a fixed point because `m`'s result becomes `acc`. `z` gets the same requirement, and `A` owes what `m` reads of `y`. |
| Anything else, including `=`, `to_json`, UDFs, casts and `array_distinct` | `*` on every input. |

### 2. Pruning

Each attribute of a Parquet `LogicalRelation(HadoopFsRelation)` gets its type pruned to its
requirement. A struct that is needed only for presence keeps one leaf: the first scalar field, or
the first field pruned recursively when none is scalar. It never becomes an empty struct.

The relation is rebuilt as `SchemaPruning` builds one:
`hadoopFsRelation.copy(dataSchema = pruned)(session)` and `output = attrs.withDataType(pruned)`,
with the same `ExprId`s. `FileSourceStrategy` takes the read schema from those attribute types, so
the rule needs nothing else from the physical layer. It does not rely on `SchemaPruning` running
after it.

### 3. Re-typing

The plan is rewritten bottom-up, recording each operator's new output types by `ExprId`. Within an
expression, a recursive pass handles each node as follows:

- **Attributes** get their new types.
- **`GetStructField` and `GetArrayStructFields`** have their ordinal (and `field`/`numFields`)
  re-resolved **by name** against the new child type.
- **A higher-order function** has its arguments re-typed first. The rule then calls
  `HigherOrderFunction.bind`, the analyser's own hook, which supplies each lambda's new argument
  types. It makes new `NamedLambdaVariable`s with the same `ExprId` and re-types the lambda body
  under them.
- **Every other expression** is rebuilt with `mapChildren`, so a type derived from its children
  follows automatically.

### 4. Two cleanups, both needed for the `Generate` case

- **A `named_struct` field that nothing reads becomes a typed `null`.** Otherwise it still
  references its input. For example, `meta`, which is `_birthDate`, in
  `named_struct('value', birthDate, 'meta', _birthDate)` when only `.value` is used.
- **A pass-through attribute in a `Project` that nothing above reads is dropped from the
  `Project`.** This removes the `Project [birthDate, _birthDate]` that `ColumnPruning` leaves under
  a `Generate`, which the singular-meta follow-up identified as the mechanism. The `Generate`'s
  `unrequiredChildIndex` is recomputed against the new child.

### 5. Guards

The rule bails out and returns the plan unchanged, logging the reason, in these cases:

- the result is unresolved;
- its output schema differs from the original;
- any expression fails `checkInputDataTypes`;
- anything throws.

It is idempotent: on a plan already pruned it changes nothing. This is required in a fixed-point
batch. A session conf (`spike.lambdaPruning.enabled`) turns it off for comparisons.

## Injection

`SparkOptimizer` in 4.0.2 orders the relevant batches as follows:

```
… operator optimisation (fixed point)  ← injectOptimizerRule
  "Pre CBO Rules" (once)               ← injectPreCBORule
  "Early Filter and Projection Push-Down" (once): SchemaPruning, V2ScanRelationPushDown, …
  … PartitionPruning, Extract Python UDFs, …
  "User Provided Optimizers" (fixed point) ← spark.experimental.extraOptimizations
```

`extraOptimizations` runs **after** `SchemaPruning`. That does not matter here, because the rule
rewrites the relation itself rather than preparing the plan for `SchemaPruning`.

Measured with `InjectionProbe` (cases 1b, 2c, 3b and 4b, in one JVM per mode):

| How installed | Prunes | Results equal |
| --- | --- | --- |
| `spark.experimental.extraOptimizations` on an existing plain session | yes | yes |
| `withExtensions(_.injectOptimizerRule(…))` on a new session | yes | yes |
| `withExtensions(_.injectPreCBORule(…))` on a new session | yes | yes |
| `builder().withExtensions(…).getOrCreate()` when a session already exists | **no**: the existing session is returned without the extension | yes |
| `extraOptimizations` over a Delta table | yes | yes |
| `extraOptimizations` over a Delta table with `delta.columnMapping.mode = name` (physical names differ) | yes | yes |

**Runtime injection is possible, through `extraOptimizations` only.** It is a public (if
`@Experimental`) field of every `SparkSession` and takes effect for queries planned afterwards.
`PathlingContext.create(spark)` could install the rule there, and that would also cover sessions
created from Python and R.

Extensions are fixed when the session is built, and `spark.sql.extensions` is a static conf. A
server or a user who builds the session can choose `injectPreCBORule`. That is the cleanest slot:
it runs once, on the final logical shape, just before `SchemaPruning`.

## Results

### Cases 1 to 4: hand-built expressions (`CaseProbe`)

| Case | Reads without the rule | Reads with it | Results equal |
| --- | --- | --- | --- |
| 1a `filter(name, x -> x.family is not null).family` | all of `name` | `name[].family` | yes |
| 1b engine traversal `filter(filter(name, x -> x is not null).family, y -> y is not null)` | all of `name` | `name[].family` | yes |
| 1c `transform(name, x -> x.family)` | all of `name` | `name[].family` | yes |
| 1d 1b inside `inline(array(struct(…)))` | all of `name` | `name[].family` | yes |
| 1e control: `filter(name, x -> x is not null)` output whole | all of `name` | all of `name` | yes |
| 1f control: `transform(name, x -> to_json(x))` | all of `name` | all of `name` | yes |
| 1g `exists(filter(name, …), x -> x.period.start > c)` | `id`, all of `name` | `id`, `name[].period.start` | yes |
| 1h `size(filter(name, x -> x is not null))`, presence only | all of `name` | `name[].id` | yes |
| 1i `size(filter(location, …))`, no scalar field at the top | all of `location` | `location[].extension[].url` | yes |
| 1j control: `_birthDate` and `name[]._family`, pruned differently, meet in `concat` | all of both | all of both (guard bailed) | yes |
| 1k `filter(name, x -> x._family is not null).family` | all of `name` | `name[].family`, `name[]._family.id` | yes |
| 2a `location.period.where(start > c).start`, engine shape: `filter(filter(filter(location, …).period, p -> p is not null), p -> p.start > c).start` | all of `location` | `location[].period.start` | yes |
| 2b 2a comparing the annotation `p._start.__start` | all of `location` | `location[].period.start`, `location[].period._start.__start` | yes |
| 2c 2a inside `inline` | all of `location` | `location[].period.start` | yes |
| 3a `arrays_zip(flatten(filter(dosage, …).timing.event), flatten(filter(dosage, …).timing._event).__start)` | all of `dosage` | `dosage[].timing.event`, `dosage[].timing._event[].__start` | yes |
| 3b 3a inside `inline` | all of `dosage` | the same two leaves | yes |
| 3c raw `meta` for a repeating primitive below a repeating element: `inline(…transform(flatten(transform(filter(dosage, …), d -> zip_with(d.timing.event, d.timing._event, (v, m) -> named_struct('value', v, 'meta', m)))), e -> e.value))`, output only | all of `dosage` | `dosage[].timing.event`, `dosage[].timing._event[].id` | yes |
| 3d 3c filtering on `e.meta.__start > c` | all of `dosage` | `dosage[].timing.event`, `dosage[].timing._event[].__start` | yes |
| 3e 3c paired by `arrays_zip(d.timing.event, d.timing._event)`, output `.event` | all of `dosage` | `dosage[].timing.event`, `dosage[].timing._event[].id` | yes |
| 3f 3e filtering on `e._event.__start > c` | all of `dosage` | `dosage[].timing.event`, `dosage[].timing._event[].__start` | yes |
| 4a `s = named_struct('value', birthDate, 'meta', _birthDate)`; `inline(array(struct(transform(filter(array(s), v -> v is not null), e -> e.value))))` | `birthDate`, all of `_birthDate` | `birthDate` | yes |
| 4b 4a with `v.meta.__start > c` in the filter | `birthDate`, all of `_birthDate` | `birthDate`, `_birthDate.__start` | yes |
| 4c 4a filtering on `v.meta is not null` | `birthDate`, all of `_birthDate` | `birthDate`, `_birthDate.id` | yes |
| 4d control: `select s.value`, which already prunes | `birthDate` | `birthDate` | yes |

#### Null shapes

The edge rows show that presence survives pruning to a leaf that is itself null:

- Row `e1` has `name = [null, {extension only}]`. It gives `size(filter(name, …)) = 1`, read from
  `name[].id`, which is null in the surviving element.
- Its `location` has three elements: a null one, an extension-only one, and one with a null
  `extension` but a non-null `period`. The count is 2, read from `location[].extension[].url`.
- Row `e2`, whose `_birthDate` exists only through its extensions, gives `[null]` for 4c, read from
  `_birthDate.id`, which is null.

These match the unpruned reads with the vectorised reader (the default) and with
`spark.sql.parquet.enableVectorizedReader=false`, the parquet-mr row reader. Parquet's definition
levels record how deep each value is defined, so any one leaf answers whether its ancestors are
null. Treating `isnotnull` as "needs no fields" is therefore safe for Parquet.

### Case 4 extended: the eight A-raw models of the singular-meta spike (`ARawProbe`)

The A-raw models build the element `struct(value, meta = raw _x)`. They take a union as `concat`
plus the dedupe `aggregate`, `where` as `filter` over `CASE WHEN`, and comparisons as lambdas, all
in the view's `inline` wrapper. The data is `small/Patient.parquet`, 2,000 rows.

| FHIRPath modelled | Reads without the rule | Reads with it | Results equal |
| --- | --- | --- | --- |
| `birthDate` | `birthDate`, all of `_birthDate` | `birthDate` | yes |
| `birthDate \| deceasedDateTime` | both values, both whole `_x` | both values, `_x.{__start,__end}` of each | yes |
| `birthDate.combine(deceasedDateTime)` | both values, both whole `_x` | both values | yes |
| `(birthDate \| deceasedDateTime).first()` | both values, both whole `_x` | both values, `_x.{__start,__end}` of each | yes |
| `where(active)` union | `active`, both values, both whole `_x` | `active`, both values, `_x.{__start,__end}` of each | yes |
| `birthDate.where($this > @1990)` | `birthDate`, all of `_birthDate` | `birthDate`, `_birthDate.__start` | yes |
| `(…).where($this > @1990).exists()` | both values, both whole `_x` | both values, `_x.{__start,__end}` of each | yes |
| `where(birthDate > @1990).birthDate` | `birthDate`, all of `_birthDate` | `birthDate`, `_birthDate.__start` | yes |

Every model now reads no more than the engine and A-system read in that spike (`RANGE`). Four
read less: they need no `__end`, or no range at all. The union's dedupe reads only `__start` and `__end`. Before `aggregate` was supported,
the four union cases still read the whole `_x`.

### Case 5: the real engine (`EngineProbe`)

Views run through `data.view("Patient").json(…).execute()`, with the rule added to the existing
session's `extraOptimizations` after `PathlingContext.create(spark)`.

| View column | Reads without the rule | Reads with it | Results equal |
| --- | --- | --- | --- |
| `name.family` | all of `name` | `name[].family` | yes |
| `name.given` | all of `name` | `name[].given` | yes |
| `name.period.start` | all of `name` | `name[].period.start` | yes |
| `name.where(period.start > @1990).family` | all of `name` | `name[].family`, `name[].period.start`, `name[].period._start` | yes |
| `name.where(use = 'nickname').given` | all of `name` | `name[].use`, `name[].given` | yes |
| `name.exists()` | all of `name` | `name[].use` | yes |
| `name.count()` | all of `name` | `name[].use` | yes |
| `address.period.start` | all of `address` | `address[].period.start` | yes |
| `address.city \| name.family` | all of `name` and `address` | `name[].family`, `address[].city` | yes |
| `forEach: name`, `family` | all of `name` | `name[].family` | yes |
| `forEach: name`, `period.start` | all of `name` | `name[].period.start` | yes |
| `name.first().family` | all of `name` | `name[].family` | yes |
| `name.where(family.exists()).given` | all of `name` | `name[].family`, `name[].given` | yes |
| multi-column view: `id`, `birthDate`, `unionAll` of `forEachOrNull: name` / `family` and `forEach: address` / `city`, `where: name.where(use = 'nickname').exists()` | `id`, `birthDate`, all of `name` and `address` | `id`, `birthDate`, `name[].use`, `name[].family`, `address[].city` | yes |
| `birthDate`, singular-meta data | `birthDate`, `_birthDate.{__start,__end}` | the same | yes |
| `birthDate.where($this > @1990)`, same | `birthDate`, `_birthDate.{__start,__end}` | the same | yes |
| `birthDate \| deceasedDateTime`, same | both values and ranges | the same | yes |
| `(birthDate \| deceasedDateTime).where($this > @1990).exists()`, same | both values and ranges | the same | yes |

`name[].period._start` is "whole" only because the engine stores just `__start` and `__end` in it.
The engine's real plans needed no expression support beyond what the
SQL cases used. The multi-column view contains two Pathling-specific expressions,
`struct_prod` and `struct_prod_outer`. The rule treats them as unknown, so their inputs are read
whole. Those inputs are structs that the view builds from fields it has already extracted, not
stored columns, so the scan is still pruned.

One bug was found here and fixed: re-typing the value of a dropped struct field
failed when that value read a field that had itself been pruned away. Its type is now taken from
the original when it cannot be re-typed.

## What it does not handle

- **A type mismatch between values pruned differently that meet in one constructor.** For
  example, `concat(array(_birthDate), transform(name, x -> x._family))` when `_birthDate.id` is
  also read (case 1j). `concat`, `array`, `CASE` and `coalesce` require equal child types. The type
  check catches this and the rule gives up on the **whole plan**. It is safe, but the plan is not
  pruned. Production would insert a narrowing projection (`named_struct` of the common fields, or a
  `transform` for arrays) above the wider child, or prune both sides to the union of their
  requirements.
- **Expressions not in the table above.** They make their inputs whole: `=` and `<=>` on structs,
  `array_distinct`/`union`/`except`/`contains`, `sort_array`/`array_sort`, `map_*` and every map
  type, casts, `to_json`, UDFs, and any
  Pathling-specific expression. This is conservative and correct. It loses pruning only where such
  an expression sees a whole struct.
- **Operators other than `Project`, `Filter`, `Generate` and pass-through operators.**
  `Aggregate`, `Union`, `Window` and `Expand` make every attribute they read whole. None of the
  views measured produced them. Whether `unionAll` in a view becomes a `Union` operator was not
  measured.
- **Data sources other than V1 Parquet** (`HadoopFsRelation` with `ParquetFileFormat`, which
  includes Delta's subclass). DataSource V2 scans, ORC and caches (`InMemoryRelation`) are not
  touched.
- **Subqueries.** Plans inside subquery expressions are not traversed. An outer attribute that a
  subquery reads is a child of the subquery expression, so it gets `*`, which is safe. By the time
  `extraOptimizations` runs, correlated subqueries have already been rewritten to joins.
- **Choice of the presence leaf.** The rule keeps the first scalar field, which is often `id`.
  This is correct but not the cheapest; the cheapest would be the narrowest column. It does not
  matter in practice, because whenever anything else is read, presence costs nothing.

## Correctness risks

1. **Silent wrong field.** The rule rewrites types under expressions it does not know.
   - An expression that holds a struct ordinal or a data type of its own would read the wrong
     field after re-typing, with no type error. In Spark that is a `GetStructField` or
     `GetArrayStructFields` that the rule fails to reach, or a `Cast` or `Literal` of a struct
     type. In Pathling it would be a custom expression.
   - The demand analysis gives an unknown expression's inputs `*`, so its inputs keep their full
     types. This is what protects such expressions, and it depends on the analysis and the
     re-typing agreeing exactly.
   - Production should invert the default: re-type only through a whitelist of expression classes
     whose types derive from their children, and treat anything else as a boundary.
2. **Presence relies on Parquet definition levels.** It was measured with both readers on the edge
   rows above. ORC was not tested, and the rule is limited to Parquet.
3. **Lambda variable identity.** Re-typing and demand both key on the `ExprId` of a lambda
   variable. A plan in which the same variable `ExprId` is bound by two lambdas over arguments that
   need different shapes would merge their demands. That is safe, since it is a union, but the
   result would also be typed by whichever `bind` runs. None was seen: the analyser gives each
   lambda fresh `ExprId`s, even where the engine reuses a `Column`.
4. **Dropping `Project` entries and nulling `named_struct` fields depends on the demand being
   complete.** The analysis must visit every reference. An operator that references attributes
   without exposing them in `expressions` would break this. The plan-level guards (resolved,
   unchanged output schema, type check) would not catch that kind of omission. Only a test matrix
   would.
5. **Interaction with other rules.** With `injectOptimizerRule`, the later operator-optimisation
   rules see re-typed plans and still produced the same results. With `extraOptimizations`, only
   the rule's own batch follows. Adaptive execution re-optimises physical stages only, and one run
   with it on gave the same reads and results. Plan caching and `Dataset.cache` were not tested.
6. **Plan-wide bail-out.** One unsupported construct anywhere in a plan with several views loses
   pruning for all of them. This is safe, but the performance gain is erratic. A production rule
   should fall back per relation attribute, not per plan.

## Production size and maintenance

**Size.** The prototype is about 410 lines of Scala, of which about 320 are the rule. A production
version would add:

- the whitelist for re-typing;
- narrowing projections instead of the mismatch bail-out;
- fallback per attribute;
- configuration and logging;
- Java-facing installation in `PathlingContext`.

That is a rough estimate of **800 to 1,200 lines**, plus a test matrix of a similar size or
larger. The matrix covers every supported expression and operator, singular and repeating
cardinality, the null shapes above, and both Parquet readers and Delta. It runs every FHIRPath and
view conformance suite with the rule on and compares results with it off. Pathling already
compiles Scala in `encoders`, so the language is not a new dependency.

**What it depends on.** Compared against the Spark 3.5.6 and 4.0.2 sources:

| Internal API | 3.5.6 → 4.0.2 |
| --- | --- |
| `LogicalRelation` | **Changed**: gained a fifth field, `stream`, in 4.0. `copy(relation=, output=)` is source-compatible but not binary-compatible. |
| `HadoopFsRelation.copy(dataSchema=)(session)` | Unchanged; `SchemaPruning` uses the same call. |
| `HigherOrderFunction.bind`, `LambdaFunction`, `NamedLambdaVariable` | Unchanged (`bind` has been final over `bindInternal` since 3.x). |
| `GetStructField`, `GetArrayStructFields`, `CreateNamedStruct`, `Generate`, `Inline` | Unchanged signatures. |
| `ElementAt`, `ArrayExists`, `ArrayAggregate`, `ZipWith`, `ArraysZip`, `Size` | Unchanged signatures. The prototype matches some by accessors and some by full patterns; a full pattern would break if a field were added. |
| `SparkSessionExtensions.injectOptimizerRule`/`injectPreCBORule`, `ExperimentalMethods.extraOptimizations` | Present in both. |
| Batch order: operator optimisation, "Pre CBO Rules", "Early Filter and Projection Push-Down" (`SchemaPruning`), …, "User Provided Optimizers" | The same in both. |

Only these two versions were compared. How the APIs looked before 3.5 was not checked.

**Maintenance.**

- Catalyst carries no compatibility promise. The rule would need recompiling, and probably small
  fixes, for each Spark minor version, as the `LogicalRelation` change shows.
- New higher-order functions or expression forms would lose pruning until they are added. They
  would not lose correctness, provided the whitelist default is used.
- The largest unknown is **Databricks**, where Pathling also runs. Databricks Runtime ships its
  own fork of Catalyst and its own scan planning (Photon). Whether a rule compiled against Apache
  Spark is binary-compatible there, and whether its scans honour a re-typed `HadoopFsRelation`, was
  not tested.

## Reproduction

All the scripts are in `scripts/annotations-design/lambda-pruning-rule/`. `run.sh` compiles the
Scala with the 2.13.16 compiler from `~/.m2`. It writes its classes and data under
`.local/work/issue/2832_41-42_decimal-and-annotations/spikes/lambda-pruning-rule/`. The full output
of the last run is `results.txt` there.

```bash
run.sh cases            # cases 1–4 (adds -rowreader for parquet-mr, -aqe for adaptive, -v to trace)
run.sh inject           # injection modes, Delta and Delta with column mapping
run.sh araw             # the A-raw models (needs singular-meta-pruning/small)
run.sh engine           # case 5 through the library API (writes NDJSON with gen_patients.py)
run.sh all
```

| File | Role |
| --- | --- |
| `LambdaPruning.scala` | The rule. `LambdaPruning.install(spark)` and `uninstall(spark)` change `extraOptimizations`. |
| `CaseProbe.scala` | Writes the case data and runs cases 1 to 4. |
| `InjectionProbe.scala` | Runs one injection mode per JVM. |
| `ARawProbe.java` | Runs `SingularMetaProbe.model("A-raw", …)` with the rule off and on. |
| `EngineProbe.java` | Runs case 5. |
| `gen_patients.py` | Writes the NDJSON for case 5. |
| `build.sh`, `env.sh` | Classpath and compilation. |
