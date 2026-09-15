# Spark type findings — absent elements under a sparse schema

Measured on PySpark 4.0.1 (project targets Spark 4.0.2; these are stable
behaviours, not version-fragile ones). Script:
`scripts/nulltype_test.py`.

## Serialisation of `void` (NullType) and empty structs

| Case | Parquet | ORC | JSON |
|---|---|---|---|
| Top-level `void` column | ✗ `UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE` | ✗ same | ✓ written, field omitted |
| `void` nested inside a struct | ✗ whole struct rejected | — | — |
| `struct<>` (no fields) | ✗ `EMPTY_SCHEMA_NOT_SUPPORTED_FOR_DATASOURCE` | — | ✗ same error |
| `struct<id:string>`, all null | ✓ | — | ✓ |

An empty struct is also unparseable as a declared read schema.

## Type coercion

| Expression | Result |
|---|---|
| `coalesce(void, struct<id,family>)` | ✓ resolves to `struct<id,family>` |
| `coalesce(struct<id>, struct<id,family>)` | ✗ `DATATYPE_MISMATCH.DATA_DIFF_TYPES` |
| `array_union(array<void>, array<struct<…>>)` | ✓ |

`void` is Spark's bottom type and widens to anything. A minimal struct does
not, so it cannot stand in for an absent element anywhere the value may be
combined with a present one.

## JSON egress

Both `to_json` and `write.json` behave identically. Default
`ignoreNullFields=true`:

| Value | JSON |
|---|---|
| top-level `void` column | field **omitted** — `{"id":0,"present":"v"}` |
| `void` field inside a struct with other populated fields | field omitted — `{"s":{"code":"c"}}` |
| struct whose every field is null in that row (`struct<absent:void>` or all-null `struct<id:string>`) | **`{"s":{}}`** — empty object, not omitted |
| `array<void>` | **`{"a":[null]}`** — a one-element null array, not omitted |

With `ignoreNullFields=false` the nulls are emitted explicitly
(`{"absent":null}`), which is not valid FHIR JSON.

So `void` is exactly right for JSON egress — an absent element vanishes, which
is what FHIR requires. Two cases still need pruning at egress, and neither is
specific to `void`: a struct all of whose fields are null in a given row, and an
array of nulls. Reading the output back re-infers without the omitted field.

## Consequences

1. A `void` column cannot be written to Parquet or ORC. A ViewDefinition column
   over an element absent from the schema would therefore fail at write, not
   merely carry a surprising type. Typing the leaf is a correctness requirement.
2. `struct<>` is unusable in every direction, so `struct<id:string>` is the only
   viable minimal representation of a complex element — and `id` is a real FHIR
   element on every `Element`, so it is conformant rather than synthetic.
3. Because a minimal struct does not coerce, it can only be applied where a
   column escapes the engine, never as the internal representation of absence.
4. An `id`-only floor in the stored schema leaks `{}` into JSON egress, so the
   egress path must prune empty objects and empty arrays before `to_json`
   regardless of whether the floor is adopted.

---

# Nested schema pruning row drop — reproduced

Measured on PySpark 4.0.1, raw Parquet with `mergeSchema`. Scripts:
`scripts/rowdrop.py`, `scripts/rowdrop_anchor.py`.

Two files in one directory. The first carries `name: array<struct<family, given>>`
populated; the second was written without the `family` leaf at all.

| Query | pruning on | pruning off |
|---|---|---|
| `explode(name)`, select **only** `family` | **2 rows** — the second file's resource vanishes | 4 rows, correct |
| same, also selecting sibling `given` (in both files) | 4 rows, `family` null | 4 rows |
| `size(name)` | 2 for both rows | 2 for both |

Silent: no error, a resource with two names reported as having none. `size(name)`
returning 2 shows the array is not intrinsically null — the loss is specific to a
pruned read requesting only a leaf the file lacks.

## The trigger is read-schema vs file-schema divergence, not merging

Isolated with `scripts/rowdrop_isolate.py`:

| Case | Result |
|---|---|
| single narrow file, explicit **wider** read schema, select only the missing leaf | **0 rows** (should be 2) |
| single narrow file, native sparse schema | 2 rows — a sparse schema per se is fine |
| two **uniform** narrow files, `mergeSchema` | 4 rows — merging per se is fine |
| two **divergent** files, `mergeSchema` | 1 row (should be 3) |
| two **divergent** files, explicit wide read schema | 1 row — same, so `mergeSchema` is not the mechanism |
| single wide file, nothing missing | correct |

The condition is simply that the read schema declares a leaf an individual
physical file does not contain. How that arises — `mergeSchema`, an explicit
`.schema()`, or Delta reading its schema from the transaction log — is
irrelevant.

Both halves of this programme walk into it: pruning each batch to its own data
makes files within one table diverge as new elements start appearing, and
reading with an explicit definition-derived schema (mandated for the category-C
path) makes any narrower file a trigger.

This admits a third mitigation alongside the two below: remove the divergence
rather than work around it, by writing or compacting every file in a table at
the table's schema rather than at the batch's.

**Mechanism.** Parquet stores nested data in Dremel form; the array's shape lives
in the repetition and definition levels of the leaf columns actually read. A leaf
missing from a file carries no repetition levels, so with nested pruning
requesting only that leaf there is nothing to reconstruct the array from and it
reads as null. `explode` of a null array yields no rows.

## The design's proposed mitigation is insufficient as stated

"Every struct in a pruned schema retains at least one leaf physically written in
every file" does **not** work on its own — pruning pushes down only the leaf the
query requests, so a retained-but-unrequested anchor is never read. Measured with
an anchor leaf `name.id` present in both files, all null in the narrow one:

| Query | Rows |
|---|---|
| select only the missing leaf `family` | 2 — still drops |
| select `family` **and** the anchor | 4 ✓ |
| select only the anchor | 4 ✓ |

So the mitigation is two-part, and the second half is an engine change:

1. **Schema floor** — every struct surviving pruning retains an anchor leaf
   physically written in every file. `id` is the natural choice: it is a real
   FHIR element on every `Element`, so the floor is conformant rather than
   synthetic, and it is the same floor that makes a minimal complex struct
   writable at all.
2. **The engine co-selects the anchor** on every projection that traverses into
   a repeating complex element. An all-null anchor is sufficient — it still
   carries the repetition levels.

Cost: one extra all-null string leaf per traversed repeating struct, against
reading every leaf of every touched struct under
`spark.sql.optimizer.nestedSchemaPruning.enabled=false`.

Still to confirm during implementation: the same behaviour and the same
mitigation on Delta, and on nested `forEach` over more than one level.

## Confirmed on Delta

Delta 4.0.0 / Spark 4.0.1, `scripts/rowdrop_delta.py`. Ordinary schema
evolution, no explicit read schema anywhere:

```
commit 1: batch with no `family` leaf         -> file A
commit 2: batch carrying it, mergeSchema=true -> file B; log schema gains family
```

| Query | Rows (expected 3) |
|---|---|
| `explode(name)`, select only `n.family` | **1** — the first batch's resource is gone |
| select `n.family` + `n.given` | 3 |
| pruning off, only `n.family` | 3 |

Delta reads at the log schema always, with no `mergeSchema` flag involved on
read, so it is structurally more exposed than raw Parquet rather than less.

The triggering recipe is three ordinary operations: load a batch, later load a
batch containing an element the first lacked, then run a view projecting only
that element from a repeated parent.

Note this is **latent in the current codebase**, not introduced by this work: any
table written across an encoder change that added an element has files missing
that leaf. It is rare today only because the dense encoder writes every leaf into
every file, confining divergence to version changes. A data-fitted schema makes
divergence the normal state.

## Which access patterns are actually affected

`scripts/patterns.py`, single narrow file read with the wider schema.

| Pattern | pruning on | pruning off | |
|---|---|---|---|
| `array_size(name)` / `size(name)` | 2 | 2 | safe |
| `name.given` (leaf present in the file) | correct | correct | safe |
| `explode(name)` yielding the whole struct | 2 rows | 2 rows | safe |
| `transform(name, x -> x.family)` | `[None, None]` | `[None, None]` | safe |
| `exists(name, x -> x.family is not null)` | false | false | safe |
| `name.family` (array field extraction) | `None` | `[None, None]` | **affected** |
| `explode(name)` then `n.family` | **0 rows** | 2 rows | **affected** |
| `explode(name.family)` | **0 rows** | 2 rows | **affected** |

`name.family` and `transform(name, x -> x.family)` are semantically identical yet
behave differently: the lambda references the whole element, which blocks the
pruning, while the field-extraction sugar is rewritten into a single-leaf read.
That is the same reason anchor co-selection works — it forces a leaf the file
actually has into the pushed-down schema.

**Consequence for Pathling.** `name.family` collapsing to `None` instead of
`[None, None]` is harmless in FHIRPath terms, because traversal removes nulls and
both give an empty collection. The damage is confined to patterns where
cardinality survives the projection: exploding a repeated element and then
reading a single leaf of it — SQL-on-FHIR `forEach` / `forEachOrNull`, and
`repeat`. The anchor therefore only needs co-selecting at explode sites, not on
every traversal.

## Pathling's engine is not exposed

`scripts/pathling_shape.py`. Pathling never calls `explode` — no occurrence in
`fhirpath`, `encoders` or `library-api`. `UnnestingSelection` implements
`forEach` as `transformWithIndex((element, index) -> …).flatten()`, a `transform`
whose lambda takes the **whole element**, and the single row-multiplying step is
one `inline(...)` at the end of the projection (`Projection.java:137`) over an
already-computed array.

| Shape | pruning on | pruning off |
|---|---|---|
| Pathling `forEach`: `inline(transform(name, x -> struct(x.family)))` | **2 rows, correct** | 2 rows |
| Pathling nested `forEach`: `inline(flatten(transform(name, x -> transform(x.given, …))))` | **2 rows, correct** | 2 rows |
| naive `explode(name)` then `n.family` | 0 rows | 2 rows |

The whole-element lambda blocks the leaf pruning, so the read never reduces to an
absent leaf and the repetition levels are always available.

**Consequences.**

1. The design's open risk does not apply to the query engine as currently built.
   It would apply to an engine rewritten to use `explode`.
2. It becomes a **design constraint**: unnesting must keep using whole-element
   lambdas and must never be reshaped into leaf-level projection pushdown. Worth
   a regression test over a deliberately divergent fixture.
3. Still to confirm against the real engine rather than this SQL emulation, which
   omits `transformWithIndex`, `IfArray`, `StructProduct` and `TraceExpression`.
4. Direct SQL consumers of Pathling-written data are still exposed if they write
   their own `explode`. That is a documentation matter for the published schema
   contract, not an engine defect.

---

# Decimal lexical form by ingest path

`scripts/decimals.py`, Spark 4.0.1. Target schema declares the decimal field
as `STRING` (the Parquet on FHIR representation).

| Source token | JSONL file | multiline file | `RDD[String]` / `from_json` |
|---|---|---|---|
| `1.50` | `1.50` | `1.50` | `1.5` |
| `1e2` | `1e2` | `1e2` | `100.0` |
| `1.0e-7` | `1.0e-7` | `1.0e-7` | `1.0E-7` |
| `1234567890123456789012345678901234567890.5` | exact | exact | **`1.2345678901234568E39`** |
| `0.000000001` | exact | exact | `1.0E-9` |
| `100` | `100` | `100` | `100` |

The string-dataset path does not merely normalise the lexical form — it routes
the number through a **double**, losing 24 significant digits on the 40-digit
case. That is data loss rather than formatting, and the driver-2 gate names a
40-digit decimal explicitly.

`PathlingContext.encode(Dataset<String>)` is on this path, as is every
`from_json` use and the category-C mechanism behind server writes. File-based
JSONL and uncompressed multiline are byte-exact, confirming the design's
appendix.

## No Spark option fixes the string-dataset path

`JacksonParser`'s `StringType` branch (spark-catalyst 4.0.2, line 296+):

```scala
case VALUE_STRING => UTF8String.fromString(parser.getText)   // unconditionally exact
case other =>
  startLocation.contentReference().getRawContent match {
    case byteArray: Array[Byte] if exactStringParsing => …byte-exact slice…
    case positionedReadable: PositionedReadable if exactStringParsing => …byte-exact slice…
    case _ => generator.copyCurrentStructure(parser)          // re-serialised
  }
```

The branch is selected by the type of Jackson's content reference, fixed by the
`createParser` overload the reader uses:

- `CreateJacksonParser.string` — `createParser(record: String)` — raw content is a
  `String`, so `case _`. Hardcoded at `classic/DataFrameReader.scala:182` for the
  `Dataset[String]` overload.
- `CreateJacksonParser.text` — `createParser(record.getBytes, 0, len)` — raw
  content is a byte array, so exact. This is the file path.

`json(RDD[String])` delegates to `json(Dataset[String])` (line 159), so both are
the same code path.

Options measured, none of which help:

| Attempt | `1.50` / 40-digit decimal |
|---|---|
| `primitivesAsString=true` (inferred `string`) | `1.5` / `1.2345678901234568E39` |
| explicit string schema + `primitivesAsString` | same |
| `prefersDecimal=true` | infers `double` — value exceeds decimal precision |
| `spark.sql.json.enableExactStringParsing=true` | unchanged |

`primitivesAsString` reaches only `JsonInferSchema`; the target type was never the
issue. `enableExactStringParsing` is consulted only inside the two exact branches,
so it can turn exactness off, never on.

This also explains why quoting the token in a pre-pass is sound rather than a
workaround: `VALUE_STRING -> parser.getText` has no config and no stream-type
dependency.

### This is a Spark bug — an incomplete fix for SPARK-48148

[SPARK-48148](https://issues.apache.org/jira/browse/SPARK-48148), *"JSON objects
should not be modified when read as STRING"*, describes exactly this problem,
with a precision-loss example: `{"b": -999.99999999999999999999999999999999995}`
read as `STRING` yielding `-1000.0`. Resolved for 4.0.0; the fix introduced
`spark.sql.json.enableExactStringParsing` and the byte-slicing branches.

The fix handles only two content-reference types, `Array[Byte]` and
`PositionedReadable`. The `String`-backed parser used by `json(Dataset[String])`
and `from_json` still falls through to the re-serialising `case _`, so the
ticket's stated goal is not met on those paths and no configuration reaches them.

It appears fixable rather than fundamental: the exact branches slice using
`startLocation.getByteOffset`, and `JsonLocation` also exposes `getCharOffset`,
so a `case s: String` could slice the original string by character offset. Even
without that, the fallback could write `parser.getText()` rather than routing the
number through a double.

Worth reporting upstream with the repro in `scripts/decimals.py`. The spec
must assume it stays unfixed.

### What a quoting pre-pass would involve

`scripts/quoted.py`. Schema `struct<i:int, l:bigint, b:boolean, d:string, dt:date>`,
read from a string dataset.

| Input | `i` | `d` (FHIR decimal) |
|---|---|---|
| all unquoted (today) | 5 | `'1.5'` — lossy |
| **only the decimal quoted** | **5** | **`'1.50'` — exact** |
| all numbers quoted | null (PERMISSIVE) / error (FAILFAST) | `'1.50'` |

`CANNOT_PARSE_JSON_FIELD: ... the value 5 of the JSON token type VALUE_STRING to
target Spark data type "INT"`. Spark does not coerce quoted numbers to numeric
targets, so a blanket "quote every number" pre-pass is not viable. With only the
decimal quoted, `+1.50`, `1.0e-7` and a 40-digit value all survive exactly.

The pre-pass therefore requires:

1. The set of decimal-typed paths per resource type, derived from the
   definitions. Not a flat list — decimals occur under repeated elements, choice
   types (`valueQuantity.value`, `valueDecimal`) and extensions at arbitrary
   depth, so it is a tree matcher whose shape depends on the same nesting and
   open-type configuration that governs the schema.
2. A per-document rewrite in a UDF: tokenise with Jackson, track the path, and
   where a number token sits at a decimal path emit `parser.getText()` quoted,
   copying everything else verbatim.
3. `spark.read().json()` with the derived schema.

Two consequences worth weighing:

- It needs per-row Jackson regardless, so ingest becomes two tokenisation passes
  rather than one. That weakens the case for it against parsing once in
  `mapPartitions` and emitting Rows directly.
- An incorrect path set fails **silently** under `PERMISSIVE` — numeric fields
  null out — and loudly only under `FAILFAST`.

## VARIANT does not preserve decimals

`scripts/variant.py`. `parse_json` stores a number as decimal where it fits,
otherwise as double.

| Source | `schema_of_variant` | Value read back |
|---|---|---|
| `1.50` | `DECIMAL(2,1)` | `1.5` — trailing zero lost at parse |
| `1e2` | `DOUBLE` | `100.0` |
| `1.0e-7` | `DOUBLE` | `1.0E-7` |
| 40-digit | `DOUBLE` | `1.2345678901234568E39` |
| `0.000000001` | `DECIMAL(9,9)` | exact |
| `-999.99999999999999999999999999999999995` | `DECIMAL(38,35)` | exact |

`to_json`, `variant_get(…, 'string')` and `cast(var as struct<…>)` all agree.
Variant is better than the string-dataset path for values fitting
`DECIMAL(38, s)`, but still normalises trailing zeros and exponent notation and
still degrades beyond 38 digits. It is not a losslessness mechanism.

`schema_of_variant_agg` does work for cross-row inference, merging an empty
`ARRAY<VOID>` with `ARRAY<OBJECT<x:BIGINT>>` correctly, and producing
`OBJECT<id: STRING, n: ARRAY<OBJECT<x: BIGINT>>, v: DOUBLE>`. Note it unified
`DECIMAL(2,1)`, `DECIMAL(9,9)`, `DECIMAL(38,35)`, `BIGINT` and `DOUBLE` by
widening to `DOUBLE` — the lossy type.

### Design-doc correction

The design gives three reasons not to infer the schema from data. The first —
"a repeating element present once in one batch and twice in another infers as a
struct versus an array of structs" — does not hold for FHIR JSON, which always
represents a repeating element as an array even when it has one element. The
other two (inference widening incompatible types to `STRING` rather than failing,
and `Patient.bogusField` being indistinguishable from an element absent from the
dataset) stand.

## Custom-built VARIANT is lossless, and `spark-variant` is Catalyst-free

`scripts/variant2.py`. When the variant carries a decimal as a **string**
rather than as a parsed number, every form survives exactly:

| Source | `variant_get(…,'string')` | `cast(var as struct<v:string>)` | `to_json` |
|---|---|---|---|
| `1.50`, `1e2`, `1.0e-7`, 40-digit, `+1.50` | exact | exact | exact |

`schema_of_variant_agg` over that collection gives `OBJECT<id: STRING, v: STRING>`,
and casting to a declared struct ignores keys the struct does not declare and
nulls keys the document does not carry — both measured.

`VariantBuilder` lives in its own artifact, `spark-variant`, package
`org.apache.spark.types.variant` — **not** `org.apache.spark.sql.catalyst`. Its
only dependencies are `spark-tags`, `spark-common-utils` and `jackson-core`, so
it satisfies the no-internal-Catalyst constraint and could sit in `fhir-schema`.
Public API:

```java
public static Variant parseJson(com.fasterxml.jackson.core.JsonParser, boolean)
public void appendString(String)        // exact decimal text
public void appendDecimal(BigDecimal)   // preserves scale, capped at 38 digits
public void appendLong(long) / appendBoolean / appendNull
public int addKey(String) / finishWritingObject(…) / finishWritingArray(…)
public Variant result()
```

`VariantVal` is in `spark-unsafe` (`org.apache.spark.unsafe.types`), also not
Catalyst.

### The resulting ingest design

1. Parse to variant in `mapPartitions`, driving `VariantBuilder` from a Jackson
   walk; decimals via `appendString` so they are exact regardless of magnitude.
   Non-conformant keys are detected in the same pass, which the ignore/fail
   switch needs anyway because `from_json` silently skips unknown fields in every
   mode including `FAILFAST`.
2. Derive the schema: definitions supply types and cardinality;
   `schema_of_variant_agg` supplies which elements are present, for pruning.
3. `cast(variant as <derived struct>)` materialises the dataset.

One parse pass, lossless on every ingest route, and the only option that
decouples parsing from schema determination — which is what a data-pruned schema
requires, since presence is only known after seeing the data.

Feasibility items to confirm during implementation:

- Whether a `mapPartitions`/UDF can return a `VariantType` column (encoder
  support for `VariantVal`).
- Cost of `schema_of_variant_agg` over a large dataset: a full pass plus driver
  aggregation.
- Whether the intermediate variant must be materialised, or can pipeline into
  the cast.

### Parquet variant shredding is not usable in Spark 4.0.2

Parquet defines a VARIANT logical type with shredding — typed sub-columns plus a
residual. Spark 4.0.2 has the skeleton, entirely behind internal flags that all
default to false:

| Config | State |
|---|---|
| `spark.sql.variant.allowReadingShredded` | internal, default false |
| `spark.sql.variant.writeShredding.enabled` | internal, default false |
| `spark.sql.variant.pushVariantIntoScan` | internal, default false |
| `spark.sql.variant.forceShreddingSchemaForTest` | "FOR INTERNAL TESTING ONLY" |

There is no public way to declare a shredding schema.

Independently of maturity, a stored VARIANT column **is not Parquet on FHIR**:
the specification defines concrete typed columns, and no other consumer could
read a variant column, which defeats adopting the format. A Delta table gaining
a variant column also moves to reader 3 / writer 7.

Variant therefore belongs in the ingest pipeline as an intermediate, not as the
storage format.

**Future direction worth revisiting.** If shredding matures and gains a public
shredding-schema API, storing a variant whose shredding schema equals the PoF
schema would give PoF-shaped physical columns plus a lossless residual carrying
exactly the non-conformant content that the ignore/fail switch must otherwise
discard.

## 13. Nested schema pruning does not reach the engine's array access

Measured on PySpark 4.0.1, local Parquet, script
`scripts/widen_prune.py` and `widen_prune2.py`. Stored schema:

```
name: array<struct<family:string, given:array<string>, big:string>>
addr: struct<city:string, state:string, big:string>
```

`ReadSchema` from the executed plan, which is what Parquet actually reads:

| Access pattern | Pruned? | ReadSchema for `name` |
|---|---|---|
| `select("name.family")` (implicit extraction) | yes | `array<struct<family>>` |
| `explode(name)` then `.family` | yes | `array<struct<family>>` |
| `inline(name)` then `.family` | **no** | whole struct |
| `transform(name, x -> x.family)` | **no** | whole struct |
| `transform` then `explode` | **no** | whole struct |

Singular structs behave differently and prune normally: `addr.city` reads
`struct<addr:struct<city>>`.

**This is the same mechanism as the row-drop hazard, seen from the other side.**
Pruning and row loss both come from reducing the read to individual leaves.
`explode` gets the pruning and takes the risk; `transform` + `inline` — what
`UnnestingSelection` actually does — takes neither. The safety measured in
finding 8 is paid for in I/O width.

Consequences:

- For repeated elements the engine reads whole elements regardless of what the
  expression projects. Narrowing the read therefore depends entirely on the
  **stored** schema being fitted to the data; there is no pruning to fall back
  on. This strengthens driver 1's case for the fitted schema rather than
  weakening it.
- A widening projection that rebuilds a struct — `named_struct(x.a, x.b, null
  as c)` — is **free for singular structs**: `SimplifyExtractValueOps` collapses
  `GetStructField(CreateNamedStruct)` and pruning survives (`addr.city` still
  reads `struct<addr:struct<city>>`).
- For arrays a widening rebuild costs nothing either, because nothing was being
  pruned. It does defeat pruning for a *direct SQL consumer* writing
  `name.family`, but such a consumer reads the stored layout, not a widened one.

So the runtime cost of presenting fitted storage through a widened read schema
(option B in the schema-binding question) is plan size and planning time, not
I/O. That was not obvious and inverts the assumption that widening is expensive.

## 14. A `RuntimeReplaceable` tolerant-traversal expression is pruning-neutral

Read from the Spark 4.0.2 sources (`spark-catalyst_2.13-4.0.2-sources.jar`,
extracted to `scripts/cat402`).

`Optimizer.defaultBatches` order:

| Line | Batch | Contains |
|---|---|---|
| 174 | `Finish Analysis` | **`ReplaceExpressions`** (`Optimizer.scala:319`) |
| 224 | `operatorOptimizationBatch` | `ColumnPruning` → `NestedColumnAliasing` |
| 232 | `Early Filter and Projection Push-Down` | `earlyScanPushDownRules` → `SchemaPruning` |

So a `RuntimeReplaceable` is rewritten to its replacement **before every pruning
rule runs**. An expression that resolves to `GetStructField` when the field is
present, and to `Literal(null, T)` when it is not, is therefore indistinguishable
from a statically written one by the time pruning happens — it neither gains nor
loses the pruning measured in finding 13.

`Expression.scala:418-431` gives the rest for free:

```scala
trait RuntimeReplaceable extends Expression {
  def replacement: Expression
  override def nullable: Boolean = replacement.nullable
  override def dataType: DataType = replacement.dataType
  final override def eval(input: InternalRow = null): Any = { ... }
```

`eval` is final and codegen comes from the replacement, so the implementation is
one `lazy val replacement` computed from `child.dataType`. No `doGenCode`.

**Residual risk**, not testable from PySpark: `dataType` delegates to
`replacement`, so anything probing `dataType` before the child is resolved would
force `replacement` early. The default `resolved = childrenResolved && ...`
guards the normal paths, and every built-in `RuntimeReplaceable` carries the same
exposure, but it needs a JVM test against an unresolved child.

Wrapping it in a `Column` uses the mechanism already established in the
query-time toolkit: `org.apache.spark.sql.classic.ExpressionUtils.column` /
`.expression`, as `ColumnFunctions.structProduct` does.

## 15. The fallback type for an absent element: only `void` is a bottom type

Measured on PySpark 4.0.1, script `scripts/fallback_type.py`. Candidates for
the type of `Literal(null, T)` standing in for an absent complex element.

| Operation | `void` | `struct<>` | `struct<id:string>` |
|---|---|---|---|
| Construct the literal | ✓ | ✓ | ✓ |
| `coalesce(fallback, struct<id,family>)` | ✓ → `struct<id,family>` | ✗ `DATA_DIFF_TYPES` | ✗ `DATA_DIFF_TYPES` |
| Traverse into it (`.family`) | ✗ `INVALID_EXTRACT_BASE_FIELD_TYPE` | ✗ `FIELD_NOT_FOUND` | ✗ `FIELD_NOT_FOUND` |
| Write to Parquet | ✗ (omitted in JSON) | ✗ `EMPTY_SCHEMA_NOT_SUPPORTED` | ✓ |

And for a repeating element, where the engine's `transform` needs an array:

| Operation | `void` | `array<void>` | `array<struct<>>` |
|---|---|---|---|
| `transform(fallback, ...)` | ✗ `UNEXPECTED_INPUT_TYPE` | ✓ | ✓ |
| `transform` with a real body | — | ✓ → `array<string>` | ✓ |
| `coalesce(fallback, array<struct<...>>)` | — | ✓ → `array<struct<...>>` | ✗ `DATA_DIFF_TYPES` |

**An empty struct is not a bottom type.** Spark's struct widening is field-wise
and needs matching arity and names, so `struct<>` unifies with nothing but
`struct<>` — it fails FR-027 exactly as a minimal `struct<id:string>` does.
`NullType` is the only type that widens to everything, and its bottom-ness
propagates through the array constructor: `array<void>` widens to
`array<struct<...>>` while `array<struct<>>` does not.

The row that looks like a problem — traversal into the fallback fails for *every*
candidate — is not one. It is why the tolerant expression must be emitted at
every traversal step rather than only where absence is suspected: nothing
composes if a plain `GetStructField` is ever emitted over a fallback. The
expression intercepts before that.

**Resulting rule for `fallbackType`:**

| Absent element | Fallback type |
|---|---|
| Singular primitive | the definition's Spark type — this is what gives typed absence |
| Repeating primitive | `array<` the definition's Spark type `>` |
| Singular complex | `void` |
| Repeating complex | `array<void>` |

Declare the type wherever the definition gives an unambiguous one; use the bottom
type wherever a concrete shape would over-constrain the combination.

## 16. `combine` and `union` across divergent fitted schemas

Measured on PySpark 4.0.1, script `scripts/combine_types.py`. Two collections
of the same FHIR type reached by different paths, so the same FHIR type with
different fitted SQL schemas:

```
a = array<struct<id, family>>                          -- Patient.name
b = array<struct<id, given, period<start>>>            -- Patient.contact.name
```

`combine` is `concat` and `union` is `array_union` (`CombiningLogic.java:83`,
`:69`).

| Case | Result |
|---|---|
| `concat(array<void>, a)` | ✓ → `a`'s type |
| `array_union(array<void>, a)` | ✓ → `a`'s type |
| `concat(a, b)` | ✗ `DATATYPE_MISMATCH.DATA_DIFF_TYPES` |
| `array_union(a, b)` | ✗ `DATATYPE_MISMATCH.BINARY_ARRAY_DIFF_TYPES` |
| `cast(a as b's type)` | ✗ `CAST_WITHOUT_SUGGESTION` |
| by-name projection of both into the merged type, then `concat` | ✓ |
| ... then traverse `.family` | ✓ → `['Smith', null]` |

So **absence is free** — `array<void>` combines with anything and yields the
other side's type — and the case that actually needs work has nothing to do with
absence: two *present* collections whose fitted shapes differ.

### Never reconcile by casting

A struct cast of differing arity is refused, but a **same-arity cast silently
reorders by position**:

```
cast(named_struct('family','Smith','given','Jo') as struct<given:string,family:string>)
  -> Row(given='Smith', family='Jo')
```

Names ignored, no error. This is the same class of defect as the positional
`CodingSchema` decoding this work replaces. Reconciliation must be a by-name
`named_struct` projection.

### The merge is the reader's merge

The merged type is the recursive field-wise union of the two actual SQL types —
the same operation as merging divergent file schemas for FR-041/042. One
implementation, two callers. It must recurse, because a nested child can differ
too (`period` present on one side only, and `period`'s own fields may differ).

### The seam already exists

`SameTypeBinaryOperator.invoke` already reconciles the operands' **FHIR** types
via `FhirPathBinaryOperator.reconcileTypes` ("promote IntegerCollection to
DecimalCollection"), and `CombiningLogic.prepareArray` already normalises
`DecimalCollection` to `DECIMAL(32,6)` "so that two operands with different
precisions can be merged without schema mismatch". SQL-type reconciliation
generalises the decimal case at the same seam.

It also short-circuits statically when either side is `EmptyCollection`
(`SameTypeBinaryOperator.java:43-49`), so under the static-absence design the
void case never reaches `concat` at all.

### Bearing on the C/D choice

The merged type depends on both operands' resolved types, so under D
(`ResolveOrNull`) this needs a *second* `RuntimeReplaceable`. Under C
(`UnboundColumn`) the binder already knows both types and the merge is ordinary
code. A point in C's favour that the absence question alone did not surface.

## 17. Struct field order is part of the type, and struct equality is positional

Measured on PySpark 4.0.1.

Field **order** is part of the type — same fields, different order, no common
type:

```
a = array<struct<id:string,family:string>>
b = array<struct<family:string,id:string>>
concat(a, b)       -> DATATYPE_MISMATCH.DATA_DIFF_TYPES
array_union(a, b)  -> DATATYPE_MISMATCH.BINARY_ARRAY_DIFF_TYPES
```

But equality does **not** fail. It coerces, and it coerces **positionally**:

```
named_struct('id','X','family','Y') = named_struct('family','X','id','Y')
  -> true
```

Field names are ignored; the comparison is `id` against `family`. Two
`HumanName` values whose fitted schemas order fields differently therefore
compare **equal when they are not**, with no error — the same silent-corruption
class as positional `CodingSchema` decoding and the same-arity struct cast in
finding 16.

**Requirement that follows.** Field order must be canonical — definition order,
filtered to the fields present — at every point a struct type is produced:
schema derivation, the merge of two fitted types, the merge of divergent file
schemas, and the by-name reconciliation projection. Then order divergence cannot
arise within Pathling and the hazard is structurally impossible rather than a
rule to remember. Data written by another Parquet on FHIR implementation may
still order fields differently, so reconciliation must project by name rather
than trust the order it is given.

This also settles how the reconciliation expression must be shaped: every operand
must be projected against the **same ordered operand list**, not pairwise against
its peer, or the two projections can produce different target types.
