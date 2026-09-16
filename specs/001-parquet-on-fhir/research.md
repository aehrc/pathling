# Research: Parquet on FHIR

Phase 0 decisions. Measurements referenced here were taken on PySpark 4.0.1 and
Delta 4.0.0 against Spark source 4.0.2, and are recorded in full in
`evidence/spark-type-findings.md` with reproduction scripts.

---

## R-001 Scope of the engine migration

- **Decision**: The engine migrates to the new *schema* only. The query-time
  Catalyst toolkit is untouched.
- **Rationale**: "Migration to the new schema" and "removal of internal Catalyst
  API from the execution path" are separate problems. Entangling them would put
  a rewrite of the engine's most subtle machinery — trace collection, row
  indexing, tree transforms — on the critical path of a layout change, with no
  way to bisect a regression.
- **Alternatives considered**: Lifting the engine off Catalyst at the same time
  (roughly doubles the programme and overturns the design's first decision);
  naming it as a defined successor (deferred as unnecessary ceremony).

## R-002 Module structure

- **Decision**: Additive. New `fhir-schema` (definitions plus derivation) and
  new `io` (the new encoding). `encoders` untouched. Build order
  `utilities -> fhir-schema -> {encoders, io} -> terminology -> fhirpath -> library-api`.
- **Rationale**: The existing encoders must stay usable, because the
  data-migration tooling may need them, and what ultimately becomes of them
  depends on what that tooling requires. Adding modules avoids a large
  pure-motion change on the critical path. The definition package must still move
  down, because schema derivation sits below the engine and cannot depend on it.
- **Alternatives considered**: Splitting `encoders` into a HAPI bridge and a
  query toolkit first, as the superseded design proposed (deferred); putting the
  new encoding inside `encoders` (shares a module with the thing it replaces and
  forfeits the dependency-level Catalyst ban); a new module above `fhirpath`
  (cannot be reached from `fhirpath` tests, which must produce data in the new
  layout, without a dependency cycle).

## R-003 Enforcing the Catalyst boundary

- **Decision**: `fhir-schema` resolves `spark-sql-api` only, with a build rule
  banning `spark-catalyst` and `spark-sql`. `io` gets a build rule failing on any
  `org.apache.spark.sql.catalyst` import.
- **Rationale**: The `io` module needs `spark-sql` for column expressions, which
  pulls Catalyst in transitively, so a dependency ban is impossible there. Each
  module gets the strongest enforcement it can carry.
- **Alternatives considered**: A dependency ban on `fhir-schema` only (leaves the
  transform code unguarded, which is exactly where the old code reached into
  Catalyst); import checks on both (weaker for `fhir-schema`, where a true ban is
  available).

## R-004 Transition

- **Decision**: Flag day. On completion the engine reads only the new layout.
- **Rationale**: Keeps the engine single-convention and the diff honest. Carrying
  two conventions would need an explicit sunset or become permanent.
- **Consequence**: The test estate moves in one step. It does so through the
  category-C path — construct the object, serialise to JSON, read with the
  derived schema — so the fluent builders survive unchanged and only the
  fixture-loading mechanism changes.
- **Alternatives considered**: Reading both layouts selected by detection (defers
  the cost but risks permanence); keeping the old encoders usable in tests
  (fixture *construction* migrates gradually but the data is then unreadable by
  the engine, so nothing is actually gained).
- **Addendum — the first alternative is adopted for the transition.** The
  decision above holds as an *end state*: on completion the engine reads only the
  new layout. It does not hold for the path there. From M2 the engine reads both,
  selected from the resolved schema, which is what lets the engine be rewritten
  behind a green build and makes the switch a writer flip. The stated objection —
  that carrying two conventions risks permanence — is answered by naming the
  sunset rather than by refusing the alternative: T100e removes the previous
  reader in M6, T100f settles FR-053's end-state wording, and decision 51 records
  the arms as a deliberate trade. The consequence above also softens: the test
  estate no longer has to move in one step, because the dense dimension carries
  the suite while the pruned one is switched on. See decisions 47, 48 and 51, and
  the addendum to 40.

---

## R-005 Absent elements under a sparse schema

- **Decision**: Traversal emits a **tolerant traversal expression** that resolves
  after the input schema is known: to the field where the schema carries it, and
  to a typed null where it does not. Implemented as a `RuntimeReplaceable` added
  to the query-time expression toolkit.
- **Rationale**: It is the only option that preserves the engine's current
  contract — an expression becomes a column with no reference to any dataset, and
  that column is valid over any conformant schema. It is also the cheapest: one
  small class, and it gives absent primitives their definition-derived type,
  which removes three requirements rather than adding any.
- **Fallback type** follows one principle: *the definitions' type where that type
  is unambiguous, the bottom type where a concrete shape would over-constrain
  later combination.* Singular primitive → the definition's type; repeating
  primitive → an array of it; singular complex → `void`; repeating complex →
  `array<void>`.
- **Measurements that constrain this** (findings 14–15):
  - `Optimizer.defaultBatches` runs `ReplaceExpressions` (in `Finish Analysis`)
    before `ColumnPruning`, `NestedColumnAliasing` and `SchemaPruning`, so the
    rewritten plan is indistinguishable from a statically written one and pruning
    is unaffected. `RuntimeReplaceable` supplies `dataType`, `nullable` and a
    final `eval`, so there is no codegen to write.
  - Only `void` is a bottom type. `coalesce(void, struct<…>)` resolves;
    `coalesce(struct<>, struct<…>)` and `coalesce(struct<id>, struct<…>)` both
    fail with `DATA_DIFF_TYPES`. An empty struct is not a lesser evil than a
    minimal struct — Spark's struct widening is field-wise and needs matching
    arity and names, so an empty struct unifies with nothing but itself.
  - Bare `void` fails for repeating elements, because the engine wraps arrays in
    `transform`, which requires an array type. `array<void>` survives `transform`
    and still widens to `array<struct<…>>`.
  - Traversal *into* any fallback fails for every candidate type. That is not a
    defect but the reason the tolerant expression must be emitted at every
    traversal step rather than only where absence is suspected.
- **Residual risk — measured, and it does not materialise.**
  `RuntimeReplaceable.dataType` delegates to `replacement`, so a path that probes
  `dataType` before the child is resolved would force the replacement early.
  Every built-in `RuntimeReplaceable` carries the same exposure, but it is not
  testable from PySpark and needed a JVM test over an unresolved child. T009a ran
  that test as a throwaway spike: across twenty runs spanning eleven plan shapes
  and two child constructions, nothing forced the replacement before the child
  resolved,
  nothing threw, and every plan optimised to the shape a statically written
  traversal would produce. The design stays on R-017's option D. See
  `evidence/t009a-analyzer-gate.md` — which also records the three construction
  choices T038e must repeat, and one trap: under Spark 4's `ColumnNode` API the
  child an engine hands over is a `ColumnNodeExpression`, which reports
  `resolved == true` while still wrapping an unresolved node, so a caller cannot
  read anything into `resolved()` at construction time. The wrapper does not
  reach the analyzer, so this is not a hazard inside the expression itself.
- **Alternatives considered**: the static empty collection — untyped, `void` in
  SQL — which was the earlier decision here and was **changed** because it cannot
  meet the schema-agnostic column contract: a statically pruned column is only
  valid for the schema it was built against, and returns empty on a wider dataset
  that does carry the element. Also considered and rejected: a concrete minimal
  structure for complex elements (fails FR-027, per the measurements above), and
  a schema floor (R-006).

## R-006 The pruning floor

- **Decision**: No minimum-field floor. Prune to populated elements only.
- **Rationale**: A complex element appears only where some descendant leaf is
  populated, so a field-less structure never arises and the unwritable-empty-
  struct case is unreachable by construction. The floor's other motivation — an
  anchor leaf for the row-drop mitigation — evaporated with R-007.
- **Alternatives considered**: Retaining `id` on every surviving complex element
  (costs a nullable column per element for a risk that does not apply).

## R-007 Nested schema pruning and row loss

- **Decision**: Not a defect for Pathling, and no mitigation machinery. It
  becomes a design constraint plus a regression test.
- **Rationale**: The defect is real and reproduced, but Pathling's engine is not
  exposed to it.
- **Evidence**:
  - Reproduced on raw Parquet and on Delta. The trigger is a read schema
    declaring a leaf that an individual physical file lacks — however that
    arises: `mergeSchema`, an explicit read schema, or Delta reading its log
    schema after ordinary schema evolution. A uniform sparse schema is safe, and
    merging alone is safe; divergence is the condition.
  - Mechanism: Parquet stores nested data in Dremel form, and an array's shape
    lives in the repetition levels of the leaves actually read. Pruning to a leaf
    the file lacks leaves no repetition levels, so the array reads as null and
    `explode` yields no rows — silently.
  - **Pathling never calls `explode`.** Unnesting is a `transform` whose lambda
    takes the whole element, with a single `inline` at the end of the projection
    over an already-computed array. Measured: that shape returns the correct rows
    with pruning enabled, in both the simple and the nested case, where the
    `explode` shape returns none.
  - The design's proposed mitigation was insufficient as written: a retained but
    unrequested anchor leaf is never read, so retaining one changes nothing
    unless the query also selects it.
- **Constraint this creates**: unnesting must keep using whole-element lambdas
  and must never be reshaped into leaf-level pushdown. Pinned by a regression
  test over a deliberately divergent fixture, confirmed against the real engine
  rather than the SQL emulation used here.
- **The same mechanism, from the other side** (finding 13): the access pattern
  that is safe from row loss is also the one that gets no nested pruning.
  Measured `ReadSchema` for a stored `array<struct<family,given,big>>`:
  `select("name.family")` and `explode(name)` then `.family` both prune to
  `array<struct<family>>`; `inline(name)` then `.family` and
  `transform(name, x -> x.family)` both read the whole structure. Singular
  structs prune normally. Pruning and row loss are two faces of reducing the
  read to individual leaves; `explode` takes the pruning and the risk, and the
  engine's pattern takes neither.
- **Consequence for driver 1**: for repeated elements the engine reads whole
  elements whatever the expression projects, so narrowing the read depends
  entirely on the **stored** schema being fitted to the data. There is no pruning
  to fall back on. This strengthens the case for the fitted schema rather than
  weakening it.
- **Also**: direct SQL consumers of Pathling-written tables remain exposed if
  they write their own `explode`. A documentation matter for the layout contract.
- **Alternatives considered**: Anchor co-selection at unnesting sites, and
  disabling nested schema pruning session-wide — both unnecessary, and the second
  taxes driver 1 directly.

---

## R-008 Ingest mechanism

- **Decision**: Read JSON with an inferred schema, then transform into the
  definition-derived target with SQL. Inference never determines the stored
  schema.
- **Rationale**: It is the mechanism the prototype demonstrates working, and
  presence information for pruning falls out of the inferred schema. Types,
  cardinality and conventions are imposed by the transform, so the stored schema
  remains definition-derived.
- **Alternatives considered**: Reading with an explicit derived schema (avoids
  the inference pass, but pruning then needs its own presence-discovery pass, and
  the strictness switch needs its own detection because `from_json` silently
  skips unknown fields in every mode, `FAILFAST` included); parsing per partition
  and emitting rows directly (requires the target schema up front, so pruning
  needs a separate discovery pass over raw input).

## R-009 Lexical decimal preservation

- **Decision**: Accept the limitation for the initial implementation. Document
  it, report it upstream, and record a follow-up. Benchmark the alternative after
  the initial implementation and decide then.
- **Evidence**:
  - File-based JSONL and uncompressed multiline reads are byte-exact. Reading a
    *dataset of strings* and `from_json` are not, and the loss is severe: the
    value is routed through a double, so a 40-digit decimal returns with 17
    significant digits. `1.50` becomes `1.5`, `1e2` becomes `100.0`.
  - This is an incomplete fix for [SPARK-48148](https://issues.apache.org/jira/browse/SPARK-48148),
    *"JSON objects should not be modified when read as STRING"*, resolved for
    4.0.0. The fix handles only byte-array and positionally-readable content
    references; the string-backed parser used by the dataset overload and
    `from_json` still re-serialises. The dataset-of-strings and RDD overloads are
    the same code path — the latter delegates to the former.
  - **No configuration reaches it.** The exactness flag is consulted only inside
    the two exact branches, so it can disable exactness but never enable it.
    `primitivesAsString` affects only inference. `prefersDecimal` infers a double
    for large values.
  - The current implementation is not affected, because it parses with FHIR
    tooling rather than Spark's reader; the exposure arrives with the new
    mechanism.
- **Alternatives considered and kept on the table**:
  - *Quoting pre-pass*: rewrite number tokens at decimal-typed paths into string
    tokens before parsing. Sound rather than a workaround, because the
    string-token branch is unconditionally exact. Measured to preserve `1.50`,
    `+1.50`, `1.0e-7` and 40 digits exactly. Requires a definition-derived path
    matcher — a blanket "quote every number" fails, because a quoted number
    against an integer target is rejected — and it needs per-row tokenisation, so
    ingest becomes two passes. An incorrect path set nulls numeric fields
    *silently* under the permissive mode. **Recorded as a follow-up.**
  - *Direct `String -> layout` parsing building a variant*: measured to be
    lossless on every route, a single pass, detecting non-conformant content in
    the same pass, and the only option that decouples parsing from schema
    determination. `VariantBuilder` is public API in its own artifact
    (`spark-variant`, depending only on tags, common-utils and Jackson), so it
    satisfies the Catalyst constraint. **Logically better; performance unknown.
    Benchmarked against the chosen approach after the initial implementation, and
    the final decision made then.**
  - *Staging through a temporary file*: uses only guarantees Spark already
    provides, but materialises a streaming input to disk and helps neither
    `from_json` nor the object path.

## R-010 Variant as a storage format

- **Decision**: No. Variant belongs in the ingest pipeline as an intermediate at
  most, never as the stored representation.
- **Rationale**: A stored variant column is not Parquet on FHIR — the
  specification defines concrete typed columns, and no other consumer could read
  it, which defeats adopting the format. A Delta table gaining one also moves to
  a higher reader and writer version.
- **Also**: Parquet variant *shredding* would in principle give typed columns
  plus a lossless residual, but in Spark 4.0.2 every relevant switch is internal
  and defaults to false, and there is no public way to declare a shredding
  schema.
- **Future direction**: if shredding matures, storing a variant whose shredding
  schema equals the layout would give layout-shaped physical columns plus a
  residual carrying exactly the non-conformant content the strictness switch must
  otherwise discard.
- **Measurement**: `parse_json` normalises trailing zeros (`1.50` becomes
  `DECIMAL(2,1)`), renders exponent forms differently, and falls to double beyond
  38 digits. It preserved the SPARK-48148 example exactly.
  `schema_of_variant_agg` merges across rows correctly but unified a column of
  mixed decimals and integers by widening to `DOUBLE`.

## R-011 Query-time decimal precision

- **Decision**: Unchanged from today.
- **Rationale**: The existing cap is FHIR-motivated and documented in the source:
  FHIR defers `decimal` to XML Schema, which permits a documented cap; six
  decimal places covers the precision FHIR calls out for location coordinates.
  With storage becoming lexical, the cap now applies only to computation, which
  is a strict improvement. Every existing decimal test holds and a decimal view
  column's type is unchanged.
- **Correction to the superseded design**: it states that existing tables store
  decimals at 38 digits of precision. They do not — the stored type is the
  32-digit one. The 38-digit figure is the unscaled quantity representation.
- **Alternatives considered**: Arbitrary precision at query time (touches every
  arithmetic, comparison and aggregation path and changes a decimal column's
  output type for existing users); widening to Spark's maximum (a behaviour
  change no driver asks for, discarding the documented rationale).

## R-012 Annotations

- **Decision**: Decimal, date and quantity annotations emitted by default, each
  individually disableable. The engine computes correctly without any of them.
- **Rationale**: Annotations are optional in the specification, so a conformant
  file may arrive with none. An engine that requires them can read only files
  Pathling itself wrote, which defeats adopting the format. Emitting them by
  default keeps the fast paths available where Pathling controls the write.
- **Design note**: whether an annotation is used is decided from the schema, not
  per row, so the choice is made at planning time.

## R-013 Reference keys

- **Decision**: Compute the versioned key at query time from the conformant id
  and version elements. If the join benchmark shows a precomputed value is
  needed, it returns as an **annotation**, not as a bare synthetic column.
- **Rationale**: The stored layout stays conformant, and the engine must work
  without it either way — the same rule as every other annotation. The original
  precomputed column existed for join performance, which is a measurable
  question rather than a structural one.
- **Sequencing**: the portable reference-join fixtures land before the
  conventions change, so the behaviour is pinned by tests that survive the
  change of fixture mechanism.

## R-014 Primitive metadata and annotation access

- **Decision**: One mechanism — resolve a named sibling of a primitive within its
  parent structure. A collection traversed to a primitive retains a handle on its
  parent and its element name.
- **Rationale**: The layout stores a primitive's id and extensions in a sibling
  group, and every annotation is likewise a sibling of the element it annotates.
  A single sibling-resolution facility therefore serves both primitive metadata
  navigation and every annotation fast path. Primitives never asked for either
  are untouched, so the most-used path in the engine does not change.
- **Alternatives considered**: Eagerly pairing each primitive with its metadata
  (uniform, but changes the column representation of every primitive).

## R-015 Schema merging

- **Decision**: Merging enabled by default on both write and read, with an
  opt-out on read and the cost measured.
- **Rationale**: With schemas fitted to data and data arriving over time,
  divergence between files in one table is the steady state. Without merging on
  read, the lexicographically first file's schema wins and the rest are silently
  null-filled — a worse failure than slow. Without merging on write, a divergent
  append to a transactional table fails outright.
- **Gate**: measure merge cost over a realistic file count on object storage;
  the result can force a fallback for raw files.

## R-016 Reconciling collections whose SQL shapes differ

- **Decision**: One variadic reconciliation expression, also a
  `RuntimeReplaceable`, which computes the recursive field-wise merged type from
  its resolved operands and projects each **by name** into it. The operators are
  unchanged: reconciliation happens where the operands are already prepared,
  generalising what `CombiningLogic.prepareArray` does today for decimals.
  Struct field order is canonical — definition order, restricted to fields
  present — everywhere a structure type is produced.
- **Rationale**: Under a fitted schema, two collections of the same FHIR type
  reached by different paths have different SQL shapes, and every operation
  needing a common type fails on them. The seam already exists:
  `SameTypeBinaryOperator.invoke` reconciles the operands' *FHIR* types via
  `reconcileTypes`, and `prepareArray` already normalises `DecimalCollection` to
  a common SQL type "so that two operands with different precisions can be merged
  without schema mismatch". This generalises that from decimals to structures.
  The merged type is the same recursive field-wise union that merging divergent
  file schemas needs, so one implementation serves both. That implementation is
  pure structure mechanics and lives in `utilities`, because both callers must
  reach it and `encoders` must not gain a dependency on `fhir-schema`. The
  **canonical order is an input to it, not something it derives**: two
  subsequences of a total order do not determine that order, since `[id, family]`
  and `[id, given]` do not say which of `family` and `given` comes first.

  That input is **not a flat ordering**. The merge recurses, so it needs the
  canonical order for whatever type it has descended into, at a depth its
  operands determine rather than one known statically. FHIR's definition graph is
  cyclic — extensions are self-recursive, and a reference carries an identifier
  that carries a reference — so the expanded schema tree is infinite. Dense mode
  bounds it by configuration and pruned mode by data, but the merge must reach
  whatever depth the operands actually carry. The input is therefore a **lazily
  navigable canonical structure**, answering two questions at any node: the field
  order here, and the structure under a given field name. Navigation on demand is
  what makes an infinite tree representable without forcing an expansion the
  operands never ask for.

  The interface is declared in `utilities` beside the merge, so `encoders` sees
  the interface and never `fhir-schema`. The implementation over the definitions
  sits beside `SchemaBuilder` in `fhir-schema`, which already walks the same
  cyclic graph and must decide the same positions, so derivation and merging
  cannot drift. Canonical order covers the layout's own fields as well as the
  definition's, since the annotations and the metadata groups beside primitives
  have positions and no definition element (FR-057).
- **Measurements that constrain this** (findings 16–17):
  - `concat` and `array_union` over `array<struct<id,family>>` and
    `array<struct<id,given,period>>` fail with `DATA_DIFF_TYPES` and
    `BINARY_ARRAY_DIFF_TYPES`. A by-name projection of both into the merged type
    then succeeds, with correct values and traversable results.
  - `array<void>` — an absent operand — combines with anything and yields the
    other side's type, so absence needs no reconciliation.
  - Field **order** is part of the type: `struct<id,family>` and
    `struct<family,id>` have no common type.
  - Structure **equality** does not fail on divergent order. It coerces
    positionally and ignores names, so
    `named_struct('id','X','family','Y') = named_struct('family','X','id','Y')`
    is **true**. Inconsistent field order is therefore a silent wrong answer, not
    an error, which is what makes canonical order a requirement rather than a
    convention.
  - A same-arity struct cast reorders positionally with no error, which is why
    reconciliation must project by name and never cast.
  - Folding the binary form gives the same type and the same values as one flat
    n-way merge — verified — *because* canonical ordering makes the merge
    commutative and associative on types. The expression is variadic anyway, so
    both call shapes are available: binary operators fold naturally from the
    parse tree, and a flat n-ary site passes the whole operand list.
- **Alternatives considered**: reconciling by cast (silently wrong, above);
  widening every complex element to its definition-derived shape (reintroduces
  the dense shape and its nesting bounds into the fitted path); leaving the
  operations to fail (a semantics regression — `combine` must work).

## R-017 How the engine is reconciled with a fitted schema

Three approaches were considered. **D is chosen**; A and C are recorded as future
options at the user's direction, and remain compatible with D because the
representation of absence (R-005) and the reconciliation mechanism (R-016) are
the same under all three.

- **D — tolerant expressions (chosen)**. Traversal and reconciliation resolve
  inside Spark, after the input schema is known. Fewest API changes: the
  expression-to-column API keeps its exact signature and its
  dataset-independence. Costs two additions to the query-time expression toolkit
  and carries R-005's residual analyzer risk.
- **A — evaluation-time binding (future option)**. The evaluator takes the input
  schema and decides absence when the expression is built.
  `DatasetEvaluatorBuilder.withDataset` already exists, so this is plumbing a
  `StructType` through `ResourceResolver` rather than a new abstraction. Rejected
  for now because a column built against one schema is silently wrong when
  applied to a wider one, and because it would require an overload on the
  column-returning API.
- **C — unbound column representation (future option)**. The engine returns an
  object describing the computation, bound to a schema and a column when the
  dataset is known — a reader over the local data type, which the 120
  `getValue()` call sites lift pointwise. Needs no Catalyst at all, makes every
  schema-dependent decision explicit in one place, and would additionally yield a
  required-schema manifest and therefore Pathling-level error messages. Rejected
  for now because it changes the artefact type, so it does not preserve the
  column-returning API, and because it is a much larger change for behaviour D
  already delivers. It is the fallback if R-005's residual risk materialises, and
  it can be layered on later without redoing traversal.
- **Also considered and rejected outright**: a custom analyzer resolution rule
  requiring session extensions (intrusive for a library); a UDF over the parent
  structure (its input type must be declared statically, which is the knowledge
  being sought); `to_json` with `get_json_object`, and variant-backed traversal
  via `variant_get` — the only route to genuine agnosticism with no schema
  knowledge at all, and tied to the ingest-mechanism benchmark rather than to
  this question; and a schema floor (R-006).
