# Decisions — Parquet on FHIR spec (issue #2367)

Settled during the spec interview. Supplements
`openspec/parquet-on-fhir-design.md`; where they conflict, this file wins and the
design document is to be corrected.

## Scope

1. **Engine migration is schema-only.** The FHIRPath execution engine migrates to
   the Parquet on FHIR layout — sparse schemas, absent elements, annotation-free
   files, new decimal/quantity/coding/reference/extension conventions. The
   query-time Catalyst toolkit (`StructProduct`, `IfArray`, `TransformTree`,
   `RowIndexCounter`, `TraceExpression`) is relocated, not rewritten. Decision 1
   of the design register stands.
2. **This spec supersedes the OpenSpec changes** for this programme. It restates
   everything, including the five already-proposed changes; those directories
   are abandoned.
3. **Downstream boundary is `library-runtime` plus the Python and R bindings**,
   and the docs under `site/docs/libraries`. The server catches up separately per
   the pin strategy.
4. **Server pin is capped at library 10.0.0.** The convention changes are
   breaking for data at rest, so the library takes a major bump at the first
   convention change; the server pins at the last 9.x and must reach 10.x before
   the 9.x line stops receiving fixes.

## Absent elements and result typing

5. **Simplest approach first: a missing field yields a static `EmptyCollection`.**
   Untyped, `void` in SQL. Accepted knowingly, with the consequences documented.
6. **Shareable views carry an explicit `type`,** and a view column that declares
   a FHIR type must be cast to that type on output. This does **not** work today:
   `ProjectedColumn.getValue()` casts only at precedence level 1 (an explicit
   `sqlType` tag), never at level 2 (the declared FHIR type). It becomes a
   functional requirement.
7. **`getSqlType()` keeps throwing** when no type information is available, with
   a better message naming the column, the path and the remedy. Reached only via
   `RepeatSelection`; `FhirViewExecutor` uses `getSqlTypeHint`, level 1 only.
   Consequence: an undeclared column over an absent element fails on sparse data
   where it works on dense.
8. **A void column can escape through `fhirPathToColumn`** and
   `CollectionDataset.getValueColumn()`/`toIdValueDataset()`. Documented in Java,
   Python and R: unwritable to Parquet and ORC, omitted in JSON.
9. **Sibling column combination through `StructProduct` must tolerate a void
   column** — the hazard `RepeatSelection`'s `expectedElement` was introduced to
   avoid. Needs test coverage.
10. **Follow-up issue after the initial implementation:** typed absent elements —
    definition-derived typing at the boundary, `struct<id:string>` for complex
    results, and the resulting cross-mode type guarantee.

## Schema

11. **No minimum-field floor.** Prune to populated elements only: a complex
    element appears only if some descendant leaf is populated, so a fieldless
    struct never arises and the Parquet/JSON empty-schema failure is unreachable.
12. **Egress prunes all-null structs and null-only arrays.** Any struct whose
    fields are all null in a row serialises as `{}`, and an array of nulls as
    `[null]`; both break the round trip. Derived from the losslessness contract,
    not a new decision.

## Nested schema pruning row drop

13. **Not a defect for Pathling's engine, and no mitigation machinery.**
    Reproduced on raw Parquet and Delta, but Pathling never calls `explode`;
    `UnnestingSelection` uses a whole-element `transform` lambda and defers a
    single `inline` to the end of the projection, which blocks the leaf pruning.
14. **It becomes a design constraint**: unnesting uses whole-element lambdas and
    must never be reshaped into leaf-level pushdown. Pinned by a regression test
    over a deliberately divergent fixture, confirmed against the real engine
    rather than the SQL emulation.
15. **Documented for direct-SQL consumers** of Pathling-written tables, who are
    still exposed if they write their own `explode`.

## Decimals

16. **Query-time precision stays `DECIMAL(32,6)`.** Storage becomes lexical
    `STRING`, so the FHIR-motivated cap now applies only to computation.
    `__x_numeric` is a decode fast path, not a semantic change.
17. **Ingest: approach (a) for the initial implementation** — JSON reader,
    SQL-transformed into the PoF schema. Lexical decimal preservation on the
    `Dataset<String>` path is a **documented limitation**.
18. **File the Spark issue** — a SPARK-48148 follow-up with the repro in
    `scratchpad/decimals.py`: the exact-string-parsing fix covers only byte-array
    and `PositionedReadable` content references, so `json(Dataset[String])` and
    `from_json` still re-serialise, routing numbers through a double.
19. **Follow-up issue: the quoting pre-pass.** Quote number tokens at
    decimal-typed paths before parsing, which is unconditionally exact. Requires
    a definition-derived path matcher; a blanket "quote every number" does not
    work, because Spark rejects a quoted number against an `INT` target.
20. **Approach (b) stays on the table and is benchmarked.** Direct
    `String -> PoF variant` parsing with `VariantBuilder` is logically better —
    lossless on every route, one parse pass, detects non-conformant content in
    the same pass, and decouples parsing from schema determination — but its
    performance is unknown. After the initial implementation, compare (a) and (b)
    on performance and make the final decision then.

## Annotations

21. **Decimal, date and quantity annotations are emitted by default,** each
    individually disableable through the annotation processor registry. The
    engine must compute correctly with any or all absent, asserted by running the
    suite over annotation-free files.

## Primitive extensions

22. **FHIRPath navigation of `_field { id, extension }` is in scope** — a new
    engine capability with its own user story and tests.

## Done criteria for the engine migration

23. The `fhirpath` suite, both YAML conformance baselines and the SQL-on-FHIR
    compliance suite pass in sparse mode, with the curated dense subset green;
    the exclusion baselines gain no new entries and obsolete entries are removed;
    the driver-2 round-trip harness passes over the R4 spec examples and a
    Synthea corpus. HAPI fixtures reach Spark through the category-C path, so the
    fluent builders survive unchanged.

## Corrections to the design document

24. Storage today is `DECIMAL(32,6)` (`DecimalCustomCoder`), not `DECIMAL(38,6)`.
    38 is `FlexiDecimal.MAX_PRECISION`, the unscaled quantity type.
25. The nested-pruning open risk does not apply to Pathling's engine, and the
    anchor-leaf mitigation as written was insufficient anyway — a retained but
    unrequested leaf is never read.
26. The first argument against schema inference — a repeating element inferring
    as struct versus array of structs — does not hold for FHIR JSON, which always
    represents a repeating element as an array. The other two arguments stand.

## Late additions

27. **Read with an inferred schema, then SQL-transform to the definition-derived
    target.** Inference never determines the stored schema, so decision 4 holds:
    types, cardinality and conventions are imposed by the transform. Presence for
    pruning falls out of the inferred schema.
28. **Reference keys are computed at query time** from the conformant `id` and
    `meta.versionId`. The old `id_versioned` existed for join performance; if the
    join benchmark shows it is needed, it returns as a PoF **annotation** under
    the annotation naming convention — a fast path the engine can do without —
    never as a bare synthetic column.
29. **`mergeSchema` on by default.** `DeltaSink` appends with it, or a divergent
    second batch fails with `DELTA_FAILED_TO_MERGE_FIELDS`. `ParquetSource` reads
    with it, with an opt-out, because without it the lexicographically first
    file's schema wins and the rest are silently null-filled. The driver-4 gate
    (mergeSchema cost over a realistic file count on object storage) can force a
    fallback.
30. **Sibling resolution is one mechanism for both primitive extensions and
    annotations.** A collection traversed to a primitive retains a handle on its
    parent struct and element name; `.id` and `.extension` resolve `_<field>`,
    and the annotation fast paths resolve `__<field>_numeric`,
    `__<field>_start` / `__<field>_end` and the quantity canonical annotation the
    same way. Primitives never asked for either are untouched. Because absence is
    decided by the schema rather than the data, the fast-path-versus-compute
    choice is made at plan time.
31. **Assumption (not grilled, routine):** `EncodingConfiguration` gains the
    schema mode, the non-conformant-content switch (ignore/fail) and the
    per-annotation toggles. `max_nesting_level`, `enable_extensions` and
    `enabled_open_types` keep their current semantics for the dense schema and do
    not apply to the pruned one, per design decision 9.

## Structure (revised — supersedes the module plan in the design document)

32. **The `encoders` split is deferred and the existing encoders stay intact.**
    Separating the query toolkit from the HAPI bridge, and retiring the bridge,
    is not part of this work. What becomes of `encoders` depends on what the
    data-migration tooling needs, since that tooling may have to use it. The
    query toolkit stays where it is; `spark-toolkit` is not created.
33. **The definition abstraction still moves down** into a new low module, which
    also holds schema derivation. Moving definitions out of `fhirpath` was never
    the part being deferred.
34. **The new encoding goes into its own new `io` module**, beside `encoders`.
    Build order: `utilities -> fhir-schema -> {encoders, io} -> terminology ->
    fhirpath -> library-api`, with `fhirpath` depending on all three. Nothing is
    retired and nothing is split — only additions plus one package move.
35. **Catalyst enforcement is two mechanisms.** `fhir-schema` resolves
    `spark-sql-api` only, with a build rule banning `spark-catalyst` and
    `spark-sql`. The `io` module needs `spark-sql` for column expressions, which
    pulls Catalyst in transitively, so it gets a build rule failing on any
    `org.apache.spark.sql.catalyst` import instead.
36. **Flag day, not dual-layout.** On completion the engine reads only the new
    layout. The old encoders remain for migration tooling but the engine does not
    read what they write. The test estate therefore moves in one step, via the
    category-C path (object -> JSON -> read with the derived schema), so the
    fluent builders survive unchanged.

## 37. Absent-element representation — supersedes decision 9

Decision 9 recorded "statically return `EmptyCollection` on missing fields", with
an untyped column as the accepted consequence. **Superseded.** A statically pruned
column is valid only for the schema it was built against and returns empty on a
wider dataset that does carry the element, which breaks the contract that an
expression becomes a column with no dataset in hand.

Replaced by a tolerant traversal expression resolving after the input schema is
known, with absent elements typed per finding 15. Recorded as R-005 in
`../research.md`, with the two rejected bindings —
evaluation-time binding, and an unbound column representation — recorded as
future options in R-017 at the user's direction.

Consequences: typed absence for primitives, so the untyped-column problem narrows
to absent *complex* elements only; and shape reconciliation becomes a requirement
in its own right (findings 16–17, R-016), because two collections of one FHIR type
reached by different paths have different fitted shapes.
