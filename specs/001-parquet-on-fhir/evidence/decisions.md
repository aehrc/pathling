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

## 38. Milestone sequencing — the public API switches with the engine, not before

The task list is organised into four milestones: the layout (M1), bundles and XML
(M2), the engine (M3), polish (M4). M1 and M2 change nothing a user can observe.

This **fixes a defect in the earlier sequencing.** T070, T081 and T082 rewire
`PathlingContext.encode`, `PathlingContext.decode` and `NdjsonSink` to the new
layout, and they sat in US1 and US2 — before US3 moved the engine. The Phase 4
checkpoint said in as many words that the engine could not yet read what had just
been written, while the Implementation Strategy called Setup + Foundational + US1
+ US2 "a complete, defensible increment". Both cannot be true: with the public
encoder writing the new layout and the engine reading the old one, every
encode-then-query path is broken for the whole of that window. The three tasks
move to M3 and land after US4, so the encoder never writes what the engine cannot
read.

Consequences for the other blocks:

- **The definition abstraction (T010–T019) and schema derivation (T028–T033) stay
  in M1.** The ingest transform writes *into* the derived schema, and FR-008 and
  FR-012 forbid taking types or cardinality from the data, so there is no encoding
  milestone without them.
- **The structure merge and canonical ordering (T038f–T038i) move to M1**, beside
  `SchemaBuilder`. They are pure structure mechanics over `spark-sql-api` types
  with no engine dependency, and keeping them there means derivation, reconciliation
  and file merging share one notion of canonical field order by construction rather
  than two that can drift. Only the two Catalyst expressions — `ResolveOrNull` and
  `MergeCast` — remain in M3.
- **US5 stays in M3** even though its merge dependency is now in M1: its subject is
  batches arriving over time through the public write path, which does not exist
  until T070.
- **The T038a gate is unchanged in substance but cheaper.** Nothing in M1 or M2
  depends on tolerant traversal, so a failure costs less than it did. It is still
  worth running as a spike during M1, being the programme's largest unknown.

Task IDs were frozen through the restructuring, so both traceability tables are
unaffected and the ID set is identical to the pre-restructuring one.

### Addendum to 38 — layout detection moves with the flip, not ahead of it

US7 was first placed in M1, carrying the previous plan's rationale that it should
"exist before there is anything for it to catch". That rationale is wrong under the
milestone structure, and inverted. The detector rejects the layout an earlier
release wrote — which, until the public API switches, is the only layout anyone has
and the one the engine still reads. Wiring it in during M1 would reject every
existing user's data for being exactly what the M1 engine expects. Before the flip
there is nothing for it to catch *and* it would catch the wrong thing.

US7 is therefore sequenced immediately before the public API switch. It still
depends only on Setup and can be built at any time; it must not be wired in
earlier. This also keeps M1 within the stated scope — the layout and the encoding
to and from JSON — since a read-path guard is neither.

## 39. `encoders` is extended, not unmodified — correcting an overstated claim

T128 asserted `git diff main -- encoders/` is empty. That could never have held:
T038e and T038l add `ResolveOrNull.scala` and `MergeCast.scala` under
`encoders/src/main/scala/au/csiro/pathling/sql/`, which is the whole point of
FR-052 permitting the query-time toolkit to be extended. The assertion is narrowed
to what FR-051 actually requires — the HAPI bridge, the schema converter and the
existing expressions unchanged, and the module still resolving at its coordinates.

`spec.md` was already correct: FR-051 scopes to the *encoding implementation*, and
FR-052 says the toolkit "MAY be extended". `plan.md` was not, describing `encoders`
as `UNCHANGED` and "neither renamed, split nor modified"; corrected to name the two
additions. This is a specification correction surfaced by the resequencing, not a
consequence of it.

## 40. M3 runs without a green build, and that is accepted

Two overlapping spans leave M3 red from Phase 8 to Phase 12.

T037 defaults the test framework to pruned in Phase 8, while T110 in Phase 10 is
what makes traversal to an absent element yield empty. A schema fitted to a small
fixture set omits most of each resource, so between those points the FHIRPath DSL
suite, both YAML conformance baselines and the SQL-on-FHIR compliance suite meet
Spark analysis failures rather than empty collections.

Phase 9 converts the engine by replacement, not addition: after T095 it cannot
read a previous-layout decimal, and after T097 it cannot reach an extension
through a field identifier. The public API keeps writing that layout until Phase
12, so every `library-api` test that encodes and then queries is broken across
Phases 9 to 11.

Two alternatives were put and not adopted. Running the migration in dense mode —
where the derivation is unpruned and so carries no absent elements beyond the
nesting and open-type bounds that already constrain the encoder — would have
allowed the public API to switch at the end of Phase 9 and given M3 a green,
releasable interior checkpoint. It was not adopted; the author accepted the red
window instead. Keeping the engine dual-layout across the window was ruled out
earlier, by the assumption that the engine reads only the new layout on completion
and the test estate therefore moves in one step.

*Consequence*: a Phase 9 failure cannot be distinguished from a Phase 10 failure
by the build. The programme still lands as several pull requests, as `plan.md`
says, but none of them falls inside Phases 8 to 12: that span is one unreleasable
piece.

## 41. The Delta upsert path widens the target, reversing a deliberate guarantee

`DeltaSink`'s upsert path refuses to widen the target schema today, and
`NarrowMergeTest` asserts it: a narrower source merges leaving the target's extra
columns null, and a wider source fails on the struct mismatch. The test's own note
says the refusal is what keeps the tolerance from becoming schema evolution the
caller did not ask for.

That reasoning assumed a schema derived from encoder configuration and therefore
stable between batches. Under a fitted schema it is derived from the data, so two
batches diverging is the steady state and the refusal would fire on ordinary use.
FR-041 and FR-042 cover read and append only, so without this decision append
would silently widen while upsert refused — two answers to the same question.

T117a enables schema auto-merge on the upsert path. T100d rewrites
`NarrowMergeTest` against the new behaviour.

## 42. There is no rollback after the switch, and none is possible

Phase 12 is described as the point of no return, and the question was raised of
shipping a configuration that kept the previous layout writable, since the old
encoder remains in the build.

There cannot be one. FR-053 has the engine reading only the new layout after the
switch, so a configuration that kept writing the previous layout would produce
data that same release could not query. The rollback mechanism is the one users
already have, which is pinning to the previous library version.

Recorded here so that the absence of a switch reads as a consequence of FR-053
rather than as an omission. It belongs in the release note.

## 43. Canonical order is a navigable structure, not a flat ordering

R-016 established that the canonical field ordering is an input to the structure
merge rather than something it derives, because two subsequences of a total order
do not determine that order. The shape of that input was left as "the field
ordering", which is wrong.

The merge recurses, so it needs the canonical order for whatever type it has
descended into, at a depth its operands determine rather than one known
statically. FHIR's definition graph is cyclic — extensions are self-recursive, and
a reference carries an identifier that carries a reference — so the expanded
schema tree is infinite. Dense mode bounds it by configuration and pruned mode by
data, but the merge must reach whatever depth the operands actually carry. A flat
list orders one level and leaves every level beneath it in discovery order, which
passes a single-level test and is wrong.

The input is therefore a lazily navigable canonical structure, answering two
questions at any node: the field order here, and the structure under a given field
name. Navigation on demand is what makes an infinite tree representable without
forcing an expansion the operands never ask for.

*Where it lives*: the interface is declared in `utilities` beside the merge, so
`encoders` sees the interface and never `fhir-schema`, so the rule that
`encoders/pom.xml` is not modified (T009, FR-051) is satisfied without a
trade-off. The
definition-backed implementation sits beside `SchemaBuilder` in `fhir-schema`,
which already walks the same cyclic graph and must decide the same positions, so
derivation and merging cannot drift into two notions of canonical order.

*Consequence for the specification*: FR-057 defined canonical order as definition
order restricted to the fields present, which leaves the layout's own fields —
the annotations, and the metadata group beside a primitive — without a position.
They have no definition element, so two implementations could both claim to be
canonical and still produce structs that compare positionally wrong, which is the
failure FR-057 exists to prevent. FR-057 now fixes their positions relative to the
element they accompany. FR-058 needed no change, since it only ever required a
recursive field-wise union.

## 44. Non-conformant content is ignored by default, and the mode is a flag

`contracts/library-api.md` left the default of the strictness switch to the
implementation and required it to be stated. It is **ignore**, with failing as
the opt-in.

The contract enumerates the behaviour changes a caller must be told about, and an
ingest that begins failing on content the previous implementation accepted is not
among them. The previous implementation parsed leniently, FR-043 preserves the
encoding signatures and SC-008 rules out an incompatible public API change, so a
fail-by-default switch would break working pipelines on upgrade under a
requirement set that promises the opposite. Ignoring is not silent: FR-018 forbids
silent truncation in either position, so ignored content is still detected.

The schema mode is carried as a flag defaulting to the fitted schema, rather than
as a type of its own. The derivation distinguishes the two by which of
`SchemaBuilder.dense` and `SchemaBuilder.pruned` is called, so the choice is
already binary there, and a parallel two-valued type would be a second
representation of the same thing. A top-level enumeration would not violate the
letter of the coding conventions — `TerminologyMode` is one — but it would add a
representation the derivation does not take.

*Consequence for the specification*: the Strictness row of
`contracts/library-api.md` now states the default. This also supersedes the
remainder of decision 31, which still reads as though `EncodingConfiguration`
gains these options; the Key Entities note in `spec.md` and T033 already place
them on a new surface beside it, because FR-051 forbids modifying the module that
class lives in.

## 45. A choice expands to the types FHIR declares, not to the definition library's aliases

A choice that admits a reference is declared in FHIR as `Reference(X|Y|Z)`, one
type. The definition library accepts a name per target — `productMedication`,
`productSubstance` — and a further name for an untyped resource,
`productResource`, all of them aliases for the same reference. The expansion took
the full set of valid names, so `ActivityDefinition.product` became five fields
where FHIR declares two.

**The expansion is now the declared type list.** A declared class the library
reports as a reference target contributes one `<element>Reference` field, at the
position of the first such target; the targets that follow contribute nothing. A
class that is not a target takes the name the library gives it. The untyped
resource alias appears in no declaration and so is never emitted.

The alternative was to keep the aliases, which costs three dead columns per
reference-bearing choice in every stored structure, forever, and puts names into
the layout that no FHIR instance can populate. Field order is part of the type
(FR-057), so a column emitted once is emitted for the life of the layout. There
is no reading of the data that would fill them.

*Consequence beyond the layout*: `ofType()` and `as` over a reference-bearing
choice now see `Reference` once rather than the target resources. That is the
FHIR-declared surface, so it is the surface the engine should present.

*What holds the rule*: the sweep over every choice in every R4 resource computes
its oracle from the definition library alone — the valid names, less the target
aliases, less the untyped resource alias — rather than from the expansion under
test, so it fails if the collapse drops too much as readily as if it drops too
little. The name-building fallback that used to hide a type the library maps to
no name is gone; such a type is now an inconsistency in the definitions and
raises. Open types need no special handling: `Extension.value[x]` declares
`Reference` itself, reports no reference targets, and has no untyped-resource
alias, so the rule leaves all 59 of its variants in place.
