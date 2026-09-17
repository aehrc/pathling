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
    `__<field>_start` / `__<field>_end` and the quantity canonical annotation
    (`__<field>_canonical_exact`, per decision 46) the same way. Primitives never
    asked for either are untouched. Because absence is decided by the schema
    rather than the data, the fast-path-versus-compute choice is made at plan
    time.
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
to absent _complex_ elements only; and shape reconciliation becomes a requirement
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

- US2 "a complete, defensible increment". Both cannot be true: with the public
  encoder writing the new layout and the engine reading the old one, every
  encode-then-query path is broken for the whole of that window. The three tasks
  move to M3 and land after US4, so the encoder never writes what the engine cannot
  read.

Consequences for the other blocks:

- **The definition abstraction (T010–T019) and schema derivation (T028–T033) stay
  in M1.** The ingest transform writes _into_ the derived schema, and FR-008 and
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

### Addendum to 38 — the milestone labels in this entry are superseded

The core finding stands: the public API must not write what the engine cannot
read, so T070, T081 and T082 land after the engine, never before it. Everything
below about where the definition abstraction, the derivation and the structure
merge belong stands too.

What has changed is the numbering and the placement of two blocks. There are now
six milestones — the layout (M1), the engine made layout-tolerant (M2), ingest
formats (M3), the flip (M4), the gaps (M5), completion (M6) — so every "M2",
"M3" and "M4" in this entry refers to the earlier four-milestone scheme. Two
statements above are also overtaken:

- **US5 no longer waits for the public write path.** It moves to M2, ahead of the
  flip, because divergent fitted schemas arise the moment the new layout is
  written; the batches are written through `io`'s own entry points until M4.
- **"Nothing in M1 or M2 depends on tolerant traversal"** was true of the old M2,
  which was bundles and XML. The new M2 is the engine, and it does depend on it.
  T009a resolved the gate in Phase 1 regardless, and it passed.

See the addendum to decision 40 for why the sequencing was revised.

### Addendum to 38 — layout detection moves with the flip, not ahead of it

US7 was first placed in M1, carrying the previous plan's rationale that it should
"exist before there is anything for it to catch". That rationale is wrong under the
milestone structure, and inverted. The detector rejects the layout an earlier
release wrote — which, until the public API switches, is the only layout anyone has
and the one the engine still reads. Wiring it in during M1 would reject every
existing user's data for being exactly what the M1 engine expects. Before the flip
there is nothing for it to catch _and_ it would catch the wrong thing.

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

`spec.md` was already correct: FR-051 scopes to the _encoding implementation_, and
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

_Consequence_: a Phase 9 failure cannot be distinguished from a Phase 10 failure
by the build. The programme still lands as several pull requests, as `plan.md`
says, but none of them falls inside Phases 8 to 12: that span is one unreleasable
piece.

### Addendum to 40 — reversed. The red window does not occur

**This decision no longer holds, and the sequencing it described has been
replaced.** Both spans are designed out rather than accepted.

The second alternative it records — keeping the engine dual-layout across the
window — was ruled out on the assumption that the engine reads only the new
layout on completion and the test estate must therefore move in one step. That
assumption was not wrong, but it was applied too early: FR-053 constrains the
_end state_, and nothing required the transition to reach it in a single step.
Dual-layout reading then turned out to be cheap, for the reasons in decisions 47
and 48, and FR-053 now states the transitional permission explicitly.

So the engine converts by addition, the test framework gains _two_ dimensions
rather than one, and every milestone ends green with none running red in the
middle. **This addendum originally named only the schema mode**, which was not
enough: it conflated two axes and left a third unstated. Decision 52 separates
them. M2 carries the engine rewrite behind a green build, and the
switch in M4 is a writer flip.

The author accepted the red window when the alternative looked expensive, and
revised that on evidence when it did not. The cost that replaces it is recorded
in decision 51.

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

### Addendum to 42 — reversed in part. There is a rollback until M6

The reasoning held only while the engine read exactly one layout. From M2 it
reads both, so data written in the previous layout remains queryable by the
release that switched, and pinning the writer back is a genuine escape hatch
rather than a way to produce data the release cannot read.

Two limits. The hatch closes at T100e, when the previous-layout reader is removed
and FR-053 reaches its end state. And what it protects is the _engine_, not the
data: a warehouse written after the flip carries no primitive ids or extensions
until M5 (decision 49) and no annotations until then either (decision 50), so
rolling the writer back does not recover content that was never stored.

Version pinning remains the coarser mechanism, and is still what the release note
should lead with.

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

_Where it lives_: the interface is declared in `utilities` beside the merge, so
`encoders` sees the interface and never `fhir-schema`, so the rule that
`encoders/pom.xml` is not modified (T009, FR-051) is satisfied without a
trade-off. The
definition-backed implementation sits beside `SchemaBuilder` in `fhir-schema`,
which already walks the same cyclic graph and must decide the same positions, so
derivation and merging cannot drift into two notions of canonical order.

_Consequence for the specification_: FR-057 defined canonical order as definition
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

_Consequence for the specification_: the Strictness row of
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

_Consequence beyond the layout_: `ofType()` and `as` over a reference-bearing
choice now see `Reference` once rather than the target resources. That is the
FHIR-declared surface, so it is the surface the engine should present.

_What holds the rule_: the sweep over every choice in every R4 resource computes
its oracle from the definition library alone — the valid names, less the target
aliases, less the untyped resource alias — rather than from the expansion under
test, so it fails if the collapse drops too much as readily as if it drops too
little. The name-building fallback that used to hide a type the library maps to
no name is gone; such a type is now an inconsistency in the definitions and
raises. Open types need no special handling: `Extension.value[x]` declares
`Reference` itself, reports no reference targets, and has no untyped-resource
alias, so the rule leaves all 59 of its variants in place.

## 46. Both canonical quantity annotations are emitted, under one toggle

Decision 21 settled that a quantity carries a canonical annotation. It was then
read as a choice between the specification's `__<field>_canonical` and a
magnitude-preserving one of Pathling's own, and the second was chosen, because
the specification types its value as a fixed-point decimal whose absolute
precision is constant regardless of magnitude: canonicalisation shifts magnitude
by arbitrary powers of ten, so a nanogram rounds to zero and quantities differing
by orders of magnitude compare equal. That reasoning is intact, but it only ever
argued that the specification's form cannot serve the engine. It never argued
that a file should not carry it.

**Both are emitted**: `__<field>_canonical`, the specification's, at the type the
specification gives it; then `__<field>_canonical_exact`, Pathling's, carrying
the same canonicalisation without the fixed scale. The engine reads the second.
An interchange consumer reading the specification's layout finds the first, under
the name the specification gives it, which it would not if Pathling emitted only
its own. The cost is one further column beside a quantity, which is small against
being unreadable to every other implementation of the layout.

**The order is spec form first**, and it is stated rather than left to fall out
of the code. Field order is part of the type (FR-057), so a consumer comparing
structures positionally depends on it, and interchange-first puts the
specification's annotation where a reader of the specification expects it.

**One toggle governs the pair.** Under FR-021 the individually disableable unit
is the annotation _kind_ — decimal, date, quantity — and these are one kind in two
representations, as the date range annotation is one kind in two fields. A caller
disabling the quantity annotation is saying it does not want the canonicalisation
stored, not choosing between two spellings of it. Splitting into two toggles
remains available additively if an interchange-only consumer ever asks for the
specification's form without the engine's.

_The shape of the exact form_: the previous Pathling layout carried
canonicalisation in two fields, `_value_canonicalized` and `_code_canonicalized`
— value and base unit code. A single `__<field>_canonical_exact` slot must
therefore carry the unit as well as the value; on the value alone, one metre and
one second compare equal. The Spark type is settled with the encoder (T066), but
that constraint is fixed here, not left to be rediscovered.

_Consequence for the specification_: FR-005 required the specification's
annotation to be absent and now requires both. The deviations table in
`contracts/storage-layout.md` records an addition rather than an omission, the
capability-regression rows in `evidence/encoder-scope.md` are resolved rather than
live, and `contracts/engine-semantics.md` says which of the two the engine reads.

## 47. The engine converts by dispatch, not by replacement

The migration of the engine was sequenced as a replacement: Phase 9 would move
decimal decoding, quantity handling, extension access and reference keys to the
new layout, losing the previous one as it went. That is what made the window in
decision 40 unavoidable.

It is not necessary. The tolerant traversal expression T009a proved is already a
dispatcher — its `replacement` is a `lazy val` matching on the resolved child's
`dataType`, choosing a field reference where the field is present and a typed
null where it is not. Branching on which _layout_ a column is in is the same
mechanism with a richer match, over discriminators that are all present in the
resolved schema: a `DECIMAL(32,6)` value with a `_scale` companion against a
lexical string, `_value_canonicalized` against `__<field>_canonical_exact`, a
`_fid` field and a root `_extension` map against inline extensions, a stored
versioned key against its absence.

_The rule that keeps this sound._ Every branch of a dispatching replacement MUST
yield the same `dataType`. That is what keeps T108's fitted-versus-dense
equality true and therefore FR-054. **This entry originally weakened the rule
across layouts**, requiring only agreement on the FHIRPath-visible type, because
the extension structures genuinely differ in shape and the difference was
absorbed as traversal continued. Decision 55 removes the need for that: the
previous layout is normalised to the new layout's shape inside the expression,
so the strong form holds everywhere. Two layouts never meet inside one query, so
FR-056 reconciliation is unaffected.

_Where the dispatch lives — superseded._ This entry placed it in the engine's
collection classes. Decision 55 moves it into the traversal expression, which is
the same mechanism applied at one site rather than six.

_The rule this inherits._ FR-023 requires an annotation's use to be decided from
the schema rather than per row. Layout dispatch is held to the same standard: it
resolves once per schema, at analysis time, never per row.

## 48. Old-layout extension access collapses into one expression

Supporting both layouts looked dearest for extensions, because the previous
mechanism is structurally unlike the new one: a `_fid` on each element and a
root-level `_extension` map, against inline nesting.

Inspection says otherwise. `_extension` is named in exactly one file in the whole
engine, `ResourceCollection`, and the access itself is one fixed shape in
`Collection.traverseExtension` — the root map, keyed by the element's own `_fid`.
There is nothing to keep alive beyond that one expression.

That the map is reached by ordinary traversal of the resource's own column,
rather than as a separate top-level column, is also what makes decision 55
possible: it puts the map within reach of the traversal expression's own child
chain, so normalisation needs no reference the expression cannot carry.

Better than that, supporting both is a net simplification. `extensionMapColumn`
is a constructor parameter on fourteen collection classes, carried for the single
reason `ResourceCollection`'s own javadoc gives: preserving the resource-level map
across copies with a different column representation, because the resource handle
is lost during traversal. T094 restores that handle — it was already required for
annotations and primitive metadata — so the map can be re-derived at the traversal
site and the parameter removed. T094a does this, and it is layout-independent: it
can land as soon as T094 does, and would be worth doing even if there were only
one layout.

## 49. Primitive ids and extensions are deferred past the flip, writing included

Both halves of FR-003's metadata group and all of FR-034 move to M5.

The reading half costs nothing to defer: the previous layout cannot represent
primitive ids or extensions at all, and the engine cannot navigate them today, so
there is no behaviour to regress. The spec already called this new capability
rather than migration.

The writing half is a real cost, and it is the author's decision taken with that
cost stated. FR-017 makes the pruned-schema round trip unconditional, and
`_birthDate: {id, extension}` is legal FHIR JSON, so not populating the group
means dropping content. The group itself is derived by `fhir-schema` from Phase 3
either way; it is T061, the transform, that is deferred — so on a pruned schema
the group prunes away for want of data, and on a dense one it is present and
null. FR-017 therefore carries a carve-out until T078c closes it.

_What bounds it._ FR-018 forbids silent truncation, so the carve-out requires the
loss to be **detectable**, on the same terms the dense-bounds clause already sets.
T057b asserts it and T067a implements it, both in M1. Without that pair the
carve-out would be an aspiration rather than a requirement.

_What makes it survivable._ Adding the metadata group in M5 changes struct shape
between files written before and after, which is exactly the divergence US5's
merge exists to handle, and US5 ships in M2, ahead of it. `MetadataGroupStructure`
is already built in `fhir-schema`, so nothing is wasted by the deferral — it is
simply not wired into the `io` transform until M5.

_What users should be told_: a warehouse written between the flip and M5 will
want re-importing at M5 if its sources carry primitive metadata. T042's remedy
already points at re-import; this adds a second occasion for it.

## 50. The flip lands annotation-free

The encoder emits no annotations at the switch. Each kind — the decimal numeric
annotation, the date bounds, the quantity canonical pair — lands afterwards in
M5, one at a time.

FR-022 is what makes this safe and is not weakened by it: an annotation is a fast
path and never required for correctness, so the engine computes every value from
the structure. That requirement was always going to be met; this decision only
means it is met _first_ rather than alongside.

_The consequence to plan around._ At the flip every annotated operation runs its
computed path — lexical decimal decoding, UCUM canonicalisation per row, computed
reference keys for joins — so the flip is where query performance is at its worst
and most honest. T136 therefore stops being a record and becomes the input that
orders M5. FR-033 already anticipates the specific case: if a precomputed
reference key proves necessary for join performance it is expressed as an
annotation, and that is a question T136 answers rather than guesses.

_What it stages._ FR-021 is met progressively and in full only at the end of M5,
and FR-023 lands with the first kind, since choosing a fast path presupposes one
exists. T089 changes character with it: the annotation-free suite is the ordinary
path at the flip rather than a special case, and the annotated case is what needs
per-kind coverage afterwards.

## 51. M2 builds code M6 deletes, deliberately

The dispatch arms decision 47 introduces are not a product feature by default.
Their purpose is to keep the build green while the engine is rewritten, and to
make the flip a writer flip rather than a migration. T100e removes them in M6.

This is the cost that replaces the red window, and it is worth naming rather than
discovering: some of the work in M2 exists to buy a green build and a safe flip,
not to ship. **Decision 55 shrinks it considerably**: the arms are now branches
of one expression rather than second paths through six files, so what M6 deletes
is a single site and the collection classes were only ever written once. What it buys in exchange is a true before-and-after over the same
data for T110 — the riskiest change in the programme — which the previous
sequencing could not offer, because under it there was no green baseline to
compare against.

_Open until T049a._ If the source boundary ends up routing earlier-layout data
rather than refusing it, the arms become a product feature, deserve product-grade
coverage, and T100e's scope shrinks accordingly. That is why T049a is a gate
rather than an ordinary task. **It has been moved to Phase 7**, at the head of
M2: it sets the coverage standard for the arms, so settling it in Phase 13 would
have decided in M4 how work written in M2 should have been tested. Its wiring
stays in Phase 13.

## 52. Three axes, not one, and the test framework carries two of them

Decision 40's addendum claimed the red window was designed out because the
schema mode became a dimension. That was right about the conclusion and wrong
about the reason, in a way that would have produced a red window anyway.

There are three independent axes, and an argument that covers one covers
neither of the others.

1. **Absence.** A field the definitions describe that the schema does not carry.
   Tolerant traversal (T110) is what makes it empty rather than an analysis
   failure.
2. **Conventions.** A field that is present _in a different shape_ — a lexical
   decimal against a fixed-precision numeric, an inline extension against a
   `_fid` and a root map, a quantity with or without its canonicalised
   companion. Tolerant traversal does nothing for these; the Phase 8 dispatch
   arms (T095–T098) do.
3. **Density.** How much of the layout a schema carries. This is the axis the
   addendum named, and the only one T037 addresses.

Two consequences, both now in the task list.

**Absence is not peculiar to the fitted schema, and T110 moves to Phase 7.** The
existing encoder already omits fields past its nesting bound, so the current
layout has absent fields too. The engine half-handles them today: the
`UnresolvedFallbackIfMissingField` trait is applied at the repeat directive and
the variant transform, but ordinary traversal calls `getField` bare, which is
why `FhirViewExtraTest` carries two excluded `forEach` cases under issue #2625.
Emitting the tolerant expression at every step closes that gap, so it is owed to
the current layout and belongs ahead of any fixture movement rather than two
phases behind it. T101–T107 and T110–T113 move with it.

**Conventions need their own dimension, T037a.** Routing the fixtures onto the
JSON path is what puts them on the new conventions, and the engine does not
learn those until Phase 8. Moving them first would not produce a loud failure;
it would produce a silent one, a decimal comparison comparing strings. So the
layout is a dimension defaulting to the previous conventions, opted into per
test as each dispatch arm lands, and flipped at T100g.

_Consequence_: T037 may keep pruned as its default, but only because T110 now
precedes it in the same phase. Under the previous ordering that default was the
unsafe one, and the addendum's own argument required the opposite.

## 53. Layout detection covers the write path, not only the read path

FR-037 and SC-006 as written name sources. The detector tasks followed them and
touched `FileSource` and `CatalogSource` only.

That leaves the write path open, and decision 41 is what makes it matter: with
auto-merge enabled on append and upsert, a write into a table holding an earlier
layout no longer fails cleanly. Where column types conflict it fails with a
storage-layer schema error rather than the actionable message FR-037 promises.
Where they do not — a resource type carrying no decimals, for instance — it
succeeds, and the merge leaves one table carrying marker fields from both
layouts.

That hybrid is the case decision 47's soundness argument assumes cannot arise,
since it holds that two layouts never meet inside one query. Nothing enforced
it. FR-037a now states the write-side obligation, T046a applies the check before
the merge is attempted, T042a asserts it, and T039a covers the mixed schema that
none of T039's four cases reached.

## 54. Two missing-field mechanisms, and a gate to choose between them

T038e adds a `RuntimeReplaceable` that resolves a traversal to a field reference
or a typed null. `UnresolvedFallbackIfMissingField` already does a similar job
in the same file, reached as `nullIfMissingField` and `emptyArrayIfMissingField`
from the repeat directive and the variant transform.

Keeping both without deciding which owns which site would leave two answers to
one question in the module FR-052 protects. The choice is also not cosmetic: the
existing mechanism works by catching an `AnalysisException` inside `mapChildren`,
which is precisely the analyzer-ordering fragility T038d exists to guard against,
whereas the new one is a declared replacement the analyzer resolves normally.

T110a settles it before T110 lands, and records which sites each mechanism owns.

## 55. The two layouts meet inside the traversal expression, and nowhere else

Decision 47 established that the engine converts by dispatch rather than
replacement. It placed the dispatch in the engine: the decimal collection, the
quantity collection and encoding, the base and resource collections, and the
reference machinery each learned to read both layouts. That is four sites, six
files, and a second path through each of them.

It does not have to be there. The traversal expression already resolves against
the input schema, so it can **normalise** the previous layout to the new one
instead of passing the difference upward. Above the expression the engine then
sees one layout, and every collection class is written once.

_What made this look impossible, and why it was wrong._ The objection was
extensions. The previous layout keys a resource-level map by each element's
field identifier, and that map is not the child being traversed, so reaching it
appeared to need a reference the expression cannot carry: the analyzer spike
found that `CheckAnalysis` rejects a `RuntimeReplaceable` whose replacement is
unresolved, which rules out naming a column from inside the replacement.

The map is not a separate column. `ResourceCollection` reaches it by ordinary
traversal of the resource's own column representation, so it is a field on the
resource struct. The expression can therefore walk its own child's extraction
chain down to the resource root and emit a struct-field access beside it — built
entirely from resolved leaves, which is exactly what the spike says is required.
The expression stays unary and no unresolved reference is introduced.

_What this buys._

- **One site instead of six.** T094b holds every normalisation branch. T095 to
  T098 become new-layout implementations with no second path.
- **A single-file removal.** T100e deletes those branches. Decision 51's
  trade — building in M2 what M6 deletes — shrinks to that one expression.
- **A rule restored.** Decision 47 had to weaken its own soundness rule across
  layouts, because the extension structures genuinely differ in shape, and lean
  on the dispatcher being reapplied at each step to absorb the difference. Under
  normalisation every branch yields the new layout's shape outright, so the
  strong form — every branch yields the same `dataType` — holds everywhere.
  T108's fitted-versus-dense equality and FR-054 rest on the strong form.

_What is not yet settled, and why T038m is a gate._ Two things. Inside the
`transform` that wraps every repeating element the child is an
`UnresolvedNamedLambdaVariable`, and the walk to the resource root terminates
there, so the map is not self-derivable; it must be supplied from the handle
T094 retains, which would make the extension branch binary and is a change to
the construction the spike validated. And normalisation is necessarily per level
rather than per subtree, since the extension type is self-recursive and its
expansion infinite — sufficient because T110 reapplies the expression at every
subsequent step, but worth proving rather than assuming. If either fails, the
design reverts to decision 47 as originally written.

_The cost that stays._ A previous-layout decimal is normalised to its lexical
form and then parsed back, where dispatch in the decimal collection could have
read the stored numeric directly. The same holds for a stored canonicalised
quantity and a stored reference key. The previous layout is transitional and
T136 measures the computed paths, so this is recorded rather than treated as an
objection.

## 56. Type unification is one entry point, and it closes a defect that predates this work

Unification today is two mechanisms with disjoint callers.
`FhirPathBinaryOperator.reconcileTypes` promotes the FHIR type, by implicit cast
only, and is reached from equality, comparison and arithmetic.
`CombiningLogic.prepareArray` normalises a SQL shape, by an `instanceof` check
for exactly one type, and is reached from union and combine. No site does both.

**Neither unifies complex types.** `convertibleTo` reduces to
`typeEquivalentWith`, which compares the FHIR type and the FHIRPath type and
says nothing about structure. Two collections of one complex type whose SQL
shapes differ are therefore reported compatible and handed to the equivalent-type
path, which compares mismatched structs.

_This is already broken, before anything in this programme._ Under the previous
layout and a dense schema the exposure is bounded but real: the encoder truncates
a recursive expansion at its nesting bound, so the same recursive type met at two
depths — a `Questionnaire.item` at one level against one at the level below —
carries two different struct types. Comparing them fails although the FHIR types
are compatible.

The fitted schema does not introduce the problem, it widens it. Absence is no
longer confined to recursion depth, so two non-recursive elements of one type
reached by different branches can differ in shape too. **The solution is the same
for both**, which is why the fix belongs here rather than in a separate defect
change: by-name projection into the merged type, under the canonical order
(FR-056 to FR-058).

Two consequences for the tasks.

- **One entry point, and every site routed through it.** T113a builds it and
  deletes the decimal special case; T113b routes equality, comparison,
  arithmetic, union, combine, conditional selection, membership and choice
  traversal through it. Enumerating sites is what the layout dispatch did before
  decision 55, and the failure mode of a missed site is identical: a Spark
  analysis error rather than anything the engine can explain.
- **The defect is pinned before it is fixed.** T104b runs the recursive case
  against unmodified behaviour and records the failure, so the fix is
  demonstrable and the release note is accurate about what changed.

## 57. The strictness switch governs the definitions, and neither the JSON nor the values

The switch (FR-018, decision 44) asks one question: is this content something the
definition set describes? Two neighbouring questions are deliberately outside it,
and both are answered loudly regardless of how it is set.

**Malformed JSON fails the read.** The reader runs in `FAILFAST` mode. Spark's
permissive default substitutes a row of nulls for a document it cannot parse,
which is precisely the silent truncation FR-018 forbids — and it would be
governed by a switch whose default is to ignore. A document that is not JSON is
not content outside the definition set; it is not content.

**A malformed value fails at execution.** A primitive is stored by a plain
`cast`, not a `try_cast`, so `"abc"` in an integer element raises rather than
becoming null. This rests on ANSI mode, which is Spark 4's default and which
nothing in Pathling disables: the only switch is `pathling.test.ansiEnabled`,
an ad-hoc developer property in the `fhirpath` test harness that no build
profile sets. Were ANSI ever turned off by default, a malformed value would
become a silent null and this decision would have to be revisited — `try_cast`
plus a finding is the alternative.

The line between the two is the definitions. The switch is about a _schema_
disagreement, where ignoring is a coherent choice because the content simply has
nowhere to go. A value that contradicts its own declared type is not a
disagreement about the schema.

**Amended after M1's first review: the policy stands, and its mechanism did not.**
The review raised a malformed boolean as a defect, and probing it showed the
reasoning above was wrong in three ways, all measured.

- **ANSI mode is the caller's, not Pathling's.** "Nothing in Pathling disables
  it" was true and beside the point: a library runs in someone else's session.
  With it off, `"not-a-boolean"` and `99999999999` stored as null, and
  `"valueInteger": 1.5` stored as `1` — silent truncation, which FR-018 forbids
  outright, reached by a setting this decision assumed nobody would change.
- **With ANSI on, the cast still accepts what FHIR does not.** Spark casts `yes`,
  `y`, `t` and `1` to true. `"active": "yes"` was stored as `true`.
- **A decimal is stored as text, so no cast ever sees it.** `"value": "abc"` was
  stored, and egress then emitted it with its marks leaked into the string.

None of this touches the policy. A value contradicting its own declared type
still fails regardless of the switch, for the reason given above: it is not a
schema disagreement, so ignoring it has nowhere coherent to put it. What changed
is how the failure is reached. `PrimitiveValues` checks each value against the
lexical form the R4 definitions declare for its type — the `regex` extension on
the `boolean`, `integer`, `unsignedInt`, `positiveInt` and `decimal` profiles,
taken from the specification's own files rather than written from memory — and
casts with `try_cast`, which does not depend on ANSI mode. A value failing either
raises an error naming the element and its declared type. It does not name the
value, which is patient data and is headed for a log.

The review proposed the other remedy this decision named: `try_cast` plus a
finding, so the switch could ignore a malformed value. That was not taken, because
it would reverse the policy rather than repair it; a user who wants it is asking
for a different decision, and this one should be overturned explicitly if so.

Types stored as text other than decimal — dates, codes, identifiers — are not
checked. Storing them changes nothing about them, and validating FHIR content is
not this layout's job; a decimal is the exception because the layout's own egress
depends on it being one.

T057c and T059a. The cost was not measurable: `io`'s round-trip classes ran no
slower with the check than without it.

## 58. Shape mismatch is detected in both directions, not just the one named

T057a names one case: a repeating element supplied as a single object. The
reverse — a singular element supplied as an array — is the more dangerous of the
two and is detected on the same terms.

Spark will not refuse it. Asked to store `["male"]` where the definitions declare
a singular `code`, it renders the array as its own text and stores the string
`[male]`, which is neither an error nor the input. That is a silent corruption
where the case T057a names is merely a loss, so a check that covered only the
named direction would leave the worse half open.

Both are therefore one finding kind, reported through the switch and never
coerced. Leaf-against-structure travels with them, because the target type
derived from the definitions is the authority on all three questions at once.

## 59. Conformance is judged against the canonical structure, not the derived schema

The obvious check — compare the observed keys against the fields of the schema
the derivation produced — is wrong in the dense mode, and wrong in a way that
would hide the very loss FR-017 asks about.

A dense schema omits fields for two unrelated reasons. Some elements the
definitions do not describe, which is what the strictness switch is for. Others
the definitions do describe, but the configured nesting depth, extension switch
or open types bound them away. Judging against the derived schema conflates the
two and reports the second as undescribed content, which is both untrue and the
wrong remedy: the caller's recourse is to raise the bound, not to fix the data.

The check therefore runs against the canonical structure, which is the
definitions and nothing else. The bounded loss is a separate question with a
separate answer, `BoundsCheck` at T078b.

Name resolution is not an alternative to either. The definition library resolves
`valuePatient` and `valueResource`, names decision 45 establishes that no FHIR
instance can populate and the layout has no column for, so a check built on it
would accept content that is then silently dropped. Verified: with detection
against the canonical structure, `Observation.valuePatient` is reported.

## 60. The metadata group is stripped before the schema is fitted, until M5

A fitted schema is fitted to the data, and the transform reads that as _the data
as stored_ rather than the data as supplied. Primitive id and extension keys are
therefore removed from the observed schema before it is handed to the pruner.

Without this, a source carrying `_birthDate` produces a stored schema with a
metadata group in it that every row leaves null, because the transform does not
populate the group until T061. A column that is null by construction is worse
than an absent one: it reads as capability the layout does not yet have.

The content is reported rather than dropped in silence, which is what FR-017's
carve-out requires of it and what T057b pins.

**This has a consequence at T061.** When the metadata group is populated, the
strip must come out, or the group will be written into rows of a schema fitted
not to carry it. The strip and the population are one change in two milestones,
and T078c is where the carve-out closes.

## 61. An extension needs no case of its own

The layout stores an extension inline, meaning as the structure the definitions
describe, on the structure carrying it, recursing as far as the source does.
That is what the ordinary structural mapping does, so there is no
extension-specific path in the transform and the task that called for one
(T062) has no implementation.

This was measured rather than assumed. An extension component was written, and
hard-wiring its predicate to false left every test green — the branch returned
exactly what the branch beside it returned. It was removed.

The result belongs to the layout rather than to the transform. The previous
encoding needed extension-specific code because it had nowhere to put an
extension: a scalar column cannot carry one, so extensions were hoisted into a
map at the root of the resource and keyed by an identifier added to every
composite. Neither the map nor the identifier exists here, and with them gone an
extension is an ordinary element. T052 is where inline storage is stated, since
there is no component for it to point at.

Extensions on _primitive_ elements are the other half of FR-003 and are
unaffected: they belong in the metadata group beside the element, which is
`PrimitiveMetadataTransform` at T061.

## 62. A decimal is marked into the document and unquoted out of it

A decimal is stored as text so that its lexical form survives (FR-002), and it
must appear in the document as a number. The JSON writer quotes text and offers
no way to say otherwise, per column or at all, so egress marks the value and
unmarks the document: the stored text is wrapped in U+0001 on the way in, and
one `regexp_replace` over the finished document removes the marks and the quotes
around them together.

This is a deliberate ugliness in an otherwise declarative path, and it is
recorded because it is exactly the kind of thing a later reader tidies away.
Tidying it away silently re-quotes every decimal, and a round trip that compares
text to text stays green while doing it.

_Why the mark is safe._ The writer escapes a control character, so a value
carrying the six characters of the escape sequence is escaped again and cannot
match a pattern that requires one backslash. Only a raw control character in the
data could collide, and only where the whole value is a mark, the characters a
decimal is written with, and the other mark.

_Why it is only decimals._ Every other primitive is stored as text the document
also quotes, or as an integer, long or boolean the writer renders as itself.
There is no second class of value with this problem.

_How it is held._ `DecimalRoundTripTest` asserts on the raw document that a
decimal and a string carrying identical characters come out as `1.50` and
`"1.50"` respectively, so the discrimination is proven to be by definition
rather than by content. The comparator refuses a number matched against its
text, so the round trip cannot pass by comparing strings to strings.

## 63. An array drops its null elements, which collides with primitive metadata

FR-019 requires that an array whose elements are all null be omitted rather than
written as an array of nulls. The implementation drops null elements
individually and omits the array when none survive, because an array of three
values one of which pruned away is an array of two, not an absent element.

**That is right for M1 and wrong from M5.** FHIR's primitive metadata mechanism
is positional: `"given": ["Jane", null]` beside `"_given": [null, {...}]` uses
the null in `given` to hold the place of a value whose id or extensions live in
the parallel array. Dropping it shifts every position after it, and the two
arrays no longer describe the same element.

Nothing is broken today, because the metadata group is neither written (decision 60) nor read back, so a positional null carries no information and the Synthea
corpus does not exercise one. The collision arrives with T061.

Two consequences.

- **T061 must revisit this**, together with the strip in decision 60. Populating
  the metadata group without changing the pruning writes a group whose positions
  do not line up with the element beside it, which is worse than not writing it
  at all. T078c is where the carve-out closes and where both are settled.
- **T076's corpus does not expose it, which was measured rather than assumed.**
  The expectation recorded here was that the R4 examples would carry a value
  array with a positional null and so round-trip one element short. They do not.
  Of the 2,911 example resources the specification publishes, 81 carry primitive
  metadata and **none** carries a null inside a value array; the single null in
  the entire corpus sits inside `_given`, which the harness's exclusion removes
  wholesale, leaving the `given` beside it untouched. T076's exclusion therefore
  did not need widening, and the narrower assertion is correct rather than
  lucky.

    **This does not discharge the hazard, and T061 must still revisit it.** What
    was measured is a property of the corpus, not of the layout: no published
    example happens to place a value beside metadata in a way that requires the
    placeholder. The moment Pathling _writes_ the metadata group it must write
    those placeholders itself, and the pruning above would drop them. The
    collision arrives with T061 exactly as stated; what has changed is only that
    no existing test will catch it first, so T061 has to bring its own.

## 64. An inline resource is dropped for want of a type, and mis-reported as undescribed

Three elements in R4 hold an inline arbitrary resource: `DomainResource.contained`,
`Parameters.parameter.resource` and `Bundle.entry.resource`. The layout represents
none of them, which is right. It accounts for them in two different ways, and only
one of those is correct.

`DefinitionCanonicalStructure.addElement` omits `contained` by name, with a comment
saying why, and `StrictnessCheck` reports its presence as a `containedResource`
finding. That is FR-006 working exactly as written.

The next branch of the same method returns early when `ElementDefinition.getFhirType()`
is empty — "an element the definitions do not give a type cannot be represented".
An element declared as the abstract `Resource` has no `FhirType`, so it takes that
branch and never reaches the structure. `StrictnessCheck` then finds observed
content with no matching entry and reports `undescribedContent`: _the definitions
describe no element of this name_.

**That sentence is false.** The definitions do describe `Parameters.parameter.resource`
— it is declared, 0..1, of type `Resource`. What is true is that the layout cannot
represent its type. The outcome is right and the diagnosis is wrong, which is worse
than it sounds: a finding is the only thing a user gets, and this one sends them
looking for a typo in their data.

Measured by T076, over `parameters-example.json` in the specification's own examples.

Two consequences.

- **`Bundle.entry.resource` takes the same branch**, and M3's explode path (T058,
  T068) goes through it. Whoever writes that will meet this as "the definitions
  describe no element of this name" for an element the definitions plainly describe.
- **The remedy is not T076's.** It spans the definition abstraction, the kinds
  `NonConformantContent` offers, and `StrictnessCheck`'s dispatch, and the right
  shape is probably a fourth kind naming an inline resource rather than widening
  `containedResource`. T134e raises it. T076's job was to find it.

## 65. Each milestone is its own issue and its own pull request, against a spec-only baseline

`ssh:build` delivers a spec bundle as one pull request. This programme is too
large for that: M1 alone is 64 tasks and 44 commits, and the six milestones
together are 203. A reviewer given all of it at once cannot hold the contract and
the diff in mind together, which is the failure the adversarial review exists to
prevent.

So the delivery is restructured, and the branch topology carries the structure:

- **`issue/2367` holds the specification and nothing else.** It is reset to
  `b5fe0679d6`, the last commit that touched only `specs/`. It is the baseline
  every milestone targets, and it never carries implementation.
- **Each milestone is a sub-issue of #2367** — #2761 M1, #2762 M2, #2763 M3,
  #2764 M4, #2765 M5, #2766 M6 — on its own `issue/<n>` branch, with its own
  pull request against `issue/2367`. The milestone table records the numbers.
- **Each pull request gets its own adversarial review**, scoped to that
  milestone's phases.

**The spec bundle is not split.** The milestone table is already the partition,
and the functional requirements cut across milestones — FR-016's round trip is
proved in M1 and extended in M3, M5 and M6; FR-017's carve-out is opened in M1
and closed in M5. A reviewer needs the whole contract to judge any part of it.
What is scoped is the review, not the specification.

**This makes the reviewer prompt a deviation from `ssh:build`'s.** An adversarial
reviewer handed the full spec and one milestone will correctly report every
later-milestone requirement as unmet, and be useless. The scoped prompt therefore
adds three constraints: judge only the phases this milestone names; treat a
requirement another milestone owns as out of scope rather than as a failure; but
**a milestone that forecloses or contradicts a later requirement is in scope**,
because that is the one failure the scoping would otherwise hide.

CI needs no change. `.github/workflows/test.yml` triggers on a bare
`pull_request:` with no branch filter, so a pull request against `issue/2367`
gets the same checks as one against `main`.

## 66. Ignoring undescribed content can shorten an array

Found by M1's first review, and not a defect.

With the switch set to ignore, an occurrence of a repeating element whose only
content is undescribed has nothing left once that content is dropped, and FR-019
omits it: `Patient.name = [{"family":"Keep"},{"bogusField":"onlyThis"}]` stores one
name, not two. The dropped field is reported as a finding; the change in the
array's length is not reported separately.

That is what ignoring means. Nothing conformant was left in the second
occurrence, and FR-019 forbids storing an empty object in its place. It is the
same cascade the metadata exclusion needed in the round-trip harness (decisions
60 and 63), arising from content the definitions do not describe rather than from
content the layout does not yet write. It is recorded because no decision said
so, and because a caller reading positions across the source and the stored
data needs to know that ignoring can move them. Setting the switch to fail is
the remedy for a caller who cannot accept that.
