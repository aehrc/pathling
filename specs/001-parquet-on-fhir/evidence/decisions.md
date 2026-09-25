# Decisions — Parquet on FHIR spec (issue #2367)

Settled during the spec interview. Supplements the earlier draft design for
this programme; where they conflict, this file wins.

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

**Addendum — 17, 18 and 19 are moot, and 16 and 20 stand (decision 68).** The
lexical form of a decimal is no longer preserved on any path: ingest routes a
number through a double and stores its text, so the file path and the
`Dataset<String>` path now agree. 17's documented limitation therefore has
nothing to describe, 18's Spark issue no longer affects this layout, and 19's
quoting pre-pass has nothing to preserve. **16 stands unchanged** — storage is
still text and query-time precision is still `DECIMAL(32,6)` — and is called out
because a first reading of decision 68 appeared to overturn it. 20 stands, with
one of approach (b)'s advantages, losslessness on every route, no longer
distinguishing it.

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

### Addendum to 44 — reversed. There is no switch

Decision 68 removes the strictness switch entirely. Ignoring is not the default
any more, it is the only mode: content the definitions do not describe is nulled
and one warning names it. `SchemaConfiguration.failOnNonConformantContent` goes,
and with it the option T070 would have threaded onto `PathlingContext`, Python
and R. The reasoning for ignoring *by default* survives intact — a fail-by-default
ingest would break working pipelines on upgrade — and what is withdrawn is only
the opt-in half. The schema-mode half of this decision is unaffected.

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

### Addendum to 57 — reversed, on the terms this decision set

This decision closed by naming its own reversal condition: the alternative remedy
of `try_cast` plus a finding "would reverse the policy rather than repair it; a
user who wants it is asking for a different decision, and this one should be
overturned explicitly if so." Decision 68 is that overturning, taken explicitly.

A value contradicting its declared type no longer fails. Validation is deferred
along with everything else in decision 68's first scope point, and the mechanism
that replaces it is narrower than `try_cast` plus a finding: the converter
compares the column's inferred type against what the element's encoding admits,
and nulls the element where they disagree. `LEXICAL_FORMS` and the `raise_error`
path go with it, so the three measured defects this decision recorded — ANSI mode
being the caller's, the cast accepting `yes`, and a decimal no cast ever sees —
are addressed by the class check rather than by a lexical guard. Two things this
decision protected are consequently lost and are recorded as accepted: value-range
validation, so `positiveInt` of `0` is stored; and per-value granularity, so one
non-conformant value voids its element for the whole file.

**Malformed JSON still fails the read.** The `FAILFAST` reader mode is untouched:
a document that is not JSON is not content outside the definition set, it is not
content.

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

### Addendum to 59 — moot while the check is absent

Decision 68 removes `BoundsCheck` from M1. The distinction this decision drew is
still the right one and is not withdrawn: a bound is not a disagreement with the
definitions, and reporting one as undescribed content would point the caller at
the wrong remedy. There is simply nothing to keep separate while the bounds go
unreported. Reinstate this decision with the check.

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

### Addendum to 60 — the strip disappears with the separate pruning step

Decision 68 collapses derivation into the transform, so there is no observed
schema handed to a pruner and therefore nothing to strip beforehand.
`ResourceTransformer.stored()` deletes. The behaviour this decision wanted is
unchanged and is now structural: a metadata group is a node the single traversal
declines to emit while nothing populates it, so no column that is null by
construction can arise.

The consequence at T061 survives in a different form. When the group is
populated, what changes is the traversal's decision to emit it rather than a
strip that has to be removed — one place either way, and still the same change
in two milestones.

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

### Addendum to 62 — reversed. There are no marks

Decision 68 removes lexical decimal storage, and with it the reason this
decision existed. A decimal is stored as the text of a double and written by
casting that text back to a double, so the JSON writer emits a bare number
without help. The U+0001 mark, `markedValue`, the `MARKED` pattern and the
document-wide `regexp_replace` in `unmarkedDocument` all delete, which also
removes a regular expression pass over every serialised document. The warning
this decision carried — that a round trip comparing text to text stays green
while the marks leak — no longer applies, because there is nothing to leak.

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

### Second addendum to 63 — the pruning that dropped the placeholder is gone

Decision 71 deletes the filter this decision is about. A positional null in a
value array now survives the round trip: measured on
`"given":["Ann",null,"Bee"]`, the document comes back with the null in place,
where before it came back two values long. What T061 has to solve is therefore
only the half that remains — Pathling does not yet write the metadata group, so
there is nothing to align the placeholder with — and not the collision between
that group and a filter. Should the pruning return, as a custom serde or as an
ingest-side invariant (T134h), the placeholder is the case that must be carried
with it, and this decision is why.

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

### Addendum to 66 — the remedy this decision named no longer exists

Decision 68 removes the strictness switch, and this decision's closing sentence —
"Setting the switch to fail is the remedy for a caller who cannot accept that" —
goes with it. Array shortening is now unconditional. Because the round trip
treats array order as significant, it joins the exception list in decision 68
rather than remaining a documented behaviour with an escape hatch. The finding
that reports the dropped field is still raised, as a warning.

### Second addendum to 66 — the shortening itself no longer happens

Decision 71 defers the pruning that shortened the array. An occurrence whose only
content is undescribed now keeps its position and is written as an empty object,
so the array's length is preserved and the document that carries it is not
conformant FHIR. The input that produces it is outside what the layout accepts,
and `PrunedSchemaGuaranteeTest` asserts the empty object rather than the shorter
array so that the cost is visible. What this decision found is unchanged — that
ignoring content can leave an occurrence with nothing in it — and only the
treatment moved.

## 67. The definitions report R4 types, and the engine is left alone

FR-015 had the definition abstraction report a type as a module-local value
rather than as the R4 enumeration, so that a type code R4 does not contain would
be representable. T018 introduced that value as `FhirType`, and T019 converted
the engine to it. The requirement is withdrawn and both are undone: the
abstraction reports `FHIRDefinedType`, and `fhirpath` differs from the baseline only
in the imports T010–T011 changed and its dependency on `fhir-schema`.

**Nothing in this programme needed it.** A provider for another version or for
profiles is a non-goal, so every type the abstraction reports comes from HAPI's R4
model. That was checked rather than assumed: walking every element reachable from
every R4 resource, 825 element definitions including each type a choice can take,
finds no `@DatatypeDef` name the enumeration does not contain. The only use of a
code outside R4 was the test that proved FR-015 held. `FhirTypeCoverageTest` now
pins the opposite property through the abstraction itself, because a dense schema
visits every element and would fail derivation outright if R4's model ever
declared a type its own enumeration lacks.

**The cost was out of proportion to that.** T019 touched about fifty files in the
engine: the collection hierarchy, the type system, search, the view executor and
the YAML and DSL test support. Its dispatch on a non-enum value also cost the
compiler's help, turning `TokenMatcher`'s `switch` into an equality chain and
admitting a misspelt `FhirType.of(...)`. A milestone that is meant to leave the
engine's behaviour unchanged is easier to review when the engine's diff is
visibly only motion.

**One semantic difference, unreachable here.** `FHIRDefinedType.NULL.toCode()`
returns Java `null`, where `FhirType.NULL.toCode()` returned `"null"`, so
`PrimitiveTypes.isPrimitive` would throw rather than answer for that sentinel.
Nothing reaches it: the HAPI-backed definitions report an absent type as an empty
`Optional` and never as the sentinel, and the sentinel's only users are in the
engine, which does not derive schemas. A provider that reports it would have to
be written to say "no type" the way the abstraction already does.

**What it defers.** Whatever delivers a non-R4 or profile-backed provider will
have to introduce a version-neutral type then, and by then `fhir-schema` and `io`
consume the reported type as well as `fhirpath`. That is accepted: the change is
mechanical, and it will be made against a real second provider rather than a
hypothetical one.

**The engine's dependency on `io` is removed with it.** T009 added `io` to
`fhirpath` ahead of any use, on the grounds that the engine's tests must
eventually write data in the new layout (R-002). Nothing in M1 does, so the
dependency is added by the milestone whose tests first need it.

## 68. M1 is narrowed, and schema derivation collapses into the transform

The scope of M1 is narrowed to the value layer, and the derivation of the stored
schema stops being a step of its own. Eight scope points were settled, and three
structural consequences followed from them.

**The scope points.** Annotations, the primitive metadata group, `contained`
resources and support for non-FHIR fields are deferred to later phases. Non-FHIR
fields, and FHIR fields whose JSON encoding type contradicts the definitions, are
ignored: the field is nulled and one warning names what was dropped, with no
failing mode and no switch. Decimals need not preserve precision or lexical form,
only numeric equivalence, so standard Spark JSON read and write are used with a
double as the serde intermediate at both ends; storage remains `StringType`, as
the Parquet on FHIR specification requires. The round-trip invariant holds
subject to the exceptions these create. The schema continues to be derived
through the `fhir-schema` abstraction. Conversion follows the spike's
converter-table idiom, with a converter named for every primitive type.
Conversion is unit tested on specialised cases. Large-scale round-trip testing
over synthetic resources is limited.

**The structural consequences.** Every primitive gets its own named converter
rather than a default branch. The bounded and unbounded traversals of the
definition graph become two implementations of one strategy. Schema derivation
collapses into the transform, so there is no derived `StructType` and the stored
schema is the result of the columns the transform builds.

Every deferred item is additive: none changes the shape of what is stored, only
what is stored beside it or what is reported about it.

### The decimal path

A decimal is read with standard JSON inference — no `primitivesAsString` — which
yields `DoubleType`, or `LongType` where every value in a file is integral. The
converter casts that to `StringType`. At egress the stored text is cast back to
`DoubleType` and the JSON writer emits a bare number. The stored text is
therefore what a double round trip produces, not the source's lexical form:
`1.50` stores as `1.5`, and a forty-digit decimal stores at the double's
precision. Both remain numerically equal to the source.

Storage stays conformant, because `PrimitiveTypes` keeps `decimal -> StringType`.
Query-time precision is unaffected, because the engine still reads text and
decodes to `DECIMAL(32,6)`: decision 16 and FR-035 stand exactly as written. An
earlier reading of this change, in which a decimal was *stored* as a double,
would have broken both; that is not what was decided.

`DecimalTransform` is deleted in full — the `primitivesAsString` read option, the
U+0001 mark, `markedValue`, and the document-wide `regexp_replace` in
`unmarkedDocument`, which was a regular expression pass over every serialised
document. With both ingest paths now routing through a double, the file path and
the `Dataset<String>` path agree, so the documented per-path limitation has
nothing left to describe and FR-020 is withdrawn rather than reworded.

`DoubleType` is used at egress rather than the spike's `DECIMAL(38,6)`, which
quantises a second time at a different precision, renders every decimal at six
decimal places, underflows magnitudes below `1e-6` to zero and overflows above
about `10^32` to null.

Measured on PySpark 4.0.1, script `evidence/scripts/double_serde.py`. The stored
text is Java's `Double.toString`, and the JSON writer renders the same form:
`1.50` stores and writes as `1.5`, `100` in a mixed file as `100.0`, `0.0000001`
as `1.0E-7`, `1e20` as `1.0E20`, `1.5e2` as `150.0`, and forty digits as
`1.2345678901234568E39`. Every one satisfies FHIR R4's decimal regex, which
permits an exponent and an optional sign in it, so the stored text stays
conformant at both ends.

Two observations follow. The stored text depends on the file's other values: a
file whose decimals are all integral infers `LongType` and stores `100` as `100`,
where the same value in a file containing any fraction stores as `100.0`. Both
parse to the same decimal, so nothing downstream is affected, but the stored
bytes for one resource are not a function of that resource alone. And egress
never writes a bare integer, because the cast is to a double.

### The converter table

A map from FHIR primitive type to a converter, in the spike's idiom but total
over the primitives rather than covering the five the spike had. What is not
taken from the spike is its walk: it iterates the inferred columns and passes
unknown ones through unaltered, which is the opposite of the ignoring rule, and
it takes cardinality from the data, which FR-008 and FR-012 forbid.

The acceptance rule is per column, by shape and class. Inference types a column
from every value in a file, so one value can re-type the column; the converter
compares the inferred type against what the element's encoding admits, and where
they disagree the element is nulled and logged, for the whole column. Shape is
unchanged by this decision: `shapeMatches` already nulls an element whose
cardinality contradicts the definitions, and one given as a structure where the
definitions declare a leaf or the other way about.

`decimal` accepts `DoubleType` and `LongType` and stores `StringType`. `integer`,
`unsignedInt` and `positiveInt` accept `LongType` and store `IntegerType`.
`integer64` accepts and stores `LongType`. `boolean` accepts and stores
`BooleanType`. `base64Binary` accepts `StringType` and stores `BinaryType`. The
remaining fifteen accept and store `StringType`, and each is a named entry rather
than a default branch, because the types they carry diverge as soon as anything
is added beside one of them and a table with a default branch has to be
restructured before that addition can land. A totality test asserts the table
covers whatever the definitions can report.

Every cast is a `try_cast`. Inference never produces `IntegerType`, so an
`integer` element always arrives as `LongType` and a value beyond 2^31 would
raise under ANSI and be silently wrong without it. Measured:
`try_cast(3000000000L AS INT)` is null and `try_cast(7L AS INT)` is 7.

`decimal` accepting `LongType` is not leniency. `"value": 100` is conformant FHIR
for a decimal element, and inference gives `LongType` whenever no value in the
file carries a fraction, so rejecting it would reject correct input. The
asymmetry with `integer` is deliberate: a `DoubleType` column against an integer
element means some value in that file carried a fraction, which is not
conformant.

Two consequences are stated rather than left to be discovered. A single bad value
can void an element for a file: one `1.5` among integers re-types the column and
the rule then drops that element for every resource in the file. This is the cost
of a per-column check, and it is accepted, because casting per value turns `1.5`
into `1` and `"yes"` into `true`, which is a plausible wrong answer rather than an
absence. And value-range validation goes: `positiveInt` of `0` and `unsignedInt`
of `-1` pass the class check and are stored, because the lexical-form regexes in
`PrimitiveValues` are no longer needed once the class check subsumes the cases
they were written for.

### base64Binary becomes binary

The Parquet on FHIR specification maps `base64Binary` to the Parquet `binary`
primitive type with no logical type. In Spark, `BinaryType` writes as Parquet
`BINARY` unannotated, whereas `StringType` writes as `BINARY` annotated `STRING`.
`PrimitiveTypes`' `base64Binary -> StringType` is therefore a deviation from the
specification, unrecorded in FR-001's deviation list and described as "Text" in
the data model. Fixing it removes a deviation rather than adding one.

It also converges with the existing encoder, which maps
`Base64BinaryType -> DataTypes.BinaryType` (`R4DataTypeMappings.scala:217`) and
already applies `unbase64` on the way back (`StringCollection.java:341-347`), so
the new layout agreeing with the old one here makes the M2 engine work smaller.

The ingest converter is `unbase64`; the egress converter is `base64` wrapped in
`regexp_replace(..., "[\r\n]", "")`. The strip is still required on Spark 4.0.2,
where `spark.sql.chunkBase64String.enabled` defaults to true
(`SQLConf.scala:3831-3837`) and `base64()` breaks its output into lines of at
most 76 characters.

This costs one round-trip exception. FHIR R4's `base64Binary` regex is
`(\s*([0-9a-zA-Z\+\=]){4}\s*)+`, which permits whitespace between groups, so
decoding and re-encoding canonicalises a wrapped value onto one line: the same
bytes, a different string. The same regex omits `/`, a known erratum, which is a
further reason not to guard base64 lexically.

### The traversal, and why there is no derived schema

Two walks over the definition graph become one, and the derived `StructType`
disappears with the second of them.

Before this decision, `SchemaBuilder.pruned(resourceType, stored(observed))`
derived a target type and the projection then walked that target pulling from
`observed`. The two had to agree with each other about what presence means, and
the cost of keeping them in agreement was visible in the code:
`ResourceTransformer.stored()` existed only to strip metadata groups before
pruning, so that the target did not carry a column the projection would never
populate; `shapeMatches` and `observedField` re-derived a correspondence the
first walk had already established; and `column()` carried an
`IllegalStateException` for the case where the two disagreed.

There is now one walk, driven by the definitions, returning `Optional<Column>`
per node. The observed schema is threaded through it as an argument rather than
modelled as anything: where it lacks a child, the pruned mode returns empty and
the node does not appear, and the dense mode returns a typed null. The stored
schema is `select(columns).schema` — never constructed, so it cannot disagree
with what is written.

**The strategy.** What the two modes disagree about is one question, asked at
every node of a cyclic graph: which children to descend into, and when to stop.
Dense answers from configuration — nesting depth, the extension switch, the
open-type set. Pruned answers from the data, and terminates for free because the
data is finite.

That question already has an interface. `CanonicalStructure` in `utilities` is
`fieldOrder()` plus `field(name) -> Optional<CanonicalStructure>`, and `field()`
is documented as expanding only when called, which is what the cyclic graph
required in the first place. A bounded implementation returns `Optional.empty()`
at the bound, and that is the termination signal. No interface change is needed.
The unbounded implementation is today's `DefinitionCanonicalStructure`, which
egress and the structure merge need because both want the full order regardless
of what any dataset carries; the bounded one carries the three bounds out of
`SchemaBuilder` and becomes the only place FR-044 lives. A third implementation
driven by a supplied target schema falls out later for free, and is what reading
into an existing table's layout wants (FR-042, T118).

One detail must not be reversed: the bound belongs in `field()`, never in
`fieldOrder()`. The contract says `fieldOrder()` reports the full order at a node,
unrestricted by what any structure carries, and `StructureMerge` depends on that.
A bounded cursor still names every child and refuses to expand the cut ones.

**The one place a type is still needed.** Dense mode must emit something for a
subtree the data does not carry, and `lit(null).cast(t)` needs a `t`. It does not
need an explicit `StructType`: built structurally, the type is correct by
construction — `struct(lit(null).cast(leaf) as name, ...)` recursively, and a null
of the same construction for an absent repeating element. The engine already
types absent elements this way (FR-055). What must not happen is a fallback that
materialises a `StructType` for this case alone, because that reintroduces the
second derivation and with it the drift FR-010 exists to prevent.

**Why dense mode stays.** Collapsing only the pruned path would have left
`SchemaBuilder.dense` as a second derivation, which FR-010 forbids. Under the
strategy there is one traversal and two cursors, so FR-010 is satisfied by
construction rather than by a promise the code has to keep, and dense mode stays
in M1 at no structural cost.

**Three things to watch.** `DefinitionCanonicalStructure` memoises per FHIR type
(T038i); a depth-bounded cursor cannot, because the same type at different
remaining depths gives different answers, so the key becomes (type, remaining
depth) — a correctness bug wearing the clothes of an optimisation. Termination
needs a test per cursor, because a cursor bug on a cyclic graph does not fail, it
hangs. And the post-order prune must be safe in dense mode: a bound cuts whole
subtrees rather than emptying a struct, so applying FR-011's rule uniformly
should be harmless, but it should be asserted rather than assumed.

**The cost.** `SchemaBuilder` and `SchemaPruner` delete, and with them the
independent testability of the schema. T028, T029, T030 and T030a assert
derivation against a definition context today; those assertions move onto
`transform(...).schema`, which is a stronger test — it checks what is written
rather than a model of it — but a slower and less isolated one, needing a session
and a fixture. Error locality goes too: "the derivation is wrong" and "the
projection is wrong" stop being distinguishable failures. `fhir-schema` keeps the
definition abstraction, `CanonicalStructure` and its implementations,
`LayoutEntry`, `LayoutFields` and `PrimitiveTypes`. Nothing downstream is
affected — layout detection reads schemas from files, the merge works on
`CanonicalStructure`, and the engine's absent-element typing needs
`PrimitiveTypes` and a definition rather than a derived schema.
`ResourceTransformer` was the only production consumer of either deleted class.

This is the largest change in this decision and it rewrites code that is finished
and green. What makes it affordable is that the interface it needs already exists
and already has the right shape.

### Ignoring, and what silent means

Ignoring is the only mode. The field is nulled and one warning names what was
dropped; there is no fail branch, no switch, and no configuration. This overturns
decision 44, which made ignoring the default with failing as an opt-in, and
decision 57, which held that a value contradicting its declared type fails
regardless of the switch. Decision 57 anticipated the request: "a user who wants
it is asking for a different decision, and this one should be overturned
explicitly if so." This is that overturning.

Choosing warn-and-continue keeps most of the machinery, and this decision says so
rather than claiming a deletion it does not make. A warning has to name what it
dropped, so `StrictnessCheck` survives whole — all four branches: `contained`, an
undescribed key, a metadata group, and a shape mismatch — each now producing a
warning where it used to produce either a warning or a failure.
`NonConformantContent` survives too, losing only `outsideDenseBounds`. What
actually goes is narrow: `SchemaConfiguration.failOnNonConformantContent`, the
raising branch of `ResourceTransformer.report()`, the `raise_error` path and
`LEXICAL_FORMS` in `PrimitiveValues`, and every test that asserts a failure.

The `contained` and metadata-group branches keep their warnings although both are
deferred. Deferring a capability does not require dropping it in silence, and the
warning costs one line in a channel that exists anyway.

Removing the switch has a consequence decision 66 did not anticipate. That
decision recorded that ignoring can shorten a repeating element, and named
setting the switch to fail as the remedy for a caller who cannot accept it. That
remedy no longer exists. Array shortening is now unconditional, and since the
round trip treats array order as significant, it is a round-trip difference that
must be named rather than left to the harness to discover.

### Dense-mode bounds reporting is removed

`BoundsCheck` is removed from M1 and parked as a follow-up. It reported the
content that the dense mode's three configured bounds leave out of the schema,
naming which bound was responsible. The bounds themselves stay — they are what
makes a dense schema over a cyclic definition graph finite, and FR-044 preserves
their meaning — so only the reporting goes, and dense-mode ingest now drops that
content silently. This is no regression against the current release, whose
encoder also truncates silently at its nesting bound, and it does not touch the
default mode, where the bounds never applied.

Decision 59, which separated this check from the strictness check so that a bound
would not be reported as undescribed content, is moot while the check is absent
and should be reinstated with it.

### What the round trip now proves

FR-016's guarantee survives, with numbers compared numerically rather than
lexically, and subject to an exception list long enough to state in one place:
primitive element ids and extensions (unchanged, FR-017's carve-out); `contained`
resources (unchanged, FR-006); `Bundle` (unchanged, FR-007); decimal lexical form
— trailing zeros, exponent notation as written, and precision beyond a double;
`base64Binary` whitespace; non-FHIR fields and fields whose JSON encoding type
contradicts the definitions, including the array shortening that follows from
dropping them; a conformant value dropped because a sibling value re-typed its
column; and content the dense bounds drop, which was previously detectable.

`SemanticJson` compares numbers as `Double.parseDouble(a) == Double.parseDouble(b)`,
which needs no tolerance and admits all five of T072's forms. Its kind comparison
stays, because it is what stops a serialiser that quotes every number from passing
a comparison built on text.

**The honest cost.** M1's exit criterion moves from "lossless, or loud about what
was lost" to "lossless for the resources we test". The ignoring rule and the
reduced corpus compound: detection is removed at the same time as the corpus that
would have exercised it is reduced, and the result lands on the M4 flag day,
where this specification names silent loss as the worst failure mode in the
programme. This is a legitimate trade for a faster M1, recorded as a trade with
named follow-ups rather than absorbed.

### Testing

One converter test per primitive type, in both cardinalities, since the converter
recurses through arrays and the scalar/array split is where this codebase has
historically broken. A totality test asserting the table covers every primitive
the definitions can report belongs beside `FhirTypeCoverageTest`.

`SyntheaRoundTripTest` (T077) is the synthetic corpus and is deferred.
`SpecExampleRoundTripTest` (T076) is reduced rather than removed: its corpus is
hand-authored R4 specification material, not synthetic, and it is what found
decision 64 and the `ActivityDefinition.timingTiming` cascade, neither of which a
unit test would have produced. `PersistedRoundTripTest` (T078d) is cheap and is
the only test that proves the Parquet layer, so it stays.

Cutting the other way, the derivation tests gain rather than lose. T028, T029,
T030 and T030a now assert against the schema that is actually written, and each
cursor needs a termination test, which the bounded one does not have today
because the bounds lived inside `SchemaBuilder`.

_Consequence for the specification_: FR-002 keeps "stored as text" and loses "in
their lexical form"; FR-006 loses its detection half; FR-010 is reworded, since
"with pruning skipped" describes a step that no longer exists; FR-016 compares
numbers numerically; FR-017 loses its dense clause; FR-018 is withdrawn for this
phase; FR-020 is withdrawn outright. FR-001 needs no change, because
`base64Binary` was never among the deviations FR-002 to FR-005 record — it was an
unrecorded one, and it is now removed rather than documented. FR-008, FR-009,
FR-011, FR-012, FR-021, FR-035, FR-044 and FR-057
are unchanged, several of them better served than before. SC-001 is reworded to
the corpus that survives. `data-model.md` and `contracts/storage-layout.md` carry
the decimal, `base64Binary` and `contained` rows that change with these.

Findings 18 to 22 in `spark-type-findings.md` each carry a note saying how this
decision bears on them, and none is deleted, because they are measurements.
Finding 18 described an alternative to the marking this decision removes and is
superseded. Finding 19 no longer applies to Pathling once `primitivesAsString`
is gone, though it remains a correct observation about Spark. Findings 20, 21
and 22 stay live: 21 is load-bearing for a second reason now, since egress must
consult the definitions to know that a text column is a decimal to be cast for
the writer, and 22's exponent clause is what makes the stored text conformant.
Finding 23 is added, recording the measurement above, from
`evidence/scripts/double_serde.py`.

### Addendum to 68 — a decimal also accepts a decimal column

The converter for `decimal` accepts `DecimalType` at any precision, beside
`DoubleType` and `LongType`. Inference gives `DecimalType(p,0)` to a column
whose values are all integral where one of them is beyond the range of a long:
measured on Spark 4.0.2, `100` and `12345678901234567890` in one file infer
`decimal(20,0)`. Without this, that conformant content was dropped for the
whole file with an encoding-mismatch warning, a loss FR-016's exception list
did not name. The asymmetry with `integer` stands, because no integer element
can hold such a value.

The stored text of such a column is that of the decimal, so
`12345678901234567890` stores exactly rather than at a double's precision.
Egress still casts to a double, so the value is written back numerically
equal to the source but not textually identical, as every other decimal is.

## 69. The dense schema mode becomes a milestone of its own, after the gaps

The dense schema mode leaves M1 and becomes M6, after the annotations and the
primitive metadata group. It is raised as issue #2771. What was M6 becomes M7 and
keeps issue #2766, because an issue number need not match a milestone ordinal. The three options that bound
the dense mode stay in the public API throughout, accepted and without effect on
the new layout until M6 lands.

### Why this is available now and was not before

Decision 68 kept the dense mode in M1 for a structural reason: collapsing only
the pruned path would have left `SchemaBuilder.dense` as a second derivation,
which FR-010 forbids. That reason does not survive its own decision. With
`SchemaBuilder` deleted outright there is no second derivation to leave behind,
so deferring the dense mode leaves one traversal with one strategy rather than
two walks that have to be kept in step. FR-010 is met vacuously until there is a
second mode to meet it against, and it is met by construction when that mode
arrives, because the strategy is where the two differ and nothing else is.

The deferral was proposed once before in this programme on structural grounds and
withdrawn when the strategy formulation dissolved them. The grounds now are scope.

### What leaves M1

T031a, the bounded cursor, moves whole. It exists only to make a dense schema
over a cyclic graph finite, and it was the most intricate piece of new code M1
had left. T030, the mode-parity test, moves with it, as do T033 and T033a, the
configuration.

Two shrink. T030a keeps definition order and loses "a pruned structure's field
order is a subsequence of the dense one", which needs a dense structure to be a
subsequence of. T031c keeps the unbounded termination test, and the bounded one
arrives with the cursor as T031d.

One thing disappears rather than moving. Decision 68's "one place a type is still
needed" — the typed null a dense schema must emit for a subtree the data does not
carry — has no occasion to arise: in the pruned mode an absent child is
`Optional.empty()` and nothing needs a type. The dense post-order prune question
goes with it, and both return in M6 as T031e.

`SchemaConfiguration` is deleted rather than reduced. The strictness switch went
with decision 68, the annotation toggles belong to M5 and the schema mode now
belongs to M6, which leaves the class with no live field. `ResourceTransformer.of`
takes the definitions alone. The class is recreated in M5 by the first toggle that
needs it, or in M6 by the mode, whichever lands first.

The strategy interface is not extracted in M1. The traversal walks
`DefinitionCanonicalStructure` directly, whose `elementStructure` already returns
an `Optional` in the shape a bounded implementation will want, and the interface
is extracted in M6 with both implementations in hand rather than guessed from one.

### The options stay in the public API and do nothing

`max_nesting_level`, `enable_extensions` and `enabled_open_types` are public in
Java, Python and R. FR-044 gives them meaning for the dense schema only, so from
the flip until M6 they are accepted and have no effect on data written in the new
layout. They are kept rather than deprecated or made to fail: FR-043 preserves the
encoding signatures, SC-008 rules out an incompatible public API change, and the
mode they bound is deferred rather than abandoned.

This is documented rather than warned about. A warning would fire on every context
carrying a non-default value, for a window that ends when the mode lands, and the
contract is where a user finds out what an option does.

That window is the cost of this placement and it is not small. M5 is twenty-one
tasks across two phases, landing as a series of independent increments. FR-009 and
FR-044 are unmet for its duration, and SC-002's curated dense subset with them.

### The hazard this creates in M5, and what is done about it

The argument for placing the dense mode after M5 rather than before it is that M5
is where the layout's width is settled — annotations and the primitive metadata
group each add columns — so building the bounded traversal afterwards builds it
once, against the final column set, rather than against a layout about to grow.

That holds only if M5 decides its dense questions deliberately, and two of its
tasks decide them whether or not they mean to. An annotation is a column beside an
element, and whether a dense schema carries one for every element it describes is
a width decision: `evidence/merge-cost.md` measures a dense `Observation` at 1371
leaf columns, and an annotation per element multiplies that. And FR-017's carve-out
already asserts dense behaviour — the metadata group, "on a pruned schema it does
not appear and on a dense one it is present and null" — which T078c closes in M5,
with no dense mode to check it against.

So the questions go into the tasks that decide them rather than being left to be
discovered in M6. T063 to T066 state what a dense schema does with an annotation
column, and T078c states what it does with the metadata group. Neither needs a
dense mode to answer; both need someone to have asked.

### What M4 can no longer assert

T108 asserts that one column expression, built without reference to any dataset,
gives equal results on a fitted and on a dense dataset (FR-054, SC-010). With the
dense mode in M6 its dense arm cannot run at the flip.

It is not moved, because the property it protects — that the expression-to-column
API's unchanged signature is honest — is wanted at the flip rather than two
milestones after it. It runs at M4 over two fitted datasets of differing width,
which exercises the same schema independence, and regains its dense arm in M6 as
T108b. SC-010 is therefore satisfied in two parts.

### Corrections to decision 68 carried here

Two claims in decision 68 do not survive contact with the code. They are corrected
here rather than left in the instructions T031a carries into M6.

The traversal cannot be written against `CanonicalStructure`. It needs the FHIR
type, the cardinality and the entry kind, all of which live on `LayoutEntry` and
are reachable only through `DefinitionCanonicalStructure`. And the extension
switch and the open-type set cut primitive leaves, which `field()` returning empty
cannot express, since it returns empty for every primitive already. The strategy is
therefore a small interface of its own in `fhir-schema` — the entries at a node,
whether a bound admits one, and the cursor beneath it — and only the nesting bound
rides on the descent. Decision 68's "no interface change is needed" stays true of
`utilities`' `CanonicalStructure`, which keeps serving the merge untouched; it is
not true of what the traversal is written against.

And "(type, remaining depth)" is the wrong memoisation key. `SchemaBuilder` counted
recurrences per type identity along the path, with a reference bounded at zero,
which is not a scalar depth: two paths reaching the same type with different
path-count maps yield different subtrees. The correction is not a better key. The
bounded cursor is not memoised at all — a dense schema is built once per transform
— and its state stays out of `DefinitionCanonicalStructure`'s shared memo.

_Consequence for the specification_: FR-009, FR-010 and FR-044 carry a staging
note placing their delivery in M6; FR-017's dense clause and the dense row of
`data-model.md`'s guarantee table carry the same. SC-002's dense subset and
SC-010's dense arm are delivered in M6, and both rows say so. The milestone table
gains M6 and renumbers Completion to M7; the phase map gains Phase 17 and
renumbers Completion to Phase 18. `contracts/library-api.md`'s paragraph on the
options whose reach narrows gains the window in which they bound nothing at all.
Nothing in FR-011, FR-012, FR-057 or FR-058 changes: they were never about the
mode.

## 70. The transform is structured in and structured out, and JSON text is a layer over it

`io`'s public API is split in two. `ResourceTransformer` maps between datasets
and knows nothing of text: `toLayout` takes FHIR JSON already parsed into a
dataset and returns the layout, and `toJsonShape` takes the layout and returns
the dataset the JSON writer serialises. `FhirJsonReader` and `FhirJsonWriter`
apply it to JSON text, in files or in a `Dataset<String>`, and know nothing of
FHIR beyond the resource type they are told.

### Why

`PathlingContext.encode` takes a `Dataset<String>` or a column of strings, and
`decode` returns a `Dataset<String>`. Before this split `io` read only paths, so
T070 had no entry point to call, and the serialiser fused building the document
with writing it as text, so `NdjsonSink` (T082) could reach files only by
writing a column of strings. With the split, encode, decode and the sink are
each one call.

It also settles where M3 lands. A bundle is exploded into rows of JSON-shaped
entries (T068) and XML is parsed into JSON (T069), and both then meet the same
`toLayout` without either touching it. Anything that already holds FHIR JSON as
structures, such as a table of raw resources or `from_json` over a stream, is an
input with no text round trip.

And it lets the transform be tested on datasets built with an explicit schema,
including schemas inference never produces, rather than only on what a file of
JSON happens to infer.

### What each layer may know

The transformer holds everything the definitions decide: the traversal, the
converter table, the strictness check and its findings, and `EmptyPruning`,
which is a column expression over the definitions' shape. `io.egress` is folded
into `io.transform`, because the two directions share the definitions and the
converters and are tested as inverses; one class with two directions says so,
and two classes named for their direction do not.

The reader holds the JSON read and its `FAILFAST` mode, because what to do with
text that is not JSON is a question about text. The writer holds
`ignoreNullFields`, the same option on both of its outputs: to `to_json` for a
`Dataset<String>`, and to the JSON writer for files, which then write the
shaped dataset directly with no column of strings between. Omitting a null field
is serialisation; making an empty structure null is not, because the JSON writer
cannot tell an empty structure from a present one.

`ResourceTransformer.source()` and `read()` are removed. A caller who wants the
findings before storing reads the JSON and passes its schema to
`ResourceTransformer.findings`, which is unchanged.

### The input contract

`toLayout` accepts what Spark's JSON reader infers from FHIR JSON — every
element under its FHIR JSON name, structures as structures, repeating elements
as arrays — together with the lossless widenings of a primitive's JSON type that
a hand-built dataset would reasonably carry:

- `integer`, `unsignedInt`, `positiveInt` and `integer64` accept `ByteType`,
  `ShortType`, `IntegerType` and `LongType`. Inference gives only the last.
- `decimal` accepts the same four integral types, beside `DoubleType` and
  `DecimalType` at any precision. An integral value is an exact decimal, and its
  text is the text the decimal is stored as.

`FloatType` is not accepted for `decimal`. A float cannot hold most decimal
fractions, so a float column has already lost the value before the transform
sees it, and accepting it would store the rounding silently. `DateType` and
`TimestampType` are not accepted for the temporal types, because a Spark date or
timestamp cannot hold a FHIR partial date or an offset, and "JSON-shaped" means
the JSON data model rather than any type a value could be converted from.

Acceptance is still a mismatch check. A type outside the set is reported as an
encoding mismatch and the column is stored as a typed null, exactly as before,
so the widening admits more conformant input and admits nothing that
contradicts the definitions.

### One resource type per input

The input to `toLayout`, and so to the reader, carries resources of the one type
it is told. This is documented and not enforced.

The alternative was for the reader to filter on the `resourceType` of each
document before inference. It was not taken for `io`: bulk export writes one
type per file, and the filter would cost a pass over the text on every read to
serve an input the API does not describe. What happens when the constraint is
broken is recorded here so that it is not rediscovered: inference unions every
type's fields into one schema, so the findings name every field of the other
types as undescribed content, and each row of another type is stored as a
resource of the named type, keeping its own `resourceType` string, with whatever
fields the two types share.

The public API is a different matter. Today's `encode` parses every document and
keeps those of the requested type, discarding the rest without a word
(`EncodeResourceMapPartitions.processResources`), so a mixed `Dataset<String>`
is an input it accepts and its callers may rely on. FR-043 and SC-008 preserve
that, so the filter belongs to T070: `PathlingContext.encode` selects the
documents of the requested type before handing them to the reader. The cost of
that pass is paid only on the path whose contract already implies it.

### The egress output is for serialising

`toJsonShape` returns a decimal as a double, because a double is what the JSON
writer emits as a bare number (decision 68). Its output is the intermediate the
writer serialises and is not a form to store: written to Parquet it would hold a
decimal at a double's precision. Its javadoc says so.

_Consequence for the specification_: T059 and T079 carry a note that this
decision relocated them, and T080's file moves to `io.transform`. A new task,
T080a, records the split in M1. T070, T081 and T082 are reworded to call the
reader and the writer, and T070 to keep `encode`'s selection of the requested
type. `plan.md`'s module layout loses `io/egress` and gains
`io/json`. Nothing in the functional requirements changes: FR-019's omission is
met by the same two mechanisms, divided differently between the layers.

## 71. Empty pruning leaves M1 and returns with the decimal serde

Neither direction of the transform removes what the output would otherwise write
as empty. `EmptyPruning` is deleted. Egress returns a stored structure that is
null as a null, and examines nothing else: the layout is assumed to carry no
structure whose every field is null, and no array holding a null or holding
nothing. (The addendum below corrects this: conformant input carries both, and
they pass through.) FR-019 is deferred with it, and both come back with the
decimal serde.

### Why

M1's review of the rescoped implementation measured what the pruning cost. To
decide whether a structure was empty, `EmptyPruning.structure` built the same
child columns twice — once in the `isNotNull` chain that answered the question
and once in the `struct` that was the answer — and `EmptyPruning.array` did the
same with the filtered array. Each level of nesting therefore doubled the
expression, and an array of structures quadrupled it. A single conformant
`Questionnaire` whose `item` nests eight deep exhausted a 4 GB heap instead of
round-tripping; at six levels the plan was 3.8 million characters and took 45
seconds. Measured here at twelve levels: the pruning dies with `Java heap space`
in 29 seconds, and without it the same document round-trips in 5. The R4 and
Synthea corpora are shallow, so 330 green tests said nothing about it.

The blow-up is fixable, and the fix is known. The emptiness test is recursive
and data-dependent — there is no static answer, because a structure is empty
only in the rows where everything beneath it happens to be null — but the
predicate does not have to be built out of the values it guards. Built instead
from the stored leaves, with `exists` over an array rather than `size` over a
built one, it is polynomial. Better still, ingest can carry the invariant
itself, as FR-011 already says of the schema: prune there, where the data is
being inspected anyway, and egress needs only `when(stored.isNotNull(), …)`.

Both are deferred rather than built, because pruning and the decimal share a
boundary. A decimal loses its lexical form because the document is parsed and
rendered by Spark's JSON reader and writer, which route a number through a
double (decision 68). An empty structure is written because that same writer
renders what it is given. A custom serde — reading and writing the document
ourselves, most likely as a UDF over the layout — decides both at once: nothing
parses a decimal into a double, and nothing writes an object we did not intend
to write. Building the pruning now as a column expression, and again inside the
serde later, is work M1 does not need to do. Whichever way the serde goes, the
ingest-side invariant above is the fallback and should not be rediscovered.

### The assumption this rests on

The input carries no element that requires pruning. Specifically: a JSON
document carries no empty object and no empty array, and no element whose only
content is undescribed or contradicts the definitions; and layout data from any
producer carries no structure whose every field is null, and no array holding a
null or holding nothing.

Measured against the corpora this milestone has, the assumption holds. **It
holds because of the corpora, not because of conformance**: they happen never to
give an element primitive metadata as its only content, and conformant R4 input
that does, such as the data-absent-reason pattern, produces a document that is
not conformant FHIR. The addendum below says what conformant input produces. With
the pruning removed, the specification examples (168 round trips over 25 resource
types), the Synthea types, the Parquet round trip and every other test pass
unchanged. Two tests changed, and both are the ones that constructed the
excluded input deliberately: `EgressOmissionTest`, which built the empty shapes
by hand, is deleted, and the array-shortening case in `PrunedSchemaGuaranteeTest`
now asserts the empty object that replaces it.

Where the assumption is violated the document that comes back is not conformant
FHIR: an element that stored nothing is written as `{}`, so an array of them
comes back as `[{}]`. An empty array in the source is omitted where it is alone
in the file, because it is inferred with a `NullType` element and takes the
absent path. Where another document in the file types the column, the empty
array is stored and written back as `[]`, and an empty object as `{}`, with no
finding. That input is not conformant, so it is outside the assumption, but
the reader passes it through silently; T134h records it with the rest of the
residue. The layout is also wrong for a reader that is not the
writer — from M2 the engine queries these columns directly, and a present but
all-null structure answers `exists()` with true. That is the price, and it is
why this is deferred rather than dropped. It is not a licence to write an empty
structure; it is a statement that M1 does not detect one.

### What remains

Egress still returns a null where the stored structure is null. That is not
pruning and does not examine the data: `struct(…)` in Spark is never null, so
reading the fields of a null structure and rebuilding them would make an absent
element present, and every absent element would be written as `{}`. One
`isNotNull` on the stored column per level answers it, which is why the
expression is now linear in the schema rather than exponential in its depth.

_Consequence for the specification_: FR-019 is deferred, with T073, T074, T075
and T080 withdrawn and `EgressOmissionTest` deleted. Decision 66's array
shortening no longer happens, and its addendum records what replaces it. T080a
stands, but decision 70's list of what the transformer knows no longer includes
`EmptyPruning`. A new task, T080b, pins the nesting depth that the pruning could
not survive, because no corpus in this repository reaches it. The follow-up is
T134h, which is raised with the decimal work rather than on its own. The
deferral is also marked everywhere the specification still promised the
omission: US2's second acceptance scenario, the two edge cases on an all-null
structure and a null-only array, rules 2 and 3 of the data model's round-trip
rules, and the Assumptions section. The three exception lists — FR-016, the data
model's and the storage-layout contract's — named the array shortening this
decision ended, and now say what happens instead. The addendum below restates
the assumption, carries it into FR-019, and adds T080c.

### Addendum to 71 — the assumption is corpus-true, and conformant input empties structures

Round 4 of M1's review found that conformant R4 input violates the assumption
as stated, and that the transform creates the emptiness rather than passing it
on. `{"name":[{"family":"Keep"},{"_family":{"extension":[…]}}]}` comes back as
`{"name":[{"family":"Keep"},{}]}`, because the layout is projected onto the
definitions and M1 does not store a primitive's id and extensions. Nothing in
the input was empty. The measurements behind this addendum are in
`null-and-empty-handling.md`.

**The emptiness is kept, not pruned.** A structure whose only content was
primitive metadata is an element that exists. FHIRPath counts it: fhirpath.js
answers 3 for `name.count()` on that document. The released encoder counts it
too, because it stores the item as a structure that only its `_fid` keeps
non-null. The engine drops a null from an array of structures as it traverses
one, so pruning the item — dropping it or nulling it in place, which read the
same — gives 2, and shifts every later index. So pruning here would depart from
both the released encoder and the specification, and M1's criterion is that the
answer on the new layout equals the answer on the released encoder until M5
changes both on purpose.

That criterion first held only where something else read with the structure kept
its element's column: alone, a metadata-only structure had no column and was
omitted. Decision 72 removes that condition. The criterion now holds wherever the
primitive is singular; where it repeats, the released encoder drops the
structure and the layout keeps it, and decision 72 records why the layout is
right to.

The rule that follows is to prune a structure according to why it is empty,
never according to where it sits. One emptied by the carve-out exists and stays.
One emptied by non-conformant content is not an element, and pruning it is
right, but it is outside M1, whose requirement is conformant input.

**What conformant input produces.** Where every document in a file conforms (a
file here, and below, is whatever is read in one call: a file, a directory or a
dataset of strings), the layout carries exactly two kinds of emptiness, and both
are kept on purpose until M5 stores primitive metadata:

1. A structure whose only content was a primitive's id and extensions. It keeps
   its place and is written as `{}`, whatever other conformant documents are
   read with it (decision 72). A complex element's own `id`, `extension` and
   `modifierExtension` are stored content, so any of them keeps a structure from
   being empty; only primitive metadata can leave one with nothing.
2. A repeating primitive's positional null. The layout keeps it, as the released
   encoder does, because FHIR aligns it with the `_x` array. Until M5 writes that
   array it is written bare, so `"given":["Ann",null,"Bee"]` and `"given":[null]`
   can appear.

Neither output is conformant FHIR. Both are FR-016 item 1's loss, reported at
read time as FR-017 requires, and both disappear in M5, when the metadata is
written beside them.

**What is residue.** A structure emptied by non-conformant content is written as
`{}` if anything else in the file keeps its column: undescribed or contradicting
content (FR-016 item 5), and a value re-typed by a sibling (item 6). Item 6 is a
property of the file, so it reaches a document that is conformant on its own —
`{"photo":[{"contentType":"image/png"},{"size":10}]}` comes back with `{}` for
its second photo when another document in the file carries `"size":1.5`. That
is why conformance is stated per file. This residue, and layout data written by
another producer, wait for the custom JSON serde (T134h).

**The M2 hazard narrows.** A present but all-null structure answering `exists()`
true is correct where the structure held primitive metadata, because the element
exists. It is wrong only where non-conformant content emptied it, and T134h is
narrowed to that.

**Why not prune only the residue now.** Ingest pruning that counts `_x` as
presence would store the same thing for conformant input, and prune only the
residue. It costs about eighty lines and a revision of decisions 66 and 71, for
input M1 does not promise to handle and the serde is planned to handle. It is
worth its cost before M4 only if the serde will not land before the public API
switches.

_Consequence for the specification_: FR-019 carries this assumption in full, and
says it is true of the corpora rather than of conformant input. FR-016's items
1, 5 and 6, the data model's exception list and the storage-layout contract say
what is written where content was an element's only content. The Assumptions
section carries the deferral. T134h is narrowed. T080c pins the two conformant
cases and the item 6 case, because no conformance suite carries any of these
shapes and a later pruning would otherwise pass unnoticed.

## 72. A primitive carried only as its metadata is stored as a typed null

Where the source carries a primitive only as its metadata group — `_family` with
no `family` — and the group has the outer shape FHIR gives it, the layout stores
the primitive as a null of the type the definitions give it. The structure
holding it therefore has a stored field and is kept, whatever other conformant
documents are read with it, and is written as `{}`. A group that another
document re-types no longer has that shape, and keeps nothing (see "Malformed
metadata groups"). Before this, such a primitive had no column, and a structure
holding nothing else was omitted wherever nothing else read with it gave the
element a column.

### Why

Round 6 of M1's review showed that the omission was a choice, not a
constraint. Decision 71's addendum, as round 5 amended it, said that keeping the
structure "would need a column the file gives no type to". The file gives
`family` no type, but the definitions do, and the transform already builds a
null of the declared type for an element whose observed shape contradicts them.
The same null serves here, and the change is one branch in the field planning of
`ResourceTransformer`.

The omission cost three things, which this decision restores:

- **Parity with the released encoder, for a singular primitive.**
  `FhirEncoders.forR4()`, run in round 6 on the same inputs, stores the lone
  `name` holding only `_family`, `contact.name` holding `_family`, and
  `Encounter.period` holding only `_start`: `size(name)` is 1 and `period is not
  null` is true. The layout omitted all three, so `name.count()` would have
  answered 0 against 1. Decision 71's addendum makes that parity M1's criterion.
- **The addendum's principle**, to prune a structure by why it is empty and never
  by where it sits. Whether the structure survived depended on its neighbours.
- **An honest round trip over the corpus.** The omission was not only
  contrived. `ActivityDefinition/referralPrimaryCareMentalHealth` in the
  specification's examples carries a `timingTiming` whose only content is
  `_event`, and the round-trip harness removed emptied containers from the
  source side — the cascade decision 68 recorded — so the layout's loss of an
  element FHIR says exists went unseen. The cascade is removed, and
  `timingTiming` now comes back as `{}` on both sides. That instance is a
  repeating primitive, so it is a divergence from FHIR that the harness hid,
  not from the released encoder, which drops it too; see below.

### Where it departs from the released encoder

A repeating primitive carried only as `_x` — `{"name":[{"_given":[{…},{…}]}]}`,
or that `timingTiming` — is kept by the layout and dropped by the released
encoder. The released encoder reads through HAPI's JSON parser, which discards
an `_x` array with no `x` array beside it, so the element, and the structure
holding only it, never reach the encoder. Round 7 of the review measured it on
both inputs and on the corpus file itself: the layout answers `size(name)` 1 and
`timingTiming is not null` true, the released encoder no `name` and false.

The layout keeps it. FHIR says the element exists, fhirpath.js counts it, and M5
stores the `_x` array, after which the layout would keep the element whatever
this decision said. Parity with the released encoder is M1's criterion because
the encoder usually implements FHIR; it is not a reason to copy a parser's loss,
and restricting the change to singular primitives would restore parity only to
undo it at M5. The divergence is recorded on T101, the first M2 task whose
answers meet it, so a parity test over both layouts expects it rather than
discovering it, and `DeferredPruningTest` pins the layout's side.

For a repeating primitive the stored value is a null array, not an array of
positional nulls, so `_given` with two entries leaves `given` null. T061 is
amended to decide how the value array aligns with the group it writes.

### Malformed metadata groups

The branch fires only on a group in the outer shape FHIR gives it: an object
beside a singular primitive, an array of objects beside a repeating one. What
the group holds is not checked — `{"bogus":1}` and `{"id":5}` pass, and are
reported as metadata and keep their structure, so the report still agrees with
what is stored — because M5, which writes the group, has to descend into it and
is where its content is checked (T061). `"_family":"x"`,
`"_family":[{…}]`, `"_family":null`, `"_given":{…}`, `"_given":[[…]]` and
`"_given":[null]` are non-conformant, and alone they keep nothing, like the rest
of such content. Round 7 found that the first cut fired on any group, which
would have kept `name` as `{}` on content that is not FHIR.

Such a group is reported as a shape mismatch, not as primitive metadata. Round 8
found that it was reported as the metadata that is merely not yet stored, which
under this decision implies the structure is kept, when it is not. The check and
the branch share one predicate, `StrictnessCheck.isConformingMetadataGroup`, so
what is reported as metadata is exactly what keeps a structure.

The group's type is inferred for the whole input, so one malformed group
re-types it for every document read with it. `{"name":[{"_family":{…}}]}` is
conformant, but beside `{"name":[{"_family":"x"}]}` its group is read as a
string, and its `name` is lost with the malformed one. That is FR-016 item 6, a
conformant value lost because another document re-typed its column, with the
per-input condition of item 5: a re-typed element keeps its own column as a
typed null, so its structure survives as `{}`, but a re-typed group is not the
element's column, so the structure it alone kept is written as `{}` only where
something else read with it keeps the structure's column, and is otherwise
omitted. It is reported as the shape mismatch that caused it. The released
encoder, which parses each document alone, keeps the first document's `name`;
parity is not owed for an input that is not conformant.
`ResourceTransformerTest` and `StrictnessTest` pin the omission, the diagnosis
and the cross-document case, both where the structure is omitted and where
something else keeps it as `{}`, for a singular and a repeating primitive.

### What it does not change

The stored value is a null, so the metadata itself is still lost and still
reported, under FR-017's carve-out, until M5. The output is `{}` where FHIR
would carry `"_family":{…}`, which is FR-016 item 1's loss and not conformant
FHIR, exactly as it already was where a neighbour kept the column. M5 writes
the metadata group beside the null, which is what FHIR's JSON means by a
primitive with no value, so this column is the one M5 needs rather than
scaffolding it has to remove.

Content the definitions do not describe, or contradict, keeps the per-input
condition: where it was an element's only content, the element is kept as `{}`
only if something else read with it keeps its column (FR-016 item 5). That is
residue, outside what M1 promises, and T134h carries it.

FR-011 is amended rather than broken. A structure appears only where some
descendant leaf is populated, and a primitive whose id or extensions the source
carried is populated: FHIR says the element exists. The field-less structure
FR-011 exists to prevent still never arises, because the structure holds the
typed null.

_Consequence for the specification_: FR-011 says what counts as populated.
FR-016 item 1, FR-019, the edge case, the Assumptions bullet, the data model and
the storage-layout contract drop the per-input condition for primitive metadata
that round 5 added, and say where the released encoder agrees. A malformed
metadata group is reported as a shape mismatch. T101 carries the
repeating-primitive divergence instead of the one round 5 recorded. T061 carries
the alignment question. T080d records the change, T080c's cases now assert the
structure is kept, including where only a repeating primitive's `_x` held it,
and T076 and T078c say the cascade is gone.

## 73. M2 ports the engine progressively and ends with the `fhirpath` suites on the new layout

M2's objective is that the FHIRPath engine supports the new layout while keeping
its support for the previous one. It gets there by porting one feature at a time
behind the normalisation decision 55 places in the traversal expression, and it
ends with the `fhirpath` module's own suites running on the new layout **by
default**, and green. The public API still switches in M4, not earlier.

The order is fixed:

1. **T038m first.** The approach depends on whether the traversal expression can
   normalise the previous layout, above all the `_fid` extension-map lookup
   inside a `transform` lambda and the self-recursive extension type. If it
   cannot, the fallback is dispatch in the collection classes (decision 47), and
   the rest of M2 is replanned against that before any feature is ported.
2. **Coverage, then absent elements.** T020–T027 and T049a, then the
   absent-element block. Tolerance of an absent field comes first because every
   later step reaches the data through it.
3. **The port**, in this order: decimals, quantity canonicalisation, Coding by
   name, inline extensions (removing `extensionMapColumn`), reference keys. Each
   step adds its normalisation branch to the traversal expression. The existing
   suite stays green on the previous layout, which now reads through the
   branch, and a small set of new-layout evaluation tests shows the ported
   feature works there too.
4. **The switch of the `fhirpath` suites.** A full run on the new layout with
   the failures sorted by cause (T100h), then T100 and T100g, which move here
   from M4.

### Why progressive, and not everything at once

Not mainly to avoid a red build: nothing on `issue/2367` reaches `main` until
the programme merges. The reason is diagnosis. Replacing the fixtures, the schema
and the engine together turns every failure into three possible causes: a
fixture, an expected value, or the engine. Normalising the previous layout at the
traversal point runs the whole existing suite over each ported feature as it
lands, so every step is small enough to bisect and the regression check comes
for free. The branches are not throwaway, either: they are how the previous
layout stays readable until T100e.

The approach has one known weakness, and the plan is built around it. The
previous layout is dense and bounded, so emulating the new layout on it never
produces what only real new-layout data produces: columns missing because the
schema is pruned, one complex type with different shapes at different paths,
decimals whose scale is set by their text, and extensions on primitive elements.
"Green on the previous layout" therefore proves the engine on the emulated shape
only. Two things close the gap. The new-layout evaluation tests added at each
step are aimed at exactly those four cases. And the new-layout arm of the
fixtures (T034–T036, T037a) is built early, as an opt-in run that does not gate
the build, so the number of failures left before the switch is always known.

### What moves

- **The switch of the `fhirpath` suites moves from M4 to M2.** T100 and T100g
  move whole. T100 already covers only the `fhirpath` module's suites. The
  `library-api` conversions (T100d) and the Python and R runs (T100b) depend on
  the public API writing the new layout, and stay in M4.
- **Phase 10 moves from M2 to M4.** Divergent files read as one dataset and
  Delta widening on upsert are IO behaviour, not engine work, and they are the
  only part of M2 a user could observe. M2 now joins the milestones that change
  nothing a user can see, apart from the two fixes noted below. Phase 10 keeps
  its number.
- **Variadic reconciliation stays in M2 until the measurement says otherwise.**
  It was proposed for M5. But the pruned schema gives one complex type different
  shapes at different paths as soon as the suites switch, so whatever the switch
  needs has to land in M2 or be excluded with approval. T100h counts the
  failures that come from it. What the switch does not need, T104b being the
  likely case since it pins a defect already on `main`, may then move to M5. That
  choice is made at T100h and not before.
- **The previous-layout arm of the fixtures** stays available as an opt-in run
  after the switch. Until M4 the `library-api`, Python and R suites exercise the
  previous layout through `PathlingContext`, which still writes it. From M4 a CI
  job runs the `fhirpath` suites on the previous layout (T100i), so the
  normalisation stays covered until T100e removes it.

### The rule on existing tests

Existing tests and conformance exclusions stay unchanged throughout M2. Any
change to one needs the programme owner's approval, per test. Three are known:

- **T110 closes #2625**, so the two `FhirViewExtraTest` exclusions start
  reporting that an excluded test passed. Removing them in M2 is **approved**.
- **T111 casts a view column to its declared FHIR type on output.** On the
  previous layout this should change no output type: a declared `decimal` is
  already `DECIMAL(32,6)`, which is what `FhirPathType.DECIMAL` maps to. Any
  existing test whose expected output type does change is listed for approval.
- **Decimals on the previous layout are normalised too**, so the whole existing
  suite exercises the new decimal path. The query-time type does not change on
  either layout (FR-035). The value can change in one respect, the scale. The
  previous layout keeps the source scale in `_scale`, while the new layout
  stores the form a double round trip gives. The normalisation carries `_scale`
  into the text it produces, so no existing test changes during the port. An
  existing test that renders a decimal as text, `1.50` against `1.5`, may
  change at the switch, when its fixture is written in the new layout, and is
  listed for approval then.

Whatever else T100h finds is listed the same way.

### What else it corrects

T020–T022, T088 and T098 assumed a `resolve()` that joins to the target
resource. The engine's `resolve()` returns type information only, for `is` and
`ofType` (#2522), and joins go through `getResourceKey()` and
`getReferenceKey()`. The engine already builds the resource key from the plain
`id` element, and only the encoder reads the stored versioned id. So those tasks
are rewritten against the functions that exist, and T098 is reduced to confirming
that no stored versioned key is read and testing it on the new layout.
`spec.md`'s US3 acceptance scenario 4 carried the same assumption and is
restated against the key functions.

_Amends_ decisions 47, 51, 52 and 55 as to where the switch of the test estate
happens, and supersedes the placement of Phase 10 in M2.
