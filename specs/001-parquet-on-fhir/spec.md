# Feature Specification: Parquet on FHIR

**Feature**: `001-parquet-on-fhir`
**Created**: 2026-09-14
**Status**: Draft

Replaces Pathling's Catalyst-encoder-based FHIR encoding with a schema-driven
encoding conforming to the [Parquet on FHIR](https://github.com/aehrc/parquet-on-fhir)
specification, and migrates the FHIRPath execution engine and its dependencies
to the new layout.

Tracks GitHub issue [#2367](https://github.com/aehrc/pathling/issues/2367).

This specification is the single source of truth for the programme. It
supersedes the earlier draft design and the five change proposals
under `openspec/changes/` that belong to this work.

## Why

Five drivers, in the order they motivate the work.

1. **Performance** of encoding and decoding, and of FHIRPath execution, through
   reduced schema width. A hypothesis, not a measurement; nothing is gated on
   it.
2. **Lossless representation of FHIR**, permitting a full `JSON → storage → JSON`
   round trip.
3. **Attribute-level extensions and ids**, and unconstrained or data-inferred
   depth for recursive types.
4. **Automatic merging** of Parquet and Delta files written with differing
   sparse schemas into a common schema.
5. **Removal of the dependency on internal Spark Catalyst API in the encoding
   path.** The current encoder is built from hand-authored Catalyst expression
   trees against internal, partly deprecated API, which breaks on Spark version
   upgrades.

Driver 5 is why the encoder is replaced rather than modified.

## User Scenarios & Testing _(mandatory)_

### User Story 1 - Data is stored in the Parquet on FHIR layout (Priority: P1)

A user loads FHIR JSON through the library and the data is written in the
Parquet on FHIR layout: decimals as strings with a numeric annotation,
primitive ids and extensions in `_field` groups, extensions inline, dates
carrying range annotations, and quantities carrying both the specification's
canonical annotation and a magnitude-preserving one beside it.

**Staged.** M1 delivers the decimals as text, the inline extensions and the
reporting of content this layout does not store. Every annotation, and the population of the `_field` groups,
land in M5 — so scenarios 1, 2 and 4 below are met in part at M1 and in full at
the end of M5. FR-021's staging note, FR-017's carve-out and decision 49 record
why; nothing here is descoped.

**Why this priority**: Every other story reads what this one writes. The layout
is the object of the change.

**Independent Test**: Load a corpus, inspect the stored schema and values
against the specification, without any FHIRPath evaluation.

**Acceptance Scenarios**:

1. **Given** a resource carrying a decimal, **When** it is stored and read back,
   **Then** the stored value is text numerically equal to the source and a numeric
   annotation accompanies it. *(The annotation is staged to M5. The source's own
   lexical form is not preserved — decision 68.)*
2. **Given** a resource carrying an extension on a primitive element, **When** it
   is stored, **Then** the extension appears in the `_field` group beside that
   element. *(Staged to M5. Until then the traversal declines to emit the group,
   and the dropped content is reported rather than silent — FR-017's
   carve-out.)*
3. **Given** a resource carrying an extension on a complex element, **When** it is
   stored, **Then** the extension appears inline on that element and no
   root-level extension map or field-id column is emitted.
4. **Given** a quantity, **When** it is stored, **Then** the specification's
   canonical annotation accompanies it, followed by a second annotation whose
   precision preserves magnitude across unit conversion. *(Staged to M5.)*
5. **Given** content the definition set does not describe, **When** it is
   loaded, **Then** the load succeeds, the content is discarded, and a warning
   names it. *(Decision 68 removed the switch; there is no failing mode.)*

---

### User Story 2 - FHIR JSON round-trips without loss (Priority: P1)

A user loads conformant FHIR JSON and exports it again, and receives
semantically equal resources: object key order ignored, array order significant,
numbers compared numerically, subject to the exceptions FR-016 lists.

**Why this priority**: Driver 2. It is the claim the format is adopted for, and
it is the strongest single check that the layout and its transforms are correct.

**Independent Test**: Round-trip a corpus of FHIR R4 specification examples plus
a Synthea dataset, asserting semantic equality resource by resource, with no
FHIRPath evaluation involved.

**Acceptance Scenarios**:

1. **Given** a conformant resource, **When** it is loaded and exported,
   **Then** the exported resource is semantically equal to the source.
2. **Given** a resource with an element absent, **When** it is exported,
   **Then** that element is absent from the output rather than present and null,
   and no empty object or null-only array appears where the source had nothing.
   *(Decision 71 defers the second half with FR-019, so nothing checks it. It
   holds for conformant JSON, where every `{}` and bare `null` M1 writes stands
   for source content it did not store, but it does not hold for layout data
   written by another producer. Decision 71's addendum lists what M1 writes in
   their place.)*
3. **Given** a pruned schema, **When** conformant input is round-tripped,
   **Then** the guarantee holds without reference to the dense bounds, subject
   to FR-016's exceptions. *(Until M5 those include primitive id and extension
   content, per FR-017's carve-out; the exclusion is asserted rather than
   incidental, and T078c removes it. The scenario asserting that content the
   dense bounds drop is detectable was withdrawn by decision 68 with
   `BoundsCheck`.)*

---

### User Story 3 - The FHIRPath engine evaluates over the new layout (Priority: P1)

A user runs existing FHIRPath expressions, ViewDefinitions and searches over
data in the new layout and gets the same answers as before.

**Why this priority**: Without it the stored data is unusable. This is the
"migration of the execution engine" that defines the scope of this work.

**Independent Test**: Run the FHIRPath suite, both conformance baselines and the
SQL-on-FHIR compliance suite over data in the new layout.

**Acceptance Scenarios**:

1. **Given** a decimal stored as text, **When** an expression compares or
   computes on it, **Then** the result matches the result over the previous
   layout.
2. **Given** a file carrying no annotations at all, **When** any expression is
   evaluated, **Then** the result is the same as over an annotated file.
3. **Given** a Coding column narrower than the canonical layout, **When** a
   terminology operation is applied, **Then** it reads the fields by name and
   the absent ones are null.
4. **Given** a reference, **When** `resolve()` is applied, **Then** the referenced
   resources are returned without relying on a stored versioned-key column.
5. **Given** a quantity with no canonical annotation, **When** it is compared
   across units, **Then** the canonical value is computed and the comparison
   succeeds.

---

### User Story 4 - Queries work over a schema fitted to the data (Priority: P1)

A user queries a dataset whose stored schema carries only the elements the data
populates, and expressions naming elements absent from that schema evaluate to
empty rather than failing.

**Why this priority**: The sparse schema is what delivers drivers 1 and 4, and it
is the behaviour most likely to break existing expressions.

**Independent Test**: Run the suite in sparse mode; assert that an expression
over an absent element yields empty.

**Acceptance Scenarios**:

1. **Given** an element defined by FHIR but absent from the stored schema,
   **When** an expression traverses to it, **Then** the result is an empty
   collection.
2. **Given** an element FHIR does not define, **When** an expression traverses to
   it, **Then** an error is raised.
3. **Given** a choice element where only one variant was ever populated,
   **When** an expression selects an unpopulated variant by type, **Then** the
   result is empty.
4. **Given** a ViewDefinition column declaring a FHIR type over an absent
   element, **When** the view runs, **Then** the output column carries the
   declared type.
5. **Given** a ViewDefinition column with no declared type over an absent
   primitive element, **When** the view runs, **Then** the output column carries
   the type the definitions give the element.
6. **Given** an expression combining an absent element with a populated one,
   **When** it is evaluated, **Then** it succeeds rather than failing on a type
   mismatch.
7. **Given** one column expression, **When** it is applied to a fitted dataset
   and to a dense dataset holding the same resources, **Then** the results are
   equal. *(Asserted at the flip across two fitted schemas of differing width,
   and in this form in M6 with the dense mode; decision 69.)*
8. **Given** two collections of the same FHIR type reached by different paths,
   whose fitted schemas differ, **When** they are combined, **Then** the result
   holds every element of both and can be traversed.

---

### User Story 5 - Files written at different times read as one dataset (Priority: P2)

A user loads a second batch containing elements the first batch did not carry,
and queries the dataset as a whole without losing data from either batch.

**Why this priority**: Driver 4, and the steady state for any incrementally
loaded warehouse once schemas are fitted to data.

**Independent Test**: Write two batches with divergent schemas; query across
both and assert every resource appears with correct cardinality.

**Acceptance Scenarios**:

1. **Given** two batches whose schemas differ, **When** they are read as one
   dataset, **Then** the union of elements is available and no resource is
   missing.
2. **Given** such a dataset, **When** a view unnests a repeating element and
   projects a single leaf that only one batch populated, **Then** resources from
   both batches appear, with the unpopulated leaf null.
3. **Given** an append whose schema differs from the table's, **When** it is
   written to a Delta table, **Then** the write succeeds and the table schema
   widens.

---

### User Story 6 - Primitive ids and extensions are reachable from FHIRPath (Priority: P2)

A user writes an expression that navigates to the id or extensions of a
primitive element, which the previous layout could not represent at all.

**Why this priority**: Driver 3 is only half delivered if the data is stored but
unreadable. New capability rather than migration, so it follows the migration
stories.

**Independent Test**: Evaluate expressions reaching a primitive's id and
extensions over fixtures carrying them.

**Acceptance Scenarios**:

1. **Given** a primitive element carrying an extension, **When** an expression
   navigates to its extensions, **Then** the extensions are returned.
2. **Given** a primitive element carrying an id, **When** an expression navigates
   to its id, **Then** the id is returned.
3. **Given** a primitive element carrying neither, **When** either is navigated
   to, **Then** the result is empty.

---

### User Story 7 - Existing data is detected and users are told what to do (Priority: P2)

A user points the library at data written by an earlier release and gets an
actionable error rather than a confusing failure or silently wrong answers.

**Why this priority**: The worst failure mode in the programme is old data
flowing into an engine expecting new conventions. Correct against the current
codebase, so it can land early.

**Independent Test**: Read data in the previous layout through each source and
assert the error.

**Acceptance Scenarios**:

1. **Given** data in an unsupported layout, **When** it is read, **Then** an
   error at read time names the resource type, the detected layout, the expected
   layout and the remedy.
2. **Given** data in a supported layout that is merely narrow, **When** it is
   read, **Then** the read succeeds.
3. **Given** a schema carrying no marker fields, **When** it is read, **Then** the
   read succeeds, because marker absence is not evidence of an unsupported
   layout.
4. **Given** detection disabled for one source, **When** otherwise-rejected data
   is read through it, **Then** the read proceeds and other sources are
   unaffected.

---

### User Story 8 - Encoding no longer depends on internal Spark API (Priority: P3)

A maintainer upgrades Spark without the encoding path breaking on changes to
internal Catalyst API.

**Why this priority**: Driver 5 and the reason for replacement rather than
modification, but it is a property of the finished system rather than a user
journey, and it is verified by construction.

**Independent Test**: The module holding FHIR definitions and schema derivation
resolves no Spark Catalyst dependency, enforced by the build.

**Acceptance Scenarios**:

1. **Given** a dependency on internal Catalyst API added to the definition and
   schema module, **When** the build runs, **Then** it fails.
2. **Given** an import of internal Catalyst API added to the new encoding code,
   **When** the build runs, **Then** it fails.
3. **Given** the new encoding path, **When** it is inspected, **Then** it
   contains no expression encoder, no hand-authored serializer or deserializer
   expression tree and no FHIR object in a per-row plan.
4. **Given** the existing encoding implementation, **When** the work is
   complete, **Then** it is unmodified and still resolves at its current Maven
   coordinates.

---

### Edge Cases

- A decimal exceeding a double's precision: stored at that precision, computed
  at the documented cap. *(Decision 68; it was stored exactly while the lexical
  form was preserved.)*
- A decimal whose source form carries trailing zeros or an exponent: stored in
  the form a double round trip produces, numerically equal to the source.
- A FHIR element whose JSON encoding type contradicts the definitions, or whose
  column a sibling value has re-typed: nulled for that column, and reported.
- A resource carrying `contained` resources: dropped and reported, never
  represented.
- A struct whose every field is null in a given row: must not serialise as an
  empty object. *(Deferred by decision 71, and not checked in M1. It is written
  as `{}` where the structure's only content was primitive metadata, which is
  kept on purpose until M5 (decision 72), and where non-conformant content
  emptied it, including a value in another document that re-typed a shared
  column.)*
- An array whose every element is null: must not serialise as a null-only array.
  *(Deferred by decision 71, and not checked in M1. A repeating primitive keeps
  its positional nulls, and until M5 writes the `_x` array they align with they
  are written bare, so `"given":[null]` can appear.)*
- An array of arrays where a repeating element is declared, such as
  `"given":[["a","b"]]`: FHIR JSON has no nested arrays, so this contradicts
  the definitions as a single value does. It is reported as a shape mismatch
  and stored as absent, never as the text of the inner array.
- A source key named like one of the layout's annotations, such as
  `__birthDate_start`: an annotation is derived, never read, so the key is
  reported as undescribed content rather than checked as the element it
  annotates.
- A Bundle: accepted as a transport carrier and exploded to per-type tables,
  never stored as a resource type.
- XML input: converted to JSON before ingest; capability preserved.
- An expression whose result is a complex type reaching a caller as a column:
  its type reflects the data, and this is documented.
- A view over an absent primitive element whose column declares no type: the
  definitions supply the type, so it succeeds.
- Two collections of the same FHIR type reached by different paths, whose fitted
  schemas differ: combined by projecting both into the merged type, never by a
  cast.
- Two structures carrying the same fields in a different order: a hazard rather
  than an inconvenience, because struct equality compares positionally and
  ignores names. Canonical field order removes it.

## Requirements _(mandatory)_

### Storage layout

- **FR-001**: The system MUST store FHIR data in a layout conforming to the
  Parquet on FHIR specification, with the deviations recorded in FR-002 to
  FR-005.
- **FR-002**: (Annotation staged to M5.) Decimals MUST be stored as text, accompanied
  by a numeric annotation. The lexical form of the source is **not** preserved:
  ingest routes a number through a double and stores its text, and egress casts
  that text back to a double, so a stored decimal is numerically equal to the
  source rather than textually identical to it (decision 68).
- **FR-003**: (Primitive metadata group staged to M5; see FR-017's carve-out.) Primitive element ids and extensions MUST be stored in the
  specification's `_field` groups; extensions on complex elements MUST be stored
  inline. The previous root-level extension map and per-composite field id MUST
  NOT be emitted.
- **FR-004**: (Staged to M5.) Dates MUST carry the specification's range annotations.
- **FR-005**: (Staged to M5.) Quantities MUST carry two canonical annotations: the
  specification's own, at the fixed-point type the specification gives it, for
  interchange; and, immediately after it, a second annotation whose precision
  preserves magnitude across unit conversion. The second exists because the
  fixed-point type makes quantities differing by orders of magnitude compare
  equal, which no engine can compare on; the first exists because a consumer
  reading the specification's layout must find the specification's annotation
  under the specification's name.
- **FR-006**: `contained` resources MUST NOT be represented. *(Decision 68
  withdraws the requirement that their presence be governed by a switch, with
  the switch itself. Their presence is still reported, as a warning.)*
- **FR-007**: `Bundle` MUST NOT be stored as a resource type.

### Schema derivation

- **FR-008**: The stored schema MUST be derived from FHIR structural
  definitions, which determine element types and cardinality.
- **FR-009**: The system MUST support a schema pruned to the elements the data
  populates, and a dense schema comprising every element the definitions
  describe. *(Staged by decision 69. The pruned schema is delivered in M1 and the
  dense one in M6, after the annotations and the primitive metadata group, so
  this requirement is unmet between the flip and M6.)*
- **FR-010**: The dense schema MUST be produced by the same traversal as the
  pruned one, differing only in the strategy that decides which children it
  descends into and where it stops, so the two cannot diverge in type,
  cardinality or field order. *(Reworded by decision 68. The earlier wording —
  "the same derivation, with pruning skipped" — described a derive-then-filter
  step that no longer exists; one traversal with two strategies satisfies this
  by construction rather than by agreement between two walks. Staged by decision
  69: until the dense mode lands in M6 there is one traversal with one strategy,
  so this is met vacuously, and met by construction when the second arrives.)*
- **FR-011**: A complex element MUST appear in a pruned schema only where some
  descendant leaf is populated, so that a field-less structure never arises. A
  primitive the source carried only as its id and extensions counts as
  populated, and is stored as a null of its declared type until M5 stores the
  metadata beside it. *(Decision 72. It was otherwise omitted, and took the
  structure holding it with it.)*
- **FR-012**: The system MUST NOT determine element types or cardinality from
  the shape of the data.
- **FR-013**: The definition abstraction MUST expose child enumeration in
  declaration order, with a choice element appearing once as a choice rather
  than pre-expanded.
- **FR-014**: The definition abstraction MUST expose cardinality sufficient to
  determine whether an element is singular or repeating.
- **FR-015**: *Withdrawn by decision 67.* The definition abstraction reports a
  type as a value of the R4 enumeration, as it did before this programme.
  Representing a type code that enumeration does not contain belongs to whatever
  delivers a provider for another version or for profiles.

### Losslessness

- **FR-016**: For input conforming to the active definition set, a
  `JSON → storage → JSON` round trip MUST produce a semantically equal resource,
  with object key order ignored, array order significant and numbers compared
  **numerically**. *(Decision 68. Numbers were compared lexically while decimals
  preserved their source form.)* The guarantee is subject to a stated list of
  exceptions:

  1. primitive element ids and extensions, until M5 (FR-017's carve-out). Where
     they were a structure's only content, the structure keeps its place and is
     written as an empty object, whatever other conformant documents are read
     with it, because the primitive is stored as a null of its declared type
     (decision 72). A metadata group in another outer shape keeps nothing,
     including a conformant one that another document read with it re-typed
     (item 6). A repeating primitive's positional nulls are written without the
     `_x` array they align with. Either way the document is not conformant FHIR
     (decision 71's addendum);
  2. `contained` resources (FR-006) and `Bundle` (FR-007);
  3. decimal lexical form — trailing zeros, exponent notation as written, and
     precision beyond a double;
  4. `base64Binary` whitespace, which decoding and re-encoding canonicalises
     away although FHIR permits it;
  5. non-FHIR fields and fields whose JSON encoding type contradicts the
     definitions. Where that was an element's only content, the element is
     written as an empty object if anything else in the file keeps its column,
     and is omitted if nothing does, so the document is not conformant FHIR
     (decision 71, which ended decision 66's shortening of the array);
  6. a conformant value dropped because a sibling value re-typed its column —
     one `1.5` among integers costs that element for every resource in the file.
     Where it was an element's only content, that element is written as an empty
     object, even in a document that is conformant on its own (decision 71's
     addendum). Where the re-typed column is a primitive's metadata group, the
     group keeps nothing, so the structure it alone kept is written as an empty
     object if anything else read with it keeps its column, and omitted if
     nothing does (decision 72);
  7. content the dense bounds drop, which is no longer detectable (decision 68).
- **FR-017**: On a pruned schema the guarantee in FR-016 MUST hold subject only
  to that requirement's stated exceptions. On a dense schema it MUST hold within
  the configured nesting, extension and open-type bounds. *(Decision 68 withdraws
  the requirement that content those bounds would drop be detectable, with
  `BoundsCheck`; it is a named follow-up. "Unconditional" on the pruned schema
  is withdrawn with it, because FR-016 now carries exceptions that apply in both
  modes. Decision 69 stages the dense sentence with the mode itself, to M6.)*

  **Carve-out, until the primitive metadata group is populated.** Primitive
  element ids and extensions are not stored before M5 — the traversal declines to
  emit the group while nothing populates it, so on a pruned schema it does not
  appear and on a dense one it is present and null — so the pruned-schema
  guarantee excludes that content until then, and the exclusion MUST be reported
  rather than silent. This is the deliberate cost of deferring FR-003's metadata
  group past the flip; it is recorded as decision 49 and closed by T078c.
- **FR-018**: *Withdrawn for this phase by decision 68.* Content the definition
  set does not describe is ignored: the field is nulled and one warning names
  what was dropped. There is no configurable switch and no failing mode, so
  truncation is no longer prevented — it is reported. Restoring a fail mode, and
  validation of values against their declared types, is a named follow-up
  (T134g). Malformed JSON is unaffected and still fails the read, because a
  document that is not JSON is not content outside the definition set.
- **FR-019**: *Deferred by decision 71.* Export omits an absent element, which
  is what a null field costs nothing to leave out. It no longer omits a
  structure whose every field is null or an array holding only nulls, and
  nothing checks for either. The assumption is decision 71's in full: a JSON
  document carries no empty object, no empty array and no element whose only
  content is undescribed or contradicts the definitions, and layout data from
  any producer carries no structure whose every field is null and no array
  holding nothing or holding a null other than a repeating primitive's
  positional one, which this reader writes on purpose. That is true of this
  milestone's corpora, not of conformant input. *(Decision 71's addendum.)*
  Where every document read together conforms, two kinds of emptiness still
  reach the output, and both are kept on purpose until M5 stores primitive
  metadata: a structure whose only content was a primitive's id and extensions
  is written as `{}` in its place (decision 72), and a repeating primitive's
  positional null is written as a bare `null` without the `_x` array it aligns
  with. Neither output is conformant FHIR. Beyond those, a structure emptied by
  non-conformant content is written as `{}` wherever the documents read together
  keep its column, and that includes a conformant document whose column was
  re-typed by another document in the file. Detecting any of it is deferred to
  the custom JSON serde that the decimal's lexical form also waits on (T134h).
  The pruning that did it grew the expression exponentially with nesting depth,
  which T080b pins, and T080c pins what is written instead.
- **FR-020**: *Withdrawn by decision 68.* Lexical form is no longer preserved on
  any ingest path, so there is no per-path limitation to document. The file path
  and the `Dataset<String>` path now agree, because both route a number through
  a double.

### Annotations

- **FR-021**: The decimal, date and quantity annotations MUST be emitted by
  default, each kind individually disableable. **Staged.** The layout ships
  annotation-free at the flip and each kind lands afterwards, so this requirement
  is met progressively and in full only at the end of M5. FR-022 is what makes
  the staging safe, and decision 50 records it. A kind carried in more than one
  field — the date range's two bounds, the quantity's two canonical forms — is
  one kind and is governed by one switch.
- **FR-022**: The engine MUST compute correctly when any or all annotations are
  absent, using an annotation only as a fast path. An annotation MUST NOT be
  required for correctness.
- **FR-023**: Whether an annotation is used MUST be decided from the schema
  rather than per row. *That half is staged to M5, because it presupposes that an
  annotation exists to choose.* The same rule governs the layout normalisation
  the engine performs, and **that half binds from M2**: which layout a column is
  read as MUST be settled from the resolved schema, never per row. That normalisation happens at one
  site, the traversal expression, so that no code above it is written twice and
  removing it later touches one file (decision 55).

### Query engine

- **FR-024**: Traversal to an element the definitions describe but the schema
  does not carry MUST yield an empty collection.
- **FR-025**: Traversal to an element the definitions do not describe MUST raise
  an error.
- **FR-026**: Selecting a choice variant absent from the schema MUST yield an
  empty collection.
- **FR-027**: Combining an absent element with a populated one MUST succeed
  rather than fail on a type mismatch.
- **FR-028**: A view column declaring a FHIR type MUST have that type applied to
  the output column.
- **FR-029**: Deriving the output type of a column carrying no type information
  MUST fail with a message naming the column, its path and the remedy. An
  element absent from the schema MUST NOT be such a column: FR-055 gives it a
  type.
- **FR-030**: A column produced for an absent primitive element MUST carry the
  type the definitions give that element, so that it can be written to Parquet
  and reported with its FHIR type on every surface that returns a column.
- **FR-031**: Coding-valued columns MUST be decoded by field name, resolved once
  per schema, with absent fields decoding as null and a structure carrying no
  recognisable field rejected with an error naming the expected and actual
  fields.
- **FR-032**: Terminology operations MUST operate correctly on a Coding column
  narrower than the canonical layout.
- **FR-033**: Reference resolution MUST NOT depend on a stored versioned-key
  column. If a precomputed key proves necessary for join performance it MUST be
  expressed as an annotation, and the engine MUST work without it.
- **FR-034**: The engine MUST be able to navigate to a primitive element's id
  and extensions. **Deferred to M5**, with FR-003's writing half, per decision 49.
  The previous layout cannot represent this content and the engine cannot
  navigate it today, so deferring it regresses nothing.
- **FR-035**: Query-time decimal precision MUST remain as it is today, and the
  cap MUST be documented.
- **FR-036**: Unnesting MUST NOT be implemented in a way that reduces a read to
  a single leaf of a repeated element, because that silently drops rows from
  files lacking that leaf.

Requirements are numbered in the order they were added. FR-054 onward were added
after the schema-binding decision and belong to this section.

- **FR-054**: Traversal MUST tolerate a field absent from the input schema, and
  MUST decide presence from the resolved input schema rather than when the
  expression is built, so that a single column expression is valid over every
  schema conforming to the definitions. This preserves the current contract, in
  which an expression is converted to a column without reference to any dataset.
- **FR-055**: An absent element MUST be represented as a null typed by the
  principle: **the type the definitions give it wherever that type is
  unambiguous, and the bottom type wherever a concrete shape would
  over-constrain later combination**. Concretely, a singular primitive takes the
  definition's type, a repeating primitive an array of it, a singular complex
  element the bottom type, and a repeating complex element an array of the bottom
  type. A concrete minimal structure MUST NOT be used for a complex element,
  because it fails FR-027.
- **FR-056**: An operation requiring two collections of the same FHIR type to
  share a SQL type MUST reconcile them by projecting each **by name** into the
  merged type. It MUST NOT reconcile by casting, because a struct cast of equal
  arity reorders fields positionally and silently compares the wrong fields.
- **FR-057**: Struct field order MUST be canonical wherever a struct type is
  produced: schema derivation, the merge of two element types, the merge of
  divergent file schemas, and the reconciliation projection. Canonical order is
  definition order, restricted to the fields present, with the layout's own
  fields — the annotations, and the metadata group beside a primitive — at fixed
  positions relative to the element they accompany. Those fields have no
  definition element, so without a stated position two implementations could both
  claim to be canonical and still produce structs that compare positionally
  wrong, which is the failure this requirement exists to prevent.
- **FR-058**: The merged type MUST be the recursive field-wise union of the
  inputs, and one implementation MUST serve both FR-056 and the merging of
  divergent file schemas in FR-041.

### Reading and writing

- **FR-037**: File-based sources MUST determine the layout of the data they read
  and reject an unsupported layout at read time, naming the resource type, the
  detected layout, the expected layout and the remedy.
- **FR-037a**: File-based sinks MUST determine the layout of the target they
  write into and reject an unsupported layout before any schema merge is
  attempted, with the same message. Schema merging on append and upsert is
  enabled by FR-042, so without this a write into a target holding an earlier
  layout either fails with a storage-layer schema error or, where no type
  conflicts, succeeds and leaves one table carrying both layouts.
- **FR-038**: Detection MUST be structural and MUST NOT require file access
  beyond the schema metadata the read already obtains.
- **FR-039**: A sparse but conforming schema, and a schema carrying no marker
  fields, MUST be accepted.
- **FR-040**: Detection MUST be disableable per source without affecting other
  sources.
- **FR-041**: Reading a dataset whose files carry differing schemas MUST return
  the union of their elements, with no resource lost and cardinality preserved.
- **FR-042**: Appending a batch whose schema differs from the table's MUST
  succeed and widen the table schema.

### Compatibility

- **FR-043**: `encode`, `encodeBundle` and `decode` MUST be preserved in Java,
  Python and R.
- **FR-044**: The nesting, extension and open-type options MUST keep their
  current meaning for the dense schema and MUST NOT apply to the pruned one.
  *(Staged by decision 69. The options remain in the public API throughout, and
  from the flip until M6 they are accepted and bound nothing, because the mode
  they bound does not yet exist in this layout. They are kept rather than
  deprecated or made to fail: FR-043 preserves the encoding signatures and SC-008
  rules out an incompatible public API change.)*
- **FR-045**: XML and Bundle ingest MUST be preserved.
- **FR-046**: Data written by earlier releases MUST NOT be readable as though it
  were the new layout; see FR-037.
- **FR-047**: The published schema documentation MUST be replaced with the new
  layout contract, including the compatibility statement for existing files and
  the caveat for direct SQL consumers.

### Module boundary

- **FR-048**: FHIR definition and schema-derivation code MUST NOT depend on
  internal Spark Catalyst API, enforced by the build as a dependency
  prohibition.
- **FR-049**: The new encoding code MUST NOT reference internal Spark Catalyst
  API, enforced by the build. Where a dependency prohibition is impossible
  because column expressions require the wider Spark SQL dependency, enforcement
  MUST be at the level of imports.
- **FR-050**: The new encoding path MUST contain no expression encoder, no
  hand-authored serializer or deserializer expression tree, and no FHIR object
  in a per-row plan.
- **FR-051**: The existing encoding implementation MUST be left in place and
  MUST continue to resolve at its current Maven coordinates, so that it remains
  available to migration tooling. It MUST NOT be modified, split or retired by
  this work.
- **FR-052**: The query-time expression toolkit MUST be preserved and MUST NOT be
  relocated by this work. It MAY be extended: the prohibitions in FR-048 and
  FR-049 are scoped to the definition, schema-derivation and encoding code, and
  do not reach the evaluation path. Satisfying FR-054 and FR-056 by adding to
  the toolkit is therefore permitted.
- **FR-053**: On completion, the query engine MUST read only the new layout. It
  MAY retain a reader for the previous layout until that reader is removed in M7,
  which is what keeps the build green while the engine is converted and makes the
  switch reversible until then. **This wording is provisional**: T100f settles it
  before M7 opens, and T049a may bear on it, since a source boundary that routes
  earlier-layout data rather than refusing it would make the previous-layout
  reader a product feature rather than transitional scaffolding.

### Key Entities

- **Resource table**: the stored representation of one FHIR resource type. Its
  schema is derived from the definitions and optionally pruned to the data.
- **Element**: a node in a resource, with a FHIR type and a cardinality taken
  from the definitions, present or absent from a given schema.
- **Annotation**: a derived value stored beside an element to accelerate a
  computation the engine can also perform from the element itself. Optional by
  definition.
- **Primitive metadata group**: the id and extensions belonging to a primitive
  element, stored beside it.
- **Schema mode**: whether a schema is pruned to the data or comprises every
  element the definitions describe. *(The dense mode is delivered in M6, decision
  69; the pruned one is the only mode before then.)*
- **Strictness switch**: *withdrawn by decision 68.* Content outside the
  definition set is ignored and reported; there is no setting.
- **Layout**: the encoding convention a stored dataset follows, detectable from
  its schema.

## Success Criteria _(mandatory)_

### Measurable Outcomes

- **SC-001**: A round trip over a structural subset of the FHIR R4 specification
  examples produces semantically equal resources for 100% of conformant input,
  with numbers compared numerically, including decimals with trailing zeros,
  exponent notation, a leading sign, a very small magnitude and forty
  significant digits. Equality is judged subject to FR-016's exception list.
  Until M5 the measurement excludes primitive id and extension content, because
  the metadata group is not populated before then and the R4 examples carry such
  content; the exclusion is asserted in the harness rather than incidental
  (T076), and T078c removes it. *(Decision 68: the Synthea corpus is deferred
  with T077 and the specification-example corpus is reduced, so this criterion
  is measured over less than it was.)*
- **SC-002**: The FHIRPath test suite, both conformance baselines and the
  SQL-on-FHIR compliance suite pass over data in the new layout, in the pruned
  schema mode, with a curated subset also passing in the dense mode. *(The dense
  half is measured in M6, with the mode; decision 69.)*
- **SC-003**: The conformance exclusion baselines gain no new entries, and every
  entry that becomes obsolete is removed rather than left to self-report.
- **SC-004**: The full engine test suite passes over files written with every
  annotation disabled.
- **SC-005**: A dataset whose files carry differing schemas returns every
  resource with correct cardinality, on both raw files and a transactional
  table.
- **SC-006**: Reading data written by an earlier release fails at read time with
  a message naming the resource type, the detected layout and the remedy, for
  every file-based source; writing into such a dataset fails the same way, for
  every file-based sink.
- **SC-007**: Encode, decode and query execution are each measured separately
  against a baseline captured before any change, with planning time measured
  separately from execution time. No outcome is required; the measurement is.
- **SC-008**: No public API of the library changes incompatibly in Java, Python
  or R.
- **SC-009**: The build fails if internal Spark Catalyst API is introduced into
  the definition and schema module.
- **SC-010**: One column expression, built once without reference to any dataset,
  applied to a fitted and to a dense dataset holding the same resources, yields
  equal results for every expression in the engine's expression-level test set.
  *(Satisfied in two parts by decision 69: at the flip across two fitted schemas
  of differing width, and in its stated form in M6 with the dense mode.)*

## Assumptions

- The FHIR version in scope is R4, as today. The definition abstraction is not
  prepared for other versions: it reports R4 types (decision 67), and no other
  provider is delivered.
- Profiles are out of scope. The definition abstraction is prepared for a
  provider backed by structure definitions, but none is delivered.
- The server catches up separately, pinned to the last library release before
  the first convention change and reaching the post-change line before the
  pinned line stops receiving fixes. Server-side concerns — writes from objects,
  reads to objects, the schema-as-contract sites and migration of persisted
  warehouses — are out of scope here.
- Removing internal Catalyst API from the *execution* path is out of scope. The
  query-time toolkit stays exactly where it is, and is extended rather than
  replaced.
- Two alternative ways of reconciling the engine with a fitted schema are
  recorded as future options, not chosen: binding the evaluator to the input
  schema at evaluation time, and representing an unevaluated expression as an
  unbound object that is bound to a schema when the dataset is known. Both are
  compatible with what is built here, because the representation of absence and
  the mechanism for reconciliation are the same under any of the three.
- The existing encoding implementation is retained intact rather than split or
  retired. Separating the query toolkit from the HAPI bridge, and retiring the
  bridge, is deferred: what becomes of it depends on what the data-migration
  tooling turns out to need, since that tooling may have to use it. The new
  encoding is built alongside it.
- The engine reads only the new layout **on completion**, and may read both
  while it is being converted. The test estate therefore moves incrementally,
  along two independent dimensions in the test framework: how much of the layout
  a schema carries, and which conventions its fields follow. The earlier form of
  this assumption — that the engine reads only the new layout throughout, and so
  the test estate moves in one step — is what made a red window look
  unavoidable. It was applied to the transition when it constrains only the end
  state. See decision 40's addendum.
- The schema mode and the per-annotation toggles are
  configuration on a **new** surface beside the existing encoding configuration,
  not on it. The existing configuration class sits in the module FR-051 protects,
  and FR-052's carve-out reaches query-time expressions only, so adding to it is
  not available. The public context accepts and exposes both, which is two
  configuration objects for one conceptual thing — the cost of leaving the
  existing implementation genuinely untouched. *(Decision 69: that surface has no
  field before M5 and is deleted in M1, since the strictness switch is withdrawn,
  the toggles are M5's and the mode is M6's. Whichever milestone needs it first
  recreates it.)*
- Migration of data at rest is a version gate plus an opt-in rewrite tool, and
  is server-side work outside this specification. A migrated warehouse does not
  carry the losslessness guarantee; a re-imported one does.
- Lexical decimal preservation is not attempted on any path (decision 68).
  Numeric equivalence is the guarantee, and a decimal is stored as the text of
  a double. The earlier form of this assumption — that preservation was
  achievable on the file path and not on the path taking resources as a dataset
  of strings, and so was a documented per-path limitation with an upstream
  report — no longer applies, because both paths now route a number through a
  double and therefore agree.
- Empty pruning is deferred, and M1 neither removes nor detects an empty
  structure or a null-only array (decision 71). The assumption that the input
  carries nothing needing pruning holds for this milestone's corpora and not for
  conformant input. Conformant input yields `{}` where a structure's only
  content was primitive metadata, and a bare positional `null` where the `_x`
  array it aligns with is not stored. Both are kept on purpose until M5, because
  the first is an element that exists and pruning it would change query answers,
  and the second holds a position (decision 71's addendum). The first is kept
  whatever other conformant documents are read with it, because the primitive is
  stored as a null of its declared type (decision 72). That matches the released
  encoder for a singular primitive; for a repeating one the released encoder
  drops the element, because the parser it reads through discards an `_x` array
  with no `x` array beside it, and the layout keeps it, as FHIR does.
  Non-conformant content yields `{}` as well, including a value in another
  document that re-types a shared column. The follow-up is the custom JSON serde
  (T134h).
- The strictness switch, value validation and dense-mode bounds reporting are
  deferred rather than abandoned (decision 68). M1 reports what it drops and
  fails on nothing but malformed JSON. The consequence is that M1's exit
  criterion is "lossless for the resources we test" rather than "lossless, or
  loud about what was lost", and that this reaches the M4 flag day. T134f and
  T134g carry the follow-ups.
