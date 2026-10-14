# Feature Specification: Parquet on FHIR

**Feature**: `001-parquet-on-fhir`
**Created**: 2026-09-14
**Revised**: 2026-10-14, to state the end state only
**Status**: In progress

Replaces Pathling's Catalyst-encoder-based FHIR encoding with a schema-driven
encoding conforming to the [Parquet on FHIR](https://github.com/aehrc/parquet-on-fhir)
specification, and migrates the FHIRPath execution engine to read it.

Tracks GitHub issue [#2367](https://github.com/aehrc/pathling/issues/2367).

This document states what holds when the programme is complete. It carries no
history: how each requirement was reached is in
[evidence/decisions.md](evidence/decisions.md) and [research.md](research.md),
durable design decisions are in [`docs/adr/`](../../docs/adr/), vocabulary is in
[`CONTEXT.md`](../../CONTEXT.md), and the work that remains is tracked as tickets
in [`issues/`](issues/). [tasks.md](tasks.md) is the record of
milestones M1 to M4. Everything ships together, in one release, when the last
ticket closes.

## Why

1. **Performance** of encoding, decoding and FHIRPath execution through reduced
   schema width. A hypothesis, measured but not gated on (SC-007).
2. **Lossless representation of FHIR**, permitting a `JSON → storage → JSON`
   round trip.
3. **Attribute-level extensions and ids**, and recursive types whose depth the
   data decides.
4. **Automatic merging** of Parquet and Delta files written with differing
   fitted schemas.
5. **No internal Spark Catalyst API in the encoding path**, which breaks on Spark
   upgrades. This is why the encoder is replaced rather than modified
   ([ADR-0004](../../docs/adr/0004-no-per-row-hapi-no-catalyst-in-encoding.md)).

## User Scenarios & Testing _(mandatory)_

### User Story 1 - Data is stored in the PoF layout (Priority: P1)

A user loads FHIR JSON or XML, as resources or bundles, and the data is written in
the PoF layout: decimals as text with a numeric annotation, primitive ids and
extensions in metadata groups, extensions inline, dates with range annotations,
and quantities with both canonical annotations.

**Independent Test**: Load a corpus; inspect the stored schema and values against
the specification, without FHIRPath evaluation.

**Acceptance Scenarios**:

1. **Given** a resource carrying a decimal, **When** it is stored and read back,
   **Then** the stored value is text numerically equal to the source, and a
   numeric annotation accompanies it.
2. **Given** a resource carrying an extension on a primitive element, **When** it
   is stored, **Then** the extension appears in the primitive metadata group
   beside that element.
3. **Given** a resource carrying an extension on a complex element, **When** it is
   stored, **Then** the extension appears inline on that element, and no
   root-level extension map or field-id column is emitted.
4. **Given** a quantity, **When** it is stored, **Then** the specification's
   canonical annotation accompanies it, followed by the exact canonical
   annotation.
5. **Given** content the definition set does not describe, **When** it is
   loaded, **Then** the load succeeds, the content is not stored, and a finding
   names it.
6. **Given** the same resources as JSON, as XML and inside bundles of either
   format, **When** each is loaded, **Then** the stored data and the findings are
   the same.

---

### User Story 2 - FHIR round-trips without loss (Priority: P1)

A user loads conformant FHIR and exports it again, and receives semantically
equal resources: object key order ignored, array order significant, numbers
compared numerically, subject to FR-016's exceptions.

**Independent Test**: Round-trip a corpus of FHIR R4 specification examples plus
a Synthea dataset, asserting semantic equality resource by resource.

**Acceptance Scenarios**:

1. **Given** a conformant resource, **When** it is loaded and exported, in JSON
   or XML, **Then** the exported resource is semantically equal to the source.
2. **Given** a resource with an element absent, **When** it is exported,
   **Then** that element is absent from the output, and no empty object or
   null-only array appears where the source had nothing (FR-019).
3. **Given** a fitted schema, **When** conformant input is round-tripped,
   **Then** the guarantee holds without reference to the dense bounds.

---

### User Story 3 - The FHIRPath engine evaluates over the PoF layout (Priority: P1)

A user runs FHIRPath expressions, ViewDefinitions and searches over data in the
PoF layout and gets correct answers.

**Independent Test**: Run the FHIRPath suite, both conformance baselines and the
SQL-on-FHIR compliance suite over data in the PoF layout.

**Acceptance Scenarios**:

1. **Given** a decimal stored as text, **When** an expression compares or
   computes on it, **Then** it is compared and computed numerically.
2. **Given** a file carrying no annotations at all, **When** any expression is
   evaluated, **Then** the result is the same as over an annotated file.
3. **Given** a Coding column narrower than the canonical structure, **When** a
   terminology operation is applied, **Then** it reads the fields by name and
   the absent ones are null.
4. **Given** a reference, **When** its `getReferenceKey()` is joined to the
   `getResourceKey()` of the resources it points to, **Then** the referenced
   resources are joined without a stored versioned-key column, and `resolve()`
   reports the target's type for `is` and `ofType()`.
5. **Given** a quantity with no canonical annotation, **When** it is compared
   across units, **Then** the canonical value is computed and the comparison
   succeeds.

---

### User Story 4 - Queries work over a fitted schema (Priority: P1)

A user queries a dataset whose stored schema carries only the elements the data
populates, and expressions naming elements absent from that schema evaluate to
empty rather than failing.

**Independent Test**: Run the suite over fitted schemas; assert that an
expression over an absent element yields empty.

**Acceptance Scenarios**:

1. **Given** an element defined by FHIR but absent from the stored schema,
   **When** an expression traverses to it, **Then** the result is empty.
2. **Given** an element FHIR does not define, **When** an expression traverses
   to it, **Then** the result is empty (FR-025).
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
   **When** it is evaluated, **Then** it succeeds.
7. **Given** one column expression, **When** it is applied to a fitted dataset
   and to a dense dataset holding the same resources, **Then** the results are
   equal. *(Subject to OQ-2.)*
8. **Given** two collections of the same FHIR type reached by different paths,
   whose fitted schemas differ, **When** they are combined, **Then** the result
   holds every element of both and can be traversed.

---

### User Story 5 - Files written at different times read as one dataset (Priority: P2)

A user loads a second batch containing elements the first did not carry, and
queries the dataset as a whole without losing data from either.

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

A user navigates to the id or extensions of a primitive element
([#437](https://github.com/aehrc/pathling/issues/437)).

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

### User Story 7 - Legacy-layout data is refused with an actionable message (Priority: P2)

A user points the library at data in the legacy layout and gets an actionable
error rather than a confusing failure or wrong answers
([ADR-0001](../../docs/adr/0001-engine-refuses-the-legacy-layout.md)).

**Independent Test**: Read and write legacy-layout data through each file-based
source and sink, and assert the error.

**Acceptance Scenarios**:

1. **Given** data in the legacy layout, **When** it is read, **Then** an error at
   read time names the resource type, the detected layout, the expected layout
   and the remedy: the legacy export, or re-import from source.
2. **Given** a table in the legacy layout, **When** new data is appended or
   merged into it, **Then** the write is refused before anything is written.
3. **Given** data in the PoF layout that is merely narrow, or carries no marker
   field, **When** it is read, **Then** the read succeeds.
4. **Given** detection disabled for one source, **When** data is read through
   it, **Then** the read proceeds, and other sources are unaffected.
5. **Given** a legacy-layout column that reaches the engine by a route detection
   does not cover, **When** an expression traverses it, **Then** evaluation
   fails with an error naming the column, rather than answering from it.

---

### User Story 8 - Encoding no longer depends on internal Spark API (Priority: P3)

A maintainer upgrades Spark without the encoding path breaking on internal
Catalyst API.

**Independent Test**: The build enforces the boundary.

**Acceptance Scenarios**:

1. **Given** a dependency on internal Catalyst API added to the definition and
   schema module, **When** the build runs, **Then** it fails.
2. **Given** an import of internal Catalyst API added to the encoding code,
   **When** the build runs, **Then** it fails.
3. **Given** the encoding path, **When** it is inspected, **Then** it contains no
   expression encoder, no hand-authored serializer or deserializer expression
   tree, and no FHIR object, HAPI or otherwise, in a per-row plan.

---

### User Story 9 - A legacy warehouse can be moved to the PoF layout (Priority: P2)

A user holding a warehouse in the legacy layout, with or without the source it
was loaded from, moves it to the PoF layout.

**Independent Test**: Export a legacy-layout Parquet and Delta table written by
the legacy encoder, re-import the result, and compare it with the source data.

**Acceptance Scenarios**:

1. **Given** a legacy-layout table, **When** the legacy export runs over it,
   **Then** it produces FHIR NDJSON, one resource per line, for every row.
2. **Given** that NDJSON, **When** it is read with the NDJSON source and written
   with any sink, **Then** the result is in the PoF layout.
3. **Given** a decimal the legacy layout stored at its fixed scale, **When** it is
   exported, **Then** it carries the stored value, and the documentation states
   that precision beyond that scale is not recoverable.

---

### Edge Cases

- A decimal exceeding a double's precision: computed at the documented cap
  (FR-035). How it is stored is OQ-1.
- A FHIR element whose JSON type contradicts the definitions: not stored, and
  reported. Whether the loss is that value alone or every value of the element
  read with it is OQ-1.
- A resource carrying `contained` resources: dropped and reported, never
  represented.
- A structure whose every field is null, or an array whose every element is
  null, in a given row: not written as `{}` or `[null]` (FR-019).
- An array of arrays where a repeating element is declared, such as
  `"given":[["a","b"]]`: a shape mismatch, reported, and stored as absent.
- A source key named like one of the layout's annotations, such as
  `__birthDate_start`: an annotation is derived, never read, so the key is
  reported as undescribed content.
- A Bundle: accepted as a transport carrier and exploded to one table per
  resource type, never stored as a resource type. References between its
  entries are resolved to relative references.
- A custom resource type, such as `ViewDefinition`: read and written in every
  format through the definition set.
- An expression whose result is a complex value of a FHIR type reaching a caller
  as a column: its shape is the fitted shape of the data it came from (FR-059).
- Two collections of the same FHIR type reached by different paths, whose fitted
  schemas differ: combined by projecting both into the merged type, never by a
  cast ([ADR-0002](../../docs/adr/0002-reconcile-shapes-by-name.md)).
- Two structures carrying the same fields in a different order: reconciled by
  name, because struct equality compares positionally and ignores names.

## Requirements _(mandatory)_

Identifiers are stable. FR-015, FR-018 and FR-020 are retired and not reused.

### Storage layout

- **FR-001**: The system MUST store FHIR data in a layout conforming to the
  Parquet on FHIR specification, with the deviations in FR-002 to FR-005.
- **FR-002**: Decimals MUST be stored as text, accompanied by a numeric
  annotation. The stored text MUST be numerically equal to the source; whether
  it preserves the source's lexical form is OQ-1.
- **FR-003**: Primitive element ids and extensions MUST be stored in the
  primitive metadata group beside the element; extensions on complex elements
  MUST be stored inline. No root-level extension map and no per-composite field
  id is emitted.
- **FR-004**: Dates MUST carry the specification's range annotations.
- **FR-005**: Quantities MUST carry two canonical annotations: the
  specification's own, at its fixed-point type, and immediately after it an
  exact one carrying the canonicalised unit
  ([ADR-0003](../../docs/adr/0003-annotations-are-optional.md)).
- **FR-006**: `contained` resources MUST NOT be represented. Their presence is
  reported as a finding.
- **FR-007**: `Bundle` MUST NOT be stored as a resource type.

### Schema derivation

- **FR-008**: The stored schema MUST be derived from the definition set, which
  alone determines element types and cardinality
  ([ADR-0005](../../docs/adr/0005-types-from-definitions-only.md)).
- **FR-009**: The system MUST support a fitted schema and a dense schema. The
  fitted schema is the default.
- **FR-010**: The dense schema MUST be produced by the same traversal as the
  fitted one, differing only in the strategy that decides which children it
  descends into and where it stops, so the two cannot diverge in type,
  cardinality or field order.
- **FR-011**: A complex element MUST appear in a fitted schema only where some
  descendant leaf is populated. A primitive the source carried only as its id
  and extensions counts as populated.
- **FR-012**: The system MUST NOT determine element types or cardinality from
  the shape of the data.
- **FR-013**: The definition abstraction MUST expose child enumeration in
  declaration order, with a choice element appearing once as a choice.
- **FR-014**: The definition abstraction MUST expose cardinality sufficient to
  determine whether an element repeats. It reports types as values of the R4
  enumeration.

### Ingest and egress

- **FR-045**: JSON and XML MUST be accepted and produced, as single resources
  and, on ingest, as bundles.
- **FR-060**: Every ingest format MUST store the same data and report the same
  findings for the same resources. XML and bundles MUST NOT behave differently
  from JSON in what they keep, coerce, report or fail on.
- **FR-061**: No ingest or egress route MAY parse into or build a FHIR object per
  row, HAPI included. The mechanism is chosen by a spike against SC-011
  ([ADR-0004](../../docs/adr/0004-no-per-row-hapi-no-catalyst-in-encoding.md)).
- **FR-062**: References between the entries of a bundle MUST be resolved to the
  relative reference of the target entry, keeping its version where the
  reference carries one.

### Losslessness

- **FR-016**: For input conforming to the definition set, a
  `JSON → storage → JSON` round trip MUST produce a semantically equal resource,
  with object key order ignored, array order significant and numbers compared
  numerically, subject to these exceptions:
  1. `contained` resources (FR-006) and `Bundle` (FR-007);
  2. `base64Binary` whitespace, which decoding and re-encoding canonicalises;
  3. content the definition set does not describe, or whose JSON type
     contradicts it, which is not stored and is reported;
  4. on a dense schema, content beyond the configured bounds (FR-017).

  OQ-1 lists further exceptions that hold until the ingest spike decides
  otherwise. The round trip through XML is held to the same guarantee.
  Evidence: [null-and-empty-handling.md](evidence/null-and-empty-handling.md).
- **FR-017**: On a dense schema the guarantee in FR-016 MUST hold within the
  configured nesting, extension and open-type bounds, and content those bounds
  drop MUST be reported.
- **FR-019**: Export MUST omit an absent element, a structure whose every field
  is null, and an array holding only nulls, except a repeating primitive's
  positional nulls, which align with its metadata group. *(Subject to OQ-1.)*
- **FR-063**: Non-conformant content MUST be reported, never silently dropped or
  coerced. Malformed JSON or XML fails the read.

### Annotations

- **FR-021**: The decimal, date and quantity annotations MUST be emitted by
  default, each kind individually disableable. A kind carried in more than one
  field is governed by one switch.
- **FR-022**: The engine MUST compute correctly when any or all annotations are
  absent, using an annotation only as a fast path.
- **FR-023**: Whether an annotation is used MUST be decided from the schema,
  never per row.

### Query engine

- **FR-024**: Traversal to an element the definitions describe but the schema
  does not carry MUST yield an empty collection.
- **FR-025**: Traversal to an element the definitions do not describe MUST yield
  an empty collection.
- **FR-026**: Selecting a choice variant absent from the schema MUST yield an
  empty collection.
- **FR-027**: Combining an absent element with a populated one MUST succeed
  rather than fail on a type mismatch.
- **FR-028**: A view column declaring a FHIR type MUST have that type applied to
  the output column.
- **FR-029**: Deriving the output type of a column carrying no type information
  MUST fail with a message naming the column, its path and the remedy. An
  absent element is not such a column (FR-055).
- **FR-030**: A column produced for an absent primitive element MUST carry the
  type the definitions give that element.
- **FR-031**: Coding-valued columns MUST be decoded by field name, resolved once
  per schema, with absent fields decoding as null and a structure carrying no
  recognisable field rejected with an error naming the expected and actual
  fields.
- **FR-032**: Terminology operations MUST operate correctly on a Coding column
  narrower than the canonical structure.
- **FR-033**: Reference resolution MUST NOT depend on a stored versioned-key
  column. A precomputed key, if a join benchmark shows one is needed, MUST be an
  annotation the engine works without. Evidence:
  [t136-benchmark.md](evidence/t136-benchmark.md).
- **FR-034**: The engine MUST be able to navigate to a primitive element's id
  and extensions.
- **FR-035**: Query-time decimal precision MUST remain as it was before this
  programme, and the cap MUST be documented.
- **FR-036**: Unnesting MUST NOT reduce a read to a single leaf of a repeated
  element, because that drops rows from files lacking that leaf.
- **FR-054**: A column expression MUST be valid over every schema conforming to
  the definitions, deciding presence from the resolved input schema rather than
  when the expression is built. *(Subject to OQ-2.)* Evidence:
  [t009a-analyzer-gate.md](evidence/t009a-analyzer-gate.md),
  [join-condition-limit.md](evidence/join-condition-limit.md).
- **FR-055**: An absent element MUST be a null typed by the definitions where
  unambiguous and by the bottom type where a concrete shape would over-constrain
  combination: a singular primitive takes its type, a repeating primitive an
  array of it, a singular complex element the bottom type, and a repeating
  complex element an array of the bottom type. Evidence:
  [spark-type-findings.md](evidence/spark-type-findings.md).
- **FR-056**: Two collections of the same FHIR type that must share a SQL type
  MUST be reconciled by projecting each by name into the merged type, never by
  a cast ([ADR-0002](../../docs/adr/0002-reconcile-shapes-by-name.md)).
- **FR-057**: Every struct type Pathling produces MUST be in canonical field
  order. A schema obtained from storage may carry any order; FR-056 reconciles
  it by name. Operands that already share a type are left unprojected.
- **FR-058**: The merged type MUST be the recursive field-wise union of the
  inputs, and one implementation MUST serve both FR-056 and FR-041.
- **FR-059**: A complex value of a FHIR type MUST leave the engine in the fitted
  shape of the data it came from, and this MUST be documented. A complex value
  of a System type MUST have one fixed, complete shape, whatever produced it.
  The engine-built `System.Quantity` MUST NOT carry the legacy layout's shape;
  the same for `System.Coding` is
  [#2804](https://github.com/aehrc/pathling/issues/2804).

### Reading and writing

- **FR-037**: File-based sources MUST determine the layout of the data they read
  and refuse the legacy layout at read time, naming the resource type, the
  detected layout, the expected layout and the remedy.
- **FR-037a**: File-based sinks MUST determine the layout of the target they
  append or merge into and refuse the legacy layout before any schema merge, with
  the same message. An overwrite is not refused.
- **FR-038**: Detection MUST be structural and MUST NOT require file access
  beyond the schema metadata the read already obtains.
- **FR-039**: A fitted but conforming schema, and a schema carrying no marker
  field, MUST be accepted.
- **FR-040**: Detection MUST be disableable per source, for data the detector
  misclassifies, without affecting other sources.
- **FR-041**: Reading a dataset whose files carry differing schemas MUST return
  the union of their elements, with no resource lost and cardinality preserved.
  Evidence: [merge-cost.md](evidence/merge-cost.md).
- **FR-042**: Appending a batch whose schema differs from the table's MUST
  succeed and widen the table schema, through every sink and save mode that
  writes into an existing table. A non-Delta catalog table MUST instead refuse a
  batch whose schema differs, before anything is written, naming the remedy.
  Evidence: [catalog-append-schema.md](evidence/catalog-append-schema.md).

### Legacy layout

- **FR-046**: The engine MUST read only the PoF layout. Legacy-layout data MUST
  NOT be readable as though it were the PoF layout.
- **FR-053**: Where a legacy-layout column reaches the engine by a route
  detection does not cover — a source with detection disabled, a dataset the
  caller supplies, a dataset built with the legacy encoders — the engine MUST
  fail, once per schema, with an error naming the column, rather than answer
  from it.
- **FR-064**: A legacy export MUST convert a legacy-layout Parquet or Delta table
  to FHIR NDJSON, as a reusable component and as a command-line tool, outside
  `library-runtime` and the language bindings. Its losses MUST be documented.

### Compatibility

- **FR-043**: `encode`, `encodeBundle` and `decode` MUST be preserved in Java,
  Python and R, in the forms taking a resource name. Encoding a streaming
  dataset MUST work in the dense schema mode.
- **FR-044**: The nesting, extension and open-type options MUST bound the dense
  schema and MUST NOT apply to the fitted one. Evidence:
  [encoder-scope.md](evidence/encoder-scope.md).
- **FR-047**: The published documentation MUST describe the PoF layout, its
  schema modes and annotations, the refusal of the legacy layout, the legacy
  export, and the caveat for direct SQL consumers.

### Module boundary

- **FR-048**: Definition and schema-derivation code MUST NOT depend on internal
  Spark Catalyst API, enforced by a dependency prohibition in the build.
- **FR-049**: Encoding code MUST NOT reference internal Spark Catalyst API,
  enforced by the build at the level of imports.
- **FR-050**: The encoding path MUST contain no expression encoder, no
  hand-authored serializer or deserializer expression tree, and no FHIR object
  in a per-row plan.
- **FR-051**: The legacy `encoders` module MUST remain at its Maven coordinates,
  unmodified in its encoding behaviour, because the legacy export depends on it.
- **FR-052**: The query-time expression toolkit MAY use and extend internal
  Catalyst API; FR-048 and FR-049 do not reach it.

### Key Entities

See [`CONTEXT.md`](../../CONTEXT.md): resource table, layout (PoF and legacy),
schema mode (fitted and dense), definition set, annotation, primitive metadata
group, canonical field order, finding, absent element, FHIR type, System type,
reconciliation, legacy export.

## Success Criteria _(mandatory)_

- **SC-001**: A round trip over the FHIR R4 specification examples and a Synthea
  dataset produces semantically equal resources for 100% of conformant input,
  through JSON and through XML, with numbers compared numerically, judged
  subject to FR-016's exceptions.
- **SC-002**: The FHIRPath test suite, both conformance baselines and the
  SQL-on-FHIR compliance suite pass over the PoF layout with fitted schemas, and
  a curated subset passes with dense schemas.
- **SC-003**: The conformance exclusion baselines gain no new entries, and every
  obsolete entry is removed.
- **SC-004**: The full engine test suite passes over files written with every
  annotation disabled.
- **SC-005**: A dataset whose files carry differing schemas returns every
  resource with correct cardinality, on raw files and on a Delta table.
- **SC-006**: Reading legacy-layout data fails at read time with a message naming
  the resource type, the detected layout and the remedy, for every file-based
  source; appending or merging into it fails the same way, for every file-based
  sink.
- **SC-007**: Encode, decode, planning and execution are each measured against
  the baseline captured before any change. No outcome is required. Evidence:
  [baseline.md](evidence/baseline.md),
  [t136-benchmark.md](evidence/t136-benchmark.md).
- **SC-008**: No public API of the library changes incompatibly in Java, Python
  or R, except the removal of the Java `encode` overloads taking a HAPI class and
  returning a dataset of it, which only the legacy encoder could produce.
- **SC-009**: The build fails if internal Catalyst API is introduced into the
  definition and schema module or the encoding code.
- **SC-010**: One column expression, built without reference to any dataset,
  applied to a fitted and to a dense dataset holding the same resources, yields
  equal results for every expression in the engine's expression-level test set.
  *(Subject to OQ-2.)*
- **SC-011**: Encode and decode are no slower than at the PoF switch, measured on
  the same benchmark, within the measured drift of about 10%
  ([t136-benchmark.md](evidence/t136-benchmark.md)).
- **SC-012**: A legacy-layout table written by the legacy encoder, exported and
  re-imported, holds every resource of the source, equal to it except for the
  losses FR-064 documents.

## Assumptions

- The FHIR version is R4. The definition abstraction reports R4 types, and HAPI
  remains the source of the structural definitions
  ([#2818](https://github.com/aehrc/pathling/issues/2818)).
- Profiles are out of scope.
- The server is pinned to the last release before the PoF layout. It moves to
  the PoF layout separately, using the legacy export to migrate its warehouse,
  and maps the library's refusal to its own errors. That work is outside this
  specification.
- Removing internal Catalyst API from the query-time toolkit is out of scope.
- Distinguishing `System.Coding` from `FHIR.Coding` in the type system is
  [#2804](https://github.com/aehrc/pathling/issues/2804), outside this
  specification, except for the shape of the engine-built `System.Quantity`
  (FR-059).

## Open questions

- **OQ-1: Which representation losses does the ingest and egress spike
  remove?** The spike chooses between transforming the schema Spark's reader
  infers and a parser of our own driven by the definitions, for JSON and XML,
  in both directions, under FR-049, FR-050 and SC-011. Its outcome decides the
  following, each stated here as the target with today's behaviour as the
  fallback if the spike does not remove it:
  - the decimal's lexical form (FR-002). Today a number passes through a double,
    so trailing zeros, exponent notation and precision beyond a double are not
    preserved;
  - the granularity of a contradicting value (FR-016 item 3). Today one value
    whose JSON type contradicts the definitions, or that re-types its column
    beside conformant values, costs that element for every resource read with
    it, and a number inferred as text beside strings is kept as text without a
    finding;
  - empty structures and null-only arrays on export (FR-019). Today a structure
    emptied by non-conformant content is written as `{}`;
  - per-value rather than per-column findings, and reporting of values a
    converter cannot store, such as an integer beyond the range of an int.
- **OQ-2: Are column expressions bound to a schema?** Today a column expression
  is built without a dataset and resolves presence against whatever schema it
  meets (FR-054), which lets callers combine FHIRPath and search columns with
  other SQL filters, but rests on a fragile root reference that needed special
  handling in join conditions. The alternatives are binding at evaluation time,
  and an unbound expression bound to a schema when the dataset is known. The
  answer decides FR-054, SC-010 and the fitted-versus-dense parity tests. Any
  change is weighed against SC-008.
