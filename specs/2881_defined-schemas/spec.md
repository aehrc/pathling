# Feature Specification: Load FHIR resources with a schema defined up front

**Feature**: `2881_defined-schemas`
**Created**: 2026-11-25
**Status**: Draft
**Issue**: [#2881](https://github.com/aehrc/pathling/issues/2881) (Load FHIR resources with a
schema defined up front), with its
[notes](https://github.com/aehrc/pathling/issues/2881#issuecomment-7150594803).
**Follows**: `specs/001-parquet-on-fhir/` (#2367). It replaces that specification's dense mode
(tickets 18 and 19). The inferred schema here is 001's fitted schema.

## Context

Since Parquet on FHIR (11.0, not yet released), the encoder infers each batch's schema from the
data in that batch. Two batches of the same resource type can therefore have different schemas,
and a table's schema becomes the merge of every batch loaded so far. That works for loading and
querying through Pathling, which is why it stays the default. It fails in three cases:

- **Streaming ingestion.** A stream has no complete data to infer from, so a streaming encode is
  refused. 10.x supported it, because its schema was known up front.
- **Downstream streaming consumers.** A job that reads a Pathling table as a stream stops whenever
  the table's schema changes, and every append that brings a new element changes it.
- **Consumers outside Pathling.** Direct SQL, dbt, BI tools and Delta Sharing recipients fail on an
  element no batch has carried yet.

This feature lets the user define the schema up front. Every way data enters Pathling accepts an
optional schema per resource type, and every batch is projected to it. Sinks take no schema: data
reaches a sink already shaped by the source it was loaded with.

## Terms

The terms are those of `CONTEXT.md`: **uber-schema**, **PoF schema**, **inferred schema**,
**defined schema**, **schema set**, **base schema**, **projection** and **absent element**. The
decisions behind them are ADR-0007 (the engine evaluates every PoF schema as the uber-schema) and
ADR-0008 (a defined schema takes the place of the inferred schema). In addition:

- **Schema** (on the API): Spark's JSON form of a struct type, as Spark itself writes and reads it.
  In Python a JSON-compatible dictionary, in R a list, in Java a JSON string or a map.

## User Scenarios & Testing _(mandatory)_

### User Story 1 - Load and write with a defined schema (Priority: P1)

A data engineer appends a daily NDJSON drop to a Delta lake. They take the lake's schema set from
the lake itself, pass it to the read and to the write, and the lake's schema never changes,
whatever elements the day's data does or does not carry.

**Why this priority**: It is the core capability every other story rests on, and on its own it
fixes the downstream streaming consumers and the external consumers.

**Independent Test**: Load a schema set of PoF schemas; read three NDJSON batches with differing content
through it and append each to a Delta table; the table's schema equals the given schema after every
append.

**Acceptance Scenarios**:

1. **Given** a Delta lake, **When** the user asks the lake's source for its schemas, saves them as
   JSON, loads them back and passes them to an NDJSON read, **Then** every resource type the read
   returns has exactly the lake's base schema plus the annotations the configuration enables.
2. **Given** a schema set of PoF schemas and a batch lacking elements the schema has, **When** the batch is
   loaded with the set, **Then** the missing elements are present and null.
3. **Given** a schema set of PoF schemas and a batch carrying elements the schema lacks, **When** the batch
   is loaded with the set, **Then** those elements are absent from the result, with no error.
4. **Given** a Delta table and batches with differing content, **When** each is loaded with the
   table's own schema set and appended, **Then** the table's schema is unchanged
   after every append.
5. **Given** data loaded with a schema set, **When** it is written to any sink, **Then** each named
   resource type is written with that set's schema.
6. **Given** the same resources as JSON, XML and stored Parquet, **When** each is loaded with the
   same schema set, **Then** the three results are equal.
7. **Given** a schema set naming only some of the source's resource types, **When** the source is
   read, **Then** the named types are projected and the others are inferred as today.

---

### User Story 2 - Encode a stream with a defined schema (Priority: P1)

A developer reads FHIR bundles from Kafka, encodes them with a defined schema, and writes each
micro-batch of encoded resources to Delta or Parquet with a Pathling sink inside Spark's
`foreachBatch`, as the 10.x streaming documentation did. Every check a Pathling sink makes applies
to each micro-batch. Pathling reads the result back like any other table.

**Why this priority**: Streaming encode worked in 10.x and is refused in 11.0. It is the use case
that inference cannot serve at all.

**Independent Test**: Encode a streaming source of NDJSON lines with a schema, write each
micro-batch to Delta and to Parquet with a Pathling sink in `foreachBatch`, and read both back
through Pathling.

**Acceptance Scenarios**:

1. **Given** a streaming dataset of FHIR JSON or XML and a schema, **When** the user encodes it,
   **Then** the result is a streaming dataset with the schema, in every language binding.
2. **Given** a streaming dataset of bundles and a schema, **When** the user encodes it as bundles,
   **Then** the same holds.
3. **Given** a streaming dataset and no schema, **When** the user encodes it, **Then** the encode is
   refused with a message telling the user to supply a schema.
4. Removed with FR-014. A raw `writeStream` is neither promised nor tested.
5. **Given** the streaming example in the documentation, **When** it is run, **Then** it works.

---

### User Story 3 - Removed

Removed. A stream written through a Pathling sink inside `foreachBatch` gets every check the sink
makes, so a separate destination check is not needed (see Out of Scope).

---

### User Story 4 - Take a schema set from existing data (Priority: P2)

A user asks any data source for its schema set, saves it, edits it, shares it, and passes it to a
later load.

**Why this priority**: Until a schema generator exists, existing data is the only source of a
schema. It is also how a table's schema is carried to the next load.

**Independent Test**: For every source, get the schema set, round-trip it through a JSON file, and
pass it to every source.

**Acceptance Scenarios**:

1. **Given** any data source, **When** the user asks for its schemas, **Then** they get a schema
   set with one entry for each resource type it holds.
2. **Given** a Parquet source whose files have differing schemas, **When** the user asks for its
   schemas, **Then** each schema is the union the reader merges those files into.
3. **Given** a schema set from any source, saved as JSON and loaded back, **When** it is passed to
   any source, **Then** it is accepted. A single schema from it is accepted by `encode` and
   `encodeBundle`.

---

### Edge Cases

- **A defined schema that is not a PoF schema** for its resource type is treated as inference
  treats the shape it observes: the elements it names are kept, with the type, cardinality and
  order the definitions give them, and fields the definitions do not describe are dropped. The
  output is always a PoF schema, and differs from the given schema only where the given one is not
  a PoF schema.
- **Annotation fields in the given schema** are ignored. The configuration decides which
  annotations the output carries, as in an inferred load.
- **Primitive metadata groups** are kept or dropped as the given schema says, whatever the
  primitive metadata option is set to. A schema without `_birthDate` therefore drops a
  data-absent-reason extension on `birthDate` even when the option is on.
- **A resource type the schema set names but the source lacks** behaves as today: it is not listed,
  reading it is an error, and sinks write nothing for it. Reading it as an empty table is a
  follow-up (#2997).
- **A resource type the source holds but the set does not name** is inferred, as today.
- **A schema that cannot be read** (malformed JSON, or not a struct) is refused with an error naming
  the resource type.
- **A key in the set that is not a resource type code** is refused with an error naming it.
- **An empty schema set** behaves as no schema set.
- **A Delta merge (upsert) with a schema narrower than the table** replaces each matched resource
  in full, so the fields the schema lacks become null on every matched row. This is accepted and
  documented.
- **An append through a Pathling sink of data whose schema differs from the table's** widens the
  table, as an inferred append does today.
- **A value whose stored type differs from the definitions' type** is not decided here. Read
  non-conformant data as empty, with no modes (#2884) settles it: the read never fails, and a value
  that cannot be matched reads as null.

## Requirements _(mandatory)_

### Functional Requirements

Inputs:

- **FR-001**: The NDJSON, bundle, Parquet, Delta, catalog table, bulk and dataset sources MUST each
  accept an optional schema set, in Java, Python and R.
- **FR-002**: `encode` and `encodeBundle` MUST each accept an optional single schema, for JSON and
  XML input, in Java, Python and R.
- **FR-003**: The Parquet source's existing schema option MUST change from Spark struct types to the
  schema set of FR-001. It is unreleased, so this needs no deprecation.
- **FR-004**: Pathling MUST accept a schema only in the JSON form defined under Terms, not as a
  Spark struct type.
- **FR-005**: A schema set MUST be accepted in the same form by every source, and a
  schema from a set MUST be accepted by `encode` and `encodeBundle`.

Projection:

- **FR-006**: Projecting a batch to a schema MUST give the result that inference gives when it
  observes exactly the given schema's base schema: the elements and primitive metadata groups the
  schema names are kept, with type, cardinality and order from the definitions; elements the batch
  lacks are present and null; content the schema has no field for is dropped, with no error.
- **FR-006a**: When a source reads stored data whose schema is known without reading rows
  (Parquet, Delta, catalog tables and datasets), it MUST log at debug level the paths that the
  defined schema drops. They MUST be found by comparing the stored schema with the defined schema
  alone, with no extra Spark job and no per-row work. NDJSON, bundle and XML reads with a defined
  schema run no inference, so finding dropped content would need per-row work: they MUST NOT detect
  it, and log nothing about it.
- **FR-007**: Annotation fields in a given schema MUST be ignored. The output's annotations MUST be
  those the configuration enables, computed as in an inferred load.
- **FR-008**: When the given schema is a PoF schema, the output's base schema MUST equal it exactly. When
  it is not, the output MUST still be a PoF schema.
- **FR-009**: Projection MUST be the same on every input path. JSON, XML and stored Parquet or Delta
  holding the same resources, loaded with the same schema, MUST give equal data.
- **FR-010**: A source MUST project each resource type its schema set names and infer every other
  type as today.
- **FR-011**: Removed. Sinks take no schema set: a source given the set already produces data with
  its schema, and a sink writes what it is given, as today.

Streaming:

- **FR-012**: `encode` and `encodeBundle` MUST accept a streaming dataset when given a schema, and
  return a streaming dataset with that schema.
- **FR-013**: Without a schema, a streaming encode MUST stay refused, with the exception type it
  has today and a message that tells the user to supply a schema or to encode each batch within
  `foreachBatch`.
- **FR-014**: Removed. A stream is written through a Pathling sink inside `foreachBatch` (FR-024),
  so reading back the output of a raw `writeStream` is neither promised nor tested.

Destination check:

- **FR-015**: Removed. A stream is written through a Pathling sink inside `foreachBatch`, which
  makes every check the sink makes, so no separate destination check is provided (see Out of
  Scope).
- **FR-016**: Removed, with FR-015.

Schemas from data:

- **FR-017**: Every data source MUST return its schema set on request, with one entry for each
  resource type it holds, in Java, Python and R.
- **FR-018**: For a Parquet source whose files differ, each schema returned MUST be the union the
  reader merges them into.
- **FR-019**: A schema set returned by any source, saved as JSON and loaded back, MUST be accepted
  by every source unchanged.

Compatibility:

- **FR-020**: Every existing call without a schema MUST behave exactly as it does today.
- **FR-021**: Query results (FHIRPath, SQL on FHIR, search) MUST depend on the data alone, not on
  which subset of the uber-schema the schema carries: an absent element MUST evaluate as a present
  element that is empty everywhere, at every kind of position (ADR-0007).
- **FR-021a**: Each way of loading or writing data MUST be shown to produce exactly the schema and
  data it should for a given input and defined schema.
- **FR-021b**: In the regular build, both YAML FHIRPath suites (the fhirpath.js reference suite and
  Pathling's own suite) MUST load every case with a defined schema in place of an inferred one:
    - a case whose subject is not a FHIR resource, with the schema derived from the definition the
      case's YAML gives, alone;
    - a case whose subject is a FHIR resource, from an input file or inline, with the schema
      inferred from all of its suite's input resources of that type, merged by name with the case's
      own inferred schema, so that it is never narrower than the data.

    A case whose result changes is an engine or reader defect to fix or, if it is a genuine
    conformance gap, is recorded in the exclusion baselines. These schemas are only ever wider than
    the data, so narrowing (FR-006) is covered by the load tests of FR-021a.

Documentation:

- **FR-022**: The documentation MUST describe defined schemas in every binding: how to get, save
  and pass a schema set; that content outside the schema is lost with no error, and that a wide
  schema avoids it; that annotations follow the configuration and primitive metadata follows the
  schema; the narrowing effect of a Delta merge; and that the output's schema equals the given
  schema only when the given schema is a PoF schema.
- **FR-023**: The documentation MUST describe how to bootstrap a schema set from a representative
  sample with inference, and that elements absent from the sample are dropped from later loads.
- **FR-024**: The streaming documentation MUST show a streaming encode with a schema written to
  Delta and to Parquet through a Pathling sink inside `foreachBatch`, extending the 10.x
  `foreachBatch` idiom, in each binding whose Spark API offers `foreachBatch` (Python and Java;
  sparklyr has none), and MUST state that a write with Spark's own streaming writer skips the
  checks a Pathling sink makes.

Follow-up:

- **FR-025**: The work MUST leave GitHub issues for the work this feature does not do: the schema
  generator, including the 10.x shaping options as shortcuts that generate and apply a schema; a
  schema validator; and Delta idempotent writes for Pathling's sinks.
- **FR-026**: The 001 specification's dense-mode tickets (18 and 19) and the requirements this
  feature replaces (FR-009, FR-010, FR-017, FR-043's streaming clause and FR-044 of 001) MUST point
  to this specification or the generator follow-up.

### Key Entities

- **Schema**: the JSON form of a Spark struct type for one resource type. Its base schema selects
  elements and primitive metadata groups. Its annotation fields carry no meaning on input.
- **Schema set**: resource type code → schema. Produced by every source, accepted by every source.
  Users keep and share it; Pathling does not store it.

## Success Criteria _(mandatory)_

### Measurable Outcomes

- **SC-001**: A streaming encode with a schema succeeds in Java, Python and R and returns a
  streaming dataset with the schema.
- **SC-002**: A Delta table appended to only with its own schema set, with at least three batches of
  differing content, has the same schema after every append. Here, and wherever these documents
  say a schema "equals" or is "exactly" another, two schemas are the same when their base schemas
  are equal field for field, in order, ignoring nullability (of fields, array elements and map
  values) and field metadata, with field order significant at every depth: Pathling's output is in
  canonical field order, so a test's expected schema is written in that order too. This criterion applies to a table written under the same annotation
  configuration as the loads that append to it.
- **SC-003**: For every input path (NDJSON, bundles, JSON and XML encode, Parquet, Delta, catalog
  tables, bulk, datasets), output loaded with a schema set of PoF schemas, and that output written by
  every sink, has exactly the given base schema (in the sense of SC-002) plus the annotations the
  configuration enables, including for batches that lack elements of the schema or carry elements
  it does not.
- **SC-004**: The same resources loaded from JSON, XML and stored Parquet with the same schema set
  give equal data.
- **SC-005**: Absence is tested at every kind of position (singular and repeating primitive,
  singular and repeating complex element, choice type, primitive metadata group, annotation, nested,
  and on one or both sides of a combination), each giving the result of the present-but-empty
  element; and both YAML FHIRPath suites, with every case loaded with a defined schema, give the
  results of the inferred load, apart from conformance gaps recorded in the exclusion baselines.
  The regular build time grows by no more than the new tests and the derivation of each YAML
  suite's schemas.
- **SC-006**: Every existing test that passes no schema passes unchanged.
- **SC-007**: A schema set taken from each source and round-tripped through a JSON file is accepted
  by every source in every binding.
- **SC-008**: The streaming documentation example runs.

## Assumptions

- Projection never casts values. A stored type that clashes with the definitions is left to #2884.
- Schema sets are stored and versioned by the user, not by Pathling.
- The layout column (001 tickets 29 and 30) is not implemented yet. When it is, it is treated as
  part of the base schema that ingest always writes.
- Content the definitions do not describe is dropped, as today (001 ticket 34 may change that for
  every path at once).
- A stream written with Spark's own streaming writer gets no Pathling projection at write time. It
  does not need one, because `encode` has already projected it.

## Out of Scope

- Generating schemas from the FHIR definitions: a follow-up issue (FR-025), still targeted at 11.x.
- Validating a schema against Parquet on FHIR or a target: a follow-up issue (FR-025).
- Exposing Delta's idempotent-write options on Pathling's sinks: a follow-up issue (FR-025).
- The 10.x shaping options as shortcuts: part of the generator follow-up.
- A Pathling streaming sink.
- A destination check for raw `writeStream`: `foreachBatch` with a Pathling sink keeps every check
  the sink makes, so a stream written that way needs no separate check.
- A schema option on sinks: the source's schema set already decides what a sink writes.
- Inferring a schema from a stream.
- Reading a type that the schema set names but the data lacks as an empty table: #2997.
- Reading a value whose stored type clashes with the definitions: Read non-conformant data as
  empty, with no modes (#2884).
- Changing the default from inferred.
- Migrating a source from one defined schema to another, beyond reading with one set and writing
  with another.

## Open Items

- **Absent resource types.** Resolved: a type the schema set names but the data lacks keeps today's
  behaviour (not listed, read is an error, nothing written). Reading it as an empty dataset and
  writing an empty table is #2997, "Read a type that a schema set names but the data lacks as an
  empty table".
