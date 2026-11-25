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
   is loaded with the set, **Then** those elements are absent from the result, with no error or
   warning.
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

A developer reads FHIR bundles from Kafka, encodes them with a defined schema, and writes the
encoded resources to Delta or Parquet with Spark's own streaming writer. Pathling reads the result
back like any other table.

**Why this priority**: Streaming encode worked in 10.x and is refused in 11.0. It is the use case
that inference cannot serve at all.

**Independent Test**: Encode a streaming source of NDJSON lines with a schema, write it to Delta
and to Parquet with Spark's streaming writer, and read both back through Pathling.

**Acceptance Scenarios**:

1. **Given** a streaming dataset of FHIR JSON or XML and a schema, **When** the user encodes it,
   **Then** the result is a streaming dataset with the schema, in every language binding.
2. **Given** a streaming dataset of bundles and a schema, **When** the user encodes it as bundles,
   **Then** the same holds.
3. **Given** a streaming dataset and no schema, **When** the user encodes it, **Then** the encode is
   refused with a message telling the user to supply a schema.
4. **Given** an encoded stream written with Spark's streaming writer to a Delta or a Parquet
   location following Pathling's table naming, **When** Pathling reads the location, **Then** it
   returns the streamed resources.
5. **Given** the streaming example in the documentation, **When** it is run, **Then** it works.

---

### User Story 3 - Check a destination before writing (Priority: P2)

Before starting a stream, or any write that does not go through a Pathling sink, the developer asks
Pathling to check the destination for each resource type. The check refuses a destination Pathling
could not read back, as a Pathling sink would before writing.

**Why this priority**: A stream written with Spark's own writer skips the checks a Pathling sink
makes. Without this, the first sign of a legacy-layout target or a wrong timestamp setting is
corrupt or unreadable output.

**Independent Test**: Run the check against a legacy-layout table, against a session with the wrong
Parquet timestamp setting, and against an empty location.

**Acceptance Scenarios**:

1. **Given** a destination holding a table in a layout this release does not read, **When** the
   user checks it, **Then** the check fails with the same refusal a Pathling sink gives.
2. **Given** a session that would not store timestamps as Parquet on FHIR requires, and datasets
   carrying range annotations, **When** the user checks the destination, **Then** the check
   fails with the same refusal a Pathling sink gives.
3. **Given** a location with no table, or a readable Parquet on FHIR table, and a correct session,
   **When** the user checks it, **Then** the check passes.

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
  primitive metadata option is set to.
- **A resource type the schema set names but the source lacks** behaves as today: it is not listed,
  reading it is an error, and sinks write nothing for it. Provisional (see Open Items).
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
- **A value whose stored type differs from the definitions' type** cannot occur in PoF-layout
  input, because the types come from the definitions on every path.

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
  lacks are present and null; content the schema has no field for is dropped silently.
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
- **FR-013**: Without a schema, a streaming encode MUST stay refused, with a message that tells the
  user to supply a schema.
- **FR-014**: An encoded stream written with Spark's streaming writer to a Delta or Parquet location
  that follows Pathling's table naming MUST be readable by the corresponding Pathling source.

Destination check:

- **FR-015**: Pathling MUST provide a public check, in Java, Python and R, that is given a
  destination and the datasets to be written, by resource type (streaming or not), returns each
  type's table path, and refuses, before anything is written, a destination
  that a Pathling sink would refuse for a layout it does not read or for the session's Parquet
  timestamp setting, with the same errors.
- **FR-016**: The check MUST cover Delta and Parquet locations.

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
- **FR-021b**: An opt-in run, outside the regular build, MUST re-run the SQL on FHIR compliance and
  YAML FHIRPath suites over their data loaded with a defined schema wider than the data, and give
  the same results as the inferred run.

Documentation:

- **FR-022**: The documentation MUST describe defined schemas in every binding: how to get, save
  and pass a schema set; that content outside the schema is lost without warning, and that a wide
  schema avoids it; that annotations follow the configuration and primitive metadata follows the
  schema; the narrowing effect of a Delta merge; and that the output's schema equals the given
  schema only when the given schema is a PoF schema.
- **FR-023**: The documentation MUST describe how to bootstrap a schema set from a representative
  sample with inference, and that elements absent from the sample are dropped from later loads.
- **FR-024**: The streaming documentation MUST show a streaming encode with a schema written with
  Spark's streaming writer to Delta and to Parquet, using the destination check of FR-015, and MUST
  state what such a write skips compared with a Pathling sink.

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
- **Destination**: a Delta or Parquet location holding one table per resource type, named as
  Pathling's sinks name them.

## Success Criteria _(mandatory)_

### Measurable Outcomes

- **SC-001**: A streaming encode with a schema succeeds in Java, Python and R, and its output
  written to Delta and to Parquet is read back by Pathling with every resource present.
- **SC-002**: A Delta table appended to only with its own schema set, with at least three batches of
  differing content, has the same schema after every append.
- **SC-003**: For every input path (NDJSON, bundles, JSON and XML encode, Parquet, Delta, catalog
  tables, bulk, datasets), output loaded with a schema set of PoF schemas, and that output written by
  every sink, has exactly the given base schema plus the configured annotations, including for batches that lack
  elements of the schema or carry elements it does not.
- **SC-004**: The same resources loaded from JSON, XML and stored Parquet with the same schema set
  give equal data.
- **SC-005**: Absence is tested at every kind of position (singular and repeating primitive,
  singular and repeating complex element, choice type, primitive metadata group, annotation, nested,
  and on one or both sides of a combination), each giving the result of the present-but-empty
  element; and the opt-in parity run gives identical results over inferred and widened defined
  schemas. The regular build time grows by no more than the new load and absence tests.
- **SC-006**: Every existing test that passes no schema passes unchanged.
- **SC-007**: A schema set taken from each source and round-tripped through a JSON file is accepted
  by every source in every binding.
- **SC-008**: The streaming documentation example runs.

## Assumptions

- Projection never casts values. Types come from the definitions on every path, so PoF-layout input
  cannot clash with them.
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
- A schema option on sinks: the source's schema set already decides what a sink writes.
- Inferring a schema from a stream.
- Changing the default from inferred.
- Migrating a source from one defined schema to another, beyond reading with one set and writing
  with another.

## Open Items

- **Absent resource types.** A type the schema set names but the data lacks keeps today's behaviour
  (not listed, read is an error, nothing written). This is provisional and MUST be confirmed with
  the Pathling owner before release; the alternative is an empty dataset and an empty table with the
  schema.
