# Implementation Plan: Load FHIR resources with a schema defined up front

**Feature**: `2881_defined-schemas` | **Date**: 2026-11-25 | **Spec**: [spec.md](spec.md)

## Summary

Every source and `encode`/`encodeBundle` accept an optional defined schema per
resource type, in Spark's JSON form. Following ADR-0008, a defined schema takes the place of the
inferred schema: it is turned into the observed shape that the ingest walk
(`ResourceTransformer.toLayout`) is given. The walk then types, orders and prunes it from the
definitions exactly as it does an inferred shape. Because the read uses a fixed schema
(`from_json` / `from_xml`), there is no inference pass, so a streaming encode works.

Data already in the PoF layout (Parquet, Delta, tables, datasets) is
projected by name to the schema that the same walk yields for the defined schema. The projection
uses a Column-API projector extracted from `DeltaSink.conformToTarget`, and annotations are then
recomputed from the configuration.

The feature also adds:

- `schemas()` on every source;
- a public destination check that runs the sinks' layout and timestamp refusals and returns each
  type's table path;
- an opt-in parity profile.

Following ADR-0007, correctness rests on load tests and on the existing and extended absence tests.
The conformance suites are not run per schema source.

## Technical Context

**Language/Version**: Java 21 (core modules), Python 3.10+, R 3.5+.
**Primary Dependencies**: Apache Spark 4 SQL (Column API only in `io` and `fhir-schema`), Delta
Lake, Py4J (Python), sparklyr and jsonlite (R).
**Storage**: Parquet and Delta tables in the PoF layout, one table per resource type, named
`<path>/<fileNameMapper(type)>.parquet`.
**Testing**:

- JUnit 5 in `io`, `library-api` and `fhirpath`.
- pytest in `lib/python/tests`.
- testthat in `lib/R/tests/testthat`.
- An opt-in Maven profile `definedSchemaParity` in `fhirpath/pom.xml`.

**Target Platform**: Spark 4 on JVM 21: local, Databricks, and Spark clusters.
**Project Type**: Library: `io` (reading and projection), `library-api` (public surface), and the
language bindings.
**Performance Goals**: A load with a defined schema does no inference pass, so it does one fewer
Spark job than an inferred load. The regular build grows only by the new tests (SC-005).
**Constraints**:

- `io` may not use internal Catalyst API.
- A streaming dataset must never reach an action (no `.schema()` of `spark.read.json(Dataset)`,
  no `.rdd`).
- Existing calls without a schema are unchanged (FR-020).

**Scale/Scope**: Seven sources, two encode entry points, three bindings.

## Constitution Check

The constitution is the global `CLAUDE.md`, the project `CLAUDE.md` and `.claude/CLAUDE.md`, and
`CONTRIBUTING.md`.

- **Simplicity**: there is one mechanism. The defined schema becomes the observed shape. The only
  new pieces are:
    - deriving an observed shape from a PoF schema;
    - one projector, extracted from existing code, not new;
    - the plumbing.

    There is no schema-mode enum, no validator and no cast (ADR-0008). The parity run reuses the
    existing `TestSchemaMode` switch and its `PofDatasets.fromJson` choke point. ✅

- **Test-driven development**: every task phase starts with failing tests. Load tests
  (FR-021a/SC-003) and absence tests (FR-021/SC-005) come before the reader, projector and API
  changes. ✅
- **Java conventions**:
    - Google style, enforced by spotless before compilation.
    - `final` everywhere.
    - Jakarta nullability annotations.
    - One top-level class per file, with no new records, enums or inner classes. `DataSourceBuilder`
      is already a record, and adding a component to it extends existing code rather than
      introducing a record.
    - `Optional`/`Stream` composition.
    - Substantial logic in `*Logic`-style classes: `DefinedSchemaLogic`, `Projection`.

    ✅

- **Module boundaries**: the projection and the observed-shape derivation live in `io`, which may
  use only the Column API. `conformToTarget` is already Column API, so extracting it to `io` is
  allowed. `MergeCast` (Catalyst, `encoders`) is not used. ✅
- **Comments and commits**: full sentences ending in periods; `<type>: objective` commit messages. ✅
- **Only strictly necessary code**: no code or tests beyond what an FR needs. The XML parse-schema
  builder reuses `XmlIngestShape` rather than adding a second parser. ✅
- **Visibility of system status**: errors name the resource type (unreadable schema, unknown key,
  streaming without a schema, destination refusals). ✅

| Violation                                                     | Why needed                                                                                       | Simpler alternative rejected because                                                               |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| A new public method on `PathlingContext` (`checkDestination`) | A stream written with Spark's own writer bypasses every sink, so the check cannot live on a sink | Running the check implicitly inside `encode` was rejected: `encode` does not know the destination. |

## Project Structure

### Documentation (this feature)

```text
specs/2881_defined-schemas/
├── spec.md
├── plan.md            # This file.
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── java-api.md
│   ├── python-api.md
│   └── r-api.md
├── evidence/          # Measurements the decisions rest on (specs/README.md).
├── checklists/
│   └── requirements.md
└── tasks.md
```

### Source code (repository root)

```text
io/src/main/java/au/csiro/pathling/io/
├── SchemaSet.java              # New: parses the JSON schema set, refuses unknown keys or non-structs.
├── DefinedSchemaLogic.java     # New: PoF schema → observed shape, and → layout schema.
├── Projection.java             # New: by-name Column projector, extracted from DeltaSink.
├── ResourceTransformer.java    # Observed-shape derivation that drops undescribed fields.
├── FhirFormatReader.java       # read / readBundles overloads taking a defined schema.
├── FhirJsonReader.java         # from_json with the derived shape; no inference.
├── FhirXmlReader.java          # Parse schema from the defined schema; no inference.
└── XmlIngestShape.java         # Builds a parse schema from a fixed shape.
io/src/test/java/au/csiro/pathling/io/
├── DefinedSchemaLogicTest.java      # New.
├── DefinedSchemaReadTest.java       # New.
├── DefinedSchemaStreamingReadTest.java  # New.
├── DefinedSchemaParityTest.java     # New: JSON = XML = stored (FR-009).
├── ProjectionTest.java              # New.
└── SchemaSetTest.java               # New.

library-api/src/main/java/au/csiro/pathling/library/
├── PathlingContext.java        # encode/encodeBundle schema, streaming refusal, checkDestination.
├── io/source/DataSourceBuilder.java   # withSchemas; parquetWithSchemas removed.
├── io/source/*Source.java      # Project named types; schemas().
├── io/source/QueryableDataSource.java # schemas().
├── io/sink/DeltaSink.java      # conformToTarget replaced by io Projection.
└── io/sink/DestinationCheck.java      # New: shared refusals, used by sinks and checkDestination.
library-api/src/test/java/au/csiro/pathling/library/
├── io/source/DefinedSchemaSourceTest.java   # New.
├── io/source/SourceSchemasTest.java         # New.
├── io/sink/DefinedSchemaWriteTest.java      # New: sinks keep the source's schema.
├── io/sink/DestinationCheckTest.java        # New.
└── StreamingEncodeTest.java                 # New.

fhirpath/src/test/java/au/csiro/pathling/
├── AbsentElementTest.java      # Extended to the positions not yet covered.
└── test/layout/{TestSchemaMode,PofDatasets}.java  # "defined" mode for the parity profile.
fhirpath/pom.xml                # definedSchemaParity profile.

lib/python/pathling/{datasource.py,context.py}
lib/python/tests/{test_datasource.py,test_encoders.py,test_defined_schemas.py}
lib/R/R/{datasource.R,context.R,encoding.R}
lib/R/tests/testthat/{test-datasource.R,test-encoding.R,test-defined-schemas.R}

site/docs/libraries/io/schema.md, site/docs/libraries/examples/streaming.md
```

**Structure Decision**:

- Projection and the observed-shape derivation sit in `io`, beside `ResourceTransformer`, because
  `FhirReader` needs them and `library-api` already depends on `io`.
- Public surface sits in `library-api`.
- The bindings mirror the Java surface.

## Phase 0: Research

See [research.md](research.md). Two points are to be confirmed by a measurement recorded in
`evidence/` before the read is built (tasks T002 and T003):

- how Spark's JSON reader returns a number for a string-typed field;
- that `from_json` over a streaming dataset needs no action.

## Phase 1: Design & Contracts

- [data-model.md](data-model.md): schema, schema set, observed shape, layout schema, destination.
- [contracts/](contracts/): the Java, Python and R surfaces.
- [quickstart.md](quickstart.md): daily Delta append, Kafka stream to Delta and Parquet, schema
  round trip, destination check.
