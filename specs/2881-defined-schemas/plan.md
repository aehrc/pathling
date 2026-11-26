# Implementation Plan: Load FHIR resources with a schema defined up front

**Feature**: `2881-defined-schemas` | **Date**: 2026-11-25 | **Spec**: [spec.md](spec.md)

## Summary

Every source and `encode`/`encodeBundle` accept an optional defined schema per resource type, as a
Spark `StructType` (in R, a string holding its JSON form). Following ADR-0008, a defined schema
takes the place of the inferred schema: it is turned into the observed shape that the ingest walk
(`ResourceTransformer.toLayout`) is given. The walk then types and prunes it from the
definitions exactly as it does an inferred shape, and the result takes the defined schema's field
order (research R14). Because the read uses a fixed schema
(`from_json` / `from_xml`), there is no inference pass, so a streaming encode works. Before any
of this, the defined schema passes the check of a schema against the uber-schema that #2884 adds
for inferred schemas, once it is re-scoped, and a defined schema that is not a PoF schema is
refused (FR-006b).

Data already in the PoF layout (Parquet, Delta, tables, datasets) is
projected by name to the schema that the same walk yields for the defined schema, after its stored
schema passes the same check a read without a schema makes, which #2884 adds (FR-006c). The
projection never casts, and uses a Column-API projector extracted from `DeltaSink.conformToTarget`, and annotations are then
recomputed from the configuration.

The feature also adds:

- `schemas()` on every source;
- a function that canonicalises a schema set, since a load keeps the given field order (research R14);
- functions that write a schema set to a schema set file and read it back, in every binding
  (research R7);
- a debug log of the paths a defined schema drops from stored data, found by comparing schemas;
- YAML FHIRPath suites that load every case with a defined schema, in the regular build.

A stream is written to a lake through a Pathling sink inside `foreachBatch`, so it needs no
separate destination check.

Following ADR-0007, correctness rests on load tests and on the existing and extended absence tests.
The conformance suites are not run per schema source: the YAML suites load their cases with a
defined schema in place of the inferred one, not in addition to it.

## Technical Context

**Language/Version**: Java 21 (core modules), Python 3.10+, R 3.5+.
**Primary Dependencies**: Apache Spark 4 SQL (Column API only in `io` and `fhir-schema`), Delta
Lake, Py4J (Python) and sparklyr (R).
**Depends on**: #2884, which is to add the check of a schema against the uber-schema; its issue and
specification are being re-scoped to include it. This feature applies the check to defined schemas
and to stored schemas under a read with a defined schema, so the refusal tasks cannot finish
before it lands. How a stored schema's departures are handled is #2884's to decide (its OQ-B).
**Storage**: Parquet and Delta tables in the PoF layout, one table per resource type, named
`<path>/<fileNameMapper(type)>.parquet`.
**Testing**:

- JUnit 5 in `io`, `library-api` and `fhirpath`.
- pytest in `lib/python/tests`.
- testthat in `lib/R/tests/testthat`.
- The YAML FHIRPath suites in `fhirpath`, whose resolvers load each case with a defined schema.

**Target Platform**: Spark 4 on JVM 21: local, Databricks, and Spark clusters.
**Project Type**: Library: `io` (reading and projection), `library-api` (public surface), and the
language bindings.
**Performance Goals**: A load with a defined schema does no inference pass, so it does one fewer
Spark job than an inferred load. The regular build grows only by the new tests and by the
derivation of each YAML suite's schemas, once per suite and resource type (SC-005). Logging dropped
paths compares schemas only, with no Spark job (FR-006a).
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

    There is no schema-mode enum and no cast (ADR-0008), and no new validator: the check that
    refuses a schema that is not a PoF schema is the check of a schema against the uber-schema
    that #2884 adds, reused for defined schemas and for stored schemas read with one (FR-006b,
    FR-006c). The YAML suites take a
    defined schema in their existing resolvers, with no profile, fixture or new test mode. ✅

- **Test-driven development**: every task phase starts with failing tests. Load tests
  (FR-021a/SC-003) and absence tests (FR-021/SC-005) come before the reader, projector and API
  changes. ✅
- **Java conventions**:
    - Google style, enforced by spotless before compilation.
    - `final` everywhere.
    - Jakarta nullability annotations.
    - One top-level class per file, with no new records, enums or inner classes. `DataSourceBuilder`
      is already a record; `withSchemas` adds a schema-set component to it, so no record is
      introduced.
- **Public API change**: `DataSourceBuilder` is a public record of `library-api`, and adding a
  component changes its canonical constructor, adds an accessor and changes `equals`, `hashCode`
  and `toString`. That is a change to the public API, not an internal extension. The released
  canonical constructor (v10.0.0) is `DataSourceBuilder(PathlingContext)`, which the record keeps
  as an explicit constructor, so callers of it (`PathlingContext.read()` and the server tests) are
  unaffected. The current canonical constructor, `(PathlingContext, boolean)`, is unreleased and is
  replaced by one with the extra component. The Java contract records the change. ✅
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
  streaming without a schema). ✅

There are no violations to justify.

## Project Structure

### Documentation (this feature)

```text
specs/2881-defined-schemas/
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
├── SchemaSet.java              # New, internal: a schema set from a map or a file's content;
│                               # refuses unknown keys or non-structs.
├── DefinedSchemaLogic.java     # New: PoF schema → observed shape, → layout schema, and canonical form.
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
├── PathlingContext.java        # encode/encodeBundle schema, streaming refusal, canonicalSchemas.
├── SchemaSets.java             # New: toJson/fromJson of a schema set file's content.
├── io/source/DataSourceBuilder.java   # withSchemas; parquetWithSchemas removed.
├── io/source/*Source.java      # Project named types (Parquet/Delta transformer,
│                               # CatalogSource.read, DatasetSource.dataset), logging dropped
│                               # paths; schemas().
├── io/source/QueryableDataSource.java # schemas().
└── io/sink/DeltaSink.java      # conformToTarget replaced by io Projection.
library-api/src/test/java/au/csiro/pathling/library/
├── io/source/DefinedSchemaSourceTest.java   # New.
├── io/source/SourceSchemasTest.java         # New.
├── io/sink/DefinedSchemaWriteTest.java      # New: sinks keep the source's schema.
├── io/SchemaEquality.java                   # New test helper: schema equality of SC-002.
├── io/SchemaEqualityTest.java               # New.
├── SchemaSetsTest.java                      # New.
└── StreamingEncodeTest.java                 # New.

fhirpath/src/test/java/au/csiro/pathling/
├── fhirpath/AbsentElementTest.java   # Extended to the positions not yet covered.
├── test/yaml/YamlTestArgumentProvider.java  # Per-suite, per-type schemas for FHIR-resource cases.
├── test/yaml/YamlSupport.java        # Stored-layout helpers removed where unused.
├── test/yaml/resolver/FhirResolverFactory.java  # Defined read with the suite's merged schema.
├── test/yaml/resolver/ArbitraryObjectResolverFactory.java  # Defined read through FhirReader.
└── test/layout/TestSchemaMode.java   # The dense placeholder removed.

lib/python/pathling/{datasource.py,context.py,schemas.py,__init__.py}
lib/python/tests/{test_datasource.py,test_encoders.py,test_defined_schemas.py,
                  test_schema_files.py,schema_equality.py,test_schema_equality.py}
lib/R/R/{datasource.R,context.R,encoding.R,schemas.R}
lib/R/tests/testthat/{test-datasource.R,test-encoding.R,test-defined-schemas.R,
                      test-schema-files.R,helper_schema_equality.R,test-schema-equality.R}

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

- [data-model.md](data-model.md): schema, schema set, observed shape, layout schema.
- [contracts/](contracts/): the Java, Python and R surfaces.
- [quickstart.md](quickstart.md): daily Delta append, Kafka stream to Delta and Parquet through
  `foreachBatch`, schema round trip.
