# Data Model: Load FHIR resources with a schema defined up front

## Schema

- **Form**: Spark's JSON form of a `StructType` (`StructType.json()`, `DataType.fromJson`).
- **Fields**: Any. On input:
    - the base schema (fields that are not annotations) selects elements and primitive metadata
      groups;
    - annotation fields (`__numeric`, `__start`/`__end`, `__canonical_*`) are ignored.
- **Validation**:
    - Must parse as a struct. Otherwise it is refused, with an error naming the resource type.
    - No check against the PoF layout. A non-PoF schema yields the PoF schema its elements select
      (ADR-0008).

## Schema set

- **Form**: a JSON object or map: resource type code → schema.
    - Java: `String` (JSON) or `Map<String, String>`.
    - Python: `dict[str, dict]`, or a JSON string.
    - R: a named list, or a JSON string.
- **Validation**:
    - Every key must be a resource type code the definitions know, or it is refused with an error
      naming it.
    - An empty set is the same as no set.
- **Relationships**:
    - Returned by every source's `schemas()`, with one entry for each type the source holds.
    - Accepted by every source. Sinks take none: they write what the source produced.
    - A single entry is accepted by `encode` and `encodeBundle`.
- **Lifecycle**: owned and versioned by the user. Pathling never stores it.

## Observed shape (internal)

The JSON-model shape that the ingest walk is given. It is derived from a schema by
`DefinedSchemaLogic`, one per resource type:

- fields the definitions do not describe are dropped;
- annotations are dropped;
- leaf types follow the ingest converters.

## Layout schema (internal)

The output schema of the ingest walk for an observed shape, before annotation. It is the target
that stored data is projected to. When the given schema is a PoF schema, the layout schema equals
its base schema (FR-008).

## Destination

- **Fields**:
    - format: `delta` or `parquet`;
    - path: a directory;
    - the datasets to be written, by resource type.
- **Derived**: a table path per type, `<path>/<fileNameMapper(type)>.parquet`.
- **States checked**:
    - no table: passes;
    - a PoF-layout table: passes;
    - a legacy-layout table: refused;
    - the datasets carry timestamps and the session does not write INT64 TIMESTAMP(MICROS): refused.
