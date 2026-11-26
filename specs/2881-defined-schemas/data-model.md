# Data Model: Load FHIR resources with a schema defined up front

## Schema

- **Form**: a Spark `StructType` in Java and Python. In R, which has no struct type in sparklyr, a
  string holding the struct type's JSON form (`StructType.json()`, `DataType.fromJson`), which the
  user treats as opaque.
- **Fields**: Any. On input:
    - the base schema (fields that are not annotations) selects elements and primitive metadata
      groups;
    - annotation fields (`__numeric`, `__start`/`__end`, `__canonical_*`) are ignored.
- **Validation**:
    - Must be a struct type (in Python a `StructType`; in R a string that parses as a struct
      type's JSON form). Otherwise it is refused, with an error naming the resource type.
    - Must be a PoF schema for its resource type, by the check of a schema against the
      uber-schema that #2884 adds for inferred schemas, once it is re-scoped. A field the definitions do not
      describe, or one whose type or cardinality contradicts them, is refused with an error naming
      the resource type and path (FR-006b, ADR-0008). Annotation fields are not checked.
- **Field order**: the user's. The output keeps it at every depth (FR-008); canonicalisation
  (FR-027) gives canonical field order on request.

## Schema set

- **Form**: a map: resource type code → schema. No class wraps it.
    - Java: `Map<String, StructType>`.
    - Python: `dict[str, StructType]`.
    - R: a named list of strings, each a struct type's JSON form.
- **Validation**:
    - Every key must be a resource type code the definitions know, or it is refused with an error
      naming it.
    - An empty set is the same as no set.
- **Relationships**:
    - Returned by every source's `schemas()`, with one entry for each type the source holds.
    - Accepted by every source. Sinks take none: they write what the source produced.
    - A single entry is accepted by `encode` and `encodeBundle`.
- **Lifecycle**: owned and versioned by the user, as a schema set file. Pathling never stores it.

## Schema set file

The one file format for a schema set, shared by every binding, so that a file written in one is
read by the others (spec FR-028).

- **Content**: one JSON object. Each key is a resource type code, and each value is that type's
  schema in Spark's JSON form: the object that `StructType.json()` produces, not a string holding
  it.

    ```json
    {
        "Patient": {
            "type": "struct",
            "fields": [
                {
                    "name": "id",
                    "type": "string",
                    "nullable": true,
                    "metadata": {}
                }
            ]
        }
    }
    ```

- **Written and read by**:
    - Java: `SchemaSets.toJson(Map<String, StructType>)` and `SchemaSets.fromJson(String)`, in
      `library-api`, which convert between a schema set and the file's content; the caller reads
      and writes the file.
    - Python: `write_schemas(schemas, path)` and `read_schemas(path)` in the `pathling` package,
      in pure Python (`StructType.json()`, `StructType.fromJson`), with no JVM.
    - R: `pathling_write_schemas(sc, schemas, path)` and `pathling_read_schemas(sc, path)`, which
      take a Spark connection and delegate to `SchemaSets.toJson` and `SchemaSets.fromJson`, so
      the JSON is never parsed or rebuilt in R. Reading passes the file's text to `fromJson` and
      takes each schema's JSON form from the JVM with `StructType.json()`. Writing joins the
      strings into the file's content under their keys, checks it with `fromJson`, and writes what
      `toJson` returns.
- **Validation** on reading: a file that is not a JSON object is refused with an error naming the
  file; a value that is not a struct type is refused with an error naming its key; a key that is
  not a resource type code is refused when the set is given to a source, as for any set.

## Observed shape (internal)

The JSON-model shape that the ingest walk is given. It is derived from a schema by
`DefinedSchemaLogic`, one per resource type, from a schema that has passed the check of FR-006b:

- annotations are dropped;
- leaf types follow the ingest converters.

## Layout schema (internal)

The output schema of the ingest walk for an observed shape, before annotation, reordered by name
into the defined schema's field order at every depth. The walk itself builds canonical order, so
the reorder is skipped where the two already agree. It is the target that stored data is projected
to. It equals the given schema's base schema (FR-008).

## Canonical form (internal)

The given schema set with every schema reordered into canonical field order at every depth, names
and types of the fields it keeps unchanged (FR-027). It is computed from the schemas and the
definition set alone, with no Spark job: each struct's fields are sorted by the position the
canonical structure gives their names. Fields the definitions do not describe are dropped, until
#2925 gives them a place. A field whose type or cardinality contradicts the definitions is refused
with an error naming the resource type and path. Annotation fields are kept, in their canonical
places: last in their struct, a primitive's in its metadata group.

## Schema equality (tests)

Two schemas are the same when their base schemas are equal field for field, in order at every
depth (so an output that does not keep the given order fails), ignoring
nullability (of fields, array elements and map values) and field metadata (spec SC-002). The
annotation fields are compared separately, against the configuration. One test helper per binding
implements this comparison, and every test of an "exact" or "equal" schema uses it.
