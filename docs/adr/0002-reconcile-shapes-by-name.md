# Divergent shapes are reconciled by name, and Pathling-built structs use canonical field order

Under a fitted schema, two values of the same FHIR type can carry different
Spark struct types — a Coding with `display` and one without, or the same
resource type in files written at different times. Wherever Pathling must bring
them to one type (FHIRPath union and comparison, reading divergent files as one
dataset, widening a table on append), it computes the recursive field-wise union
of the inputs and projects each operand into it **by field name**, absent fields
becoming typed nulls. One merge implementation serves both query-time
combination and file-schema merging, so the two cannot disagree.

Every struct Pathling builds puts its fields in canonical field order:
definition order restricted to the fields present, with annotations and the
primitive metadata group at fixed positions beside the element they accompany.
Operands that already share a type are left unprojected.

## Considered options

- **Spark `cast` between struct types.** Rejected: a struct cast of equal arity
  matches fields by position and ignores names, so `struct<code, system>` cast to
  `struct<system, code>` silently swaps values. Struct equality is positional
  too.
- **Trust the field order found in storage.** Rejected: Spark's merged read
  schema and an evolved Delta table place newly added fields last. A
  non-canonical order is treated as one more divergent shape and reconciled by
  name.

## Consequences

The fixed positions of annotations and metadata groups are part of the stored
type, so changing them is a layout change.
