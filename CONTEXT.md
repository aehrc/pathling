# Pathling

FHIR analytics on Apache Spark: FHIR data is stored in a columnar layout and
queried with FHIRPath, SQL on FHIR views and FHIR search.

## Language

### Storage

**Layout**:
The encoding convention a stored dataset follows: how each FHIR element is
represented as columns. Detectable from a dataset's schema.
_Avoid_: format, encoding (when the convention is meant)

**PoF layout**:
The Parquet on FHIR layout: the layout Pathling writes and the only one its
engine reads.
_Avoid_: new layout

**Legacy layout**:
The layout written by Pathling releases before the PoF layout, by the
Catalyst-based encoders. Refused on read; reachable only through the legacy
export.
_Avoid_: previous layout, old layout, earlier layout

**Layout column**:
The root column, `__layout_v<n>`, that names the version of the PoF layout a
dataset follows. Recognised by its name and type; its values are all null. Not
an annotation.
_Avoid_: layout marker, version column

**Legacy export**:
The conversion of a legacy-layout table to FHIR JSON, from which it is
re-imported into the PoF layout. Lossy.
_Avoid_: migration, converter

**Resource table**:
The stored representation of one FHIR resource type.

**Uber-schema**:
The complete PoF schema of a resource type: every element the definition set
describes, at every depth, with every primitive metadata group and annotation.
Unbounded wherever types recurse, so it is never materialised. Every PoF schema
is a subset of it.
_Avoid_: full schema, complete schema, dense schema

**PoF schema**:
A subset of the uber-schema: every field it carries sits at a path of the
uber-schema with that path's type. Its fields may be in any order; a PoF schema
in canonical field order is canonical.
_Avoid_: valid schema

**Inferred schema**:
The PoF schema a load derives from the data it reads: the subset of the
uber-schema the data populates. The default.
_Avoid_: fitted schema, pruned schema, sparse schema

**Defined schema**:
A PoF schema the user supplies for a resource type, used in place of the
inferred schema. It selects the elements a load keeps, and the load's base schema
equals it, in its field order. A defined schema that is not a PoF schema is
refused, as an inferred one is.
_Avoid_: explicit schema, user schema, dense schema

**Generated schema**:
A bounded subset of the uber-schema produced from the definition set, within
nesting, extension and open-type bounds. Not yet implemented.
_Avoid_: dense schema

**Schema set**:
A mapping from resource type code to schema: a map of Spark struct types (in R,
of strings holding their JSON form). Returned by every data source and accepted
by every source. Kept on disk as a schema set file, one JSON object mapping each
resource type code to its schema in Spark's JSON form.

**Base schema**:
A schema without its annotations. Primitive metadata groups are part of it.

**Projection**:
Shaping a batch to a defined schema when it is loaded or written.
_Avoid_: cast, conform

**Definition set**:
The FHIR structural definitions that fix every element's type and cardinality.
The data never decides either.

**Annotation**:
A derived value stored beside an element to accelerate a computation the engine
can also perform from the element itself. Optional by definition.

**Primitive metadata group**:
The id and extensions belonging to a primitive element, stored beside it.

**Canonical field order**:
The order of the elements in the StructureDefinition of the type, for the FHIR
version in use, restricted to the fields present, with Pathling's own rules for
the fields FHIR does not define: `resourceType` first; a choice element expanded
in place, in the order of its types; a primitive's metadata group (`_x`)
immediately after the primitive, holding `id` and `extension`; annotations
last, a primitive's in its metadata group and a complex element's in its own
struct.
Pathling reads the definitions through HAPI's model of them, which stands in for
the StructureDefinitions; where HAPI has no order, as for the types of an open
choice, the order is taken from the R4 StructureDefinition. Deriving it from the
StructureDefinitions directly is #3006.

**Equal schemas**:
Schemas with the same fields, names and types, in the same order at every
depth, ignoring nullability and field metadata.

**Equivalent schemas**:
Schemas whose canonical forms are equal: equal up to field order.

**Canonicalisation**:
Reordering a schema's fields into canonical field order at every depth, without
changing any name or type. It needs only the schema and the definition set.
_Avoid_: normalisation

**Finding**:
A report of source content the layout does not store: content the definition
set does not describe, or whose type contradicts it.
_Avoid_: warning, validation error

### Query

**Absent element**:
An element of the uber-schema that a schema does not carry. Evaluates exactly as
the element would if it were present and empty.
_Avoid_: missing field

**FHIR type**:
A type the definition set describes, such as `FHIR.Coding`. A complex value of a
FHIR type has the shape its schema gives it, a subset of the type's shape in
the uber-schema.

**System type**:
A FHIRPath type independent of FHIR, such as `System.Quantity` or
`System.Coding`, produced by a literal or a conversion. A complex value of a
System type always has one fixed, complete shape.
_Avoid_: engine type, internal structure

**Reconciliation**:
Bringing two values of the same FHIR type with different stored shapes to one
merged shape, by field name.
_Avoid_: cast, coercion
