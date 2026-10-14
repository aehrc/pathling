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

**Legacy export**:
The conversion of a legacy-layout table to FHIR JSON, from which it is
re-imported into the PoF layout. Lossy.
_Avoid_: migration, converter

**Resource table**:
The stored representation of one FHIR resource type.

**Schema mode**:
How much of the layout a resource table's schema carries: fitted or dense.
Independent of which layout it follows.

**Fitted schema**:
A schema carrying only the elements the data populates.
_Avoid_: pruned schema, sparse schema

**Dense schema**:
A schema carrying every element the definitions describe, within configured
nesting, extension and open-type bounds.
_Avoid_: full schema

**Definition set**:
The FHIR structural definitions that fix every element's type and cardinality.
The data never decides either.

**Annotation**:
A derived value stored beside an element to accelerate a computation the engine
can also perform from the element itself. Optional by definition.

**Primitive metadata group**:
The id and extensions belonging to a primitive element, stored beside it.

**Canonical field order**:
Definition order, restricted to the fields present, with annotations and the
primitive metadata group at fixed positions beside the element they accompany.

**Finding**:
A report of source content the layout does not store: content the definition
set does not describe, or whose type contradicts it.
_Avoid_: warning, validation error

### Query

**Absent element**:
An element the definition set describes but a schema does not carry. Evaluates
to an empty collection.
_Avoid_: missing field

**FHIR type**:
A type the definition set describes, such as `FHIR.Coding`. A complex value of a
FHIR type has the shape its schema gives it, fitted or dense.

**System type**:
A FHIRPath type independent of FHIR, such as `System.Quantity` or
`System.Coding`, produced by a literal or a conversion. A complex value of a
System type always has one fixed, complete shape.
_Avoid_: engine type, internal structure

**Reconciliation**:
Bringing two values of the same FHIR type with different stored shapes to one
merged shape, by field name.
_Avoid_: cast, coercion
