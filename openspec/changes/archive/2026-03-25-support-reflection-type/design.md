## Context

The FHIRPath specification includes an STU reflection section defining a
`type()` function that returns type metadata for collection elements. The CI
build of the spec (which we are targeting) defines a simplified model with only
`SimpleTypeInfo` and `ClassInfo`, both sharing the same structural shape:
`{namespace, name, baseType}`.

Pathling's FHIRPath engine evaluates expressions over Spark DataFrames. Each
collection carries optional `FhirPathType` (System type) and `FHIRDefinedType`
(FHIR type), plus an optional `NodeDefinition`. The challenge is that primitive
collections (e.g., `BooleanCollection`) are used for both System literals
(`true`) and FHIR path elements (`Patient.active`), and Quantity/Coding
collections always carry a definition even for literals.

## Goals / Non-Goals

**Goals:**

- Implement the `type()` function per the CI build FHIRPath specification.
- Return a navigable struct with `namespace`, `name`, and `baseType` fields.
- Correctly distinguish System types (literals) from FHIR types (path-navigated
  elements).
- Pass the fhirpath.js reference test suite `testType` cases.

**Non-Goals:**

- The `trace()` function (tracked separately).
- Choice type (`value[x]`) support for `type()` — may return empty initially.
- Full FHIR type hierarchy derivation for `baseType` (e.g., `id` → `uri`).
- `ListTypeInfo` and `TupleTypeInfo` (removed in CI build spec).

## Decisions

### 1. TypeInfo as a struct column with a DefaultCompositeDefinition

**Decision**: `type()` returns a `Collection` backed by a Spark struct column
with three string fields (`namespace`, `name`, `baseType`), using a static
`DefaultCompositeDefinition` for child navigation.

**Rationale**: This follows the same pattern used by `CodingCollection` and
`QuantityCollection` for their `LITERAL_DEFINITION` singletons. The
`DefaultCompositeDefinition` enables standard child element navigation
(`.namespace`, `.name`, `.baseType`) without introducing new definition types.

**Alternatives considered**: Creating a `TypeInfoCollection` subclass was
considered but rejected — the result needs no special operations beyond child
navigation, so a plain `Collection` with a definition suffices.

### 2. Namespace resolution via definition type

**Decision**: Determine namespace by checking the definition type:

- `definition` is the `TYPE_INFO_DEFINITION` singleton → `System.Object`
- `definition.isFhirDefinition()` returns true → FHIR namespace
- `FhirPathType` is present (and not FHIR) → System namespace
- Otherwise → empty collection

**Rationale**: This cleanly distinguishes all cases:

- Literals (`true`, `1`, `'hello'`) have no definition or a
  `Default*Definition` → System.
- FHIR path elements (`Patient.active`) carry a `FhirElementDefinition` →
  FHIR.
- Quantity/Coding literals carry a `DefaultCompositeDefinition` (the
  `LITERAL_DEFINITION` singleton) → System.
- TypeInfo results carry the `TYPE_INFO_DEFINITION` singleton → System.Object.
- Empty/choice collections → empty (no type info available).

**Alternatives considered**: Adding an explicit namespace field to `Collection`
was considered but rejected as too invasive for this feature.

### 3. Name resolution

**Decision**:

- FHIR namespace: use `fhirType.toCode()` (lowercase: `"boolean"`,
  `"CodeableConcept"`).
- System namespace: use `fhirPathType.getTypeSpecifier()` (PascalCase:
  `"Boolean"`, `"String"`).

**Rationale**: These values are already available on the collection and match
the expected names in the reference tests.

### 4. Simplified baseType mapping

**Decision**: Use constant mappings:

- System types → `"System.Any"`
- FHIR resources → `"FHIR.Resource"`
- FHIR elements → `"FHIR.Element"`

**Rationale**: The spec only shows two `baseType` examples (`System.Any` and
`FHIR.Element`), and deriving the full FHIR type hierarchy from HAPI's class
hierarchy is complex (HAPI introduces intermediate classes like `Type`,
`PrimitiveType`, `BaseDateTimeType` that don't map directly to FHIR types).
The R4 hierarchy is simpler than R5 (no `DataType` intermediate), and
constant mappings cover all reference test cases.

**Known limitation**: FHIR primitive subtypes (`id` → `uri`, `code` →
`string`) report `FHIR.Element` rather than their actual FHIR parent type.

### 5. Collection factory method

**Decision**: Add a static factory method on `Collection` for constructing
collections with only a `ColumnRepresentation` and `NodeDefinition`, setting
both type fields to empty.

**Rationale**: The existing `Collection.getInstance()` requires a
`FHIRDefinedType`, which TypeInfo results don't have. The protected Lombok
constructor is accessible from within `Collection` but not from the function
provider package. A factory method follows the existing pattern of static
`build()` methods on collection classes.

## Risks / Trade-offs

- **[Choice types unsupported]** → Document as known limitation. Users can use
  `ofType()` to narrow before calling `type()`. Create a follow-up issue.
- **[Simplified baseType]** → Document limitation. Can be refined later by
  walking HAPI's class hierarchy if spec conformance tests require it.
- **[TypeInfo is not a real FHIR/System type]** → `type()` called on a TypeInfo
  collection returns `{System, Object, System.Any}`, matching the reference
  implementation behaviour.
