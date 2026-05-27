## Context

The FHIRPath `type()` function currently resolves type information statically:
`TypeInfo.fromCollection()` examines the collection's definition and returns a
single `TypeInfo` value that is broadcast to every row via `toStructColumn()`
(which uses Spark `lit()`). This works for homogeneous collections where every
element has the same type.

Choice fields (`value[x]`, `effective[x]`, etc.) are represented in Spark as
structs with one nullable field per possible type. Per row, exactly one field is
non-null. The `ChoiceElementCollection` wraps these as a `MixedCollection` with
an `UnsupportedRepresentation` — it has no column of its own, but holds a
reference to the parent collection and the `ChoiceDefinition` that enumerates
the possible types.

Currently, `TypeInfo.fromCollection()` falls through all cases for a
`ChoiceElementCollection` (no FHIR type, no system type) and returns empty.

## Goals / Non-Goals

**Goals:**

- `type()` returns correct per-row TypeInfo for choice fields, with the
  concrete type determined at runtime based on which field is non-null.
- Works for both singular choice elements (e.g., `component[0].value.type()`)
  and plural collections (e.g., `component.value.type()`).
- All choice types are FHIR element types (not resources), so `baseType` is
  always `FHIR.Element`.

**Non-Goals:**

- Supporting `type()` on other `MixedCollection` subtypes (if any exist beyond
  `ChoiceElementCollection`).
- Optimising the generated SQL — a straightforward `CASE WHEN` chain is
  sufficient.
- Changing how `is()`, `as()`, or `ofType()` work on choice fields — those
  already work correctly.

## Decisions

### 1. Handle choice collections in `TypeFunctions.type()` before existing logic

Add an `instanceof ChoiceElementCollection` check at the top of
`TypeFunctions.type()`. This avoids changing the return type of
`TypeInfo.fromCollection()` (which returns `Optional<TypeInfo>`, a static value
object) and keeps the choice-specific logic isolated.

**Alternative considered**: Changing `fromCollection()` to return a column
expression. Rejected because it would change the method's contract for all
callers and conflate static type info with dynamic column building.

### 2. New `TypeInfo.choiceTypeInfoMapper()` returns `UnaryOperator<Column>`

A new static method on `TypeInfo` builds a function that maps an element-level
Spark column to a TypeInfo struct column:

```
TypeInfo.choiceTypeInfoMapper(List<ElementDefinition>)
  → UnaryOperator<Column>
```

The returned function, when given an element column, produces a `CASE WHEN`
chain:

```sql
CASE
  WHEN element.valueQuantity IS NOT NULL
    THEN struct('FHIR', 'Quantity', 'FHIR.Element')
  WHEN element.valueString IS NOT NULL
    THEN struct('FHIR', 'string', 'FHIR.Element')
  ...
  ELSE NULL
END
```

This is used inside `ColumnRepresentation.transform()`, which handles both
singular and plural (array) representations — for arrays, the lambda is applied
element-wise via Spark's `transform()` higher-order function.

**Why a `UnaryOperator<Column>` rather than a `Column`**: The expression must
be relative to the individual element within the transform lambda. A
pre-built column would reference the parent directly and wouldn't work inside
array transforms.

### 3. Add `getAllChildTypes()` to `ChoiceDefinition` interface

The `ChoiceDefinition` interface gains a method to enumerate all possible
child types:

```java
List<ElementDefinition> getAllChildTypes();
```

- `FhirChoiceDefinition`: Iterates `RuntimeChildChoiceDefinition
.getValidChildNames()`, resolves each via `getChildByElementName()`, and
  collects the results.
- `DefaultChoiceDefinition`: Enumerates from its internal child element map.

**Why on the interface**: Enumerating all types is a natural operation on a
choice definition, and both implementations need it.

### 4. Use parent collection's column for traversal

`ChoiceElementCollection` has no column of its own (it uses
`UnsupportedRepresentation`). The parent collection's column is the struct
that contains the choice fields. The transform mapper uses
`element.getField(elementName)` to probe each choice field for non-null.

Flow:

```
choice.getParent().getColumn()          // e.g., component struct column
  .transform(mapper)                    // applies per-element in arrays
  .getValue()                           // unwraps to raw Spark Column
→ Collection.buildWithDefinition(       // wraps as TypeInfo collection
    new DefaultRepresentation(mapped),
    TypeInfo.DEFINITION)
```

## Risks / Trade-offs

- **Generated SQL size**: For choice fields with many types (e.g.,
  `Observation.value[x]` has ~11 types), the CASE WHEN chain becomes large.
  This is unlikely to cause performance issues since Spark's Catalyst optimizer
  handles conditional expressions well, and only one branch evaluates per row.
- **Type ordering**: The order of CASE WHEN clauses depends on the order
  returned by `getAllChildTypes()`. This affects which type is reported if
  multiple fields are somehow non-null (shouldn't happen with valid FHIR data).
  First-match-wins is acceptable.
- **DefaultChoiceDefinition coverage**: The `DefaultChoiceDefinition`
  implementation needs to support `getAllChildTypes()` for non-FHIR schemas
  (e.g., SQL on FHIR ViewDefinitions). If its internal structure doesn't
  readily enumerate children, this may need adaptation.
