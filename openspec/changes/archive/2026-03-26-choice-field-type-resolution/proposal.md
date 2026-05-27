## Why

The FHIRPath `type()` reflection function currently returns empty for choice
fields (e.g., `Observation.component.value[x]`). Choice fields are polymorphic
elements where the concrete type varies per row at runtime. Users need `type()`
to resolve the actual type of each element in the collection, enabling
type-aware queries on heterogeneous data without requiring `ofType()` first.

## What Changes

- Add per-row runtime type resolution for choice fields in the `type()`
  function, producing a dynamic `CASE WHEN` expression that inspects which
  choice field is non-null and returns the corresponding TypeInfo.
- Add an `getAllChildTypes()` method to the `ChoiceDefinition` interface so
  that all possible types for a choice element can be enumerated.
- Add a `choiceTypeInfoMapper()` method to `TypeInfo` that builds a
  `UnaryOperator<Column>` for transforming choice elements into TypeInfo
  structs.

## Capabilities

### New Capabilities

_(none)_

### Modified Capabilities

- `fhirpath-type-function`: Add requirements for `type()` returning correct
  per-row TypeInfo for choice fields, covering both singular and plural
  collections with heterogeneous types.

## Impact

- `fhirpath` module: `TypeInfo`, `TypeFunctions`, `ChoiceDefinition`,
  `FhirChoiceDefinition`, `DefaultChoiceDefinition`
- No API or behavioral changes for non-choice collections.
- No breaking changes.
