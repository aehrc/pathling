## 1. ChoiceDefinition interface and implementations

- [x] 1.1 Add `getAllChildTypes()` method to `ChoiceDefinition` interface returning `List<ElementDefinition>`
- [x] 1.2 Implement `getAllChildTypes()` in `FhirChoiceDefinition` using `RuntimeChildChoiceDefinition.getValidChildNames()`
- [x] 1.3 Implement `getAllChildTypes()` in `DefaultChoiceDefinition`

## 2. TypeInfo dynamic column builder

- [x] 2.1 Add `choiceTypeInfoMapper(List<ElementDefinition>)` static method to `TypeInfo` returning `UnaryOperator<Column>` that builds a CASE WHEN chain checking each choice field for non-null and returning the corresponding TypeInfo struct

## 3. TypeFunctions.type() choice handling

- [x] 3.1 Add `ChoiceElementCollection` handling in `TypeFunctions.type()` before the existing static logic: get choice types from definition, build mapper, apply via `parent.getColumn().transform(mapper)`, return Collection with `TypeInfo.DEFINITION`

## 4. Tests

- [x] 4.1 Add DSL tests for singular choice elements: `component[N].value.type()` returning correct TypeInfo for Quantity, string, boolean, and CodeableConcept values
- [x] 4.2 Add DSL tests for plural choice collections: `component.value.type()` returning heterogeneous TypeInfo per element, with count and name navigation
- [x] 4.3 Add DSL tests for choice element with no value set returning null element
- [x] 4.4 Add DSL tests for composition with other FHIRPath functions: `where()` filtering by type name, `distinct()` on type names
