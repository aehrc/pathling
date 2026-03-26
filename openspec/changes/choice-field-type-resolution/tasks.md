## 1. ChoiceDefinition interface and implementations

- [ ] 1.1 Add `getAllChildTypes()` method to `ChoiceDefinition` interface returning `List<ElementDefinition>`
- [ ] 1.2 Implement `getAllChildTypes()` in `FhirChoiceDefinition` using `RuntimeChildChoiceDefinition.getValidChildNames()`
- [ ] 1.3 Implement `getAllChildTypes()` in `DefaultChoiceDefinition`

## 2. TypeInfo dynamic column builder

- [ ] 2.1 Add `choiceTypeInfoMapper(List<ElementDefinition>)` static method to `TypeInfo` returning `UnaryOperator<Column>` that builds a CASE WHEN chain checking each choice field for non-null and returning the corresponding TypeInfo struct

## 3. TypeFunctions.type() choice handling

- [ ] 3.1 Add `ChoiceElementCollection` handling in `TypeFunctions.type()` before the existing static logic: get choice types from definition, build mapper, apply via `parent.getColumn().transform(mapper)`, return Collection with `TypeInfo.DEFINITION`

## 4. Tests

- [ ] 4.1 Add DSL tests for singular choice elements: `component[N].value.type()` returning correct TypeInfo for Quantity, string, boolean, and CodeableConcept values
- [ ] 4.2 Add DSL tests for plural choice collections: `component.value.type()` returning heterogeneous TypeInfo per element, with count and name navigation
- [ ] 4.3 Add DSL tests for choice element with no value set returning null element
- [ ] 4.4 Add DSL tests for composition with other FHIRPath functions: `where()` filtering by type name, `distinct()` on type names
