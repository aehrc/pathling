## 1. TypeInfo Definition and Collection Factory

- [x] 1.1 Create a static `TYPE_INFO_DEFINITION` (`DefaultCompositeDefinition`) with three string children: `namespace`, `name`, `baseType`
- [x] 1.2 Add a static factory method on `Collection` for building collections with only a `ColumnRepresentation` and `NodeDefinition` (no `FHIRDefinedType` required)

## 2. Type Resolution Logic

- [x] 2.1 Implement namespace resolution: `BaseFhirNodeDefinition` → FHIR, literal/no definition → System, `TYPE_INFO_DEFINITION` → System (Object)
- [x] 2.2 Implement name resolution: FHIR namespace uses `fhirType.toCode()`, System namespace uses `fhirPathType.getTypeSpecifier()`
- [x] 2.3 Implement baseType mapping: System types → `System.Any`, FHIR resources → `FHIR.Resource`, FHIR elements → `FHIR.Element`
- [x] 2.4 Handle empty/choice collections by returning empty

## 3. Function Provider

- [x] 3.1 Add the `type()` function to an appropriate function provider class (e.g., `TypeFunctions` or a new `ReflectionFunctions`) using the `@FhirPathFunction` annotation
- [x] 3.2 Build the struct column with literal values for `namespace`, `name`, `baseType` and wrap in a Collection with `TYPE_INFO_DEFINITION`

## 4. Testing

- [x] 4.1 Write DSL tests covering System primitive literals (`Integer`, `Boolean`, `String`, `Decimal`, `Date`, `DateTime`, `Time`)
- [x] 4.2 Write DSL tests covering System Quantity and Coding literals
- [x] 4.3 Write DSL tests covering FHIR primitive elements (`Patient.active`, `Patient.birthDate`)
- [x] 4.4 Write DSL tests covering FHIR complex type elements (`Patient.maritalStatus`)
- [x] 4.5 Write DSL tests covering FHIR resource types (`Patient.type()`)
- [x] 4.6 Write DSL tests for navigation into TypeInfo fields (`.namespace`, `.name`, `.baseType`)
- [x] 4.7 Write DSL tests for empty collection input and nested `type().type()` returning System.Object
- [x] 4.8 Write DSL tests for `ofType()` followed by `type()`
- [x] 4.9 Write DSL tests for FHIRPath operation results returning System types (e.g., `Patient.active.not().type()`, `where()` preserving FHIR context)
- [x] 4.10 Remove or update `testType` exclusions in `config.yaml` and verify reference test suite passes

## 5. Documentation

- [x] 5.1 Document known limitations: choice types unsupported, simplified baseType mapping
