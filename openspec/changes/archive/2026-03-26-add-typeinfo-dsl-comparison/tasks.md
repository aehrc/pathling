## 1. TypeInfoExpectation class

- [x] 1.1 Create `TypeInfoExpectation` class in `au.csiro.pathling.test.dsl` with fields `namespace`, `name`, `baseType` and static `toTypeInfo(String)` factory method
- [x] 1.2 Add unit tests for `toTypeInfo()` parsing: system types, FHIR types, and invalid format exception

## 2. Executor integration

- [x] 2.1 Add TypeInfo comparison branch in `DefaultYamlTestExecutor` to handle `TypeInfoExpectation` instances against struct Rows
- [x] 2.2 Add TypeInfo list comparison support for plural results

## 3. Test refactoring

- [x] 3.1 Refactor `TypeFunctionsDslTest` primitive type assertions to use `toTypeInfo()`
- [x] 3.2 Refactor `TypeFunctionsDslTest` FHIR type assertions to use `toTypeInfo()`
- [x] 3.3 Refactor `TypeFunctionsDslTest` choice type assertions to use `toTypeInfo()`
- [x] 3.4 Run full test suite to verify no regressions
