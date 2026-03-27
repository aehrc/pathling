## 1. Create TypeInfo value class

- [x] 1.1 Create `TypeInfo.java` in `au.csiro.pathling.fhirpath` as a `@Value`
      class with `namespace`, `name`, and `baseType` fields
- [x] 1.2 Add `DEFINITION` constant (moved from `Collection.TYPE_INFO_DEFINITION`)
- [x] 1.3 Add `toStructColumn()` instance method that builds the Spark struct
- [x] 1.4 Add factory methods: `forFhirType()`, `forSystemType()`,
      `forTypeInfo()`, `fromCollection()`

## 2. Update TypeFunctions

- [x] 2.1 Replace `resolveTypeInfoStruct()` and `buildTypeInfoStruct()` with
      calls to `TypeInfo.fromCollection()` and `toStructColumn()`
- [x] 2.2 Simplify `type()` to delegate to `TypeInfo`

## 3. Update Collection

- [x] 3.1 Remove `TYPE_INFO_DEFINITION` constant from `Collection`
- [x] 3.2 Update any references to use `TypeInfo.DEFINITION`

## 4. Verify

- [x] 4.1 Run `TypeFunctionsDslTest` — all tests pass without modification
- [x] 4.2 Run full fhirpath test suite — no regressions
