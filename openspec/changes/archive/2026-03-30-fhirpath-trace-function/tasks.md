## 1. Catalyst Expression

- [x] 1.1 Create `TraceExpression` Scala case class in `encoders/src/main/scala/au/csiro/pathling/sql/TraceExpression.scala` extending `UnaryExpression` with `CodegenFallback`
- [x] 1.2 Implement `nullSafeEval` to log the value via SLF4J and return it unchanged
- [x] 1.3 Preserve child's `dataType` and `nullable` properties

## 2. Function Provider

- [x] 2.1 Create `UtilityFunctions.java` in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/function/provider/`
- [x] 2.2 Implement `trace(Collection input, StringCollection name)` method annotated with `@FhirPathFunction`
- [x] 2.3 Wrap the input column with `TraceExpression` and return via `copyWithColumn`

## 3. Registry

- [x] 3.1 Register `UtilityFunctions` in `StaticFunctionRegistry`

## 4. Tests

- [x] 4.1 Create trace function test class using `DatasetEvaluator` pattern with `@SpringBootUnitTest`
- [x] 4.2 Test pass-through: `trace()` returns the same values as the input for strings, booleans, and complex types
- [x] 4.3 Test logging: attach `ListAppender` and verify log entries contain the trace label and value representation
- [x] 4.4 Test empty collection: `{}.trace('label')` returns empty
- [x] 4.5 Test error: `trace()` with no arguments raises an error
- [x] 4.6 Verify existing YAML reference tests for `trace()` pass (2 exclusions added for unsupported projection parameter and primitive .id extension after trace)
