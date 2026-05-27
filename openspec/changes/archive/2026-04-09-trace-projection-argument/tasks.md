## 1. TraceExpression (encoders module)

- [x] 1.1 Convert `TraceExpression` from `UnaryExpression` to `BinaryExpression` (left = pass-through, right = value to log)
- [x] 1.2 Mark `TraceExpression` with the `Nondeterministic` trait
- [x] 1.3 Override `eval()` with asymmetric null handling: null left returns null, null right skips logging but returns left
- [x] 1.4 Update `wrapWithTrace` helper in companion object to accept the projected column parameter

## 2. trace() function (fhirpath module)

- [x] 2.1 Add `@Nullable CollectionTransform projection` parameter to `UtilityFunctions.trace()`
- [x] 2.2 When projection is present, evaluate it on input to get projected column and FHIR type
- [x] 2.3 When projection is absent, use input column and FHIR type for both children
- [x] 2.4 Pass both columns to the updated `TraceExpression`

## 3. Tests

- [x] 3.1 Add pass-through tests: trace with projection returns input unchanged
- [x] 3.2 Add logging tests: trace with projection logs projected value and type
- [x] 3.3 Add collector tests: trace with projection captures projected FHIR type
- [x] 3.4 Add edge case tests: empty input with projection, null projected value
- [x] 3.5 Verify existing tests still pass (no-projection behavior unchanged)
