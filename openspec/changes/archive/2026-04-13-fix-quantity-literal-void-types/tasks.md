## 1. Fix literal encoding

- [x] 1.1 In `FlexiDecimalSupport.toLiteral` (encoders module, Scala), change the null branch from `lit(null)` to `lit(null).cast(DATA_TYPE)`
- [x] 1.2 In `QuantityEncoding.encodeLiteral` (fhirpath module), append `.cast(dataType())` to the `toStruct(...)` return value

## 2. Update test exclusions

- [x] 2.1 Remove the #2588 exclusion block from `fhirpath/src/test/resources/fhirpath-js/config.yaml` (lines 179-193)
- [x] 2.2 Evaluate the related "Indefinite calendar duration union deduplication" wontfix exclusion — remove or re-link if the fix resolves it (kept: different issue — equality semantics, not VOID types)

## 3. Add regression tests

- [x] 3.1 Add DSL test cases in `RepeatFunctionDslTest` for `repeat($this)` over combined Quantity literals (both indefinite calendar and UCUM cases)

## 4. Validate

- [x] 4.1 Run `RepeatFunctionDslTest` — all tests pass
- [x] 4.2 Run `YamlReferenceImplTest` — previously excluded expressions now pass
- [x] 4.3 Run full fhirpath module test suite — no regressions
