## 1. Add indeterminate type guard in repeatAll()

- [x] 1.1 Add empty-type check after `level0Type`/`level1Type` extraction and before the `equals()` comparison in `Collection.repeatAll()` — throw `InvalidUserInputError` when either type is `Optional.empty()`

## 2. Unify self-reference flag

- [x] 2.1 Rename `allowPrimitiveSelfRef` parameter to `allowSelfReference` in `Collection.repeatAll()` and update the `repeat()` call site
- [x] 2.2 Update `errorOnDepthExhaustion` computation to incorporate `allowSelfReference`: `!isExtensionTraversal && !allowSelfReference`
- [x] 2.3 Update Javadoc on `repeatAll()` and `repeat()` to reflect the unified flag semantics

## 3. Test helpers

- [x] 3.1 Add `createObservation()` helper (or equivalent) that produces an Observation with a populated `value[x]` choice element, following the existing `createPatient()` / `createQuestionnaire()` pattern

## 4. RepeatAllFunctionDslTest — choice type tests

- [x] 4.1 Test `value.repeatAll($this)` on Observation → indeterminate type error
- [x] 4.2 Test `value.repeatAll(ofType(Quantity))` on Observation → depth exhaustion error
- [x] 4.3 Test `repeatAll(value)` on Observation → polymorphic traversal error
- [x] 4.4 Test `value.repeatAll(first())` on Observation → mixed collection error

## 5. RepeatAllFunctionDslTest — resource-level degenerate tests

- [x] 5.1 Test `repeatAll($this)` on Patient → depth exhaustion error (already exists in testRepeatAllInfiniteRecursionAndExtensions)
- [x] 5.2 Test `name.repeatAll(%resource).gender` on Patient → depth exhaustion error

## 6. RepeatFunctionDslTest — choice type tests

- [x] 6.1 Test `value.repeat($this)` on Observation → indeterminate type error
- [x] 6.2 Test `repeat(value.ofType(Quantity))` on Observation → returns Quantity value (level_1 empty, early return)

## 7. RepeatFunctionDslTest — self-referential complex tests

- [x] 7.1 Test `repeat($this)` on Patient → returns the resource unchanged
- [x] 7.2 Test `name.repeat(%resource).gender` on Patient → returns the patient's gender value

## 8. Verification

- [x] 8.1 Run `mvn test -pl fhirpath -Dtest=RepeatAllFunctionDslTest,RepeatFunctionDslTest`
- [x] 8.2 Run `mvn test -pl fhirpath` for full regression check (2 pre-existing failures in FhirViewShareableComplianceTest, unrelated)
