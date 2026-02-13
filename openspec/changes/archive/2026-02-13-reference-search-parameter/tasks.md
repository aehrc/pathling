## 1. Unit tests for ReferenceMatcher

- [x] 1.1 Add `ReferenceMatcher` test cases to `ElementMatcherTest` covering: type-qualified ID match against relative and absolute references, exact equality of a relative reference with the search value, bare ID match, absolute URL exact match, `urn:uuid:` exact match, type mismatch, ID mismatch, partial type name non-match (e.g., `APatient/123` vs `Patient/123`), partial ID non-match, and null reference field.

## 2. Unit tests for `:not` modifier

- [x] 2.1 Add test case to verify that `SearchParameterType.REFERENCE.createFilter("not", ...)` returns a negated `SearchFilter`.

## 3. Unit tests for `:[type]` modifier

- [x] 3.1 Add test cases to verify that `SearchParameterType.REFERENCE.createFilter("Patient", ...)` prepends `Patient/` to bare ID search values. Test that already type-qualified values are passed through unchanged. Test that an unrecognised modifier (e.g., `exact`) throws `InvalidModifierException`.

## 4. Core implementation

- [x] 4.1 Add `REFERENCE` field name constant to `FhirFieldNames`.
- [x] 4.2 Update `ReferenceCollection` to use `FhirFieldNames.REFERENCE` instead of its private `REFERENCE_ELEMENT_NAME` constant.
- [x] 4.3 Create `ReferenceMatcher` class implementing `ElementMatcher` with suffix-based matching logic: absolute URI detection (starts with `http://`, `https://`, `urn:uuid:`, or `urn:oid:`) using exact equality, type-qualified `endsWith` with `/` boundary check (or exact equality for relative references), and bare ID `endsWith`.
- [x] 4.4 Update `SearchParameterType.REFERENCE` to declare `FHIRDefinedType.REFERENCE` as its allowed type and override `createFilter()` to: return a negated `SearchFilter` for the `not` modifier, return a type-qualifying `SearchFilter` for valid resource type modifiers, throw `InvalidModifierException` for unrecognised modifiers, and return a plain `SearchFilter` wrapping `ReferenceMatcher` for no modifier.

## 5. Integration tests

- [x] 5.1 Add reference search parameter entries to `TestSearchParameterRegistry` (e.g., `subject` for Observation, `general-practitioner` for Patient — where `general-practitioner` is array-valued to exercise `SearchFilter` vectorisation).
- [x] 5.2 Add integration tests to `SearchColumnBuilderTest` that verify reference search against encoded FHIR resources: single reference match, OR logic with multiple values, type-qualified matching, bare ID matching, absolute URL matching, array-valued reference parameter (e.g., `Patient.generalPractitioner`), `:not` modifier, and `:[type]` modifier.

## 6. Verification

- [x] 6.1 Run the full `fhirpath` module test suite and verify all tests pass.
