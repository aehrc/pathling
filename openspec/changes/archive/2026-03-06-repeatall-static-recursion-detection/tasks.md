## 1. Static type analysis gate in Collection.repeatAll()

- [x] 1.1 Add level_1 computation by applying transform to level_0 (resultTemplate)
- [x] 1.2 Add empty check: if level_1.isEmpty(), return level_0 directly (both primitive and complex)
- [x] 1.3 Add FHIR type comparison between level_0 and level_1
- [x] 1.4 Add error for self-referential primitive traversal (same FHIR type, primitive level_0)
- [x] 1.5 Add error for inconsistent traversal types (different non-empty FHIR type)
- [x] 1.6 Route same-type complex Extension to variantTransformTree with errorOnDepthExhaustion=false
- [x] 1.7 Route same-type complex non-Extension to variantTransformTree with errorOnDepthExhaustion=true
- [x] 1.8 Remove the isPrimitive shortcut that bypasses recursion detection

## 2. Error messages

- [x] 2.1 Define distinct error message for static primitive self-reference detection
- [x] 2.2 Define distinct error message for inconsistent traversal type detection
- [x] 2.3 Verify existing "Infinite recursive traversal detected." message in UnresolvedTransformTree is preserved

## 3. Tests

- [x] 3.1 Add test for gender.repeatAll($this) raising primitive self-reference error
- [x] 3.2 Add test for gender.repeatAll('someValue') raising primitive self-reference error
- [x] 3.3 Add test for gender.repeatAll(length()) raising primitive self-reference error
- [x] 3.4 Verify existing test for name.repeatAll(first()) still raises analysis-time error
- [x] 3.5 Verify existing test for repeatAll($this) on complex type still raises analysis-time error
- [x] 3.6 Verify existing tests for Patient.repeatAll(name), Patient.repeatAll(gender), Patient.repeatAll(maritalStatus) still pass (now via level_0 shortcut)
- [x] 3.7 Verify existing Questionnaire.repeatAll(item) tests still pass (variantTransformTree path)
- [x] 3.8 Verify existing extension traversal tests still pass (soft stop path)
