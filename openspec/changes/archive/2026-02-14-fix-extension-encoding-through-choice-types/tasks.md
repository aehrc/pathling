## 1. Test data

- [x] 1.1 Add `newObservationWithCodingExtension()` to `TestData.java` that
      creates an Observation with `valueCodeableConcept` containing a Coding with
      an extension (e.g., `ordinalValue`), plus a resource-level extension (to
      verify both are preserved)

## 2. Encoder tests (reproduce the bug)

- [x] 2.1 Add encoder test in `LightweightFhirEncodersTest.java` that
      encodes the new Observation, then decodes it and asserts the Coding's
      extension is present in the decoded resource
- [x] 2.2 Add encoder test that verifies the `_extension` map contains entries
      for both the resource-level extension and the Coding extension (two distinct
      keys)

## 3. FHIRPath tests (reproduce the bug)

- [x] 3.1 Add a FHIRPath test that evaluates
      `value.ofType(CodeableConcept).coding.extension('url').value.ofType(decimal)`
      on the new Observation and asserts the result is not null
- [x] 3.2 Add a FHIRPath test that evaluates `extension('url')` on the same
      resource to verify resource-level extensions still work alongside the fix

## 4. Fix

- [x] 4.1 In `SerializerBuilderProcessor.flattenExtensions`, replace
      `obj.children().asScala.map(p => obj.getProperty(p.getName.hashCode, p.getName, false))`
      with `obj.children().asScala.flatMap(p => p.getValues.asScala)` to bypass the
      hash mismatch for choice types

## 5. Verification

- [x] 5.1 Run the new encoder tests and confirm they pass
- [x] 5.2 Run the new FHIRPath tests and confirm they pass
- [x] 5.3 Run the full encoder test suite to check for regressions
- [x] 5.4 Run the full FHIRPath test suite to check for regressions
