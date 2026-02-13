## 1. Tests

- [x] 1.1 Add `UriMatcher` unit tests to `ElementMatcherTest`: exact match, case mismatch, partial URI, different URI, URN UUID, null handling.
- [x] 1.2 Add `UriMatcher` `:below` unit tests: prefix match, non-matching prefix, exact value with below (matches), trailing slash distinction.
- [x] 1.3 Add `UriMatcher` `:above` unit tests: element is prefix of search value (matches), element is not a prefix (does not match), exact value with above (matches).
- [x] 1.4 Add `SearchParameterType.URI` modifier tests: `:not` accepted, `:below` accepted, `:above` accepted, unsupported modifiers (e.g., `:exact`, `:contains`) rejected with `InvalidModifierException`.
- [x] 1.5 Add integration test in `SearchColumnBuilderTest` for a URI-type search parameter resolving to `FHIRDefinedType.URI` (e.g., `instantiates-uri` on CarePlan) verifying end-to-end filter creation.
- [x] 1.6 Add integration test in `SearchColumnBuilderTest` for a URI-type search parameter resolving to `FHIRDefinedType.URL` (e.g., `url` on CapabilityStatement) verifying end-to-end filter creation.

## 2. Implementation

- [x] 2.1 Create `UriMatcher` class implementing `ElementMatcher` with three modes: exact (`element.equalTo(lit(searchValue))`), below (`element.startsWith(lit(searchValue))`), and above (`lit(searchValue).startsWith(element)`).
- [x] 2.2 Update `SearchParameterType.URI` to declare `FHIRDefinedType.URI`, `URL`, `CANONICAL`, `OID`, and `UUID` as allowed and override `createFilter()` with `:not`, `:below`, and `:above` modifier support.

## 3. Verification

- [x] 3.1 Run all search-related tests and confirm they pass.
