## Why

TypeInfo-related knowledge (namespace constants, type name resolution, base type
rules, Spark struct construction, and the element definition) is scattered across
`TypeFunctions`, `Collection`, and duplicates concepts from `TypeSpecifier`. This
makes the reflection implementation harder to understand, extend, and test.

## What Changes

- Extract a `TypeInfo` value class in the `fhirpath` package alongside
  `TypeSpecifier`, consolidating all type reflection data and logic.
- Move `TYPE_INFO_DEFINITION` from `Collection` to `TypeInfo.DEFINITION`.
- Move `resolveTypeInfoStruct` and `buildTypeInfoStruct` logic from
  `TypeFunctions` into `TypeInfo` factory methods and an instance method.
- Simplify `TypeFunctions.type()` to delegate to `TypeInfo`.
- Reuse `TypeSpecifier.SYSTEM_NAMESPACE` / `FHIR_NAMESPACE` instead of raw
  string literals.

## Capabilities

### New Capabilities

### Modified Capabilities

- `fhirpath-type-function`: Implementation detail change only — no requirement
  changes. The `type()` function behaviour remains identical.

## Impact

- `fhirpath` module only.
- New file: `TypeInfo.java` in `au.csiro.pathling.fhirpath`.
- Modified files: `TypeFunctions.java`, `Collection.java`.
- No public API changes. No behavioural changes. All existing tests must
  continue to pass without modification.
