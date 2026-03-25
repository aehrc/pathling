## Why

FHIRPath specifies a reflection capability that allows expressions to access
basic type information of values via the `type()` function. This is part of the
STU section of the FHIRPath specification and is required by the fhirpath.js
reference test suite (testType1–testType23). Implementing this brings Pathling
closer to full FHIRPath compliance and unblocks use cases that rely on runtime
type introspection.

## What Changes

- Add the `type()` function to the FHIRPath engine, returning `TypeInfo`
  structures with `namespace`, `name`, and `baseType` string fields.
- Support navigation into TypeInfo results (e.g., `X.type().name`).
- Distinguish System types (literals) from FHIR types (path-navigated elements)
  based on the definition type attached to the collection.
- Handle Quantity and Coding correctly — these exist in both System and FHIR
  namespaces, distinguished by whether the collection carries a literal
  definition or a FHIR model definition.
- Use simplified `baseType` mapping: `System.Any` for System types,
  `FHIR.Element` for FHIR elements, `FHIR.Resource` for resources.

## Capabilities

### New Capabilities

- `fhirpath-type-function`: The `type()` function returning TypeInfo structures
  with `namespace`, `name`, and `baseType` fields, supporting navigation into
  these fields.

### Modified Capabilities

(none)

## Impact

- `fhirpath` module: new function provider, new TypeInfo definition and
  collection construction, updates to `Collection` for factory support.
- Reference test suite: existing `testType` exclusions can be removed once
  `type()` is implemented.
- No breaking changes to the public API.
