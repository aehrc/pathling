## Why

The [SQL on FHIR ViewDefinition spec](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html#rowindex) defines a `%rowIndex` environment variable that provides the 0-based index of the current element within the collection being iterated by `forEach` or `forEachOrNull`. Pathling's ViewDefinition support does not yet implement this variable, preventing users from preserving element ordering, disambiguating repeating elements, and constructing surrogate keys in flattened output.

## What Changes

- Add a new `%rowIndex` environment variable to the FHIRPath evaluation context.
- `%rowIndex` resolves to the 0-based index of the current element within the collection being iterated by `forEach` or `forEachOrNull`.
- At the top level (no iteration), `%rowIndex` evaluates to `0`.
- Each nesting level of `forEach`/`forEachOrNull` maintains an independent `%rowIndex` value.
- Support for `%rowIndex` within `repeat` is out of scope for this change.
- The variable is available to all FHIRPath expressions evaluated within the iteration scope, including nested `select` clauses.

## Capabilities

### New Capabilities

- `row-index-variable`: Support for the `%rowIndex` environment variable within ViewDefinition `forEach` and `forEachOrNull` iterations, providing a 0-based element index.

### Modified Capabilities

_(none)_

## Impact

- **fhirpath module**: Environment variable resolution chain needs a new resolver or mechanism to supply `%rowIndex` values that change per-element during iteration.
- **projection module**: `UnnestingSelection` (forEach/forEachOrNull) needs to track the current element index and inject it into the evaluation context.
- **views module**: `FhirViewExecutor` may need minor changes to initialise `%rowIndex` at the top level (value `0`).
- **Public API**: No breaking changes. `%rowIndex` is a new environment variable that was previously unsupported; existing ViewDefinitions and FHIRPath expressions are unaffected.
