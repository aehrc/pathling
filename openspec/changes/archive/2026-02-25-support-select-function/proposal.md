## Why

The FHIRPath `select` function is a core filtering and projection function defined in the [FHIRPath specification](https://hl7.org/fhirpath/#filtering-and-projection). It evaluates a projection expression for each item in the input collection and flattens the results. Pathling currently supports the closely related `where` function but lacks `select`, which limits the expressiveness of FHIRPath queries that need to project or transform collection elements.

## What Changes

- Add `select(projection: expression) : collection` function to the FHIRPath engine
- The function evaluates the projection expression for each item in the input collection
- Results are flattened: if evaluation produces multiple items, all are added to the output; if evaluation produces empty, nothing is added
- Empty input collections produce empty output

## Capabilities

### New Capabilities

- `fhirpath-select`: FHIRPath `select` function for projecting and transforming collection elements

### Modified Capabilities

(none)

## Impact

- **Code**: `fhirpath` module - `FilteringAndProjectionFunctions.java`, `Collection.java` (may need a new projection/flatMap method)
- **Tests**: New DSL tests for the `select` function, YAML reference test compatibility
- **APIs**: No public API changes - `select` is available through FHIRPath expression evaluation
- **Dependencies**: None
