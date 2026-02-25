## 1. Core Implementation

- [x] 1.1 Add `select` function to `FilteringAndProjectionFunctions.java` using `CollectionTransform` parameter, following the `where` function pattern
- [x] 1.2 Add a `project` method (or equivalent) to `Collection.java` that applies a `CollectionTransform` expression and returns the projected result collection

## 2. Testing

- [x] 2.1 Write DSL tests for the `select` function covering: simple property projection, expression projection, flattening of multi-valued results, empty projection results, and empty input collections
- [x] 2.2 Run YAML reference tests (`YamlReferenceImplTest`) and add exclusions for any `select`-related tests that are not yet supported, with user approval
