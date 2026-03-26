## Why

Testing `type()` function results in DSL tests is verbose — each TypeInfo assertion requires three separate field checks (namespace, name, baseType). The DSL harness supports direct comparison for Quantity and Coding via typed literals, but has no equivalent for TypeInfo. Adding a string representation enables single-assertion TypeInfo comparison, making tests more concise and readable.

## What Changes

- Add a `TypeInfoExpectation` marker class in the DSL test package with a `toTypeInfo("System.Integer(System.Any)")` factory method that parses the `Namespace.Name(BaseType)` string format.
- Add a TypeInfo comparison branch in `DefaultYamlTestExecutor` that detects `TypeInfoExpectation` instances and compares against TypeInfo struct Rows field-by-field.
- Refactor existing `TypeFunctionsDslTest` assertions to use `toTypeInfo()` where three-field checks can be collapsed into a single comparison.

## Capabilities

### New Capabilities

- `typeinfo-dsl-comparison`: DSL testing support for direct TypeInfo equality comparison using a string representation format.

### Modified Capabilities

_(none — this is a test infrastructure enhancement, no spec-level behaviour changes)_

## Impact

- **Test infrastructure**: `DefaultYamlTestExecutor` gains a new comparison branch for TypeInfo structs.
- **Test classes**: `TypeFunctionsDslTest` assertions simplified.
- **No production code changes**: This is purely test-side.
