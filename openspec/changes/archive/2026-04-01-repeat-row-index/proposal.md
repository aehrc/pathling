## Why

The `%rowIndex` environment variable is currently implemented for `forEach` and `forEachOrNull` ViewDefinition directives but not for `repeat`. The `repeat` directive flattens recursive structures (e.g., nested extensions, Questionnaire items) into rows, and users need `%rowIndex` to preserve ordering, disambiguate elements, and construct surrogate keys — the same use cases that motivated `%rowIndex` for `forEach`. This completes the `%rowIndex` implementation across all iteration directives as defined in the SQL on FHIR ViewDefinition spec.

## What Changes

- `%rowIndex` resolves to a 0-based global traversal-order index within `repeat` iterations, treating the entire flattened recursive tree as the collection being iterated.
- The counter resets to 0 for each resource row.
- Each `repeat` directive scopes its own `%rowIndex`, independent of enclosing or nested `forEach`/`forEachOrNull`/`repeat` directives.
- A stateful counter mechanism (`RowIndexCounter`) is introduced at the Spark expression level to track element positions across tree depth levels and traversal branches.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `row-index-variable`: Add requirements for `%rowIndex` within `repeat` directives, including global traversal-order semantics, per-resource reset, and scoping rules for nested `repeat`.

## Impact

- `encoders` module: New Spark expressions (`RowCounter`, `ResetCounter`) and supporting `RowIndexCounter` class.
- `fhirpath` module: `RepeatSelection` injects the row index counter into `ProjectionContext` and wraps output with counter reset.
- ViewDefinition test suite: New `%rowIndex` + `repeat` test cases added to `rowindex.json` (alongside existing forEach/forEachOrNull tests), with additional test resource data for recursively nested extensions.
- Encoder unit tests: New tests in `ExpressionsBothModesTest` for counter behaviour with `transform`, `transformTree`, and arithmetic composition.
