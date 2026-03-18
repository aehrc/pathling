## 1. Extend ProjectionContext with row index

- [x] 1.1 Add a `rowIndex` field (type `Column`) to `ProjectionContext`, defaulting to `lit(0)`
- [x] 1.2 Add a `withRowIndex(Column)` method to create a new context with a different row index
- [x] 1.3 Update `ProjectionContext` to inject `rowIndex` as a `%rowIndex` supplied variable into the evaluator's variable map when evaluating expressions

## 2. Add indexed transform support

- [x] 2.1 Add an indexed `transform` method to `ColumnRepresentation` that uses Spark's `transform(array, (element, index) -> ...)` variant, returning both the transformed column and making the index available to the caller
- [x] 2.2 Add an `evaluateElementWiseWithIndex` method (or modify the existing flow) in `UnnestingSelection` that uses the indexed transform and passes the index column into the projection context via `withRowIndex`

## 3. Wire up UnnestingSelection

- [x] 3.1 Modify `UnnestingSelection.evaluate()` to use the indexed transform, creating a per-element `ProjectionContext` that carries the current index as `%rowIndex`
- [x] 3.2 Ensure nested `UnnestingSelection` levels shadow the outer `%rowIndex` with their own index value

## 4. Handle forEachOrNull empty collection case

- [x] 4.1 Verify that when `forEachOrNull` produces a null row for an empty collection, `%rowIndex` resolves to `0` (the default from `ProjectionContext`)

## 5. Tests

- [x] 5.1 Write a ViewDefinition integration test: `forEach` with `%rowIndex` column producing correct 0-based indices
- [x] 5.2 Write a ViewDefinition integration test: `forEachOrNull` with non-empty collection producing correct indices
- [x] 5.3 Write a ViewDefinition integration test: `forEachOrNull` with empty collection producing null `%rowIndex`
- [x] 5.4 Write a ViewDefinition integration test: top-level `%rowIndex` (no forEach) resolves to `0`
- [x] 5.5 Write a ViewDefinition integration test: nested `forEach` with independent `%rowIndex` values at each level
- [x] 5.6 Write a ViewDefinition integration test: `%rowIndex` used in arithmetic expression (`%rowIndex + 1`)
- [x] 5.7 Verify existing ViewDefinition tests still pass (no regressions)
