## 1. Cherry-pick spike implementation

- [x] 1.1 Cherry-pick commit `093d39c645` from `spike/repeat_row_index` onto `issue/2560` — this brings in `RowIndexCounter`, `RowCounter`/`ResetCounter` expressions, `ValueFunctions` methods, `RepeatSelection` wiring, encoder unit tests, and initial repeat `%rowIndex` view tests

## 2. Move and extend ViewDefinition test cases (rowindex.json)

- [x] 2.1 Move the 3 repeat `%rowIndex` tests from `repeat.json` to `rowindex.json` (remove from `repeat.json`)
- [x] 2.2 Add test resource with recursively nested extensions (linear chain) to `rowindex.json` resources — reuse or adapt the extension structure from `repeat.json`
- [x] 2.3 Add test resource with branching extensions (root extension with 2 children, first child has 1 grandchild) to `rowindex.json` resources
- [x] 2.4 Add test: repeat with `%rowIndex` — branching tree, breadth-first indices (uses branching resource from 2.3)
- [x] 2.5 Add test: repeat with `%rowIndex` across multiple resources — verify counter resets to 0 per resource
- [x] 2.6 Add test: repeat nested inside repeat — independent `%rowIndex` scopes

## 3. Verification

- [x] 3.1 Run encoder unit tests (`ExpressionsBothModesTest` subclasses) in both interpreted and codegen modes
- [x] 3.2 Run ViewDefinition test suite (`ViewDefinitionTest`) to verify all `rowindex.json` tests pass
- [x] 3.3 Run existing `repeat.json` tests to verify no regressions
