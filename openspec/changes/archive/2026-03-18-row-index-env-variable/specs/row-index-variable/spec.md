## ADDED Requirements

### Requirement: %rowIndex resolves to element index within forEach

When a ViewDefinition `select` clause uses `forEach`, the `%rowIndex` environment variable SHALL resolve to the 0-based index of the current element within the collection produced by the `forEach` expression. The index reflects the element's position in the collection as evaluated by the FHIRPath expression, starting at 0 for the first element.

#### Scenario: Single forEach with multiple elements

- **WHEN** a ViewDefinition has `forEach: "Patient.name"` and the Patient has 3 names
- **THEN** `%rowIndex` SHALL be `0` for the first name, `1` for the second, and `2` for the third

#### Scenario: forEach with single element

- **WHEN** a ViewDefinition has `forEach: "Patient.name"` and the Patient has 1 name
- **THEN** `%rowIndex` SHALL be `0` for that name

#### Scenario: forEach with empty collection

- **WHEN** a ViewDefinition has `forEach: "Patient.name"` and the Patient has no names
- **THEN** no rows are produced (forEach produces no output for empty collections), so `%rowIndex` is not evaluated

### Requirement: %rowIndex resolves to element index within forEachOrNull

When a ViewDefinition `select` clause uses `forEachOrNull`, the `%rowIndex` environment variable SHALL resolve to the 0-based index of the current element within the collection produced by the `forEachOrNull` expression, following the same indexing rules as `forEach`.

#### Scenario: forEachOrNull with multiple elements

- **WHEN** a ViewDefinition has `forEachOrNull: "Patient.name"` and the Patient has 2 names
- **THEN** `%rowIndex` SHALL be `0` for the first name and `1` for the second

#### Scenario: forEachOrNull with empty collection

- **WHEN** a ViewDefinition has `forEachOrNull: "Patient.name"` and the Patient has no names
- **THEN** one row is produced with null values for all nested columns including `%rowIndex`

### Requirement: %rowIndex defaults to 0 at top level

When no `forEach` or `forEachOrNull` iteration is active (i.e. the expression is evaluated at the top level of a ViewDefinition select), `%rowIndex` SHALL evaluate to `0`.

#### Scenario: Top-level column referencing %rowIndex

- **WHEN** a ViewDefinition `select` has a column with expression `%rowIndex` and no `forEach` or `forEachOrNull` is active
- **THEN** the column value SHALL be `0` for every resource row

### Requirement: Nested iterations maintain independent %rowIndex values

Each nesting level of `forEach`/`forEachOrNull` SHALL maintain its own independent `%rowIndex`. An inner `forEach` resets `%rowIndex` to count within its own collection, and restoring the outer `%rowIndex` when the inner iteration completes.

#### Scenario: Nested forEach iterations

- **WHEN** a ViewDefinition has an outer `forEach: "Patient.name"` (Patient has 2 names) and an inner `forEach: "HumanName.given"` (first name has 2 givens, second name has 1 given)
- **THEN** for the first name: outer `%rowIndex` is `0`, inner `%rowIndex` is `0` and `1` for each given; for the second name: outer `%rowIndex` is `1`, inner `%rowIndex` is `0` for its single given

#### Scenario: Inner forEach does not affect outer %rowIndex

- **WHEN** a column expression references `%rowIndex` at the outer forEach level after an inner forEach has completed
- **THEN** the value SHALL reflect the outer iteration index, unaffected by the inner iteration

### Requirement: %rowIndex is available in nested select expressions

The `%rowIndex` variable SHALL be accessible from any FHIRPath expression evaluated within the scope of the current iteration, including columns within nested `select` clauses that do not themselves introduce a new `forEach`/`forEachOrNull`.

#### Scenario: Column in nested select without its own forEach

- **WHEN** a `forEach` iterates over `Patient.name` and a nested `select` (without its own `forEach`) contains a column with expression `%rowIndex`
- **THEN** the column SHALL resolve to the index from the enclosing `forEach`

### Requirement: %rowIndex is an integer type

The `%rowIndex` variable SHALL resolve to an integer value compatible with FHIRPath integer type, allowing arithmetic operations and comparisons.

#### Scenario: Arithmetic with %rowIndex

- **WHEN** a column expression is `%rowIndex + 1`
- **THEN** the result SHALL be the 1-based index of the current element

#### Scenario: Comparison with %rowIndex

- **WHEN** a `where` clause filters with `%rowIndex = 0`
- **THEN** only the first element of the iterated collection SHALL be included
