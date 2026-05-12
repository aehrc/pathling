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

### Requirement: %rowIndex resolves to global traversal-order index within repeat

When a ViewDefinition `select` clause uses `repeat`, the `%rowIndex` environment variable SHALL resolve to the 0-based index of the current element within the flattened collection produced by the recursive traversal. The index reflects the element's position in the complete flattened output (across all depth levels and traversal branches), not its position within a single depth level.

#### Scenario: Linear repeat with sequential indices

- **WHEN** a ViewDefinition has `repeat: ["extension"]` and the resource has a chain of 4 nested extensions (each containing one child extension)
- **THEN** `%rowIndex` SHALL be `0` for the first extension, `1` for its child, `2` for the grandchild, and `3` for the great-grandchild

#### Scenario: Branching repeat with breadth-first indices

- **WHEN** a ViewDefinition has `repeat: ["extension"]` and the resource has a root extension with 2 child extensions (the first child having 1 grandchild)
- **THEN** the root extension SHALL have `%rowIndex` `0`, its two children SHALL have `%rowIndex` `1` and `2` (in document order), and the grandchild SHALL have `%rowIndex` `3`

#### Scenario: Repeat with single element

- **WHEN** a ViewDefinition has `repeat: ["extension"]` and the resource has exactly 1 extension with no nested extensions
- **THEN** `%rowIndex` SHALL be `0` for that extension

#### Scenario: Repeat with empty collection

- **WHEN** a ViewDefinition has `repeat: ["extension"]` and the resource has no extensions
- **THEN** no rows are produced, so `%rowIndex` is not evaluated

### Requirement: %rowIndex resets to 0 for each resource row within repeat

The `%rowIndex` counter SHALL reset to 0 at the start of each resource row. The index sequence is scoped to a single resource's traversal, not global across the dataset.

#### Scenario: Counter resets across resources

- **WHEN** a ViewDefinition has `repeat: ["extension"]` and two resources each have nested extensions
- **THEN** the `%rowIndex` sequence SHALL start at `0` independently for each resource

### Requirement: repeat scopes its own %rowIndex independently from enclosing and nested directives

Each `repeat` directive SHALL maintain its own `%rowIndex` scope. A `forEach` or `forEachOrNull` nested inside a `repeat` SHALL have its own independent `%rowIndex`. Likewise, a `repeat` nested inside a `forEach` SHALL have its own independent `%rowIndex`.

#### Scenario: forEach nested inside repeat has independent %rowIndex

- **WHEN** a ViewDefinition has a `repeat: ["extension"]` with a nested `forEach: "extension"` inside it
- **THEN** the `repeat` level `%rowIndex` SHALL reflect the global traversal position, and the inner `forEach` `%rowIndex` SHALL reflect the 0-based index within that element's immediate children, independent of the outer repeat index

#### Scenario: repeat nested inside forEach has independent %rowIndex

- **WHEN** a ViewDefinition has `forEach: "name"` with a nested `repeat: ["extension"]` inside it
- **THEN** the outer `forEach` `%rowIndex` SHALL reflect the name index, and the inner `repeat` `%rowIndex` SHALL start at `0` for each name's extension traversal

#### Scenario: repeat nested inside repeat has independent %rowIndex

- **WHEN** a ViewDefinition has an outer `repeat: ["extension"]` with an inner `repeat: ["extension"]` nested inside it via a `select`
- **THEN** the outer `repeat` `%rowIndex` SHALL reflect the global traversal position in the outer flattened tree, and the inner `repeat` `%rowIndex` SHALL start at `0` independently for each element's nested extension traversal

### Requirement: %rowIndex supports arithmetic within repeat

The `%rowIndex` variable within `repeat` iterations SHALL resolve to an integer value compatible with FHIRPath integer type, allowing arithmetic operations.

#### Scenario: Arithmetic with %rowIndex in repeat

- **WHEN** a column expression within a `repeat` block is `%rowIndex + 1`
- **THEN** the result SHALL be the 1-based position of the element in the flattened traversal

### Requirement: Nested iterations maintain independent %rowIndex values

Each nesting level of `forEach`/`forEachOrNull`/`repeat` SHALL maintain its own independent `%rowIndex`. An inner iteration directive resets `%rowIndex` to count within its own collection, restoring the outer `%rowIndex` when the inner iteration completes.

#### Scenario: Nested forEach iterations

- **WHEN** a ViewDefinition has an outer `forEach: "Patient.name"` (Patient has 2 names) and an inner `forEach: "HumanName.given"` (first name has 2 givens, second name has 1 given)
- **THEN** for the first name: outer `%rowIndex` is `0`, inner `%rowIndex` is `0` and `1` for each given; for the second name: outer `%rowIndex` is `1`, inner `%rowIndex` is `0` for its single given

#### Scenario: Inner forEach does not affect outer %rowIndex

- **WHEN** a column expression references `%rowIndex` at the outer forEach level after an inner forEach has completed
- **THEN** the value SHALL reflect the outer iteration index, unaffected by the inner iteration

#### Scenario: Nested repeat and forEach maintain independent indices

- **WHEN** a ViewDefinition has `repeat: ["extension"]` containing a nested `forEach: "extension"`
- **THEN** each directive level SHALL maintain its own `%rowIndex`, with the inner `forEach` index being independent of the outer `repeat` index

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
