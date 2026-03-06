## ADDED Requirements

### Requirement: repeat function recursively traverses and deduplicates elements

The FHIRPath engine SHALL support the `repeat(projection: expression) : collection` function. The function SHALL evaluate the projection expression recursively, collecting all results, then deduplicate the output collection using equality semantics.

The function SHALL delegate recursive traversal to `repeatAll()`, then apply equality-based deduplication to the result.

#### Scenario: Single-level traversal with no duplicates

- **WHEN** `repeat(item)` is called on a Questionnaire with a single level of items
- **THEN** the result SHALL contain all items from that level (identical to `repeatAll`)

#### Scenario: Multi-level recursive traversal

- **WHEN** `repeat(item)` is called on a Questionnaire with nested items
- **THEN** the result SHALL contain items from all nesting levels

#### Scenario: Duplicate primitive values are removed

- **WHEN** `repeat($this)` is called on a collection containing duplicate primitive values (e.g., `(1 year).combine(12 months).repeat($this)`)
- **THEN** the result SHALL contain only distinct values as determined by FHIRPath equality

#### Scenario: Quantity equality used for deduplication

- **WHEN** `repeat($this)` is called on a collection of quantities where values are equal under unit conversion (e.g., 3 minutes and 180 seconds)
- **THEN** only one representative value SHALL appear in the result

#### Scenario: String deduplication

- **WHEN** `repeat` produces duplicate string values across recursion levels
- **THEN** the result SHALL contain each distinct string value only once

### Requirement: repeat function deduplicates only Equatable types

The function SHALL apply deduplication only when the result collection implements the `Equatable` interface. For non-Equatable types (complex backbone elements), the function SHALL return the `repeatAll()` result without deduplication.

#### Scenario: Complex type results not deduplicated

- **WHEN** `repeat(item)` is called on a Questionnaire with nested items
- **THEN** no deduplication SHALL be attempted on the complex item elements (result is identical to `repeatAll`)

#### Scenario: Equatable type results deduplicated

- **WHEN** `repeat` produces a collection of an Equatable type (String, Integer, Quantity, Coding, etc.)
- **THEN** the result SHALL be deduplicated using the collection's comparator

### Requirement: repeat function uses collection comparator for equality

When deduplicating Equatable results, the function SHALL use the collection's `getComparator()` method to obtain the appropriate `ColumnEquality` instance. If the comparator uses default SQL equality, the function SHALL use `array_distinct`. Otherwise, the function SHALL use `arrayDistinctWithEquality` with the comparator's `equalsTo` method.

#### Scenario: Default SQL equality types

- **WHEN** the result collection uses default SQL equality (e.g., String, Integer, Boolean)
- **THEN** deduplication SHALL use Spark's `array_distinct`

#### Scenario: Custom equality types

- **WHEN** the result collection uses custom equality (e.g., Quantity with unit conversion)
- **THEN** deduplication SHALL use `arrayDistinctWithEquality` with the collection's comparator

### Requirement: repeat function handles self-referential primitive traversal

Unlike `repeatAll()` which raises an error for self-referential primitive traversal, `repeat()` SHALL treat this as a valid case. When the projection produces a self-referential primitive type, `repeat()` SHALL return the level_0 result (from `repeatAll` with primitive self-ref allowed) and then deduplicate it. This is valid because deduplication guarantees termination.

#### Scenario: Constant projection deduplicates to single value

- **WHEN** `(1 | 2).repeat('item')` is evaluated
- **THEN** the result SHALL be `['item']` (projection produces `'item'` for each input, dedup collapses to one)

#### Scenario: Identity projection deduplicates

- **WHEN** `Functions.coll1.colltrue.repeat(true)` is evaluated
- **THEN** the result SHALL be `[true]`

#### Scenario: Quantity identity with unit conversion

- **WHEN** `(1 year).combine(12 months).repeat($this)` is evaluated
- **THEN** the result SHALL be `[1 year]` (1 year = 12 months under FHIRPath equality)

### Requirement: repeat function inherits repeatAll behavior

The `repeat` function SHALL inherit all recursive traversal behavior from `repeatAll`, including:

- Static type analysis gate (level_0/level_1 classification).
- Same-type recursion depth limiting.
- Extension traversal with soft stop.
- Error on inconsistent traversal types.
- Empty input produces empty output.
- Result supports chained FHIRPath operations.

#### Scenario: Empty input

- **WHEN** `repeat` is called on an empty collection
- **THEN** the result SHALL be empty

#### Scenario: Chained operations

- **WHEN** `Questionnaire.repeat(item).linkId` is evaluated
- **THEN** the result SHALL contain linkId values from all recursively collected items

### Requirement: repeat function is available as a FHIRPath function

The `repeat` function SHALL be registered and invocable using standard FHIRPath function call syntax (e.g., `collection.repeat(expression)`).

#### Scenario: Function invocation syntax

- **WHEN** a FHIRPath expression uses `repeat` with dot notation (e.g., `Questionnaire.repeat(item)`)
- **THEN** the function SHALL be resolved and executed correctly
