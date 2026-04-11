## ADDED Requirements

### Requirement: FHIRPath combine function

The FHIRPath engine SHALL implement the `combine(other : collection) : collection`
function as defined in the FHIRPath specification §6.5 Combining. The function
SHALL merge the input collection with the `other` collection into a single
collection **without** eliminating duplicate values. The result SHALL have no
guaranteed order.

Combining an empty collection with a non-empty collection SHALL return the
non-empty collection. Combining two empty collections SHALL return an empty
collection.

The function SHALL apply the same type reconciliation as the `|` operator
(for example promoting Integer to Decimal, normalizing Decimal precision).
Where the operand types cannot be reconciled to a common FHIRPath type, the
engine SHALL raise an invalid user input error consistent with the `|`
operator's behaviour for incompatible polymorphic types.

The function SHALL NOT introduce an iteration context. The `other` argument
SHALL be evaluated against the same context that applies to the function's
input.

#### Scenario: Combine preserves duplicates from both sides

- **WHEN** the expression `(1 | 1).combine(1 | 2)` is evaluated
- **THEN** the result is a collection of four elements containing three
  `1` values and one `2` value (order-independent)

#### Scenario: Combine preserves duplicates within a single side

- **WHEN** the expression `(1 | 1 | 2 | 3).combine({})` is evaluated
- **THEN** the result is a collection of four elements `{1, 1, 2, 3}`
  (order-independent)

#### Scenario: Combine with an empty input returns the other collection unchanged

- **WHEN** the expression `{}.combine(1 | 1 | 2)` is evaluated
- **THEN** the result is a collection of three elements `{1, 1, 2}`
  (order-independent)

#### Scenario: Combine of two empty collections is empty

- **WHEN** the expression `{}.combine({})` is evaluated
- **THEN** the result is the empty collection

#### Scenario: Combine promotes Integer to Decimal

- **WHEN** the expression `(1).combine(2.0)` is evaluated
- **THEN** the result is a collection of two Decimal elements
  `{1.0, 2.0}` (order-independent)

#### Scenario: Combine does not deduplicate equal Quantity values

- **WHEN** the expression `(1 'mg').combine(1000 'ug')` is evaluated
- **THEN** the result is a collection containing both Quantity values,
  regardless of their equality under Quantity comparison semantics

#### Scenario: Combine does not deduplicate equal Coding values

- **WHEN** two collections containing identical Coding values are combined
- **THEN** the result contains both Coding values

#### Scenario: Combine of incompatible polymorphic types raises an error

- **WHEN** the expression `(1).combine(true)` is evaluated
- **THEN** the engine raises an invalid user input error describing
  incompatible operand types

#### Scenario: Combine uses the surrounding iteration context inside select

- **WHEN** the expression `Patient.name.select(use.combine(given))` is
  evaluated against a Patient resource with multiple `name` elements
- **THEN** for each `name` element the result includes the `use` and
  `given` values of that specific `name`, preserving duplicates across
  the iteration

#### Scenario: Combine with missing argument is rejected at parse time

- **WHEN** the expression `(1 | 2).combine()` is parsed
- **THEN** the parser raises an invalid user input error indicating that
  `combine` requires exactly one argument

#### Scenario: Combine with too many arguments is rejected at parse time

- **WHEN** the expression `(1 | 2).combine(3, 4)` is parsed
- **THEN** the parser raises an invalid user input error indicating that
  `combine` requires exactly one argument
