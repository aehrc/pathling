## ADDED Requirements

### Requirement: FHIRPath union function

The FHIRPath engine SHALL implement the `union(other : collection) : collection`
function as defined in the FHIRPath specification §6.5 Combining. The function
SHALL merge the input collection with the `other` collection into a single
collection, eliminating any duplicate values using FHIRPath equality semantics
(i.e. the `=` operator). The result SHALL have no guaranteed order.

The function SHALL be observationally equivalent to the `|` operator: for any
collections `x` and `y` where `x | y` produces a result, `x.union(y)` SHALL
produce a result that compares equal under FHIRPath set-equality.

The function SHALL NOT introduce an iteration context. The `other` argument
SHALL be evaluated against the same context that applies to the function's
input, so that inside constructs such as `name.select(use.union(given))` the
`given` argument is evaluated against the current `name` element, not against
the root.

#### Scenario: Union eliminates duplicates across two non-empty collections

- **WHEN** the expression `(1 | 1 | 2 | 3).union(2 | 3)` is evaluated
- **THEN** the result is the collection `{1, 2, 3}` (order-independent)

#### Scenario: Union with an empty argument returns the deduplicated input

- **WHEN** the expression `(1 | 1 | 2 | 3).union({})` is evaluated
- **THEN** the result is the collection `{1, 2, 3}` (order-independent)

#### Scenario: Union with an empty input returns the deduplicated argument

- **WHEN** the expression `{}.union(1 | 2 | 2)` is evaluated
- **THEN** the result is the collection `{1, 2}` (order-independent)

#### Scenario: Union of two empty collections is empty

- **WHEN** the expression `{}.union({})` is evaluated
- **THEN** the result is the empty collection

#### Scenario: Union promotes Integer to Decimal

- **WHEN** the expression `(1 | 2).union(2.0 | 3.0)` is evaluated
- **THEN** the result is the collection `{1.0, 2.0, 3.0}` (order-independent)

#### Scenario: Union of incompatible polymorphic types raises an error

- **WHEN** the expression `(1).union(true)` is evaluated
- **THEN** the engine raises an invalid user input error describing
  incompatible operand types, matching the behaviour of the `|` operator

#### Scenario: Union is equivalent to the pipe operator

- **WHEN** the expressions `x.union(y)` and `x | y` are evaluated for the
  same `x` and `y` across Boolean, Integer, Decimal, String, Date, DateTime,
  Time, Quantity, and Coding inputs
- **THEN** the two results are equal under FHIRPath set-equality for every
  input pair

#### Scenario: Union uses the surrounding iteration context inside select

- **WHEN** the expression `Patient.name.select(use.union(given))` is
  evaluated against a Patient resource with multiple `name` elements
- **THEN** for each `name` element the result includes the `use` and
  `given` values of that specific `name`, and the overall result equals
  that of `Patient.name.select(use | given)`

#### Scenario: Union of Quantity collections uses Quantity equality

- **WHEN** the expression `(1 'mg').union(1000 'ug')` is evaluated
- **THEN** the two values are recognised as equal under Quantity equality
  and the result contains a single value

#### Scenario: Union of Coding collections uses Coding equality

- **WHEN** the expression
  `Coding { system: 'http://x', code: 'a' }.union(Coding { system: 'http://x', code: 'a' })`
  is evaluated
- **THEN** the result contains a single Coding value

#### Scenario: Union with missing argument is rejected at parse time

- **WHEN** the expression `(1 | 2).union()` is parsed
- **THEN** the parser raises an invalid user input error indicating that
  `union` requires exactly one argument

#### Scenario: Union with too many arguments is rejected at parse time

- **WHEN** the expression `(1 | 2).union(3, 4)` is parsed
- **THEN** the parser raises an invalid user input error indicating that
  `union` requires exactly one argument
