## Context

The FHIRPath parser resolves binary operators in `Visitor.visitBinaryOperator()`
through a two-stage lookup:

1. `BINARY_OPERATORS` map (built from `@FhirPathOperator`-annotated methods in
   `CollectionOperations`) — contains `in` and `contains`.
2. `BinaryOperatorType.fromSymbol()` enum lookup — contains all arithmetic,
   comparison, equality, and boolean operators.

The union operator `|` is not registered in either location.
`UnionOperator` exists with correct logic but is unreachable.

## Goals / Non-goals

**Goals:**

- Make `|` resolve to `UnionOperator` during FHIRPath parsing.
- Ensure the existing `CombiningOperatorsDslTest` tests execute and pass.

**Non-goals:**

- Changing the `UnionOperator` implementation itself.
- Adding new test cases beyond what already exists.

## Decisions

### Register `|` in the `BinaryOperatorType` enum

Add `UNION("|", new UnionOperator())` to the enum. This is consistent with how
all other non-collection operators (arithmetic, comparison, equality, boolean)
are registered.

**Alternative considered:** Register via `CollectionOperations` with a
`@FhirPathOperator(name = "|")` annotated method. Rejected because
`CollectionOperations` uses `MethodDefinedOperator` (method-reflection-based
dispatch), while `UnionOperator` is a class-based operator extending
`SameTypeBinaryOperator`. Mixing the two patterns would be inconsistent.

## Risks / Trade-offs

- **Low risk.** The `UnionOperator` is already tested (tests exist, just
  skipped). Wiring it in should cause those tests to pass without any other
  changes.
- The test framework's silent skipping of `UnsupportedFhirPathFeatureError`
  masked this gap. No change to that framework behaviour is proposed here, but
  it is worth noting as a broader concern.
