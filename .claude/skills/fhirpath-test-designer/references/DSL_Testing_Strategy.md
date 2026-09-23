# DSL Test Design: Input Domain Partitioning

## Paradigm

Tests for FHIRPath functions and operators are derived from the **FHIRPath
specification** and the **FHIR data model**, never from implementation
internals. The question driving test design is:

> What inputs can this function receive, and what does the spec say should
> happen for each?

This is **input domain partitioning**: systematically sampling from the
dimensions of the input space to ensure correct behavior across all valid
inputs.

## Why Not Implementation-Driven Testing?

Testing based on knowledge of implementation code paths creates tests that are:

- **Brittle** — they break or become meaningless when the implementation
  changes.
- **Incomplete** — they only cover paths you know about, not ones you missed.
- **Coupled** — they test how it works, not what it should do.

Cross-cutting concerns (empty propagation, type encoding, extension
preservation) are handled by shared infrastructure. They should be tested once
at the infrastructure level, not re-tested in every function. Function-specific
tests focus on what makes that function unique.

## Input Domain Dimensions

For any FHIRPath function, systematically consider these dimensions:

| Dimension | Partitions | Source |
|---|---|---|
| Emptiness | `{}` literal, typed-empty field, computed empty (e.g. `where(false)`), and — distinctly — an absent element, whose path does not resolve at all | FHIR + FHIRPath |
| Element type | Primitive, complex/backbone, choice type, Extension | FHIR type system |
| Cardinality | 0..1 (singular) vs 0..* (non-singular) | FHIR element definitions |
| Nesting | Flat, nested, recursive (if applicable) | FHIR resource structure |

Not every dimension is relevant for every function. The spec and the function's
signature determine which dimensions apply.

### Extension Type

The FHIR `Extension` type requires special consideration in test design.
Extensions are represented differently from other complex types: they are
accessed by URL rather than by element name, and their values are stored in
choice-typed `value[x]` elements. When a function can operate on Extension
elements, include at least one test case that exercises this access pattern to
ensure correct handling of the URL-based traversal and value extraction.

## How to Use

1. **Identify relevant dimensions.** Based on the function's specification,
   determine which input dimensions affect its behavior. A function that only
   accepts Boolean input does not need tests across all primitive types.

2. **One test per dimension.** For each relevant dimension, include at least one
   test that isolates that dimension — varying it while holding others constant.

3. **Combination tests only where the spec implies interaction.** For example,
   `repeat()` traverses recursively and may encounter mixed types along the
   way, so test recursive traversal over heterogeneous data. Do not
   combinatorially explode dimensions that are independent.

4. **One realistic integration test.** A "happy path" test using a real FHIR
   resource (`withResource`) that exercises a realistic usage of the function.

5. **Expected results come from the spec.** The expected result for each test
   case is determined by reading the specification, not by running the current
   implementation.

## Capturing Tests in OpenSpec Change Specs

When creating an OpenSpec change for a new FHIRPath function or operator,
include a test matrix in the spec artifact as part of the acceptance criteria:

```markdown
## Test Matrix

| Test case           | Dimension(s)          | Expression            | Expected |
|---------------------|-----------------------|-----------------------|----------|
| Basic usage         | Core semantics        | `items.fn()`          | [a,b,c]  |
| Empty literal       | Emptiness: literal    | `{}.fn()`             | {}       |
| Typed-empty field   | Emptiness: typed null | `emptyItems.fn()`     | {}       |
| Computed empty      | Emptiness: computed   | `items.where(false).fn()` | {}   |
| Singular primitive  | Cardinality: 0..1     | `fn(gender)`          | 'male'   |
| Non-singular complex| Cardinality: 0..*     | `fn(name)`            | [n1,n2]  |
| Choice type         | Element type: choice  | `fn(value)`           | ...      |
| Extension           | Element type: Extension | `fn(extension)` | ...      |
```

During verification (`opsx:verify`), check that each row in the test matrix
has a corresponding DSL test case.

## Relationship to DSL Test Infrastructure

The test builder supports all partitions through its API:

| Dimension | Builder support |
|---|---|
| `{}` literal | Used directly in FHIRPath expression: `{}.fn()` |
| Typed-empty primitive | `sb.stringEmpty("field")`, `sb.integerEmpty("field")`, etc. — the field is **present**, carrying a typed null |
| Empty complex element | `sb.elementEmpty("field")` — the field is **present**, carrying a null value. This is not an absent field |
| Absent element | No builder support: omit the field entirely. The path then does not resolve, which is a different condition from returning empty — see gotcha 7 in `SKILL.md` |
| Computed empty | Use filtering expressions: `field.where(false)` |
| Singular primitive | `sb.string("field", "value")` |
| Non-singular primitive | `sb.stringArray("field", "a", "b")` |
| Complex element | `sb.element("field", e -> e.string(...))` |
| Complex array | `sb.elementArray("field", e -> ..., e -> ...)` |
| Choice type | `sb.element("field", e -> e.choice("value").string(...))` |
| Extension | No map-based builder support — extensions are accessed by URL and serialised as HAPI does, which the synthetic subject cannot express. Use `withResource` with a real resource carrying the extension |
| Real FHIR resource | `builder().withResource(new Patient()...)` |
