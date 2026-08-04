---
name: fhirpath-test-designer
description: >
  Design and generate comprehensive FHIRPath test suites using input domain partitioning and
  Pathling's DSL test framework. Use this skill whenever the user asks to write tests for a
  FHIRPath feature (function, operator, type system behavior, traversal pattern, etc.), review
  existing test coverage, identify missing test cases, or discuss what dimensions a feature needs
  testing across. Trigger on phrases like "write tests for", "test coverage for", "what tests do we
  need for", "review tests for", or any mention of testing a specific FHIRPath feature. Also
  trigger when the user mentions testing dimensions like singular/plural, empty propagation,
  cardinality, or HAPI resources in the context of FHIRPath tests.
---

# FHIRPath Test Designer

You design and generate FHIRPath test suites by combining specification research, input domain
partitioning, and Pathling's fluent DSL. Your output is a test matrix reviewed by the user,
followed by generated test code.

The DSL reference below is authoritative — it was derived from
`fhirpath/src/test/java/au/csiro/pathling/test/dsl/`, and
`DslApiContractTest` in that package exercises every construct documented here. If a method you want
does not appear below, read the package rather than assuming it exists.

`references/DSL_Testing_Strategy.md` sets out the reasoning behind the partitioning approach — read
it when deciding whether a dimension is worth testing, or when justifying a matrix to a reviewer.

## Workflow

Three phases. Present findings to the user between phases.

### Phase 1: Spec research

Use the `fhirpath-spec` skill for all specification lookups. Gather:

- Signature and description
- Input/output types and collection behaviour
- All spec examples — these become mandatory test cases
- Edge cases the spec calls out (empty propagation, boundary conditions, error conditions)
- FHIR-specific considerations (choice types, primitive wrappers, extensions)
- Ambiguities where the spec is unclear or reference implementations diverge

Flag ambiguities for resolution before Phase 2. Expected results come from the spec, never from
running the implementation.

### Phase 2: Test matrix design

Apply input domain partitioning. The driving question:

> What inputs can this function receive, and what does the spec say should happen for each?

| Dimension | Partitions | When relevant |
|---|---|---|
| **Core semantics** | Spec examples, basic behaviour | Always |
| **Emptiness** | `{}` literal, typed-empty field (`stringEmpty`), computed empty (`where(false)`) | Always, for anything accepting collections |
| **Cardinality** | Singular value vs array | Whenever the function reads model fields — see below |
| **Element type** | Primitive, complex/backbone, choice type | When the function accepts general `Element` input |
| **Nesting** | Flat, nested, deeply nested | When the function involves traversal |
| **FHIR encoding** | Real resource via `withResource` | When behaviour depends on genuine FHIR encoding — see below |

**Cardinality deserves special attention.** In the Spark layer, singular elements are scalar
columns and non-singular elements are array columns. A function correct on a scalar column can
fail on an array column and vice versa. Include at least one singular field (`.string("s", "v")`)
and one array field (`.stringArray("a", "x", "y")`) whenever the function reads model fields.

**When to require a real FHIR resource (`withResource`)** — the map-based builder produces a
synthetic resource whose type is always `Test`, so it cannot express:

- Real resource types and resource-prefixed paths (`Patient.name.given`)
- Choice types (`value[x]`) as HAPI actually serialises them
- Reference resolution (`resolve()`) and contained resources
- Extensions and FHIR primitive-wrapper behaviour (`getValue()`, `hasValue()`)
- Anything where the Spark schema from real FHIR JSON differs from the map schema

Otherwise prefer `withSubject` — it is faster to read and write.

Present a matrix and wait for review:

```markdown
## Test matrix for `functionName()`

| # | Test case | Dimension | Expression | Expected | Subject |
|---|-----------|-----------|------------|----------|---------|
| 1 | Spec example | Core semantics | `'abc'.fn()` | `'ABC'` | literal |
| 2 | Empty literal | Emptiness | `{}.fn()` | `{}` | literal |
| 3 | Typed-empty field | Emptiness | `emptyString.fn()` | `{}` | subject |
| 4 | Singular field | Cardinality | `singleString.fn()` | `'V'` | subject |
| 5 | Array field | Cardinality | `stringArray.fn()` | `['X','Y']` | subject |
| 6 | Choice type | Element type | `Observation.value.ofType(string).fn()` | ... | resource |
```

Rules:
- Vary one dimension at a time; hold the others constant
- Combination tests only where the spec implies dimensions interact
- Do not combinatorially explode independent dimensions
- Flag any case whose expected result is uncertain

### Phase 3: Code generation

## DSL reference

**Location and naming.** Tests live in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/dsl/`,
named `<Capability>DslTest.java` — by capability (`StringFunctionsDslTest`), never by issue
number. Extend `FhirPathDslTestBase`. Every file needs the CSIRO Apache-2.0 copyright header;
copy it from a sibling test.

**One `@FhirPathTest` method per function**, using `group()` to organise dimensions within it.

```java
package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;

public class StringFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testUpper() {
    return builder()
        .withSubject(
            sb ->
                sb.stringEmpty("emptyString")
                    .string("singleString", "test")
                    .stringArray("stringArray", "one", "two"))
        .group("upper() spec examples")
        .testEquals("ABCDEFG", "'abcdefg'.upper()", "Lowercase input is uppercased")
        .group("upper() empty propagation")
        .testEmpty("{}.upper()", "Empty literal returns empty")
        .testEmpty("emptyString.upper()", "Typed-empty field returns empty")
        .group("upper() cardinality")
        .testEquals("TEST", "singleString.upper()", "Singular field")
        .testEquals(List.of("ONE", "TWO"), "stringArray.upper()", "Array field")
        .build();
  }
}
```

### Subject methods

| Method | Notes |
|---|---|
| `withSubject(sb -> ...)` | Map-based synthetic resource. Fields are accessed **bare** — `stringArray.first()`, no resource-type prefix |
| `withSubject(Map<String, Object>)` | Pre-built model map |
| `withResource(IBaseResource)` | Real HAPI resource. Expressions are normally **resource-prefixed** — `Patient.name.given` |

### Assertions — a description is mandatory on every one

| Method | Signature |
|---|---|
| `testEquals` | `(Object expected, String expression, String description)` |
| `testTrue` | `(String expression, String description)` |
| `testFalse` | `(String expression, String description)` |
| `testEmpty` | `(String expression, String description)` |
| `testError` | `(String expression, String description)` — any error |
| `testError` | `(String errorMessage, String expression, String description)` — specific message |
| `group` | `(String groupName)` — prefixes subsequent descriptions as `group - description` |
| `test` | `(String description, tc -> tc.expression(...).expectResult(...))` — low-level escape hatch |
| `build` | Terminates the chain, returns `Stream<DynamicTest>` |

There are **no** overloads without a description. `testEquals(expected, expression)` does not
compile.

### Model builder methods (`FhirPathModelBuilder`)

Each type has a value form, an empty form, and an array form:

| Type | Value | Empty | Array |
|---|---|---|---|
| String | `string(n, v)` | `stringEmpty(n)` | `stringArray(n, ...)` |
| Integer | `integer(n, v)` | `integerEmpty(n)` | `integerArray(n, ...)` |
| Decimal | `decimal(n, v)` | `decimalEmpty(n)` | `decimalArray(n, ...)` |
| Boolean | `bool(n, v)` | `boolEmpty(n)` | `boolArray(n, ...)` |
| Date | `date(n, v)` | `dateEmpty(n)` | `dateArray(n, ...)` |
| DateTime | `dateTime(n, v)` | `dateTimeEmpty(n)` | `dateTimeArray(n, ...)` |
| Time | `time(n, v)` | `timeEmpty(n)` | `timeArray(n, ...)` |
| Coding | `coding(n, v)` | `codingEmpty(n)` | `codingArray(n, ...)` |
| Quantity | `quantity(n, v)` | `quantityEmpty(n)` | `quantityArray(n, ...)` |
| Complex | `element(n, b -> ...)` | `elementEmpty(n)` | `elementArray(n, b1, b2, ...)` |

Date, DateTime, Time, Coding and Quantity take **FHIRPath literal strings** —
`date("d", "2024-01-15")`, `quantity("q", "10.5 'mg'")`, `coding("c", "http://loinc.org|1234-5")`.

Also available: `fhirType(FHIRDefinedType)` to annotate the FHIR type of the enclosing element,
`choice(name)` to mark a choice element, and `fhirReference()` for a Reference with empty
`reference` and `type` fields.

For `type()` assertions, import `au.csiro.pathling.test.dsl.TypeInfoExpectation.toTypeInfo` and
compare against `toTypeInfo("System.Integer(System.Any)")` or `toTypeInfo("FHIR.Patient(FHIR.Resource)")`.

## Gotchas

These are the ways generated tests actually break:

1. **One subject per method.** `withSubject` and `withResource` set builder state consumed at
   `build()` — they are **not** scoped to a `group()`. Calling either twice in one method means the
   last call applies to *every* test case in that method. If two tests need different subjects,
   they need different `@FhirPathTest` methods.
2. **`withSubject` and `withResource` are mutually exclusive** — each clears the other.
3. **The map-based subject's resource type is always `Test`.** Resource-prefixed expressions like
   `Patient.name` will not resolve. Use `withResource` for those.
4. **`testError` takes a message string, not an exception class.** `testError(SomeException.class, ...)`
   does not compile.
5. **There is no `context(...)` argument.** The DSL hardcodes the test case's context to null. If a
   test genuinely needs a context expression, write it as a YAML case under
   `fhirpath/src/test/resources/fhirpath-ptl/` instead.
6. **A single-element `List.of(x)` expectation is unwrapped to `x`** before comparison, so both
   forms are equivalent for one-item results. Use the bare value for readability.
7. **Typed-empty vs absent are different.** `stringEmpty("f")` creates field `f` with a typed null;
   omitting the field entirely means the path does not resolve. Test the dimension the spec cares
   about.
8. **`group()` persists** until the next `group()` call.

## Running the tests

```bash
mvn test -pl fhirpath -Dtest=StringFunctionsDslTest              # one class
mvn test -pl fhirpath -Dtest='StringFunctionsDslTest#testUpper'  # one method
mvn spotless:apply -pl fhirpath                                  # format
```

## Reviewing existing tests

1. Run Phase 1 for the function under test.
2. Build the matrix as if writing from scratch.
3. Compare against the existing tests and report:
   - **Missing dimensions** — matrix rows with no corresponding test
   - **Incorrect expectations** — assertions that contradict the spec
   - **Redundant tests** — several tests covering one dimension without adding value
   - **Missing FHIR-encoding coverage** — features needing `withResource` that only use `withSubject`

## What not to test

- Unicode/emoji handling unless the spec defines it
- Large inputs or performance characteristics
- Multiple variations of an identical condition
- Exhaustive type combinations beyond what the spec defines
- Cross-cutting infrastructure (empty propagation at the column level, type encoding) already
  covered once at the infrastructure level — unless this function behaves unusually
