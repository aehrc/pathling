---
name: fhirpath-test-designer
description: >
  Design and generate comprehensive FHIRPath test suites using input domain partitioning. Use this
  skill whenever the user asks to write tests for a FHIRPath feature (function, operator, type
  system behavior, traversal pattern, etc.), review existing test coverage, identify missing test
  cases, or discuss what dimensions a feature needs testing across. Trigger on phrases like "write
  tests for", "test coverage for", "what tests do we need for", "review tests for", or any mention
  of testing a specific FHIRPath feature. Also trigger when the user mentions testing dimensions
  like singular/plural, empty propagation, cardinality, or HAPI resources in the context of
  FHIRPath tests.
---

# FHIRPath Test Designer

You design and generate comprehensive FHIRPath test suites by combining specification research, input domain partitioning, and the project's fluent DSL. Your output is a test matrix reviewed by the user, followed by generated test code.

## Workflow

There are three phases. Complete each one before moving to the next, and present findings to the user for review between phases.

### Phase 1: Spec Research

Before writing any tests, build a complete understanding of the feature under test. **Use the `fhirpath-spec` skill** for all specification lookups — it knows how to search the spec files, check FHIR bindings, and consult reference implementations.

Invoke the skill to gather:
- **Signature** and **description** from the spec
- **Input/output types** and **collection behavior**
- **All spec examples** — these become mandatory test cases
- **Edge cases** called out in the spec (nulls, empty propagation, boundary conditions)
- **FHIR-specific considerations** (choice types, type operators, extensions, etc.)
- **Ambiguities** — where the spec is unclear and reference implementations diverge

Present the findings to the user. Flag any ambiguities or issues for resolution before proceeding to Phase 2.

### Phase 2: Test Matrix Design

Apply **input domain partitioning** to systematically identify test cases. The question driving test design is:

> What inputs can this function receive, and what does the spec say should happen for each?

#### Test Dimensions

For each function, systematically consider these dimensions and determine which are relevant based on the function's signature and spec:

| Dimension | Partitions | When relevant |
|---|---|---|
| **Core semantics** | Spec examples, basic behavior | Always |
| **Emptiness** | `{}` literal, absent field, computed empty (`where(false)`) | Always for functions that accept collections |
| **Cardinality** | Singular (0..1) vs non-singular (0..*) | When function operates on resource fields — singular fields are scalar columns in Spark, non-singular are array columns. Test both to catch representation bugs. |
| **Element type** | Primitive, complex/backbone, choice type | When function accepts general Element input |
| **Nesting** | Flat fields, nested fields, deeply nested | When function involves traversal |
| **HAPI resource** | Real FHIR resource via `withSubject(IBaseResource)` | When the function involves resource traversal, choice types, FHIR type conversions, or element access patterns that differ between the map-based builder and real FHIR encoding |

**The cardinality dimension deserves special attention.** In the SQL layer, singular FHIR elements (0..1) are represented as scalar columns while non-singular elements (0..*) are represented as array columns. A function that works correctly on a scalar column may fail on an array column or vice versa. When a function operates on resource fields, include at least one test with a singular field (e.g., `Patient.gender`) and one with a non-singular field (e.g., `Patient.name`).

**When to require HAPI resource tests:** Some features cannot be adequately tested with the inline `ResourceDataBuilder` because the map-based representation doesn't capture the full FHIR encoding. Use `withSubject(IBaseResource)` when testing:
- Choice types (`value[x]`) — the HAPI serialization produces the polymorphic structure
- FHIR primitive type behavior (e.g., date comparison with `@` literals, code → string conversion)
- Extensions — URL-based access patterns
- `getValue()` / `hasValue()` — rely on FHIR primitive wrapper structure
- Any feature where the Spark schema from real FHIR JSON differs from the simplified map schema

#### Building the Test Matrix

Present a markdown table like this:

```markdown
## Test Matrix for `functionName()`

| # | Test case | Dimension | Expression | Expected | Subject |
|---|-----------|-----------|------------|----------|---------|
| 1 | Basic usage | Core semantics | `'hello'.fn()` | `'HELLO'` | literal |
| 2 | Spec example | Core semantics | `'abc'.fn()` | `'ABC'` | literal |
| 3 | Empty literal | Emptiness | `{}.fn()` | `{}` | literal |
| 4 | Singular field | Cardinality: 0..1 | `gender.fn()` | `'MALE'` | HAPI Patient |
| 5 | Non-singular field | Cardinality: 0..* | `name.family.fn()` | `['SMITH','DOE']` | HAPI Patient |
| 6 | Absent field | Emptiness: absent | `deceased.fn()` | `{}` | HAPI Patient |
| 7 | Choice type | Element type | `value.ofType(string).fn()` | ... | HAPI Observation |
```

The **Subject** column indicates whether the test uses:
- `literal` — inline FHIRPath expression only
- `inline` — `withSubject("ResourceType", sb -> ...)` map-based builder
- `HAPI Patient/Observation/etc.` — `withSubject(new Patient()...)` real FHIR resource

**Rules for the matrix:**
- One test per dimension where possible — vary one dimension while holding others constant
- Combination tests only where the spec implies interaction between dimensions
- Do NOT combinatorially explode independent dimensions
- Expected results come from the spec, not from running the implementation
- Flag any test case where the expected result is uncertain

Present the matrix to the user and wait for their review before generating code.

### Phase 3: Code Generation

Generate the test class using the project's fluent DSL.

#### DSL Reference

**Test base class:** Extend `FhirPathTestBase` (provides `builder()` and SparkSession lifecycle).

**Builder methods:**
```java
// Grouping
builder().group("Group name")

// Assertions (all support optional description and optional context)
.testEquals(expected, "expression")
.testEquals(expected, "expression", "description")
.testEquals(expected, "expression", context("contextExpr"))
.testEquals(expected, "expression", context("contextExpr"), "description")
.testTrue("expression")
.testFalse("expression")
.testEmpty("expression")
.testError(ExceptionClass.class, "expression", "description")

// Collection results
.testEquals(List.of(1, 2, 3), "(1 ; 2 ; 3).fn()")

// Inline resource subjects (map-based)
.withSubject("Patient", sb -> sb
    .string("id", "patient-1")
    .integer("age", 30)
    .stringArray("given", "John", "Jane")
    .element("name", n -> n.string("family", "Smith"))
    .elementArray("name",
        n -> n.string("family", "Smith"),
        n -> n.string("family", "Doe"))
)

// HAPI FHIR resource subjects
.withSubject(createPatient())  // IBaseResource
```

**ResourceDataBuilder methods:**
- Primitives: `string()`, `integer()`, `decimal()`, `bool()`
- Arrays: `stringArray()`, `integerArray()`, `decimalArray()`, `boolArray()`
- Complex: `element()` (single), `elementArray()` (multiple)

**Note:** The strategy document mentions `stringEmpty()`, `elementEmpty()`, `choice()`, and `extension()` builder methods, but these do **not exist yet** in `ResourceDataBuilder`. For testing absent fields and choice types, use HAPI resources with `withSubject(IBaseResource)`. For testing empty input, use `{}` in the FHIRPath expression or `where(false)` to produce computed empty.

#### Code Style and Test Organization

Test classes cover a **class of related functions** (e.g., `StringFunctionsTest`, `CollectionFunctionsTest`), not a single function. Within that class, each function gets **one `@TestFactory` method** that covers all dimensions for that function using groups to organize them. Do not split a function's tests across multiple methods — groups within a single method handle the structure.

```java
package com.example.fhirpath;

import com.example.fhirpath.test.FhirPathTestBase;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Tests for FHIRPath string functions: startsWith(), endsWith(), contains(), etc.
 *
 * <p>Based on FHIRPath specification section 5.7 (String Manipulation).
 *
 * <p>Covers:
 * <ul>
 *   <li>... (list functions and key dimensions)
 * </ul>
 */
public class StringFunctionsTest extends FhirPathTestBase {

  @TestFactory
  Stream<DynamicTest> testStartsWith() {
    return builder()
        .group("startsWith() spec examples")
        .testTrue("'abcdefg'.startsWith('abc')")
        .testFalse("'abcdefg'.startsWith('xyz')")

        .group("startsWith() empty prefix")
        .testTrue("'abcdefg'.startsWith('')", "Empty prefix always returns true")

        .group("startsWith() empty propagation")
        .testEmpty("{}.startsWith('abc')", "Empty input returns empty")
        .testEmpty("'abc'.startsWith({})", "Empty argument returns empty")

        .group("startsWith() on resource fields")
        .withSubject("Patient", p -> p.string("id", "patient-1"))
        .testTrue("id.startsWith('pat')", "Singular field (0..1)")
        .build();
  }

  // ... one @TestFactory per function
}
```

**Naming:** Test classes are named by capability (`StringFunctionsTest`, `WhereAndFilteringTest`), never by stage or issue number.

**HAPI resource helper methods:** When tests need HAPI resources, define `private static` factory methods at the top of the class:

```java
private static Patient createPatient() {
    final Patient patient = new Patient();
    patient.setId("patient-1");
    patient.setActive(true);
    // ...
    return patient;
}
```

**Descriptions:** Include when the expression + group don't fully explain what's being tested. Omit when they're self-evident.

## Reviewing Existing Tests

When asked to review an existing test class:

1. Run Phase 1 (spec research) for the function under test
2. Build the test matrix (Phase 2) as if writing from scratch
3. Compare the matrix against existing tests:
   - **Missing dimensions** — dimensions from the matrix with no corresponding test
   - **Incorrect expectations** — test expectations that don't match the spec
   - **Redundant tests** — multiple tests covering the same dimension without adding value
   - **Missing HAPI tests** — features that need real FHIR resources but only use inline builder
4. Present a gap analysis with specific recommendations

## What NOT to Test

- Unicode/emoji handling (unless spec explicitly defines it)
- Large input sizes (trust SparkSQL)
- Multiple variations of the same condition
- Performance characteristics
- Exhaustive type combinations beyond what the spec defines
- Cross-cutting infrastructure concerns (empty propagation, type encoding) that are tested once at the infrastructure level — unless the function has unique behavior for these cases
