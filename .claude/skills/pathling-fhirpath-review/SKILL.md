---
name: pathling-fhirpath-review
description: >
  Review a FHIRPath implementation change in Pathling against a correctness rubric covering
  collection semantics, empty propagation, column cardinality, type coercion, error-vs-empty
  behaviour, spec fidelity, and test coverage. Use this skill when reviewing a branch, diff, or PR
  that adds or changes a FHIRPath function, operator, or evaluation behaviour, or when the user asks
  for a correctness review of FHIRPath work. Trigger on phrases like "review this FHIRPath change",
  "review the diff", "check this implementation", "is this correct", or a review request naming a
  FHIRPath function or operator.
---

# Pathling FHIRPath review

A correctness rubric for FHIRPath implementation changes. It exists so a reviewer checks the things
that actually break in this codebase, rather than producing generic code-review output.

Review is most useful in a **fresh context** that sees the diff and this rubric but not the
reasoning that produced the change. When invoked as part of a larger workflow, dispatch a subagent
with the diff range and this rubric rather than reviewing inline.

## Establish the range

```bash
BASE_SHA=$(git merge-base origin/main HEAD)
git diff --stat $BASE_SHA..HEAD
git diff $BASE_SHA..HEAD
```

Read the linked issue and the governing spec section before judging behaviour. Use the
`fhirpath-spec` skill for spec lookups — the spec decides, not intuition.

When this rubric is being applied inside a dispatched subagent — the usual case, and what the
paragraph above recommends — there is no user to answer a question, so pass `--unattended` to
`fhirpath-spec`. Without it, a missing reference-implementation pin leaves that skill waiting on an
answer that cannot arrive.

## Rubric

Work through these in order. The first five are where FHIRPath implementations actually go wrong.

### 1. Collection semantics

Every FHIRPath expression evaluates to a collection. Check each of the three input shapes is
handled as the spec requires:

- **Empty** — does `{}` propagate, or does the spec require a value (`count()` → `0`,
  `empty()` → `true`)? Empty-propagation-by-default is right for most functions and wrong for
  existence and aggregate functions.
- **Singleton** — the common case, usually correct.
- **Multi-item** — where a function expects a single item, the spec's singleton evaluation rules
  apply: one item is used, empty returns empty, and **multiple items are an error**. Silently
  taking the first item is a bug.

### 2. Column cardinality

In the Spark layer a singular FHIR element is a **scalar** column and a non-singular element is an
**array** column. Code that works on one can fail on the other. Confirm the change handles both,
and that tests cover both — a test suite using only literals exercises neither.

### 3. Null versus empty

Spark nulls and FHIRPath's empty collection are related but not identical. Check that a null column
value produces empty rather than propagating a null into a result struct, and that a typed-empty
field behaves the same as an absent one where the spec says it should.

### 4. Type coercion and promotion

- Integer to Decimal promotion in mixed arithmetic and comparison
- String conversions that must return empty rather than throw on unparseable input
- Date/time **partial precision**: comparing values of differing precision returns empty, not
  false. This has caused regressions before
- Quantity units — calendar durations and UCUM units are not interchangeable above seconds

### 5. Error versus empty

The spec distinguishes "return empty" from "signal an error", and they are easy to conflate.
`toInteger()` on a non-numeric string returns empty; `single()` on a multi-item collection errors.
Check each failure path against the spec text, and check the tests assert the right one —
`testEmpty` and `testError` are not interchangeable.

### 6. Spec fidelity

- Behaviour matches the spec section, including its examples
- Javadoc carries an `@see` link to the governing spec section, as the existing providers do
- Any deliberate divergence is called out in a comment with its reason, not left silent

### 7. Registration and annotations

- `@FhirPathFunction` on the method; a new provider class also needs a `MethodDefinedFunction.mapOf`
  line in `StaticFunctionRegistry`
- `@SqlOnFhirConformance(Profile.…)` where the function maps to a SQL-on-FHIR profile feature —
  check against sibling functions rather than guessing
- `@Nonnull` / `@Nullable` on parameters, returns, and fields
- Substantial logic lives in a `*Logic` helper (as `ConversionFunctions` delegates to
  `ConversionLogic`), not inline in the provider method

### 8. Project conventions

Only flag these where they are actually violated — do not restate them as advice.

- No inner classes, records, or enums; each type gets its own file
- `final` on variables, parameters, and methods that do not change
- Functional style with `Optional`/`Stream` over imperative null checks and loops, but broken
  after 3–4 chained calls for readability
- Comments are complete sentences ending in a period, explaining intent rather than restating code
- No `System.out` / `System.err`
- No TODOs

### 9. Test coverage

- One `@FhirPathTest` method per function, in a `*DslTest` class named by capability
- Spec examples present as test cases
- Both cardinalities covered (scalar field and array field)
- Empty covered in its distinct forms: `{}` literal, typed-empty field, computed empty
  (`where(false)`)
- Error conditions asserted with `testError`
- Tests assert spec-derived expectations, not observed implementation output

See the `fhirpath-test-designer` skill for the dimension matrix and the DSL surface.

### 10. Exclusion baseline

If the change implements a feature, the YAML exclusion baseline should have been swept — obsolete
exclusions removed, over-broad matchers narrowed, mislabelled types corrected. The build catches
exclusions that became obsolete, but not exclusions that are now over-broad. See the
`pathling-yaml-exclusions` skill.

## Severity

| Severity | Meaning |
|---|---|
| **Critical** | Wrong results, spec violation, crash, or a regression in existing behaviour |
| **Important** | A real gap that will bite: an unhandled input shape, a missing test for behaviour the change claims to support, an annotation or registration omission |
| **Minor** | Naming, structure, or convention issues with no behavioural consequence |

## What not to flag

A reviewer asked to find problems will always find some, and chasing all of them produces
defensive over-engineering. Stay on correctness and requirement gaps.

- Style choices consistent with the surrounding code
- Speculative generality — "this won't scale to a case nobody has asked for"
- Performance micro-optimisation without a measurement
- Requests for comments where the code is already clear
- Rewrites of untouched code that merely happens to be nearby
- Anything that does not survive a second read of the cited lines

## Output

Report findings grouped by severity, each with a `file:line` citation, what is wrong, and the input
that would demonstrate it. Then a single verdict line:

```
Ready to merge: Yes | No
```

State a finding once. If you are unsure whether something is a defect, say so and explain the
uncertainty rather than promoting it to a finding or dropping it silently.
