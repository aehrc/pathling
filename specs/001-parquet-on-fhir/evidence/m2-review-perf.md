# M2 review: the cost of normalising a quantity twice

The M2 review found that on the previous layout the optimised plan for
`value.ofType(Quantity).value` had grown from 41 characters at the base commit
`18e59220ec` to over 30,000. This records the cause, the fix and the
measurements before and after it.

## Cause

The traversal expression, `ResolveOrNull`, decides from the resolved type alone
whether a structure is previous-layout data to normalise. The engine's decoded
quantity (`QuantityEncoding.dataType()`) has exactly the type of a stored
previous-layout quantity: the same fields in the same order, `value` as
`DECIMAL(32,6)` beside `value_scale`, and `_value_canonicalized` as
`struct<value:decimal(38,0),scale:int>`. So no type-only test can tell them
apart, and a marker in the field metadata would not survive the struct coercion
of `when`, `concat` and union.

Traversing to a field of the decoded quantity went through `ResolveOrNull`. That
took the decoded structure for stored data, so the decimal branch rendered its
`DECIMAL(32,6)` value back to text at `value_scale`: a `CASE WHEN` over seven
scales, each case referring again to the value and the scale. Both of those are
computed from the stored text. On the previous layout that text is itself the
seven-case rendering of the stored value, so the sizes multiplied.

Quantity search had the same root. The matcher was given the decoded quantity,
so it read the six-digit decoded value rendered back to text, and on the new
layout a value with more than six fractional digits lost them before
canonicalisation.

## Fix

The branch applies only to stored data because the engine no longer hands its
decoded structures to the traversal expression:

- `DecodedRepresentation` traverses to a field, and gets a field, of the
  **stored** elements it keeps. Selecting a field commutes with decoding, and
  the stored elements are normalised once, where they were read. So
  `value.ofType(Quantity).value` is the stored text decoded once.
- The search matcher is given the stored elements
  (`DecodedRepresentation.getStored()`), so it canonicalises the stored text
  (decision 77).
- `QuantityEncoding.scaleOf` refers to the text twice rather than five times.
  The scale is the length of the fraction digits less the exponent, and a null
  text gives a null scale without a separate null check.

Binding the text once was not tried, either with `SqlFunctions.let` forced onto
its higher-order path or with Catalyst's `With`. Removing the second
normalisation already brought both suites back to no code-generation fallbacks,
so there was nothing left for it to fix. Whether either binding would be safe
inside the lambdas of a repeating element, and what it would cost, is not
measured here.

A quantity the engine builds itself, such as a literal or the union of a stored
quantity with a literal, is not a `DecodedRepresentation`. A field of one is
still read through the traversal expression and rendered once more. That is
correct, only redundant, and no suite shows it as a cost.

## Expression size and runtime

`PlanSize.java` in `scripts/m2-review/` selects each expression with
`fhirPathToColumn` over 10,000 Synthea Observations read from Parquet. It prints
the length of the optimised plan's expressions, the number of `CASE WHEN` in
them, and the best of six timed counts. The Parquet files are the reviewer's,
written through the public API for the previous layout, and through the `io`
module's `FhirJsonReader` for the new layout. Base is `18e59220ec`, before is
`7925165fe1`, and after is the fix.

| Expression                               | Base, previous |   Before, previous |   After, previous |       Before, new |       After, new |
| ---------------------------------------- | -------------: | -----------------: | ----------------: | ----------------: | ---------------: |
| `value.ofType(Quantity).value`           |  32 / 0 / 44ms | 30750 / 76 / 560ms |    772 / 1 / 54ms | 4299 / 39 / 272ms |    57 / 0 / 49ms |
| `value.ofType(Quantity).value > 5.5`     |  46 / 0 / 34ms | 30764 / 76 / 540ms |    786 / 1 / 37ms | 4313 / 39 / 254ms |    71 / 0 / 31ms |
| `value.ofType(Quantity)`                 |  26 / 0 / 43ms |  6947 / 12 / 243ms |  4612 / 7 / 260ms |   852 / 4 / 216ms |  660 / 2 / 228ms |
| `value.ofType(Quantity) > 100 'mg/dL'`   | 289 / 2 / 39ms |  3221 / 15 / 214ms | 3221 / 15 / 229ms |  902 / 12 / 199ms | 902 / 12 / 204ms |
| `value.ofType(Quantity).unit = 'cm'`     | 312 / 1 / 35ms |     889 / 9 / 40ms |    473 / 1 / 32ms |    713 / 9 / 36ms |   304 / 1 / 32ms |
| `component.value.ofType(Quantity).value` | 277 / 0 / 30ms |    3193 / 5 / 53ms |   1475 / 1 / 33ms |   2037 / 4 / 53ms |   374 / 0 / 30ms |
| `referenceRange.low.value`               | 272 / 0 / 30ms |    3212 / 5 / 34ms |   1474 / 1 / 32ms |     15 / 0 / 17ms |    15 / 0 / 14ms |

Each cell is characters / `CASE WHEN` / best time.

A field of a quantity is back to within 10 ms of base on the previous layout. The
one `CASE WHEN` left there is the previous layout's decimal rendered to text,
once, which is the normalisation itself.

A whole quantity, and a comparison of quantities, stay around five times slower
than base on both layouts. That time is the canonical form, computed per row by
the two UCUM functions from the text, where the released version read it from
the stored `_value_canonicalized` and `_code_canonicalized`. Decision 77 made
that change, and T096a, in M5, takes the canonical form from an annotation. It
is not a regression of the traversal.

The reviewer counted 41 characters for the base plan with a different tool;
`PlanSize` counts the text of `plan.expressions()`, which gives 32 for the same
plan. The reviewer's own `Bench.java`, run against the fixed jars, gives 54 ms
for `value.ofType(Quantity).value` on the previous layout and 48 ms on the new
one, against the reviewer's 47 ms at base and 467 ms before the fix.

## Suites

The same Maven invocations as the review, with JaCoCo enabled. The counts are of
the reviewer's two signatures, `grows beyond 64 KB` from Janino and
`MethodTooLarge` from JaCoCo's instrumentation.

| Run                                                    |   Base |  Before |  After |
| ------------------------------------------------------ | -----: | ------: | -----: |
| `verify -pl fhirpath -am`: 64 KB fallbacks             |      0 |       1 |      0 |
| `verify -pl fhirpath -am`: method too large            |      0 |       6 |      0 |
| `... -Dpathling.testLayout=previous`: 64 KB fallbacks  |      0 |      14 |      0 |
| `... -Dpathling.testLayout=previous`: method too large |      0 |       3 |      0 |
| `FhirViewShareableComplianceTest`, previous layout     | 59.8 s | 136.8 s | 57.1 s |
| `FhirViewShareableComplianceTest`, new layout          |      — |  22.9 s | 19.5 s |

Both suite runs are green: encoders 606, io 355 (1 skipped), terminology 535,
and fhirpath 7,329, with 922 skipped by default and 924 on the previous layout.
The fhirpath count is 34 higher than before the fix, from the tests the fix
adds. `library-api` runs 168, all passing.

## Reproduction

```bash
# The Pathling jars under test, built without tests.
mvn package -pl library-api -am -DskipTests -Djacoco.skip=true

# PlanSize over a directory holding an Observation Parquet dataset.
scripts/m2-review/run.sh scripts/m2-review/PlanSize.java <third-party classpath file> \
  <colon-separated Pathling jars> <parquet directory>
```

The third-party classpath is `mvn dependency:build-classpath` of `library-api`.
The Parquet directory holds an `Observation` dataset, written either through
`PathlingContext` or through `FhirJsonReader`.
