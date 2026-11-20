# T100o: the cost of decoding a stored quantity where it is computed with

**Result: the repeated decode shows in the size of the plan, not in its run
time.** Since decision 82 a stored quantity stays in the stored shape and is
decoded by `Collection.toEngineForm()` once per operation that computes with it.
An expression that uses one quantity in two comparisons decodes it twice, and the
optimised plan holds the decode, with its `canonical_quantity_value` and
`canonical_quantity_code` calls, once per use. Over 10,000 Observations the run
time of such an expression is within a few milliseconds of one that decodes it
once, on both layouts, which is consistent with Spark evaluating a repeated
deterministic expression of one projection or filter once. Nothing has been changed: the task
asks for the result to go to the owner first.

**The generated code, examined afterwards, does not bear out that inference**
(see [The generated code](#the-generated-code)): Spark's common-subexpression
elimination shares the `canonical_quantity_code` call between comparisons but
not `canonical_quantity_value`, which stays once per use inside the branch that
runs only where the units are comparable, and an inequality takes the projection
out of whole-stage code generation altogether.

## Conditions

- Measured at `2afe74dea6` on `issue/2764`, Spark 4.0.2, `local[4]`, Java 21, on
  an Apple M3 Pro with 11 cores. The machine is not the one the M2 figures were
  taken on, so the M2 expressions are measured again here in the same run as the
  new ones, and only figures from this run are compared with each other.
- The data is `server/src/test/resources/import-data/ndjson/Observation.ndjson`:
  10,000 Synthea Observations, 8,781 of them with a `valueQuantity` and 586 with
  components. It is written to Parquet by `WriteLayouts.java`, in the new layout
  through `PathlingContext.encode` and in the previous layout through the
  encoders reached with `PathlingContext.getFhirEncoders()`, because since the
  switch `PathlingContext` writes only the new layout (12 and 45 columns).
- `PlanSize.java` selects each expression with `fhirPathToColumn` over the
  cached dataset and prints the length of the optimised plan's expressions, the
  number of `CASE WHEN` in them, the number of `canonical_quantity_value` calls
  in them, which is one per decode of a stored quantity, and the best of six
  timed counts. Two runs were made per layout; their plan figures are identical
  and their times agree within 10 ms. The table gives the first run.

## Results

Each cell is characters / `CASE WHEN` / decodes / best time.

| Expression                                               | Previous layout        | New layout           |
| -------------------------------------------------------- | ---------------------- | -------------------- |
| `q.value` (M2)                                           | 772 / 1 / 0 / 52ms     | 57 / 0 / 0 / 47ms    |
| `q` (M2)                                                 | 4612 / 7 / 1 / 112ms   | 660 / 2 / 1 / 96ms   |
| `q > 100 'mg/dL'` (M2)                                   | 3221 / 15 / 1 / 82ms   | 902 / 12 / 1 / 59ms  |
| `q.value > 5.5` (M2)                                     | 786 / 1 / 0 / 36ms     | 71 / 0 / 0 / 32ms    |
| `q.unit = 'cm'` (M2)                                     | 473 / 1 / 0 / 32ms     | 304 / 1 / 0 / 31ms   |
| `c.value` (M2)                                           | 1475 / 1 / 0 / 33ms    | 374 / 0 / 0 / 31ms   |
| `referenceRange.low.value` (M2)                          | 1474 / 1 / 0 / 31ms    | 194 / 0 / 0 / 27ms   |
| `q > 1 'mg/dL' and q < 1000 'mg/dL'`                     | 6436 / 30 / 2 / 105ms  | 1800 / 24 / 2 / 64ms |
| `q > 1 'mg/dL' and q < 1000 'mg/dL' and q != 50 'mg/dL'` | 20120 / 65 / 5 / 109ms | 5258 / 46 / 5 / 70ms |
| `c.where($this > 100 'mm[Hg]')`                          | 3024 / 14 / 2 / 40ms   | 1865 / 13 / 2 / 39ms |
| `c.where($this > 100 'mm[Hg]' and $this < 200 'mm[Hg]')` | 3966 / 26 / 3 / 45ms   | 2807 / 25 / 3 / 43ms |
| `q.toString()`                                           | 4634 / 7 / 1 / 121ms   | 682 / 2 / 1 / 89ms   |
| `q \| 7 'g'`                                             | 5708 / 10 / 1 / 98ms   | 1756 / 5 / 1 / 78ms  |

`q` is `value.ofType(Quantity)` and `c` is `component.value.ofType(Quantity)`.

- **Plan size grows with every use.** Two comparisons of one quantity hold two
  decodes, and adding a `!=` brings the count to five, so the inequality decodes
  the stored quantity more than once.
  On the previous layout three comparisons reach 20,120 characters, against
  3,221 for one, because each decode also repeats the previous layout's
  normalisation of the value to text.
- **Run time does not.** One comparison takes 82 ms on the previous layout and
  59 ms on the new one; two take 105 ms and 64 ms, and three 109 ms and 70 ms.
  Decoding once costs about 60 ms over these rows on the previous layout (`q`
  against `q.value`) and about 50 ms on the new one, so if each decode ran, two
  and three comparisons would cost roughly 60 and 240 ms more than one does. They
  cost 23 and 27 ms more on the previous layout, which is about what the extra
  comparisons cost, so the repeated decodes appear to be evaluated once.
- **Inside the lambda of a repeating element** the plan holds one decode more
  than it has comparisons, and the time stays within 5 ms of the single
  comparison.
- **`toString()` and a union with a literal** decode once, as before.
- The M2 expressions' plan sizes are those of the "After" columns of
  `m2-review-perf.md`, apart from `referenceRange.low.value` on the new layout,
  which is 194 characters here and 15 there; it decodes nothing in either.

## Suites

The full builds at the same commit show none of the code-generation fallbacks
the M2 review counted, on either layout.

| Run                                                                      | `grows beyond 64 KB` | `MethodTooLarge` |
| ------------------------------------------------------------------------ | -------------------: | ---------------: |
| `install -pl library-runtime -am`, fhirpath on the new layout, JaCoCo on |                    0 |                0 |
| `verify -pl fhirpath -Dpathling.testLayout=previous -Djacoco.skip=true`  |                    0 |                0 |

## For the owner

Whether this matters is the owner's decision. The cost is in the size of the
plan: an expression that uses one quantity many times, on the previous layout in
particular, grows its plan by several thousand characters per use, which counts
towards the code-generation limits that the M2 review hit. The run time at this
scale does not change. The options T100o lists, in order, are binding the
decoded value once per expression (`SqlFunctions.let` or Catalyst's `With`),
reading the canonical form from the quantity annotation once M5 emits it, which
removes the UDF calls from each decode, and decoding at traversal those
quantities whose extensions cannot be reached, which reopens decision 82. None is
implemented.

## The generated code

Decision 92 closes T100o if the generated code shows Spark deduplicating the
decode. It does not, so T100o stays open.

### Method

`scripts/m2-review/Codegen.java` selects four expressions over the same
Observations, with `spark.sql.subexpressionElimination.enabled` on (the
default) and then off, and examines the code that evaluates the projection:
the whole-stage code from `queryExecution().debug().codegenToSeq()` where the
projection is part of a stage, and otherwise the `UnsafeProjection` code that
`ProjectExec` generates, produced the same way with
`GenerateUnsafeProjection.createCode`. It counts the call sites of the two UDFs
of a decode by the name each site carries for its error message, the
regular-expression matches of the parse, and the subexpression functions Spark
generated, and reports why a projection left whole-stage code generation. Where
it did, the decodes beneath an expression that cannot generate code
(`CodegenFallback`) are interpreted, and are counted from the plan.

Measured at `3f38b1090e` (the `fhirpath` code of `4f9e5aa8a6`), and, for the
comparison with eager decoding, at `633894edfc`, the commit before decision 82's
redesign (`1fee8c4046^`), where a stored quantity was decoded at traversal by
`QuantityCollection`'s `DecodedRepresentation`. That commit was built with
`mvn package` in a scratch worktree, and its new-layout data was written by its
own `io` reader with `scripts/m2-review/WriteNewLayoutM2.java` (12 columns, as
at `HEAD`). Spark 4.0.2, Java 21.

### Results

Each cell is `canonical_quantity_value` sites / `canonical_quantity_code` sites
in the generated code, then the decodes in the physical plan, then the code's
characters and its largest method's bytecode. `q` is `value.ofType(Quantity)`.

| Expression, new layout                      | Elimination on                    | Elimination off                   |
| ------------------------------------------- | --------------------------------- | --------------------------------- |
| `q`                                         | 1 / 1, plan 1; 27,562 ch, 1,570 B | 1 / 1, plan 1; 27,562 ch, 1,570 B |
| `q > 1 'mg/dL'`                             | 1 / 1, plan 1; 20,643 ch, 1,360 B | 1 / 1, plan 1; 21,290 ch, 1,466 B |
| `q > 1 'mg/dL' and q < 1000 'mg/dL'`        | 2 / 1, plan 2; 34,495 ch, 2,167 B | 2 / 2, plan 2; 38,446 ch, 2,931 B |
| `... and q != 50 'mg/dL'` (not whole-stage) | 3 / 1 + 2 interpreted, plan 5     | 3 / 3 + 2 interpreted, plan 5     |

| Expression, previous layout                 | Elimination on                     | Elimination off                     |
| ------------------------------------------- | ---------------------------------- | ----------------------------------- |
| `q`                                         | 1 / 1, plan 1; 121,594 ch, 8,259 B | 1 / 1, plan 1; 121,594 ch, 8,259 B  |
| `q > 1 'mg/dL'`                             | 1 / 1, plan 1; 74,773 ch, 5,248 B  | 1 / 1, plan 1; 75,468 ch, 5,354 B   |
| `q > 1 'mg/dL' and q < 1000 'mg/dL'`        | 2 / 1, plan 2; 127,070 ch, 7,369 B | 2 / 2, plan 2; 149,661 ch, 10,707 B |
| `... and q != 50 'mg/dL'` (not whole-stage) | 3 / 1 + 2 interpreted, plan 5      | 3 / 3 + 2 interpreted, plan 5       |

The projection code of the three-comparison case is 31,379 characters with
elimination on and 40,938 off on the new layout (122,633 and 157,723 on the
previous), which is not comparable with the whole-stage figures above it.

- **The decode is not deduplicated.** With two comparisons the plan holds two
  decodes, and the generated code calls `canonical_quantity_value` twice whether
  elimination is on or off. Only `canonical_quantity_code` is shared: with
  elimination on it moves into a subexpression function evaluated once per row,
  because the test of whether the units are comparable, which reads it, is
  evaluated unconditionally. The value is computed inside the branch that runs
  only where the units are comparable, and Spark does not eliminate an
  expression that is not evaluated on every path. That branch is also why the
  run time barely moves: the repeated value UDF runs only for rows whose units
  are comparable with the literal's, and once per comparison for those.
- **The parse is pruned, not shared.** A comparison reads only the canonical
  value and code of the decoded structure, so the optimiser drops the scale and
  its two regular-expression matches; only `q` itself, which returns the whole
  structure, keeps them.
- **The code grows with each use.** On the new layout the second comparison
  takes the code from 20,643 to 34,495 characters and the largest method from
  1,360 to 2,167 bytes of bytecode (38,446 and 2,931 without elimination); on the
  previous layout from 74,773 to 127,070 characters and 5,248 to 7,369 bytes.
- **An inequality leaves whole-stage code generation.** `q != 50 'mg/dL'`
  compiles into `exists` and `filter` over arrays, which cannot generate code
  (`ArrayExists`, `ArrayFilter`), so the whole projection is evaluated by a
  separately generated projection, with those functions and the two decodes
  beneath them interpreted. The nested width of the types involved (11 fields)
  is far from the limit of 100, so this is the cause. The inequality also
  decodes the quantity three times, which with the two of the other comparisons
  makes the five decodes of the earlier table.
- **Eager and lazy decoding generate the same code.** At `633894edfc` every
  count and size above is identical, on both layouts, and on the new layout the
  generated code of each case differs from `HEAD`'s only in the addresses of the
  UDF lambdas in its error messages, as decision 92 states: each design hands
  Spark the same tree.

### Reproduction of the generated-code check

```bash
mvn package -pl library-api -am -DskipTests -Djacoco.skip=true
mvn dependency:build-classpath -pl library-api -Dmdep.outputFile=/tmp/cp-raw.txt
# Keep the Pathling jars out of the third-party classpath, so that ~/.m2 cannot shadow them.
tr ':' '\n' < /tmp/cp-raw.txt | grep -v /au/csiro/pathling/ | paste -sd: - > /tmp/cp.txt
J=$(for m in utilities fhir-schema encoders io terminology fhirpath library-api; do \
  ls $m/target/$m-*.jar | grep -v -E 'sources|javadoc|tests'; done | paste -sd: -)
S=specs/001-parquet-on-fhir/evidence/scripts/m2-review
$S/run.sh $S/Codegen.java /tmp/cp.txt "$J" /tmp/t100o/pof /tmp/t100o-code/pof
$S/run.sh $S/Codegen.java /tmp/cp.txt "$J" /tmp/t100o/previous /tmp/t100o-code/previous

# Eager decoding, before decision 82's redesign, in a scratch worktree.
git worktree add --detach /tmp/wt-m2 1fee8c4046^
mvn -f /tmp/wt-m2/pom.xml package -pl library-api -am -DskipTests -Djacoco.skip=true
mvn -f /tmp/wt-m2/pom.xml dependency:build-classpath -pl library-api \
  -Dmdep.outputFile=/tmp/cp-m2-raw.txt
tr ':' '\n' < /tmp/cp-m2-raw.txt | grep -v /au/csiro/pathling/ | paste -sd: - > /tmp/cp-m2.txt
JM=$(for m in utilities fhir-schema encoders io terminology fhirpath library-api; do \
  ls /tmp/wt-m2/$m/target/$m-*.jar | grep -v -E 'sources|javadoc|tests'; done | paste -sd: -)
$S/run.sh $S/WriteNewLayoutM2.java /tmp/cp-m2.txt "$JM" \
  server/src/test/resources/import-data/ndjson/Observation.ndjson /tmp/t100o-m2
$S/run.sh $S/Codegen.java /tmp/cp-m2.txt "$JM" /tmp/t100o-m2/pof /tmp/t100o-code/m2-pof
git worktree remove --force /tmp/wt-m2
```

The data under `/tmp/t100o` is written by `WriteLayouts.java` as in the
reproduction below. `Codegen.java` compiles against both commits.

## Reproduction

```bash
# The Pathling jars under test, and the third-party classpath.
mvn package -pl library-api -am -DskipTests -Djacoco.skip=true
mvn dependency:build-classpath -pl library-api -Dmdep.outputFile=/tmp/cp.txt
J=$(for m in utilities fhir-schema encoders io terminology fhirpath library-api; do \
  ls $m/target/$m-*.jar | grep -v -E 'sources|javadoc|tests'; done | paste -sd: -)

S=specs/001-parquet-on-fhir/evidence/scripts/m2-review
$S/run.sh $S/WriteLayouts.java /tmp/cp.txt "$J" \
  server/src/test/resources/import-data/ndjson/Observation.ndjson /tmp/t100o
$S/run.sh $S/PlanSize.java /tmp/cp.txt "$J" /tmp/t100o/previous
$S/run.sh $S/PlanSize.java /tmp/cp.txt "$J" /tmp/t100o/pof
```
