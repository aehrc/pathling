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
