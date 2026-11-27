# Ticket 12: the quantity annotations, measured

Two measurements for ticket 12: T100o's measurement of the cost of a stored quantity where it is
computed with, repeated now that a quantity carries its canonical form (decision 92), and T096c's
measurement of what the quantity comparisons cost by falling out of whole-stage code generation
(decisions 103 and 126).

**Results.**

- **With the annotations, a comparison computes no canonical form at all.** The stored side reads
  `__canonical_exact` and the literal side carries its own as a constant, so the plan holds no call
  of the canonicalisation. Over 1,000,000 Observations, `q > 50 'mg/dL'` takes 84 ms annotated
  against 589 ms computed.
- **Without them, the canonicalisation runs once per comparison, and is shared between
  comparisons.** The generated code holds one call site for one comparison and one for two. Before
  the comparison read the canonical form as a whole, it held two sites for one comparison, one for
  the code and one for the value, which T100o had found as well.
- **The fallback from code generation costs nothing measurable.** `q != 50 'mg/dL'` falls back and
  `q > 50 'mg/dL'` does not, and they take 80 and 84 ms annotated, 584 and 589 ms computed. Beside a
  Coding filter that already falls back, each of them adds 9 to 22 ms to the filter's 255 to 285
  ms. **`=` falls back too**, for `ArrayFilter` and `ArrayForAll`, so the inequality is not
  special, and the negated equality, the candidate rewrite, is slower than the inequality.

Nothing has been rewritten on the strength of T096c. The task asks for the result to go to the
owner first.

## Conditions

- Measured at `a32bf0944d` on `issue/2832_12_quantity-annotations`, before the UCUM-only rule
  (T096b), on Spark 4.0.2, `local[4]`, Java 21, on an Apple M3 Pro with 11 cores. The machine is
  the one T100o was measured on, but only figures from this run are compared with each other.
  `PlanSize.java` and `Codegen.java` were run again at `9a58df91b2`, after the UCUM-only rule and
  the review fixes, and gave the same plans and the same generated-code counts and sizes; the data
  is all UCUM, so the rule does not reach it.
- After decision 137 (`__canonical` dropped at ingest and from the System quantity), the data was
  written again and `PlanSize.java` and `Codegen.java` run again (`*137*.txt` in `ticket-12/`). The
  rows for `q`, `q.toString()` and the union in the T100o table are from that run: each now holds one
  canonicalisation, against two before. Every comparison holds the same number of calls as before;
  their plans are a few characters to a few hundred shorter, and the inequality's interpreted calls
  fall from four to two. Encoding 100 Observations holds two calls in its plan, one per stored
  quantity (`valueQuantity` and `component[].valueQuantity`), against four before. The other figures
  in this document are from the earlier run; their counts are unchanged by the decision.
- The data is `server/src/test/resources/import-data/ndjson/Observation.ndjson`: 10,000 Synthea
  Observations, 8,781 of them with a `valueQuantity` and 586 with components. `WriteAnnotation.java`
  encodes it through `PathlingContext.encode` twice, with the canonical annotation on (the default),
  `annotated`, and off, `plain`, and writes it to Parquet. For T096c each resource is written 100
  times, with its id suffixed, which gives 1,000,000 rows.
- The engine refuses the previous layout, so unlike T100o there is no previous-layout column. The
  comparison is between the layout with and without the annotations.
- The scripts are in `scripts/ticket-12`. `PlanSize.java` and `Codegen.java` are T100o's, from
  `scripts/m2-review`, counting the one canonicalisation `quantity_canonical` in place of the two
  UDFs it replaces, and `Codegen.java` adds the equality and the inequality alone. Their output is
  in `ticket-12/`. Logging is set to warnings by `scripts/ticket-12/logback.xml`, so that it takes no
  part in the timings.

## T100o, repeated

Each cell is characters / `CASE WHEN` / calls of the canonicalisation in the optimised plan / best
time, over 10,000 Observations. `q` is `value.ofType(Quantity)` and `c` is
`component.value.ofType(Quantity)`.

| Expression | Computed | Annotated |
| --- | --- | --- |
| `q.value` | 57 / 0 / 0 / 44ms | 57 / 0 / 0 / 45ms |
| `q` | 409 / 2 / 1 / 68ms | 308 / 2 / 0 / 47ms |
| `q > 100 'mg/dL'` | 783 / 9 / 1 / 61ms | 682 / 9 / 0 / 51ms |
| `q > 1 'mg/dL' and q < 1000 'mg/dL'` | 1562 / 18 / 2 / 63ms | 1360 / 18 / 0 / 45ms |
| `... and q != 50 'mg/dL'` | 4668 / 36 / 7 / 59ms | 3961 / 36 / 0 / 50ms |
| `c.where($this > 100 'mm[Hg]')` | 1632 / 10 / 3 / 37ms | 1317 / 10 / 0 / 33ms |
| `c.where($this > 100 'mm[Hg]' and $this < 200 'mm[Hg]')` | 2449 / 19 / 4 / 42ms | 2029 / 19 / 0 / 42ms |
| `q.toString()` | 431 / 2 / 1 / 47ms | 330 / 2 / 0 / 31ms |
| `q \| 7 'g'` | 1462 / 4 / 1 / 48ms | 1361 / 4 / 0 / 39ms |

- **Annotated, no expression calls the canonicalisation.** Every comparison reads the stored
  annotation, and every literal carries its canonical form as a constant.
- **Computed, the plan holds one call per decode of a stored quantity that is compared,** where
  T100o's held two UDF calls per decode, `canonical_quantity_value` and `canonical_quantity_code`.
  A decode that is not compared but returned whole, `q`, `q.toString()` and the union, holds one
  too, since the System quantity carries only `__canonical_exact` (decision 137). Before that it
  carried both annotations and held two, one for each; those rows of the table are from the re-run
  described below. The `!=` still decodes more than once, as T100o found.
- The plans are smaller than T100o's on the new layout (`q > 100 'mg/dL'` 783 characters against 902,
  two comparisons 1,562 against 1,800), because a comparison reads the canonical form as a whole.
- `plansize-plain-before-compare.txt` is the same run before the comparison read the canonical form
  as a whole (`490068c99d`): there, every compared decode held two calls, as T100o's did.

## The generated code

`Codegen.java`, computed (`codegen-plain.txt`). Each cell is calls in the physical plan / call sites
in the generated code, with common-subexpression elimination on and off.

| Expression | Elimination on | Elimination off |
| --- | --- | --- |
| `q` (after decision 137) | 1 / 1, whole-stage | 1 / 1, whole-stage |
| `q > 1 'mg/dL'` | 1 / 1, whole-stage | 1 / 1, whole-stage |
| `q > 1 'mg/dL' and q < 1000 'mg/dL'` | 2 / 1, whole-stage | 2 / 2, whole-stage |
| `... and q != 50 'mg/dL'` | 7 / 1 + 4 interpreted, falls back | 7 / 3 + 4 interpreted, falls back |
| `q = 50 'mg/dL'` | 5 / 1 + 4 interpreted, falls back | 5 / 1 + 4 interpreted, falls back |
| `q != 50 'mg/dL'` | 5 / 1 + 4 interpreted, falls back | 5 / 1 + 4 interpreted, falls back |
| `(q = 50 'mg/dL').not()` | 10 / 2 + 8 interpreted, falls back | 10 / 2 + 8 interpreted, falls back |

- **One comparison canonicalises once, and two share it.** With elimination on, two comparisons of
  one quantity hold one call site, because the comparison of the canonical forms is evaluated
  unconditionally, so Spark shares the canonicalisation beneath it. Before the comparison read the
  canonical form as a whole (`codegen-plain-before-compare.txt`), one comparison held two sites:
  the code was read unconditionally and the value only where the codes agreed, so Spark shared
  neither, which is the behaviour T100o found for the value.
- **Annotated** (`codegen-annotated.txt`), every case holds no call site, and the code of a
  comparison is smaller: 18,629 characters for one comparison against 19,004 computed.
- **`=` falls back as `!=` does.** Both compile to array functions that cannot generate code, `=`
  to `ArrayFilter` and `ArrayForAll` and `!=` to `ArrayExists` and `ArrayFilter`, and the decodes
  beneath them are interpreted. The widest nested type is 11 fields, far from the limit of 100.

## T096c: what the fallback costs

`FallbackCost.java` evaluates each expression as a filter over the cached 1,000,000 Observations
and prints the best and mean of six timed counts after a warm-up (`fallback-*.txt`). The ordering
`q > 50 'mg/dL'` is the control: it does the same work as the equality, including the decode and
the comparison of canonical forms, and generates code.

| Expression | Whole-stage | Annotated, best / mean | Computed, best / mean |
| --- | --- | --- | --- |
| `q.value > 5.5` | yes | 64 / 70 ms | 58 / 64 ms |
| `q > 50 'mg/dL'` | yes | 84 / 100 ms | 589 / 614 ms |
| `q = 50 'mg/dL'` | no | 87 / 93 ms | 592 / 596 ms |
| `q != 50 'mg/dL'` | no | 80 / 89 ms | 584 / 587 ms |
| `(q = 50 'mg/dL').not()` | no | 97 / 105 ms | 755 / 759 ms |
| Coding filter | no | 285 / 294 ms | 255 / 264 ms |
| Coding filter and `q > 50 'mg/dL'` | no | 294 / 297 ms | 274 / 277 ms |
| Coding filter and `q = 50 'mg/dL'` | no | 301 / 305 ms | 275 / 281 ms |
| Coding filter and `q != 50 'mg/dL'` | no | 297 / 303 ms | 277 / 278 ms |
| Coding filter and `(q = 50 'mg/dL').not()` | no | 309 / 315 ms | 296 / 297 ms |

The Coding filter is `code.coding.where(system = 'http://loinc.org' and code = '2093-3').exists()`,
whose higher-order filter falls back, as decision 126 found for `UsCoreBloodPressures`.

- **The fallback does not show at this scale.** The inequality and the equality, which fall back,
  take the same time as the ordering, which does not, within the run-to-run spread, annotated and
  computed. The cost of a quantity comparison is the canonicalisation where it is computed, about
  500 ms per million rows here, and the annotation removes it.
- **Beside a filter that already falls back**, the ordering, the equality and the inequality each
  add 9 to 22 ms, and the fallback of the comparison adds nothing beyond what the ordering adds.
  The conjunction evaluates the comparison only for the 21,500 rows the Coding filter keeps, which
  is why the computed canonicalisation costs so little here.
- **The negated equality is not a rewrite worth having.** It is slower than the inequality,
  computed by 170 ms, because it compiles to two comparisons' worth of decodes.
- The Coding filter alone is about 30 ms faster on the computed data. The two datasets differ only
  in the quantity's annotation fields, which the Coding filter does not read; it was not
  investigated further.

**For the owner.** Decision 103 asked for the operator to be rewritten without the array functions
if the cost matters, and decision 126 for an issue if the fallback matters engine-wide. On these
figures it does not matter for either the quantity comparisons or beside the Coding filter's
fallback, so nothing is proposed. That `=` falls back as well is new, and is reported here rather
than acted on.

## Reproduction

```bash
W=.local/work/issue/2832_12_quantity-annotations
mvn -o package -pl library-api -am -DskipTests -Djacoco.skip=true
mvn -o dependency:build-classpath -pl library-api -Dmdep.outputFile=$PWD/$W/cp-raw.txt
# Keep the Pathling jars out of the third-party classpath, so that ~/.m2 cannot shadow them.
tr ':' '\n' < $W/cp-raw.txt | grep -v /au/csiro/pathling/ | paste -sd: - > $W/cp.txt
J=$(for m in utilities fhir-schema encoders io terminology fhirpath library-api; do \
  ls $m/target/$m-*.jar | grep -v -E 'sources|javadoc|tests'; done | paste -sd: -)
S=specs/001-parquet-on-fhir/evidence/scripts/ticket-12
O=server/src/test/resources/import-data/ndjson/Observation.ndjson

$S/run.sh $S/WriteAnnotation.java $W/cp.txt "$J" $O $W/data
for d in plain annotated; do
  $S/run.sh $S/PlanSize.java $W/cp.txt "$J" $W/data/$d
  $S/run.sh $S/Codegen.java $W/cp.txt "$J" $W/data/$d $W/code/$d
done

$S/run.sh $S/WriteAnnotation.java $W/cp.txt "$J" $O $W/data-1m 100
for d in annotated plain; do
  $S/run.sh $S/FallbackCost.java $W/cp.txt "$J" $W/data-1m/$d 7
done
```
