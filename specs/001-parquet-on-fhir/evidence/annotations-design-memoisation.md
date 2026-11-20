# Whether two scalar date UDFs over a memoised parse can replace the struct UDF

Measured for `design/annotations-in-the-engine.md` §4.6 ("memoise, do not group") and §7, and
ticket 42 (shared annotation computation). The question was whether the struct-returning
`date_range` UDF, bound once with `bindOnce`, can be replaced by two independent scalar UDFs,
`date_start(text)` and `date_end(text)`, over a per-thread memoised parse, without extra parses or
a loss of throughput, and what Spark's own subexpression elimination (CSE) already gives.

**Result: yes. Two scalar UDFs over a per-thread LRU parse about once per value in every context
and mode measured, which the struct UDF does only where it is bound by hand, and it is as fast or
faster everywhere, to within 5%. The LRU must be at least as long as the arrays a plan reads in separate passes.
Spark's CSE alone is not enough.**

- **Parses per value, arm 4 (LRU of 32), all five contexts, all three codegen modes: 0.975.** One
  parse per value, less 2.5% that hit because the data repeats values across rows. The two UDFs are
  called twice per value, so the second call is always a hit. Results are identical to the struct
  UDF's in every context, mode and cache size, on `local[4]`.
- **CSE does not do the job.** The struct UDF referenced twice parses once only in a projection of
  both bounds in one expression (and, in the two-column case, it is the best arm). It parses twice
  per value inside any `transform` lambda and across two passes, and 1.54 times per value in a
  `filter`, under whole-stage codegen. The result under whole-stage codegen differs from the
  interpreted one, so a design that relied on CSE would behave differently between modes.
- **The separate-pass case is where the LRU size matters, and it is a cliff, not a slope.** A
  cache shorter than the array parses twice per value, as with no cache. With arrays of 50
  elements, a cache of 32 gives 1.95 parses per value and no speed-up, and a cache of 64 gives
  0.95 and 42% less time.
- **The cache does not fix #2594.** A traced input still fires twice per value with two scalar
  UDFs, whether or not they cache. Binding it with `SqlFunctions.let` gives one firing, in arm 4
  as in the other arms.
- **Cost.** Under whole-stage codegen arm 4 is the fastest, or within 5% of the fastest, in every
  context. It is 20% faster than today's struct arm for a projected struct and 51% faster for two
  projected columns, because it allocates no struct and needs no `bindOnce` higher-order
  function.

## Conditions

- Spark 4.0.2, Java 21, `local[4]` on an 11-core Apple machine, in one JVM, so a static
  `LongAdder` counts every task thread. Adaptive execution off, session time zone UTC.
- Probe: `scripts/annotations-design/memoisation/MemoisationProbe.java`, launched by `run.sh` (it
  compiles with `javac`, because Spark cannot deserialise the probe's UDF lambdas from the
  single-file source launcher's class loader). Jars of this branch at `0c94065ec3`.
- The parse is `FhirDateTime.parse`, which `DateRangeKind` calls, counted by a `LongAdder` at the
  call. The struct arm returns `Row(lower, upper)` as `DateRangeKind` does; the scalar arms return
  the two `Instant`s as `TimestampType`.
- Data: 1,000,000 rows read from Parquet, each with a text `t` and an array `a` of three texts. The
  texts are drawn at random from a pool of 5,000, built from 20% years (`1970`), 20% year-months
  (`1970-03`), 30% days (`1970-03-15`) and 30% dateTimes with `Z`, `+10:00` or `-05:00` zones and
  seconds, some with a fraction. The pool repeats values, so a cache sometimes hits across rows.
  Counts were taken on 300,000 rows (parses per value do not depend on size); times on 1,000,000
  rows, 3,000,000 values in the array contexts.
- Arms: (1) struct UDF with `bindOnce`, as today; (2) struct UDF referenced twice, relying on CSE;
  (3) two scalar UDFs, no cache; (4) two scalar UDFs over a `ThreadLocal` `LinkedHashMap` in access
  order, 32 entries. A parse failure is not exercised (the data are valid).
- Contexts: `proj` one projected struct of both bounds; `proj2col` two projected columns, one per
  bound; `filter` `start <= X and end >= X`; `lambda` one `transform` returning both bounds;
  `passes` `transform(a, start)` and `transform(a, end)` as two columns; `arrfilter`
  `filter(a, x -> start <= X and end >= X)`. In `passes`, arms 1 and 2 are the same plan: each
  pass calls the struct UDF.
- Codegen: `wholeStage` is the default; `nowholestage` is `spark.sql.codegen.wholeStage=false`;
  `interpreted` adds `spark.sql.codegen.factoryMode=NO_CODEGEN`.
- Times are the median of five warm runs after one warm-up, to the `noop` sink, each configuration
  run under the shared benchmark lock. The spread of the five runs was under 5% of the median, except for arm 1 of the 50-element
  case below (1931 to 2218).
- Correctness: a checksum of the result (sums of the microsecond values, row and element counts,
  for `filter` the row count) is identical across all four arms, all three modes and every cache
  size from 1 to 128, on four task threads.

## Parses per value

A value is a row for `proj`, `proj2col` and `filter`, and an array element for the others. The
three codegen modes are in the order whole-stage / no whole-stage / interpreted; one number means
all three agree.

| Context | 1 struct + bindOnce | 2 struct twice (CSE) | 3 scalar, no cache | 4 scalar + LRU 32 |
| --- | --- | --- | --- | --- |
| `proj` (one struct) | 1.000 | 1.000 | 2.000 | 0.975 |
| `proj2col` (two columns) | 2.000 | 1.000 | 2.000 | 0.975 |
| `filter` | 1.000 | 1.536 / 1.000 / 1.000 | 1.536 | 0.975 |
| `lambda` (`transform`) | 1.000 | 2.000 | 2.000 | 0.975 |
| `passes` (two `transform`s, 3 elements) | 2.000 | 2.000 | 2.000 | 0.975 |
| `arrfilter` | 1.000 | 1.535 | 1.535 | 0.975 |

The 1.5 in a `filter` is not a second parse of every value: `and` evaluates the second bound only
where the first is true, so 54% of values reach it. Arm 4's UDF calls follow the same 1.5 or 2.0,
and its parses stay at 0.975.

Arm 2's `filter` result is the one place where the mode changes the count. Under whole-stage
codegen CSE does not reach the second operand of the `and` (1.536). Interpreted and non-whole-stage
evaluation do (1.000).

### Cache size, with the bounds read in two passes

`passes` context, arm 4, whole-stage codegen, 1,000,000 values each. Parses per value:

| Array length | cache 1 | 8 | 16 | 32 | 64 | 128 |
| --- | --- | --- | --- | --- | --- | --- |
| 3 | 1.998 | 0.994 | 0.988 | 0.975 | 0.952 | 0.908 |
| 20 | 1.998 | 1.987 | 1.975 | 0.975 | 0.951 | 0.908 |
| 50 | 1.998 | 1.988 | 1.975 | 1.950 | 0.951 | 0.908 |

The cliff is at the array length: below it, the cache holds only the end of the first pass and the
second pass misses every value. The slow decline above it is cross-row repeats from this pool, so
it measures the data, not the cache.

### A traced input (#2594)

The input text is wrapped in a non-deterministic counting UDF. `traces/value` is how often it fires.
Whole-stage and interpreted modes agree.

| Context | Arm | Traces per value, input unbound | Traces per value, bound with `SqlFunctions.let` | Parses per value |
| --- | --- | --- | --- | --- |
| `proj` and `lambda` | 1 struct + `bindOnce` | 1.000 | 1.000 | 1.000 |
| | 2 struct twice | 2.000 | 1.000 | 2.000 |
| | 3 scalar, no cache | 2.000 | 1.000 | 2.000 |
| | 4 scalar + LRU | 2.000 | 1.000 | 0.975 |

A traced input makes the struct UDF non-deterministic too, so arm 2 loses its CSE and parses twice.
Arm 1's `bindOnce` already binds the traced input, which is why its unbound column reads 1.000.

## Time

Median milliseconds of five warm runs, 1,000,000 rows, whole-stage / no whole-stage / interpreted.

| Context | 1 struct + bindOnce | 2 struct twice (CSE) | 3 scalar, no cache | 4 scalar + LRU 32 |
| --- | --- | --- | --- | --- |
| `proj` | 421 / 406 / 402 | 323 / 339 / 413 | 579 / 614 / 641 | 338 / 359 / 385 |
| `proj2col` | 672 / 658 / 667 | 313 / 328 / 386 | 568 / 596 / 610 | 328 / 365 / 361 |
| `filter` | 394 / 389 / 413 | 469 / 368 / 458 | 454 / 506 / 547 | 325 / 381 / 404 |
| `lambda` (3M values) | 1146 / 1137 / 1159 | 1878 / 1915 / 1915 | 1842 / 1853 / 1864 | 1101 / 1113 / 1156 |
| `passes` (3M values) | 1857 / 1891 / 1887 | 1858 / 1891 / 1890 | 1792 / 1819 / 1815 | 1060 / 1078 / 1091 |
| `arrfilter` (3M values) | 1056 / 1062 / 1092 | 1454 / 1473 / 1687 | 1393 / 1428 / 1516 | 1012 / 1042 / 1139 |

Arrays of 50 elements, `passes`, whole-stage, 3,000,000 values:

| Cache | 1 struct | 2 struct | 3 scalar | 4 scalar + LRU |
| --- | --- | --- | --- | --- |
| 32 | 2041 | 1892 | 1870 | 1869 |
| 64 | 1907 | 1904 | 1829 | 1055 |

A parse costs about one microsecond of CPU here (the extra 1,000,000 parses of `proj` arm 3 add
240 ms on four threads), so the whole run is dominated by the Parquet read and
the plan around it; a duplicated parse adds 40-70% to a run that does nothing else with the value.
Under whole-stage codegen arm 4 is the fastest or within 5% of the fastest in every context (the
two near misses are `proj` and `proj2col`, against arm 2's CSE). Without whole-stage codegen it is
within 11% of the fastest (`proj2col`, 365 against arm 2's 328) and usually first. Interpreted, it
is first in every context but `arrfilter` (1139 against arm 1's 1092).

## What this means for the design

- **Replace the struct UDF with two scalar UDFs over the cache.** It removes the struct, the
  `bindOnce` higher-order function and the case where two output columns each bind their own
  (`proj2col`, 2.0 parses for arm 1, 0.975 for arm 4), and the annotations can then be read one at
  a time, as the layout stores them. No context needs the struct.
- **Do not rely on Spark's CSE.** It works only for two references to one deterministic
  expression in one unconditional projection. It does nothing inside a `transform` or `filter`
  lambda, nothing across two columns that each wrap the call in a higher-order function, and
  under whole-stage codegen nothing for the second operand of an `and`. Its reach differs between
  codegen modes, and a traced input turns it off. A cache makes the parse count independent of
  all of these.
- **Cache size: at least the longest array read in separate passes, 64 is a reasonable floor.**
  The cache of 1 is useless for `passes` (1.998), and a cache at or just below the array length
  gives nothing. At or above the length, it saves the whole second parse. The §4.6 text of
  "about 16-64 entries" holds only for arrays of up to that many elements; an array of 100 values
  read in two passes would need more. A larger cache costs a bounded amount of memory per task
  thread (one `FhirDateTime` per entry), and raising it from 64 to 128 changed no timing here.
  An array longer than the cache degrades to today's cost for the struct UDF in separate passes
  (two parses per value), never to a wrong answer.
- **The separate-pass case is the one that justifies the LRU over a last-input cache.** In the
  other contexts both UDFs are called on the same value one after the other, so a cache of one
  entry would parse once. Only `passes` needs more.
- **#2594 is unchanged by the cache.** Each scalar UDF still evaluates its input, so a traced
  input fires twice per value in arm 4 as in arm 3. `SqlFunctions.let` brings it to one and does
  not change the parse count. §4.6's condition, "a traced input is still bound with
  `SqlFunctions.let`", stands as written.
- **Threads.** With four task threads and a `ThreadLocal` cache, results are identical to the
  struct UDF's in every configuration. The cache is per thread, so there is no locking and no sharing. The tasks' threads are pooled, so a cache outlives a task and a stale entry is harmless
  only because the key is the text itself and the value is immutable. This was not run on a
  cluster, only in one JVM.
- **Limits of this measurement.** The parse is the one regex-based `FhirDateTime.parse`; a
  cheaper parser would shrink every difference, and a dearer one would grow them. Values that
  fail to parse were not measured. Cache hits across rows depend on the data and are not
  something to rely on.

## Reproduction

```
specs/001-parquet-on-fhir/evidence/scripts/annotations-design/memoisation/run.sh \
  <count|time|trace> <wholeStage|nowholestage|interpreted> <array length> [rows] [cache] [contexts]
```

`count 300000` for the parse tables (`3 300000 32`), `trace 100000` for the trace table, `count`
with arrays of 3, 20 and 50 and caches of 1 to 128 for the sweep, and `time 1000000` for the times.
`MASTER` overrides `local[4]`. Raw output is in `.local/work/issue/2832_41-42_decimal-and-annotations/spikes/memoisation/`.
