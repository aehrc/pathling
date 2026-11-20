# Ticket 43: lazy annotations, measured

The before and after figures ticket 43's brief asks for: the engine's date paths timed against
the build before the ticket, over unannotated and annotated data, after the accessor model for
dates (design §4.3) and the comparison key of decision 151 were built.

**The date deduplication fix, at `bbd458ce50` against `89f52d6c01`, two rounds (owner, 2026-11-11).**

- **A union and a deduplication of dates are 32% to 37% faster over unannotated data,** where the
  pairwise deduplication of `a73b40542e` made them 15% to 19% and 5% to 8% slower. Each date's key
  is computed once: its text where it has no time part, with no parse, and otherwise its range
  through the accessors. An element is kept where `array_position` of its key in the keys is its
  own position, so only native functions run over the pairs.
- **Over annotated data a union is unchanged (+1%, −4%), and a deduplication is 4% to 11% slower.**
  The annotated dates keep their stored bounds, which the pairwise comparison read directly; the
  key is a structure built from them.
- **Still slower, over unannotated data: a comparison inside a lambda, by 5% and 10%** (as at
  `a73b40542e`; the plain conjunct reads one bound more, and binding the bounds once is #2889).
- **A union of complex values measured 9% and 21% slower over unannotated data, and 5% and 11%
  over annotated data. This is spread, not a cost of the change.** Between `a73b40542e`, where the
  same context was unchanged within the spread (+0%, −1%, +11%), and `bbd458ce50`, neither
  `encoders/src/main` nor `DefaultComparator` changed, and the merge of `issue/2832` brought in tests
  only, so the plan of a complex union is the same in both builds.
- **Searches: the only supported claim is that there is no large regression.** A date search
  measured +2% and +21%, and a string search +69% and +15%, where the third round of
  `a73b40542e`'s run put both within 3%. The method cannot resolve a small difference here. The
  contexts run in a fixed order in one JVM, after the heavy union queries. The scan dominates the
  time of a search. The data is not sorted, so no row group is skipped and a pushed filter cannot
  show. The build before drifts by itself between rounds, a string search from 125 to 108 ms. The
  extra conjunct on `__end` in `ge` is a plausible small cost of an unannotated date search, not
  attributed by these runs.

**Results, at `a73b40542e` against `89f52d6c01`, three rounds.**

- **A date that is only output under a repeating parent computes nothing,** and takes 78% less
  time over unannotated data in every round, which closes the cost ticket 42 left for this ticket
  (finding 2).
- **Over annotated data a comparison reads the stored bounds plainly,** and takes 17% to 40% less
  time as a singular comparison and 15% to 29% less inside a lambda.
- **Date search and a date filter push a filter on the stored bounds to the scan** over annotated
  Parquet (decision 144; `AnnotationPlanBaselineTest`). The probe's data is not sorted, so no row
  group is skipped and the times are unchanged within the spread, apart from one outlying first
  round.
- **Slower, over unannotated data:**
  - **a union of dates, by 15% to 19%.** The merge keeps only the bounds every side has (decision
    142, D9). The stored dates and the computed ones meet without bounds, so the pairwise
    deduplication computes each bound once for every pair it compares, where before each date
    carried its range.
  - **the deduplication of a date, by 5% to 8%,** for the same reason: a traversed date carries no
    bounds, so the pairwise comparison computes them for each pair.
  - **a comparison inside a lambda, by 5% to 12%,** where the build after the date slice alone
    measured +1% and +0% (`a1fe64b397`, below). The plain conjunct beside the comparison reads one
    bound more; the extra cost is not separated from the spread between runs.
  - **a date search, by 3% to 13%.**
- **A deduplication of dates by a key computed once per element was slower still,** +22% to +31%
  for the union and +25% for the deduplication over unannotated data, and +55% to +68% over
  annotated data (`d22d060ac7`, below). It was removed in `a73b40542e`; dates are compared pairwise,
  as before the ticket.
- **Unchanged within the spread:** a union of strings and of complex values, whose deduplication
  is now `distinct_on_key`, resolved to `array_distinct(concat(...))` rather than `array_union`;
  a string search, whose match is now `isnotnull(m) AND m` rather than `coalesce(m, false)`; and the
  other contexts.

## Conditions

- Spark 4.0.2, Java 21, `local[4]` on an Apple M3 Pro, session time zone UTC, adaptive execution
  off, Parquet timestamps written as `TIMESTAMP_MICROS`.
- Branch `issue/2832_43_lazy-annotations`. The build before is `89f52d6c01` (`issue/2832`, ticket
  42 done). The builds after are `a1fe64b397` (the date slice), `d22d060ac7` (with a deduplication
  key for dates built on `aggregate`), `a73b40542e` (pairwise again) and `bbd458ce50` (the key
  computed once per date, with `array_position`). Each build's shaded jar is
  `benchmark/target/benchmark-11.0.0-SNAPSHOT.jar`, so every jar was copied, as built, to a name
  carrying its commit: `base-89f52d6c01.jar`, `dates-a1fe64b397.jar`, `keys-d22d060ac7.jar`,
  `head-a73b40542e.jar` and `dedupe-bbd458ce50.jar`, under
  `.local/work/issue/2832_43_lazy-annotations/jars/`. Each table names the two it compares.
- 500,000 Patients from ticket 42's generator: a birth date and three names, each with a period
  start drawn from a pool of 5,000 dates of mixed precision. Written once unannotated, and once
  annotated by `PathlingContext.annotate`, and read back from Parquet.
- Each context is run once to warm up, then five times into the `noop` sink; the median is
  reported. The contexts run in the order of the tables, in one JVM per jar. The two jars of a comparison are run alternately, in two or three rounds. Within one
  jar the spread between rounds reaches about 10% on the longer contexts.

## Throughput

Median milliseconds. "B" is the build before, "A" the build after, and the change is A against B
in the same round.

### `bbd458ce50`, dates deduplicated on a key computed once per date

| Data | Context | B r1 | A r1 | B r2 | A r2 | r1 | r2 |
|---|---|---|---|---|---|---|---|
| unannotated | compare-singular | 206 | 196 | 187 | 203 | −5% | +9% |
| unannotated | compare-in-lambda | 791 | 833 | 814 | 897 | +5% | +10% |
| unannotated | output-in-lambda | 668 | 147 | 674 | 158 | −78% | −77% |
| unannotated | union | 1883 | 1183 | 1867 | 1265 | −37% | −32% |
| unannotated | distinct | 1320 | 890 | 1317 | 892 | −33% | −32% |
| unannotated | string-union | 102 | 103 | 106 | 104 | +1% | −2% |
| unannotated | complex-union | 215 | 235 | 216 | 262 | +9% | +21% |
| unannotated | string-search | 125 | 211 | 108 | 124 | +69% | +15% |
| unannotated | search | 195 | 199 | 177 | 214 | +2% | +21% |
| unannotated | filter | 201 | 190 | 190 | 194 | −5% | +2% |
| annotated | compare-singular | 37 | 19 | 32 | 34 | −49% | +6% |
| annotated | compare-in-lambda | 259 | 210 | 262 | 229 | −19% | −13% |
| annotated | output-in-lambda | 200 | 189 | 191 | 200 | −6% | +5% |
| annotated | union | 1064 | 1079 | 1115 | 1067 | +1% | −4% |
| annotated | distinct | 691 | 768 | 733 | 765 | +11% | +4% |
| annotated | string-union | 113 | 111 | 114 | 115 | −2% | +1% |
| annotated | complex-union | 443 | 465 | 443 | 491 | +5% | +11% |
| annotated | string-search | 118 | 117 | 125 | 126 | −1% | +1% |
| annotated | search | 61 | 53 | 64 | 57 | −13% | −11% |
| annotated | filter | 59 | 54 | 69 | 68 | −8% | −1% |

### `a73b40542e`, with pairwise date deduplication

| Data | Context | B r1 | A r1 | B r2 | A r2 | B r3 | A r3 | r1 | r2 | r3 |
|---|---|---|---|---|---|---|---|---|---|---|
| unannotated | compare-singular | 202 | 199 | 203 | 211 | 199 | 186 | −1% | +4% | −7% |
| unannotated | compare-in-lambda | 795 | 831 | 805 | 878 | 788 | 880 | +5% | +9% | +12% |
| unannotated | output-in-lambda | 660 | 142 | 669 | 146 | 649 | 143 | −78% | −78% | −78% |
| unannotated | union | 1886 | 2173 | 1855 | 2171 | 1832 | 2188 | +15% | +17% | +19% |
| unannotated | distinct | 1308 | 1373 | 1296 | 1357 | 1275 | 1374 | +5% | +5% | +8% |
| unannotated | string-union | 113 | 100 | 106 | 110 | 102 | 101 | −12% | +4% | −1% |
| unannotated | complex-union | 222 | 222 | 231 | 229 | 210 | 233 | +0% | −1% | +11% |
| unannotated | string-search | 106 | 134 | 110 | 111 | 106 | 107 | +26% | +1% | +1% |
| unannotated | search | 177 | 190 | 178 | 201 | 182 | 188 | +7% | +13% | +3% |
| unannotated | filter | 187 | 192 | 187 | 187 | 185 | 190 | +3% | +0% | +3% |
| annotated | compare-singular | 36 | 29 | 29 | 24 | 30 | 18 | −19% | −17% | −40% |
| annotated | compare-in-lambda | 277 | 198 | 252 | 214 | 273 | 213 | −29% | −15% | −22% |
| annotated | output-in-lambda | 193 | 187 | 187 | 185 | 191 | 184 | −3% | −1% | −4% |
| annotated | union | 1097 | 1058 | 1047 | 1082 | 1070 | 1082 | −4% | +3% | +1% |
| annotated | distinct | 701 | 697 | 679 | 702 | 686 | 689 | −1% | +3% | +0% |
| annotated | string-union | 114 | 108 | 110 | 116 | 110 | 109 | −5% | +5% | −1% |
| annotated | complex-union | 453 | 471 | 483 | 481 | 431 | 482 | +4% | −0% | +12% |
| annotated | string-search | 121 | 117 | 117 | 120 | 118 | 122 | −3% | +3% | +3% |
| annotated | search | 56 | 108 | 51 | 52 | 51 | 55 | +93% | +2% | +8% |
| annotated | filter | 54 | 80 | 54 | 56 | 53 | 54 | +48% | +4% | +2% |

The first round's annotated search and filter of the after build are single outliers; the
other two rounds agree with the earlier runs below.

### `a1fe64b397`, the date slice alone

| Data | Context | B r1 | A r1 | B r2 | A r2 | r1 | r2 |
|---|---|---|---|---|---|---|---|
| unannotated | compare-singular | 206 | 198 | 201 | 194 | −4% | −3% |
| unannotated | compare-in-lambda | 823 | 834 | 826 | 828 | +1% | +0% |
| unannotated | output-in-lambda | 663 | 148 | 674 | 144 | −78% | −79% |
| unannotated | union | 1915 | 2142 | 1885 | 2136 | +12% | +13% |
| unannotated | distinct | 1358 | 1335 | 1333 | 1331 | −2% | −0% |
| unannotated | search | 183 | 187 | 218 | 190 | +2% | −13% |
| unannotated | filter | 205 | 197 | 211 | 204 | −4% | −3% |
| annotated | compare-singular | 37 | 23 | 35 | 23 | −38% | −34% |
| annotated | compare-in-lambda | 301 | 206 | 256 | 204 | −32% | −20% |
| annotated | output-in-lambda | 207 | 191 | 192 | 185 | −8% | −4% |
| annotated | union | 1111 | 1063 | 1064 | 1039 | −4% | −2% |
| annotated | distinct | 723 | 700 | 698 | 667 | −3% | −4% |
| annotated | search | 56 | 58 | 53 | 55 | +4% | +4% |
| annotated | filter | 62 | 56 | 57 | 54 | −10% | −5% |

### `d22d060ac7`, with a deduplication key for dates (removed)

| Data | Context | B r1 | A r1 | B r2 | A r2 | r1 | r2 |
|---|---|---|---|---|---|---|---|
| unannotated | union | 1903 | 2501 | 1883 | 2304 | +31% | +22% |
| unannotated | distinct | 1310 | 1635 | 1297 | 1626 | +25% | +25% |
| annotated | union | 1067 | 1652 | 1019 | 1672 | +55% | +64% |
| annotated | distinct | 683 | 1112 | 668 | 1122 | +63% | +68% |

The key paired each date with its range and kept a date in one `aggregate` only where
`array_contains` had not seen its key. For the three or four dates of a row this costs more than
comparing every pair.

## Reproduction

```
mvn package -pl benchmark -am -DskipTests      # at each commit; keep benchmark/target/benchmark-11.0.0-SNAPSHOT.jar
bash scripts/ticket-43/run.sh <jar> <work dir> 500000 5
```

`scripts/ticket-43/LazyAnnotationProbe.java` extends ticket 42's `EngineAnnotationProbe` with the
annotated data, unions of dates, strings and complex values, a deduplication of dates, a string
search, and a date comparison used as a filter. The runs of `a1fe64b397` and `d22d060ac7` were
taken before the string and complex unions and the string search were added. The jars were built
with `mvn -o package -pl benchmark -am -DskipTests`, whose shaded jar is
`benchmark/target/benchmark-11.0.0-SNAPSHOT.jar`. The
data is written on the first run into the work directory and reused by every later run, so every
jar reads the same files.
