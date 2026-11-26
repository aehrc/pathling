# The YAML suites under the inferred and defined loads

## Baseline: today's inferred loads

Taken for T043 of [tasks.md](../tasks.md), before slice 06 changes how the YAML harness loads its
subjects, so that slice 06 (T048) can compare every case's result with this one. Nothing in the
source tree was changed for it.

- **Commit**: `43c0e6147eedfdb452741227b4bc6d2d4709660c` (`issue/2881_06a-yaml-baseline`), with the
  `sql-on-fhir` submodule at `238d9b2be4ad0d66f9b5a9e640f6337f85b7904c`.
- **Date**: 2026-11-26, Java 21.0.3 on macOS arm64, Spark 4.1.0 (the build's own).
- **Command**, from the root of the checkout:

    ```bash
    scripts/mvn-isolated.sh test -pl fhirpath -am -Dpathling.testForkCount=3 \
      -Dtest='YamlReferenceImplTest,YamlFhirPathTest' -Dsurefire.failIfNoSpecifiedTests=false \
      -Djacoco.skip=true
    ```

    It ended in `BUILD SUCCESS`: 3,092 tests run, no failures, no errors, 866 skipped.

The Surefire reports, the Maven log, the script that classified them (`classify.py`) and its output
(`baseline.tsv`, which also holds each case's raw assertion log line) are kept outside the
repository in `.local/work/issue/2881/yaml-baseline/`. To compare a later run, run it the same way
and then `python3 classify.py <surefire-reports-dir> <out.tsv>` and diff the result column by key.

### Results

A case is identified by its suite, its test method (one per case file) and its JUnit invocation
index within that method, which follows the order of the cases in the file after disabled cases
are dropped. The description and expression are given to check the identity: 17 groups of cases in
the same method share both, so they do not identify a case by themselves.

Each case has one of these results:

| Result                 | Meaning                                                                                                    |
| ---------------------- | ---------------------------------------------------------------------------------------------------------- |
| `passed`               | Not excluded, and passed.                                                                                  |
| `unsupported`          | Not excluded, and skipped because evaluation raised `UnsupportedFhirPathFeatureError`. The cause is given. |
| `excluded:error`       | Excluded with outcome `error`, and raised the expected error, so it was reported as skipped.               |
| `excluded:failure`     | Excluded with outcome `failure`, and failed its assertion as expected, so it was reported as skipped.      |
| `excluded:pass`        | Excluded with outcome `pass`, and passed, so it was reported as skipped.                                   |
| `excluded:unsupported` | Excluded, but skipped as unsupported before its outcome was checked. The cause is given.                   |
| `failed`               | Failed or errored. None did.                                                                               |

An excluded case is executed, and the run fails if it does not behave as its rule records, so every
`excluded:*` result also means the rule's recorded outcome held. A rule is shown by its `id` (an
issue number, or `null`) and its title.

Some cases never become tests, so they have no result: the 46 cases marked `disable: true` in the
case files the suites run (the log's `Skipping disabled test case` warnings: 36 in
`fhir-r4.yaml`, 4 in `6.6_math.yaml`, 5 in `5.6_string_manipulation.yaml` and 1 in
`6.2_comparision.yaml`), and the three cases of the `testConformsTo` group of `fhir-r4.yaml`,
which a `disable: true` on the group drops without a warning.

| Suite                   | Cases | `passed` | `unsupported` | `excluded:error` | `excluded:failure` | `excluded:pass` | `excluded:unsupported` | `failed` |
| ----------------------- | ----: | -------: | ------------: | ---------------: | -----------------: | --------------: | ---------------------: | -------: |
| `YamlReferenceImplTest` | 1,824 |      959 |           573 |              100 |                 65 |               3 |                    124 |        0 |
| `YamlFhirPathTest`      | 1,268 |    1,267 |             0 |                1 |                  0 |               0 |                      0 |        0 |
| Total                   | 3,092 |    2,226 |           573 |              101 |                 65 |               3 |                    124 |        0 |

Cases per test method:

| Suite                   | Method                       | Cases | `passed` | `unsupported` | `excluded:error` | `excluded:failure` | `excluded:pass` | `excluded:unsupported` |
| ----------------------- | ---------------------------- | ----: | -------: | ------------: | ---------------: | -----------------: | --------------: | ---------------------: |
| `YamlFhirPathTest`      | `testDateTimeComparison`     |    37 |       37 |             0 |                0 |                  0 |               0 |                      0 |
| `YamlFhirPathTest`      | `testMath`                   |     4 |        4 |             0 |                0 |                  0 |               0 |                      0 |
| `YamlFhirPathTest`      | `testSearchParams`           |  1227 |     1226 |             0 |                1 |                  0 |               0 |                      0 |
| `YamlReferenceImplTest` | `testAggregate`              |    29 |        0 |            18 |               10 |                  0 |               0 |                      1 |
| `YamlReferenceImplTest` | `testBooleanLogic`           |    41 |       41 |             0 |                0 |                  0 |               0 |                      0 |
| `YamlReferenceImplTest` | `testCollections`            |    14 |        8 |             2 |                0 |                  0 |               0 |                      4 |
| `YamlReferenceImplTest` | `testCombining`              |    14 |       11 |             0 |                2 |                  1 |               0 |                      0 |
| `YamlReferenceImplTest` | `testComparison`             |    95 |       84 |             1 |                6 |                  4 |               0 |                      0 |
| `YamlReferenceImplTest` | `testConversion`             |    39 |       21 |             8 |                3 |                  7 |               0 |                      0 |
| `YamlReferenceImplTest` | `testEquality`               |   171 |       91 |            48 |               20 |                  8 |               0 |                      4 |
| `YamlReferenceImplTest` | `testExistence`              |    81 |       55 |            19 |                1 |                  6 |               0 |                      0 |
| `YamlReferenceImplTest` | `testExtension`              |    21 |        7 |             0 |                2 |                  4 |               1 |                      7 |
| `YamlReferenceImplTest` | `testFactory`                |    85 |        0 |             0 |                1 |                  0 |               0 |                     84 |
| `YamlReferenceImplTest` | `testFhirQuantity`           |    14 |        6 |             1 |                7 |                  0 |               0 |                      0 |
| `YamlReferenceImplTest` | `testFhirR4`                 |   647 |      463 |           148 |               14 |                  8 |               1 |                     13 |
| `YamlReferenceImplTest` | `testFilteringAndProjection` |    27 |       19 |             3 |                0 |                  5 |               0 |                      0 |
| `YamlReferenceImplTest` | `testHasValue`               |    16 |        0 |            16 |                0 |                  0 |               0 |                      0 |
| `YamlReferenceImplTest` | `testLiterals`               |    18 |       17 |             0 |                0 |                  1 |               0 |                      0 |
| `YamlReferenceImplTest` | `testMath5`                  |    64 |        3 |            61 |                0 |                  0 |               0 |                      0 |
| `YamlReferenceImplTest` | `testMath6`                  |   157 |       35 |           106 |               15 |                  0 |               0 |                      1 |
| `YamlReferenceImplTest` | `testPaths`                  |    22 |        0 |             4 |               11 |                  2 |               0 |                      5 |
| `YamlReferenceImplTest` | `testRepeat`                 |     2 |        2 |             0 |                0 |                  0 |               0 |                      0 |
| `YamlReferenceImplTest` | `testSimple`                 |    46 |       36 |             4 |                2 |                  4 |               0 |                      0 |
| `YamlReferenceImplTest` | `testStringManipulation`     |   100 |       20 |            77 |                2 |                  0 |               1 |                      0 |
| `YamlReferenceImplTest` | `testSubsetting`             |    50 |        4 |            44 |                1 |                  1 |               0 |                      0 |
| `YamlReferenceImplTest` | `testTreeNavigation`         |     5 |        0 |             5 |                0 |                  0 |               0 |                      0 |
| `YamlReferenceImplTest` | `testTypes`                  |    50 |       25 |             4 |                3 |                 13 |               0 |                      5 |
| `YamlReferenceImplTest` | `testUtilityFunctions`       |     6 |        2 |             4 |                0 |                  0 |               0 |                      0 |
| `YamlReferenceImplTest` | `testVariables`              |    10 |        9 |             0 |                0 |                  1 |               0 |                      0 |

### Every case

One line per case, tab-separated, sorted by suite, method and index: suite, method, index, result,
the exclusion rule as `id: title` (empty if none), the `UnsupportedFhirPathFeatureError` message
(empty if none), the description (empty if the case has none) and the expression. Tabs, newlines
and backslashes within a field are escaped as `\t`, `\n` and `\\`. Trailing spaces are not kept, so two
expressions (`testFhirR4` 220 and 525) appear without the trailing space they have
in their case files.

```tsv
suite	method	index	result	rule	unsupported_cause	description	expression
YamlReferenceImplTest	testAggregate	1	unsupported		$total is not supported	group: 7.1 aggregate(aggregator : expression [, init : value]) : value - aggregate() function within the aggregate() iterator	(1|2|3).aggregate($total + (1|2|3|4).aggregate($total + $this, 0), 0) = 30
YamlReferenceImplTest	testAggregate	2	unsupported		$total is not supported	group: 7.1 aggregate(aggregator : expression [, init : value]) : value - Calculate sum of undefined input collection, start with 3	UndefinedInput.aggregate($total + $this, 3) = 3
YamlReferenceImplTest	testAggregate	3	unsupported		$total is not supported	group: 7.1 aggregate(aggregator : expression [, init : value]) : value - Using $index in an expression for the aggregate function	(10|20|30|0).aggregate($total + $index, 0) = 6
YamlReferenceImplTest	testAggregate	4	unsupported		$total is not supported	group: 7.1 aggregate(aggregator : expression [, init : value]) : value - Using the result of the aggregate function	10 + (1|2|3).aggregate($total + $this*$index, 4) = 22
YamlReferenceImplTest	testAggregate	5	excluded:unsupported	null: Parse error	$total is not supported	group: 7.1 aggregate(aggregator : expression [, init : value]) : value - aggregate() function with string initial value	('a'|'b'|'c').aggregate($total & '-' & $this, 'concat')
YamlReferenceImplTest	testAggregate	6	unsupported		$total is not supported	group: 7.1 aggregate(aggregator : expression [, init : value]) : value - aggregate() function for collection with null elements (1)	Patient.name.given.aggregate($total & '-' & $this, '')
YamlReferenceImplTest	testAggregate	7	unsupported		$total is not supported	group: 7.1 aggregate(aggregator : expression [, init : value]) : value - aggregate() function for collection with null elements (2)	Patient.name.given.aggregate($total & '-' & $this & $this.id, '')
YamlReferenceImplTest	testAggregate	8	unsupported		$total is not supported	group: 7.1 aggregate(aggregator : expression [, init : value]) : value - aggregate() function for collection with null initial value	(Patient.name.given[1] | Patient.name.given[2]).aggregate($total & '-' & $this, %context.name.given[0])
YamlReferenceImplTest	testAggregate	9	excluded:error	null: Parse error		group: 7.1 aggregate(aggregator : expression [, init : value]) : value - aggregate() function with quantity values	(1 'cm'|3 'cm'|1 'm').aggregate($total + $this, 0 'cm') = 104 'cm'
YamlReferenceImplTest	testAggregate	10	excluded:error	null: Parse error		group: 7.1 aggregate(aggregator : expression [, init : value]) : value - aggregate() function with incompatible quantity values	(1 'cm'|3 'cm'|1 'kg'|1 'm').aggregate($total + $this, 0 'cm')
YamlReferenceImplTest	testAggregate	11	excluded:error	null: Parse error		group: Extension functions - sum() function calculates the sum of input collection	(1|2|3|4|5|6|7|8|9).sum() = 45
YamlReferenceImplTest	testAggregate	12	unsupported		Unsupported function: sum	group: Extension functions - sum() function calculates the sum of undefined input collection	UndefinedInput.sum()
YamlReferenceImplTest	testAggregate	13	unsupported		Unsupported function: sum	group: Extension functions - sum() function calculates the sum of input collection with null elements	Patient.name.given.sum()
YamlReferenceImplTest	testAggregate	14	excluded:error	null: Parse error		group: Extension functions - sum() function calculates the sum of quantity values	(1 'cm'|3 'cm'|1 'm').sum() = 104 'cm'
YamlReferenceImplTest	testAggregate	15	excluded:error	null: Parse error		group: Extension functions - min() function returns the minimum value from the input collection	(7|8|9|1|2|3|4|5|6).min() = 1
YamlReferenceImplTest	testAggregate	16	unsupported		Unsupported function: min	group: Extension functions - min() function returns empty value for the undefined input collection	UndefinedInput.min()
YamlReferenceImplTest	testAggregate	17	excluded:error	null: Parse error		group: Extension functions - min() function returns the minimum quantity from the input collection	(1 'cm'|3 'cm'|1 'm').min() = 1 'cm'
YamlReferenceImplTest	testAggregate	18	unsupported		Unsupported function: min	group: Extension functions - min() function for collection with nulls (1)	Functions.collWithNull1.min()
YamlReferenceImplTest	testAggregate	19	unsupported		Unsupported function: min	group: Extension functions - min() function for collection with nulls (2)	Functions.collWithNull2.min()
YamlReferenceImplTest	testAggregate	20	excluded:error	null: Parse error		group: Extension functions - max() function returns the maximum value from the input collection	(7|8|9|1|2|3|4|5|6).max() = 9
YamlReferenceImplTest	testAggregate	21	unsupported		Unsupported function: max	group: Extension functions - max() function returns empty value for the undefined input collection	UndefinedInput.max()
YamlReferenceImplTest	testAggregate	22	excluded:error	null: Parse error		group: Extension functions - max() function returns the maximum quantity from the input collection	(1 'cm'|3 'cm'|1 'm').max() = 1 'm'
YamlReferenceImplTest	testAggregate	23	unsupported		Unsupported function: max	group: Extension functions - max() function for collection with nulls (1)	Functions.collWithNull1.max()
YamlReferenceImplTest	testAggregate	24	unsupported		Unsupported function: max	group: Extension functions - max() function for collection with nulls (2)	Functions.collWithNull2.max()
YamlReferenceImplTest	testAggregate	25	excluded:error	null: Parse error		group: Extension functions - avg() function calculates the average value for the input collection	(7|8|9|1|2|3|4|5|6).avg() = 5
YamlReferenceImplTest	testAggregate	26	excluded:error	null: Parse error		group: Extension functions - avg() function calculates the average quantity for the input collection	(7 'm' |8 'm'|9 'm'|1 'm'|2 'm'|3 'm'|400 'cm'|5 'm'|6 'm').avg() = 5 'm'
YamlReferenceImplTest	testAggregate	27	unsupported		Unsupported function: avg	group: Extension functions - avg() function for collection with nulls (1)	Functions.collWithNull1.avg()
YamlReferenceImplTest	testAggregate	28	unsupported		Unsupported function: avg	group: Extension functions - avg() function for collection with nulls (2)	Functions.collWithNull2.avg()
YamlReferenceImplTest	testAggregate	29	unsupported		Unsupported function: avg	group: Extension functions - avg() function returns an empty value for the undefined input collection	UndefinedInput.avg()
YamlReferenceImplTest	testBooleanLogic	1	passed			* and (true and true)	ok1 and ok2
YamlReferenceImplTest	testBooleanLogic	2	passed			* and (true and false)	ok1 and ups1
YamlReferenceImplTest	testBooleanLogic	3	passed			* and (true and false)	ok1 and ups1
YamlReferenceImplTest	testBooleanLogic	4	passed			* and (false and false)	ok1 and ups1
YamlReferenceImplTest	testBooleanLogic	5	passed			* and ({} and true)	emp and ok1
YamlReferenceImplTest	testBooleanLogic	6	passed			* and (true and {})	ok1 and emp
YamlReferenceImplTest	testBooleanLogic	7	passed			* and ({} and false)	emp and ups1
YamlReferenceImplTest	testBooleanLogic	8	passed			* and (false and {})	ups1 and emp
YamlReferenceImplTest	testBooleanLogic	9	passed			* and (true and null)	ok1 and i
YamlReferenceImplTest	testBooleanLogic	10	passed			* and (false and null)	ups1 and i
YamlReferenceImplTest	testBooleanLogic	11	passed			* or (true or true)	ok1 or ok2
YamlReferenceImplTest	testBooleanLogic	12	passed			* or (true or false)	ok1 or ups1
YamlReferenceImplTest	testBooleanLogic	13	passed			* or (false or false)	ups2 or ups1
YamlReferenceImplTest	testBooleanLogic	14	passed			* or ({} or true)	emp or ok1
YamlReferenceImplTest	testBooleanLogic	15	passed			* or (true or {})	ok1 or emp
YamlReferenceImplTest	testBooleanLogic	16	passed			* or ({} or false)	emp or ups1
YamlReferenceImplTest	testBooleanLogic	17	passed			* or (false or {})	ups1 or emp
YamlReferenceImplTest	testBooleanLogic	18	passed			* or ({} or {})	emp or emp
YamlReferenceImplTest	testBooleanLogic	19	passed			* or (true or null)	ok1 or i
YamlReferenceImplTest	testBooleanLogic	20	passed			* or (false or null)	ups1 or i
YamlReferenceImplTest	testBooleanLogic	21	passed			** not() for non-empty collection	(0).not()
YamlReferenceImplTest	testBooleanLogic	22	passed			* xor (true xor true)	ok1 xor ok2
YamlReferenceImplTest	testBooleanLogic	23	passed			* xor (true xor false)	ok1 xor ups1
YamlReferenceImplTest	testBooleanLogic	24	passed			* xor (false xor true)	ups1 xor ok1
YamlReferenceImplTest	testBooleanLogic	25	passed			* xor (false xor false)	ups2 xor ups1
YamlReferenceImplTest	testBooleanLogic	26	passed			* xor ({} xor true)	emp xor ok1
YamlReferenceImplTest	testBooleanLogic	27	passed			* xor (true xor {})	ok1 xor emp
YamlReferenceImplTest	testBooleanLogic	28	passed			* xor ({} xor false)	emp xor ups1
YamlReferenceImplTest	testBooleanLogic	29	passed			* xor (false xor {})	ups1 xor emp
YamlReferenceImplTest	testBooleanLogic	30	passed			* xor ({} xor {})	emp xor emp
YamlReferenceImplTest	testBooleanLogic	31	passed			* xor (true xor null)	ok1 xor i
YamlReferenceImplTest	testBooleanLogic	32	passed			* xor (false xor null)	ups1 xor i
YamlReferenceImplTest	testBooleanLogic	33	passed			* implies (true implies true)	ok1 implies ok2
YamlReferenceImplTest	testBooleanLogic	34	passed			* implies (true implies false)	ok1 implies ups1
YamlReferenceImplTest	testBooleanLogic	35	passed			* implies (false implies true)	ups1 implies ok1
YamlReferenceImplTest	testBooleanLogic	36	passed			* implies (false implies false)	ups2 implies ups1
YamlReferenceImplTest	testBooleanLogic	37	passed			* implies ({} implies true)	emp implies ok1
YamlReferenceImplTest	testBooleanLogic	38	passed			* implies (true implies {})	ok1 implies emp
YamlReferenceImplTest	testBooleanLogic	39	passed			* implies ({} implies false)	emp implies ups1
YamlReferenceImplTest	testBooleanLogic	40	passed			* implies (false implies {})	ups1 implies emp
YamlReferenceImplTest	testBooleanLogic	41	passed			* implies ({} implies {})	emp implies emp
YamlReferenceImplTest	testCollections	1	passed				coll | coll2
YamlReferenceImplTest	testCollections	2	excluded:unsupported	#2420: Unsupported equality for complex types	Unsupported equality for complex types		el in coll
YamlReferenceImplTest	testCollections	3	excluded:unsupported	#2420: Unsupported equality for complex types	Unsupported equality for complex types		not_el in coll
YamlReferenceImplTest	testCollections	4	passed			If the left-hand side of the operator is the empty collection is empty, the result is empty	ups in coll
YamlReferenceImplTest	testCollections	5	passed			if the right-hand side is empty, the result is false	el in emptycoll
YamlReferenceImplTest	testCollections	6	passed			empty in empty is empty	ups in emptycoll
YamlReferenceImplTest	testCollections	7	unsupported		Unsupported equality for complex types	If the left operand has multiple items, an exception is thrown	coll in coll
YamlReferenceImplTest	testCollections	8	excluded:unsupported	#2420: Unsupported equality for complex types	Unsupported equality for complex types		coll contains el
YamlReferenceImplTest	testCollections	9	excluded:unsupported	#2420: Unsupported equality for complex types	Unsupported equality for complex types		coll contains not_el
YamlReferenceImplTest	testCollections	10	passed				coll contains empty
YamlReferenceImplTest	testCollections	11	passed			reverse of: If the left-hand side of the operator is the empty collection is empty, the result is empty	coll contains ups
YamlReferenceImplTest	testCollections	12	passed			reverse of: if the right-hand side is empty, the result is false	emptycoll contains el
YamlReferenceImplTest	testCollections	13	passed			empty in empty is empty	emptycoll contains ups
YamlReferenceImplTest	testCollections	14	unsupported		Unsupported equality for complex types	reverse of: If the left operand has multiple items, an exception is thrown	coll contains coll
YamlReferenceImplTest	testCombining	1	passed			** Unioning empty coll with non-exists coll  is empty coll	Functions.coll1.nothing | Functions.attrempty
YamlReferenceImplTest	testCombining	2	passed			** Unioning empty coll with non-distinct coll is coll without duplicates	Functions.attrempty | Functions.coll1.colltrue.attr
YamlReferenceImplTest	testCombining	3	excluded:error	#2398: Polymorphic unions and combines not supported		** Unioning colls	Functions.attrdouble | Functions.coll1.colltrue.attr
YamlReferenceImplTest	testCombining	4	passed			** Unioning colls 2	Functions.attrdouble | Functions.coll1.coll2.attr
YamlReferenceImplTest	testCombining	5	excluded:failure	null: Indefinite calendar duration union deduplication		** should use year-to-month conversion factor (https://hl7.org/fhirpath/#equals)	(1 year | 12 months)
YamlReferenceImplTest	testCombining	6	passed			** should compare quantities for equality (https://hl7.org/fhirpath/#equals)	(3 'min' | 180 seconds)
YamlReferenceImplTest	testCombining	7	passed			** should not depend on the order of properties in an object	Functions.objects.group1 | Functions.objects.group2
YamlReferenceImplTest	testCombining	8	passed			** union of coll with nodes that have null values (1)	(Patient.name.given[0] | Patient.name.given[3]).count()
YamlReferenceImplTest	testCombining	9	passed			** union of coll with nodes that have null values (2)	Patient.name.given[0].union(Patient.name.given[3]).count()
YamlReferenceImplTest	testCombining	10	passed			** Combine empty coll with non-exists coll is empty coll	Functions.attrempty.combine(Functions.nothing)
YamlReferenceImplTest	testCombining	11	passed			** Combine empty coll with non-empty coll	Functions.attrempty.combine(Functions.coll1.colltrue.attr)
YamlReferenceImplTest	testCombining	12	excluded:error	#2398: Polymorphic unions and combines not supported		** Combine colls	Functions.attrdouble.combine(Functions.coll1.colltrue.attr)
YamlReferenceImplTest	testCombining	13	passed			** Combine colls 2	Functions.attrdouble.combine(Functions.coll1.coll2.attr)
YamlReferenceImplTest	testCombining	14	passed			** Combine coll with nodes that have null values	(Patient.name.given[0]).combine(Patient.name.given[3]).count()
YamlReferenceImplTest	testComparison	1	passed			less than, expecting true	a < b
YamlReferenceImplTest	testComparison	2	passed			less than, expecting false	b < a
YamlReferenceImplTest	testComparison	3	passed			less than, with mismatched types	c < d
YamlReferenceImplTest	testComparison	4	passed			less than, with empty value	c < e
YamlReferenceImplTest	testComparison	5	passed			less than, with null value	c < i
YamlReferenceImplTest	testComparison	6	passed			less than, with too many values	c < g
YamlReferenceImplTest	testComparison	7	passed			less than, expecting true	a < h
YamlReferenceImplTest	testComparison	8	passed			less than, expecting false	j < d
YamlReferenceImplTest	testComparison	9	passed			greater than, expecting true	b > a
YamlReferenceImplTest	testComparison	10	passed			greater than, expecting false	a > b
YamlReferenceImplTest	testComparison	11	passed			greater than, with mismatched types	d > c
YamlReferenceImplTest	testComparison	12	passed			greater than, with empty value	e > a
YamlReferenceImplTest	testComparison	13	passed			greater than, with null value	c > i
YamlReferenceImplTest	testComparison	14	passed			greater than, with too many values	c > g
YamlReferenceImplTest	testComparison	15	passed			greater than, expecting true	h > a
YamlReferenceImplTest	testComparison	16	passed			greater than, expecting false	d > j
YamlReferenceImplTest	testComparison	17	passed			greater or equal than, expecting true	b >= c
YamlReferenceImplTest	testComparison	18	passed			greater or equal than, expecting false	a >= b
YamlReferenceImplTest	testComparison	19	passed			greater or equal than, with mismatched types	d >= c
YamlReferenceImplTest	testComparison	20	passed			greater or equal than, with empty value	e >= a
YamlReferenceImplTest	testComparison	21	passed			greater or equal than, with null value	c >= i
YamlReferenceImplTest	testComparison	22	passed			greater or equal than, with too many values	c >= g
YamlReferenceImplTest	testComparison	23	passed			greater or equal than, expecting true	c >= h
YamlReferenceImplTest	testComparison	24	passed			greater or equal than, expecting false	d >= j
YamlReferenceImplTest	testComparison	25	passed			less or equal than, expecting true	b <= c
YamlReferenceImplTest	testComparison	26	passed			less than or equal, expecting false	b <= a
YamlReferenceImplTest	testComparison	27	passed			less or equal than, with mismatched types	c <= d
YamlReferenceImplTest	testComparison	28	passed			less or equal than, with empty value	c <= e
YamlReferenceImplTest	testComparison	29	passed			less or equal than, with null value	c <= i
YamlReferenceImplTest	testComparison	30	passed			less or equal than, expecting true	a <= h
YamlReferenceImplTest	testComparison	31	passed			less than or equal, expecting false	j <= d
YamlReferenceImplTest	testComparison	32	passed			group: Quantities	1 year > 1 'a'
YamlReferenceImplTest	testComparison	33	passed			group: Quantities	1 's' > 1 year
YamlReferenceImplTest	testComparison	34	passed			group: Quantities	1 'min' < 2 minutes
YamlReferenceImplTest	testComparison	35	passed			group: Quantities	1 'kg' < 2 's'
YamlReferenceImplTest	testComparison	36	excluded:failure	#2494: Unsupported comparison above second calendar durations		group: Quantities	1 year > 12 months
YamlReferenceImplTest	testComparison	37	excluded:failure	#2494: Unsupported comparison above second calendar durations		group: Quantities	1 year < 12 months
YamlReferenceImplTest	testComparison	38	excluded:failure	#2494: Unsupported comparison above second calendar durations		group: Quantities	1 minute > 59 seconds
YamlReferenceImplTest	testComparison	39	excluded:failure	#2494: Unsupported comparison above second calendar durations		group: Quantities	1 minute < 61 seconds
YamlReferenceImplTest	testComparison	40	passed			group: Quantities	1 'min' < 61 seconds
YamlReferenceImplTest	testComparison	41	passed			group: Quantities	10 seconds > 1 's'
YamlReferenceImplTest	testComparison	42	passed			group: Quantities	10 seconds < 1 's'
YamlReferenceImplTest	testComparison	43	passed			group: Quantities	1 's' > 10 seconds
YamlReferenceImplTest	testComparison	44	passed			group: Quantities	1 's' < 10 seconds
YamlReferenceImplTest	testComparison	45	passed			group: Quantities	999 'g' < 1 'kg'
YamlReferenceImplTest	testComparison	46	passed			group: Quantities	999 'g' > 1 'kg'
YamlReferenceImplTest	testComparison	47	passed			group: Quantities	1001 'g' > 1 'kg'
YamlReferenceImplTest	testComparison	48	passed			group: Quantities	1001 'g' < 1 'kg'
YamlReferenceImplTest	testComparison	49	passed			group:  Date >	@2018 > @2019
YamlReferenceImplTest	testComparison	50	passed			group:  Date >	@2018 > @2017
YamlReferenceImplTest	testComparison	51	passed			group:  Date > - Raise error on type comparison with string	@2018 > d
YamlReferenceImplTest	testComparison	52	passed			group:  Date > - Raise error on type comparison with integer	@2018 > a
YamlReferenceImplTest	testComparison	53	passed			group:  Date > - Raise error on type comparison with Time	@2018 > @T12:34
YamlReferenceImplTest	testComparison	54	passed			group:  Date > - Comparison based on the lesser precision (1)	@2018-12-31 > @2018
YamlReferenceImplTest	testComparison	55	passed			group:  Date > - Comparison based on the lesser precision (2)	@2018 > @2017-01
YamlReferenceImplTest	testComparison	56	passed			group:  Date > - Comparison with later Date from resource	@2018-12-21 > date.toDate()
YamlReferenceImplTest	testComparison	57	passed			group:  Date >= - Comparison with earlier Date	@2018 >= @2017
YamlReferenceImplTest	testComparison	58	passed			group:  Date >= - Comparison with equal Date	@2018 >= @2018
YamlReferenceImplTest	testComparison	59	passed			group:  Date >= - Comparison with later Date	@2018 >= @2019
YamlReferenceImplTest	testComparison	60	passed			group:  Date >= - Comparison with earlier Date from resource	@2020 >= date.toDate()
YamlReferenceImplTest	testComparison	61	passed			group:  Date >= - Comparison with equal Date from resource	@2019-02-25 >= date.toDate()
YamlReferenceImplTest	testComparison	62	passed			group:  Date >= - Comparison with later Date from resource	@2018 >= date.toDate()
YamlReferenceImplTest	testComparison	63	passed			group:  Date <= - Comparison with earlier Date	@2018 <= @2017
YamlReferenceImplTest	testComparison	64	passed			group:  Date <= - Comparison with equal Date	@2018 <= @2018
YamlReferenceImplTest	testComparison	65	passed			group:  Date <= - Comparison with later Date	@2018 <= @2019
YamlReferenceImplTest	testComparison	66	passed			group:  Date < - Comparison with earlier Date	@2018 < @2017
YamlReferenceImplTest	testComparison	67	passed			group:  Date < - Comparison with equal Date	@2018 < @2018
YamlReferenceImplTest	testComparison	68	passed			group:  Date < - Comparison with later Date	@2018 < @2019
YamlReferenceImplTest	testComparison	69	passed			group:  DateTime - Same date, same time	@2018-12-21T12:01 > @2018-12-21T12:01
YamlReferenceImplTest	testComparison	70	passed			group:  DateTime - Same date, different time (1)	@2018-12-21T12:01 > @2018-12-21T12:02
YamlReferenceImplTest	testComparison	71	passed			group:  DateTime - Same date, different time (2)	@2018-12-21T12:02 > @2018-12-21T12:01
YamlReferenceImplTest	testComparison	72	passed			group:  DateTime - Different dates & times (1)	@2018-12-22T12:02 > @2018-12-21T12:01
YamlReferenceImplTest	testComparison	73	passed			group:  DateTime - Different dates & times (1)	@2018-12-20T12:02 > @2018-12-21T12:01
YamlReferenceImplTest	testComparison	74	passed			group:  DateTime - Comparison with differnet precision (1)	@2018-12-20T12 > @2018-12-20T12:01
YamlReferenceImplTest	testComparison	75	passed			group:  DateTime - Comparison with differnet precision (2)	@2018-12-20T12 > @2018-12-20T11:01
YamlReferenceImplTest	testComparison	76	passed			group:  DateTime - Time zone differences (1)	@2018-12-21T12:02+03:00 > @2018-12-21T12:01+02:00
YamlReferenceImplTest	testComparison	77	passed			group:  DateTime - Time zone differences (2)	@2018-12-21T12:02+02:00 > @2018-12-21T12:01+03:00
YamlReferenceImplTest	testComparison	78	passed			group:  DateTime - Comparison with later Date from resource	@2018-12-21 < date.toDate()
YamlReferenceImplTest	testComparison	79	passed			group:  DateTime - Comparison with later DateTime from resource	@2018-12-21T12:02+02:00 < dateTime.toDateTime()
YamlReferenceImplTest	testComparison	80	passed			group:  DateTime - Comparison with later DateTime from resource (2)	@2018-12-21T12:02+02:00 > dateTime.toDateTime()
YamlReferenceImplTest	testComparison	81	passed			group:  DateTime	@2020-08-04T12:34 < @2020
YamlReferenceImplTest	testComparison	82	passed			group:  Time - Time comparison; GT (1)	@T12:03 > @T12:01
YamlReferenceImplTest	testComparison	83	passed			group:  Time - Time comparison; GT (2)	@T12:01 > @T12:03
YamlReferenceImplTest	testComparison	84	passed			group:  Time - Time comparison; GT (3)	@T12:01:34.123 > @T12:01:34.122
YamlReferenceImplTest	testComparison	85	passed			group:  Time - Time comparison with based on lesser precision (1)	@T12:01 > @T12
YamlReferenceImplTest	testComparison	86	passed			group:  Time - Time comparison with based on lesser precision (2)	@T12:02:34.324 > @T12:01
YamlReferenceImplTest	testComparison	87	excluded:error	null: Parse error		group:  Time - Time comparison with based on lesser precision (3)	@T12+04:00 > @T12:02:34.324+05:00
YamlReferenceImplTest	testComparison	88	excluded:error	null: Parse error		group:  Time - Time comparison with based on lesser precision (4)	@T12:02:34.324+05:00 > @T12+05:00
YamlReferenceImplTest	testComparison	89	excluded:error	null: Parse error		group:  Time - Comparison with later time in resource (1)	@T17:00-05:00 < timeWithT.toTime()
YamlReferenceImplTest	testComparison	90	excluded:error	null: Parse error		group:  Time - Comparison with later time in resource (2)	@T17:00-05:00 > timeWithT.toTime()
YamlReferenceImplTest	testComparison	91	excluded:error	null: Parse error		group:  Time - Comparison with later time in resource (3)	@T17:00-05:00 < timeWithOutT.toTime()
YamlReferenceImplTest	testComparison	92	excluded:error	null: Parse error		group:  Time - Comparison with later time in resource (4)	@T17:00-05:00 > timeWithOutT.toTime()
YamlReferenceImplTest	testComparison	93	unsupported		Unsupported function: now	group:  Instant - Comparison using the greater than operator	now() > DiagnosticReport.issued
YamlReferenceImplTest	testComparison	94	passed			group:  Instant - Comparison using the less than operator	DiagnosticReport.issued < DiagnosticReport.effectiveDateTime
YamlReferenceImplTest	testComparison	95	passed			group:  Instant - Comparison using the equality operator	DiagnosticReport.issued = @2005-01-27T06:40:01Z
YamlReferenceImplTest	testConversion	1	passed			** boolean - true	true
YamlReferenceImplTest	testConversion	2	passed			** boolean - false	false
YamlReferenceImplTest	testConversion	3	unsupported		Unsupported function: iif	** iif	Functions.iif(true, 'a', 'b')
YamlReferenceImplTest	testConversion	4	unsupported		Unsupported function: iif	** iif 2	Functions.iif(false, 'a', 'b')
YamlReferenceImplTest	testConversion	5	unsupported		Unsupported function: iif	** iif 3	Functions.iif(coll1[0].coll2[0].attr = 1, coll1[0].coll2[0].attr, coll1[0].coll2[1].attr)
YamlReferenceImplTest	testConversion	6	unsupported		Unsupported function: iif	** iif 4	Functions.iif(true, 'a')
YamlReferenceImplTest	testConversion	7	unsupported		Unsupported function: iif	** iif 5	Functions.iif(false, 'a')
YamlReferenceImplTest	testConversion	8	unsupported		Unsupported function: iif	** iif for collection with null values	Functions.iif(collWithNullsAndTrue[1], collWithNullsAndTrue[0], collWithNullsAndTrue[2]).id
YamlReferenceImplTest	testConversion	9	passed			** Bool to int	Functions.attrtrue.toInteger()
YamlReferenceImplTest	testConversion	10	passed			** str to int	intstr.toInteger()
YamlReferenceImplTest	testConversion	11	excluded:failure	null: Null values in collections not properly handled		** Null to int	Functions.collWithNullsAndTrue[0].toInteger()
YamlReferenceImplTest	testConversion	12	passed			** Bool to decimal	Functions.attrtrue.toDecimal()
YamlReferenceImplTest	testConversion	13	passed			** str to decimal	decstr.toDecimal()
YamlReferenceImplTest	testConversion	14	excluded:failure	null: Null values in collections not properly handled		** Null to decimal	Functions.collWithNullsAndTrue[0].toDecimal()
YamlReferenceImplTest	testConversion	15	passed			group: toDateTime - equality	datetime.toDateTime() = @2019-02-06T19:17-05:00
YamlReferenceImplTest	testConversion	16	passed			group: toDateTime - inequality	datetime.toDateTime() = @2010-02-06T19:17-05:00
YamlReferenceImplTest	testConversion	17	passed			** Null to dateTime	Functions.collWithNullsAndTrue[0].toDateTime()
YamlReferenceImplTest	testConversion	18	excluded:error	null: Parse error		group: toTime - equality	time.toTime() = @T19:15-05:00
YamlReferenceImplTest	testConversion	19	excluded:error	null: Parse error		group: toTime - inequality	time.toTime() = @T18:15-05:00
YamlReferenceImplTest	testConversion	20	excluded:error	null: Parse error		group: toTime - inequality operator	time.toTime() != @T18:15-05:00
YamlReferenceImplTest	testConversion	21	passed			** Null to time	Functions.collWithNullsAndTrue[0].toTime()
YamlReferenceImplTest	testConversion	22	passed			** Null to date	Functions.collWithNullsAndTrue[0].toDate()
YamlReferenceImplTest	testConversion	23	excluded:failure	null: Null values in collections not properly handled		** Null to boolean	Functions.collWithNullsAndTrue[0].toBoolean()
YamlReferenceImplTest	testConversion	24	passed			** bool to string	attrtrue.toString()
YamlReferenceImplTest	testConversion	25	passed			** bool to string	attrdec.toString()
YamlReferenceImplTest	testConversion	26	passed			** null to string	Patient.name.given[0].toString()
YamlReferenceImplTest	testConversion	27	passed			** string to Quantity	'1 \\'wk\\''.toQuantity() = 7 days
YamlReferenceImplTest	testConversion	28	excluded:failure	#2391: UCUM to calendar duration conversions now supported		** string to Quantity	'1 \\'wk\\''.toQuantity('days')
YamlReferenceImplTest	testConversion	29	passed			** string to Quantity	'1 \\'wk\\''.toQuantity('d') = 7 days
YamlReferenceImplTest	testConversion	30	passed			** string to Quantity - result UCUM unit code must be surrounded with single quotes	'1 \\'wk\\''.toQuantity('d').toString() = '7 \\'d\\''
YamlReferenceImplTest	testConversion	31	excluded:failure	null: Unsupported comparison for indefinite and definite time quantities		** calendar duration conversion factor	'1 year'.toQuantity() != 1 'a'
YamlReferenceImplTest	testConversion	32	unsupported		Unsupported operator: ~	** calendar duration conversion factor	'1 year'.toQuantity() ~ 1 'a'
YamlReferenceImplTest	testConversion	33	excluded:failure	null: Unsupported comparison for indefinite and definite time quantities		** Duration to Quantity	MedicationRequest.dispenseRequest.expectedSupplyDuration.toQuantity() = 3 days
YamlReferenceImplTest	testConversion	34	passed			** UCUM units	'1 \\'cm\\''.toQuantity('mm').value = 10
YamlReferenceImplTest	testConversion	35	passed			** Invalid conversion	'1 \\'cm\\''.toQuantity('g')
YamlReferenceImplTest	testConversion	36	unsupported		Unsupported operator: ~	** Implicit to quantity + toQuantity	Observation.value.toQuantity('kg') ~ 84 'kg'
YamlReferenceImplTest	testConversion	37	passed			** Invalid conversion	'1 \\'wk\\''.toQuantity('nonexistent-unit')
YamlReferenceImplTest	testConversion	38	passed			** Invalid conversion	'some not quantity string'.toQuantity()
YamlReferenceImplTest	testConversion	39	excluded:failure	null: Null values in collections not properly handled		** Null to Quantity	Functions.collWithNullsAndTrue[0].toQuantity()
YamlReferenceImplTest	testEquality	1	passed			group: 6.1.1. = (Equals) - ** strings equals	'abc' = 'abc'
YamlReferenceImplTest	testEquality	2	passed			group: 6.1.1. = (Equals) - ** strings equals	Ops.str1 = Ops.str1
YamlReferenceImplTest	testEquality	3	passed			group: 6.1.1. = (Equals) - ** strings not equals	'abc' = 'abcd'
YamlReferenceImplTest	testEquality	4	passed			group: 6.1.1. = (Equals) - ** strings not equals	Ops.str1 = Ops.str2
YamlReferenceImplTest	testEquality	5	passed			group: 6.1.1. = (Equals) - ** utf8 strings	'ɐ ɑ ɒ ɓ ɔ ɕ ɖ ɗ ɘ ə ɚ ɛ ɜ ɝ ɞ ɟ ɠ' = 'ɐ ɑ ɒ ɓ ɔ ɕ ɖ ɗ ɘ ə ɚ ɛ ɜ ɝ ɞ ɟ ɠ'
YamlReferenceImplTest	testEquality	6	passed			group: 6.1.1. = (Equals) - ** utf8 strings	'ɐ ɑ ɒ ɓ ɔ ɕ ɖ ɗ ɘ ə ɚ ɛ ɜ ɝ ɞ ɟ ɠ' = 'ə ɚ ɛ ɜ ɝ ɞ ɟ ɠɐ ɑ ɒ ɓ ɔ ɕ ɖ ɗ ɘ '
YamlReferenceImplTest	testEquality	7	passed			group: 6.1.1. = (Equals) - ** equal	1 = 1
YamlReferenceImplTest	testEquality	8	passed			group: 6.1.1. = (Equals) - ** equal	Ops.int1 = Ops.int1
YamlReferenceImplTest	testEquality	9	passed			group: 6.1.1. = (Equals) - ** not equal	1 = 2
YamlReferenceImplTest	testEquality	10	passed			group: 6.1.1. = (Equals) - ** not equal	Ops.int1 = Ops.int2
YamlReferenceImplTest	testEquality	11	passed			group: 6.1.1. = (Equals) - ** decimals equal	1.01 = 1.01
YamlReferenceImplTest	testEquality	12	passed			group: 6.1.1. = (Equals) - ** decimals equal, trailing zeroes are ignored	0.01 = 00.01
YamlReferenceImplTest	testEquality	13	passed			group: 6.1.1. = (Equals) - decimal not equal	1.01 = 1.02
YamlReferenceImplTest	testEquality	14	passed			group: 6.1.1. = (Equals) - decimal values should not be compared beyond the maximum precision	0.1 + 0.1 + 0.1 = 0.3
YamlReferenceImplTest	testEquality	15	passed			group: 6.1.1. = (Equals) - ** equal	true = true
YamlReferenceImplTest	testEquality	16	passed			group: 6.1.1. = (Equals) - ** not equal	true = false
YamlReferenceImplTest	testEquality	17	passed			group: 6.1.1. = (Equals) - ** propagate empty coll	true = Ops.unexists
YamlReferenceImplTest	testEquality	18	excluded:failure	null: Support for null values within FHIR elements		group: 6.1.1. = (Equals) - ** equality for null values	Patient.name.given[0] = Patient.name.given[3]
YamlReferenceImplTest	testEquality	19	excluded:failure	null: Unsupported comparison for indefinite and definite time quantities		group: Quantity Equality	1 year != 1 'a'
YamlReferenceImplTest	testEquality	20	excluded:failure	#2494: Unsupported comparison above second calendar durations		group: Quantity Equality	12 months = 1 year
YamlReferenceImplTest	testEquality	21	passed			group: Quantity Equality	12 'mo' = 1 'a'
YamlReferenceImplTest	testEquality	22	passed			group: Quantity Equality	1 year = 1 year
YamlReferenceImplTest	testEquality	23	excluded:failure	null: Unsupported comparison for indefinite and definite time quantities		group: Quantity Equality	1 month != 1 'mo'
YamlReferenceImplTest	testEquality	24	passed			group: Quantity Equality	1 month = 30 'd'
YamlReferenceImplTest	testEquality	25	excluded:failure	#2494: Unsupported comparison above second calendar durations		group: Quantity Equality	1 month = 30 days
YamlReferenceImplTest	testEquality	26	passed			group: Quantity Equality	1 week = 1 'wk'
YamlReferenceImplTest	testEquality	27	excluded:failure	#2494: Unsupported comparison above second calendar durations		group: Quantity Equality	1 week = 7 days
YamlReferenceImplTest	testEquality	28	passed			group: Quantity Equality	1 day = 1 'd'
YamlReferenceImplTest	testEquality	29	excluded:failure	#2494: Unsupported comparison above second calendar durations		group: Quantity Equality	1 day = 24 hours
YamlReferenceImplTest	testEquality	30	passed			group: Quantity Equality	1 day = 24 'h'
YamlReferenceImplTest	testEquality	31	passed			group: Quantity Equality	1 second = 1 's'
YamlReferenceImplTest	testEquality	32	unsupported		Quantity arithmetic operations are not yet supported	group: Quantity Equality	10 'kg.m/s2' = 5 'kg' * 4 'm' / (2 seconds * 1000 'ms')
YamlReferenceImplTest	testEquality	33	excluded:unsupported	#2420: Unsupported equality for complex types	Unsupported equality for complex types	group: 6.1.1. = (Equals) - ** two objects	Ops.complex = Ops.complexsame
YamlReferenceImplTest	testEquality	34	excluded:unsupported	#2420: Unsupported equality for complex types	Unsupported equality for complex types	group: 6.1.1. = (Equals) - ** two objects not	Ops.complex = Ops.complexother
YamlReferenceImplTest	testEquality	35	excluded:unsupported	#2420: Unsupported equality for complex types	Unsupported equality for complex types	group: 6.1.1. = (Equals) - ** two colls	Ops.coll = Ops.collsame
YamlReferenceImplTest	testEquality	36	excluded:unsupported	#2420: Unsupported equality for complex types	Unsupported equality for complex types	group: 6.1.1. = (Equals) - ** two colls not	Ops.coll = Ops.collother
YamlReferenceImplTest	testEquality	37	passed			group: 6.1.1. = (Equals) - ** two colls + empty	Ops.coll = Ops.ups
YamlReferenceImplTest	testEquality	38	passed			group: 6.1.1. = (Equals) - ** length mismatch	Ops.coll.a = 1
YamlReferenceImplTest	testEquality	39	passed			group: 6.1.1. = (Equals) - ** length match	Ops.coll.a.first() = 1
YamlReferenceImplTest	testEquality	40	passed			group: 6.1.1. = (Equals) - ** empty right	Ops.coll.a.first() = Ops.unexists
YamlReferenceImplTest	testEquality	41	passed			group: 6.1.1. = (Equals) - ** empty left	Ops.unexists = Ops.coll.a.first()
YamlReferenceImplTest	testEquality	42	passed			group: Dates & Times - DateTime literal compared with DateTime	@2018 = @2018
YamlReferenceImplTest	testEquality	43	passed			group: Dates & Times - DateTime literal compared with different DateTime	@2018 = @2017
YamlReferenceImplTest	testEquality	44	passed			group: Dates & Times - DateTime literal compared with different precision DateTime (1)	@2018 = @2018-02
YamlReferenceImplTest	testEquality	45	passed			group: Dates & Times - DateTime literal compared with different precision DateTime (2)	@2018 = @2019-02
YamlReferenceImplTest	testEquality	46	passed			group: Dates & Times - DateTime literal compared with DateTime (month)	@2018-02 = @2018-02
YamlReferenceImplTest	testEquality	47	passed			group: Dates & Times - DateTime literal compared with different DateTime (month)	@2018-02 = @2018-03
YamlReferenceImplTest	testEquality	48	passed			group: Dates & Times - DateTime literal compared with DateTime (day)	@2018-02-02 = @2018-02-02
YamlReferenceImplTest	testEquality	49	passed			group: Dates & Times - DateTime literal compared with different DateTime (day)	@2018-02-02 = @2018-02-03
YamlReferenceImplTest	testEquality	50	passed			group: Dates & Times - DateTime literal compared with DateTime (hour)	@2018-02-02T11 = @2018-02-02T11
YamlReferenceImplTest	testEquality	51	passed			group: Dates & Times - DateTime literal compared with different DateTime (hour)	@2018-02-02T11 = @2018-02-02T12
YamlReferenceImplTest	testEquality	52	passed			group: Dates & Times - DateTime literal compared with same time, diff. precision (hour)	@2018-02-02T11 = @2018-02-02T11:00
YamlReferenceImplTest	testEquality	53	passed			group: Dates & Times - DateTime literal compared with DateTime (hour, tz)	@2018-02-02T11+04:00 = @2018-02-02T11+04:00
YamlReferenceImplTest	testEquality	54	passed			group: Dates & Times - DateTime literal compared with same DateTime (hour, tz different)	@2018-02-02T22-04:00 = @2018-02-03T06+04:00
YamlReferenceImplTest	testEquality	55	passed			group: Dates & Times - DateTime literal compared with different precision DateTime (hour, tz diff)	@2018-02-02T22-04:00 = @2018-02-03T06:03+04:00
YamlReferenceImplTest	testEquality	56	passed			group: Dates & Times - DateTime literal compared with different precision DateTime (hour, tz diff)	@2018-02-02T22-04:00 = @2018-02-03T05:03+04:00
YamlReferenceImplTest	testEquality	57	passed			group: Dates & Times - DateTime literal compared with different DateTime (hour,tz)	@2018-02-02T11+04:00 = @2018-02-02T12+04:00
YamlReferenceImplTest	testEquality	58	passed			group: Dates & Times - DateTime literal compared with DateTime (minute)	@2018-02-02T11:01 = @2018-02-02T11:01
YamlReferenceImplTest	testEquality	59	passed			group: Dates & Times - DateTime literal compared with different DateTime (minute)	@2018-02-02T11:01 = @2018-02-02T11:02
YamlReferenceImplTest	testEquality	60	passed			group: Dates & Times - DateTime literal compared with DateTime (minute, tz)	@2018-02-02T11:01+04:00 = @2018-02-02T11:01+04:00
YamlReferenceImplTest	testEquality	61	passed			group: Dates & Times - DateTime literal compared with different DateTime (minute, tz)	@2018-02-02T11:01+04:00 = @2018-02-02T11:02+04:00
YamlReferenceImplTest	testEquality	62	passed			group: Dates & Times - DateTime literal compared with DateTime (second)	@2018-02-02T11:01:03 = @2018-02-02T11:01:03
YamlReferenceImplTest	testEquality	63	passed			group: Dates & Times - DateTime literal compared with different DateTime (second)	@2018-02-02T11:01:03 = @2018-02-02T11:01:04
YamlReferenceImplTest	testEquality	64	passed			group: Dates & Times - DateTime literal compared with DateTime (millisecond)	@2018-02-02T11:01:03.123 = @2018-02-02T11:01:03.123
YamlReferenceImplTest	testEquality	65	passed			group: Dates & Times - DateTime literal compared with different DateTime (millisecond)	@2018-02-02T11:01:03.123 = @2018-02-02T11:01:03.124
YamlReferenceImplTest	testEquality	66	passed			group: Dates & Times - DateTime literal compared with different DateTime (timezone)	@2018-02-02T11:01:03.123+02:00 = @2018-02-02T11:01:03.123+04:00
YamlReferenceImplTest	testEquality	67	passed			group: Dates & Times - DateTime literal compared with same DateTime in resource (1)	@2018-02 = date1.toDate()
YamlReferenceImplTest	testEquality	68	passed			group: Dates & Times - DateTime literal compared with same DateTime in resource (2)	date1.toDate() = @2018-02
YamlReferenceImplTest	testEquality	69	passed			group: Dates & Times - DateTime literal compared with different DateTime in resource	@2018-01 = date1.toDate()
YamlReferenceImplTest	testEquality	70	passed			group: Dates & Times - DateTime literal compared with different precision DateTime in resource	@2018-02-02 = date1.toDate()
YamlReferenceImplTest	testEquality	71	passed			group: Dates & Times - DateTime literal compared with a number	@2018 = int1
YamlReferenceImplTest	testEquality	72	passed			group: Dates & Times - DateTime literal compared with a string literal	@2018 = 'zzz'
YamlReferenceImplTest	testEquality	73	excluded:error	null: Parse error		group: Dates & Times - Time literal compared with same time in different timezone	@T10:04:23-04:00 = @T18:04:23+04:00
YamlReferenceImplTest	testEquality	74	excluded:error	null: Parse error		group: Dates & Times	@T19:01:23-05:00 = @T00:01:23-00:00
YamlReferenceImplTest	testEquality	75	excluded:error	null: Parse error		group: Dates & Times - Time literal compared with different time	@T10:04:23-04:00 = @T18:04:24+04:00
YamlReferenceImplTest	testEquality	76	excluded:error	null: Parse error		group: Dates & Times - Time literal, comparing timezone and Z (same time)	@T10:04:23-04:00 = @T14:04:23Z
YamlReferenceImplTest	testEquality	77	excluded:error	null: Parse error		group: Dates & Times - Time literal, comparing timezone and Z (different time)	@T10:04:23-04:00 = @T14:04:24Z
YamlReferenceImplTest	testEquality	78	excluded:error	null: Parse error		group: Dates & Times - Same time, different timezone and precision	@T10:04:23-04:00 = @T14:04Z
YamlReferenceImplTest	testEquality	79	excluded:error	null: Parse error		group: Dates & Times - Different time and different precision	@T10:05:23-04:00 = @T14:04Z
YamlReferenceImplTest	testEquality	80	passed			group: Dates & Times - Time literal compared with same time in resource	@T12:34:45 = time1.toTime()
YamlReferenceImplTest	testEquality	81	passed			group: Dates & Times - Time literal compared with different time in resource	@T12:34:46 = time1.toTime()
YamlReferenceImplTest	testEquality	82	passed			group: Dates & Times - Time literal compared with different precision time in resource	@T12 = time1.toTime()
YamlReferenceImplTest	testEquality	83	excluded:error	#2418: Bundles are not supported		group: Dates & Times - ** should not ignore null properties in arrays	Bundle.entry[1] != Bundle.entry[2]
YamlReferenceImplTest	testEquality	84	unsupported		Unsupported operator: ~	group: 6.1.2. ~ (Equivalent) - * collections for equivalence { } ~ { } will result in true.	Ops.unexists ~ Ops.unexists
YamlReferenceImplTest	testEquality	85	unsupported		Unsupported operator: ~	group: 6.1.2. ~ (Equivalent) - * string: the strings must be the same while ignoring case and normalizing whitespace.	'ab c' ~ 'Ab  C'
YamlReferenceImplTest	testEquality	86	unsupported		Unsupported operator: ~	group: 6.1.2. ~ (Equivalent) - decimal: values must be equal, comparison is done on values rounded\nto the precision of the least precise operand.\nTrailing zeroes are ignored in  determining precision.\n	1.001 ~ 01.0012
YamlReferenceImplTest	testEquality	87	unsupported		Unsupported operator: ~	group: trailing zeros after decimal	1.100 ~ 1.1
YamlReferenceImplTest	testEquality	88	unsupported		Unsupported operator: ~	group: trailing zeros after decimal	1.100 ~ 1.101
YamlReferenceImplTest	testEquality	89	unsupported		Unsupported operator: ~	group: trailing zeros after decimal	1.1 ~ 1.0
YamlReferenceImplTest	testEquality	90	unsupported		Unsupported operator: ~	group: trailing zeros after decimal	1.1 ~ 1.00
YamlReferenceImplTest	testEquality	91	unsupported		Unsupported operator: ~	group: trailing zeros after decimal	1.10 ~ 1.00
YamlReferenceImplTest	testEquality	92	unsupported		Unsupported operator: ~	group: trailing zeros after decimal	11 ~ 10
YamlReferenceImplTest	testEquality	93	unsupported		Unsupported operator: ~	group: trailing zeros after decimal	12 ~ 11
YamlReferenceImplTest	testEquality	94	unsupported		Unsupported operator: ~	group: 6.1.2. ~ (Equivalent)	1.1 ~ 1.2
YamlReferenceImplTest	testEquality	95	unsupported		Unsupported operator: ~	group: 6.1.2. ~ (Equivalent)	0.00000011 ~ 0.00000012
YamlReferenceImplTest	testEquality	96	unsupported		Unsupported operator: ~	group: 6.1.2. ~ (Equivalent)	0.00000011 ~ 0.00000010
YamlReferenceImplTest	testEquality	97	unsupported		Unsupported operator: ~	group: 6.1.2. ~ (Equivalent)	0 ~ 0.00000010
YamlReferenceImplTest	testEquality	98	unsupported		Unsupported operator: ~	group: 6.1.2. ~ (Equivalent)	1 ~ 0.00000010
YamlReferenceImplTest	testEquality	99	unsupported		Unsupported operator: ~	group: 6.1.2. ~ (Equivalent)	0.010 ~ 0.00000010
YamlReferenceImplTest	testEquality	100	unsupported		Unsupported operator: ~	group: Quantity Equivalence	1 year ~ 1 'a'
YamlReferenceImplTest	testEquality	101	unsupported		Unsupported operator: ~	group: Quantity Equivalence	12 months ~ 1 'a'
YamlReferenceImplTest	testEquality	102	unsupported		Unsupported operator: ~	group: Quantity Equivalence	1 month ~ 1 'mo'
YamlReferenceImplTest	testEquality	103	unsupported		Unsupported operator: ~	group: Quantity Equivalence	30 days ~ 1 'mo'
YamlReferenceImplTest	testEquality	104	unsupported		Unsupported operator: ~	group: Dates & Times - DateTime literal compared with DateTime	@2018 ~ @2018
YamlReferenceImplTest	testEquality	105	unsupported		Unsupported operator: ~	group: Dates & Times - DateTime literal compared with different DateTime	@2018 ~ @2017
YamlReferenceImplTest	testEquality	106	unsupported		Unsupported operator: ~	group: Dates & Times - DateTime literal compared with different precision DateTime	@2018 ~ @2018-02
YamlReferenceImplTest	testEquality	107	unsupported		Unsupported operator: ~	group: Dates & Times - DateTime literal compared with same DateTime in resource	@2018-02 ~ date1.toDate()
YamlReferenceImplTest	testEquality	108	unsupported		Unsupported operator: ~	group: Dates & Times - DateTime literal compared with different DateTime in resource	@2018-01 ~ date1.toDate()
YamlReferenceImplTest	testEquality	109	unsupported		Unsupported operator: ~	group: Dates & Times - DateTime literal compared with different precision DateTime in resource	@2018-02-02 ~ date1.toDate()
YamlReferenceImplTest	testEquality	110	excluded:error	null: Parse error		group: Dates & Times - Time literal compared with same time in different timezone	@T10:04:23-04:00 ~ @T18:04:23+04:00
YamlReferenceImplTest	testEquality	111	excluded:error	null: Parse error		group: Dates & Times - Time literal compared with different time	@T10:04:23-04:00 ~ @T18:04:24+04:00
YamlReferenceImplTest	testEquality	112	excluded:error	null: Parse error		group: Dates & Times - Time literal, comparing timezone and Z (same time)	@T10:04:23-04:00 ~ @T14:04:23Z
YamlReferenceImplTest	testEquality	113	excluded:error	null: Parse error		group: Dates & Times - Time literal, comparing timezone and Z (different time)	@T10:04:23-04:00 ~ @T14:04:24Z
YamlReferenceImplTest	testEquality	114	unsupported		Unsupported operator: ~	group: Dates & Times - Time literal compared with same time in resource	@T12:34:45 ~ time1.toTime()
YamlReferenceImplTest	testEquality	115	unsupported		Unsupported operator: ~	group: Dates & Times - Time literal compared with different time in resource	@T12:35:46 ~ time1.toTime()
YamlReferenceImplTest	testEquality	116	unsupported		Unsupported operator: ~	group: Dates & Times - Time literal compared with different precision time in resource	@T12:34:46.123 ~ time1.toTime()
YamlReferenceImplTest	testEquality	117	unsupported		Unsupported operator: ~	group: 6.1.2. ~ (Equivalent) - * For complex types, equivalence requires all child properties to be equivalent, recursively.	Ops.complex ~ Ops.complexsimilar
YamlReferenceImplTest	testEquality	118	unsupported		Unsupported operator: ~	group: 6.1.2. ~ (Equivalent) - * For complex types, equivalence requires all child properties to be equivalent, recursively.	Ops.complex ~ Ops.complexother
YamlReferenceImplTest	testEquality	119	unsupported		Unsupported operator: ~	group: 6.1.2. ~ (Equivalent) - ** equivalence for null values	Patient.name.given[0] ~ Patient.name.given[3]
YamlReferenceImplTest	testEquality	120	passed			group: 6.1.3. != (Not Equals) - ** strings equals	'abc' != 'abc'
YamlReferenceImplTest	testEquality	121	passed			group: 6.1.3. != (Not Equals) - ** strings equals	Ops.str1 != Ops.str1
YamlReferenceImplTest	testEquality	122	passed			group: 6.1.3. != (Not Equals) - ** strings not equals	'abc' != 'abcd'
YamlReferenceImplTest	testEquality	123	passed			group: 6.1.3. != (Not Equals) - ** strings not equals	Ops.str1 != Ops.str2
YamlReferenceImplTest	testEquality	124	passed			group: 6.1.3. != (Not Equals) - ** utf8 strings	'ɐ ɑ ɒ ɓ ɔ ɕ ɖ ɗ ɘ ə ɚ ɛ ɜ ɝ ɞ ɟ ɠ' != 'ɐ ɑ ɒ ɓ ɔ ɕ ɖ ɗ ɘ ə ɚ ɛ ɜ ɝ ɞ ɟ ɠ'
YamlReferenceImplTest	testEquality	125	passed			group: 6.1.3. != (Not Equals) - ** utf8 strings	'ɐ ɑ ɒ ɓ ɔ ɕ ɖ ɗ ɘ ə ɚ ɛ ɜ ɝ ɞ ɟ ɠ' != 'ə ɚ ɛ ɜ ɝ ɞ ɟ ɠɐ ɑ ɒ ɓ ɔ ɕ ɖ ɗ ɘ '
YamlReferenceImplTest	testEquality	126	passed			group: 6.1.3. != (Not Equals) - ** equal	1 != 1
YamlReferenceImplTest	testEquality	127	passed			group: 6.1.3. != (Not Equals) - ** equal	Ops.int1 != Ops.int1
YamlReferenceImplTest	testEquality	128	passed			group: 6.1.3. != (Not Equals) - ** not equal	1 != 2
YamlReferenceImplTest	testEquality	129	passed			group: 6.1.3. != (Not Equals) - ** not equal	Ops.int1 != Ops.int2
YamlReferenceImplTest	testEquality	130	passed			group: 6.1.3. != (Not Equals) - ** decimals equal	1.01 != 1.01
YamlReferenceImplTest	testEquality	131	passed			group: 6.1.3. != (Not Equals) - ** decimals equal, trailing zeroes are ignored	0.01 != 00.01
YamlReferenceImplTest	testEquality	132	passed			group: 6.1.3. != (Not Equals) - decimal not equal	1.01 != 1.02
YamlReferenceImplTest	testEquality	133	passed			group: 6.1.3. != (Not Equals) - ** equal	true != true
YamlReferenceImplTest	testEquality	134	passed			group: 6.1.3. != (Not Equals) - ** not equal	true != false
YamlReferenceImplTest	testEquality	135	passed			group: 6.1.3. != (Not Equals) - ** propagate empty coll	true != Ops.unexists
YamlReferenceImplTest	testEquality	136	excluded:failure	null: Support for null values within FHIR elements		group: 6.1.3. != (Not Equals) - ** inequality for null values	Patient.name.given[0] != Patient.name.given[3]
YamlReferenceImplTest	testEquality	137	passed			group: Dates & Times - DateTime literal compared with DateTime	@2018 != @2018
YamlReferenceImplTest	testEquality	138	passed			group: Dates & Times - DateTime literal compared with different DateTime	@2018 != @2017
YamlReferenceImplTest	testEquality	139	passed			group: Dates & Times - DateTime literal compared with different precision DateTime (1)	@2018 != @2018-02
YamlReferenceImplTest	testEquality	140	passed			group: Dates & Times - DateTime literal compared with different precision DateTime (2)	@2018 != @2018-01
YamlReferenceImplTest	testEquality	141	passed			group: Dates & Times - DateTime literal compared with different precision DateTime (3)	@2018-02-03 != @2018-01
YamlReferenceImplTest	testEquality	142	passed			group: Dates & Times - DateTime literal compared with same DateTime in resource	@2018-02 != date1.toDate()
YamlReferenceImplTest	testEquality	143	passed			group: Dates & Times - DateTime literal compared with different DateTime in resource	@2018-01 != date1.toDate()
YamlReferenceImplTest	testEquality	144	passed			group: Dates & Times - DateTime literal compared with different precision DateTime in resource	@2018-02-02 != date1.toDate()
YamlReferenceImplTest	testEquality	145	excluded:error	null: Parse error		group: Dates & Times - Time literal compared with same time in different timezone	@T10:04:23-04:00 != @T18:04:23+04:00
YamlReferenceImplTest	testEquality	146	excluded:error	null: Parse error		group: Dates & Times - Time literal compared with different time	@T10:04:23-04:00 != @T18:04:24+04:00
YamlReferenceImplTest	testEquality	147	excluded:error	null: Parse error		group: Dates & Times - Time literal, comparing timezone and Z (same time)	@T10:04:23-04:00 != @T14:04:23Z
YamlReferenceImplTest	testEquality	148	excluded:error	null: Parse error		group: Dates & Times - Time literal, comparing timezone and Z (different time)	@T10:04:23-04:00 != @T14:04:24Z
YamlReferenceImplTest	testEquality	149	passed			group: Dates & Times - Time literal compared with same time in resource	@T12:34:45 != time1.toTime()
YamlReferenceImplTest	testEquality	150	passed			group: Dates & Times - Time literal compared with different time in resource	@T12:35:46 != time1.toTime()
YamlReferenceImplTest	testEquality	151	passed			group: Dates & Times - Time literal compared with different precision time in resource (1)	@T12:34 != time1.toTime()
YamlReferenceImplTest	testEquality	152	passed			group: Dates & Times - Time literal compared with different precision time in resource (2)	@T12:35 != time1.toTime()
YamlReferenceImplTest	testEquality	153	unsupported		Unsupported operator: !~	group: Dates & Times - DateTime literal compared with DateTime	@2018 !~ @2018
YamlReferenceImplTest	testEquality	154	unsupported		Unsupported operator: !~	group: Dates & Times - DateTime literal compared with different DateTime	@2018 !~ @2017
YamlReferenceImplTest	testEquality	155	unsupported		Unsupported operator: !~	group: Dates & Times - DateTime literal compared with different precision DateTime (1)	@2018 !~ @2018-02
YamlReferenceImplTest	testEquality	156	unsupported		Unsupported operator: !~	group: Dates & Times - DateTime literal compared with different precision DateTime (2)	@2019 !~ @2018-02
YamlReferenceImplTest	testEquality	157	unsupported		Unsupported operator: !~	group: Dates & Times - DateTime literal compared with same DateTime in resource	@2018-02 !~ date1.toDate()
YamlReferenceImplTest	testEquality	158	unsupported		Unsupported operator: !~	group: Dates & Times - DateTime literal compared with different DateTime in resource	@2018-01 !~ date1.toDate()
YamlReferenceImplTest	testEquality	159	unsupported		Unsupported operator: !~	group: Dates & Times - DateTime literal compared with different precision DateTime in resource	@2018-02-02 !~ date1.toDate()
YamlReferenceImplTest	testEquality	160	excluded:error	null: Parse error		group: Dates & Times - Time literal compared with same time in different timezone	@T10:04:23-04:00 !~ @T18:04:23+04:00
YamlReferenceImplTest	testEquality	161	excluded:error	null: Parse error		group: Dates & Times - Time literal compared with different time	@T10:04:23-04:00 !~ @T18:04:24+04:00
YamlReferenceImplTest	testEquality	162	excluded:error	null: Parse error		group: Dates & Times - Time literal, comparing timezone and Z (same time)	@T10:04:23-04:00 !~ @T14:04:23Z
YamlReferenceImplTest	testEquality	163	excluded:error	null: Parse error		group: Dates & Times - Time literal, comparing timezone and Z (different time)	@T10:04:23-04:00 !~ @T14:04:24Z
YamlReferenceImplTest	testEquality	164	unsupported		Unsupported operator: !~	group: Dates & Times - Time literal compared with same time in resource	@T12:34:45 !~ time1.toTime()
YamlReferenceImplTest	testEquality	165	unsupported		Unsupported operator: !~	group: Dates & Times - Time literal compared with different time in resource	@T12:35:46 !~ time1.toTime()
YamlReferenceImplTest	testEquality	166	unsupported		Unsupported operator: !~	group: Dates & Times - Time literal compared with different precision time in resource (1)	@T12:34 !~ time1.toTime()
YamlReferenceImplTest	testEquality	167	unsupported		Unsupported operator: !~	group: Dates & Times - Time literal compared with different precision time in resource (2)	@T12:35 !~ time1.toTime()
YamlReferenceImplTest	testEquality	168	unsupported		Unsupported operator: !~	group: 6.1.4. !~ (Not Equivalent) - * collections for equivalence { } !~ { } will result in true.	Ops.unexists !~ Ops.unexists
YamlReferenceImplTest	testEquality	169	unsupported		Unsupported operator: !~	group: 6.1.4. !~ (Not Equivalent) - ** non-equivalence for null values	Patient.name.given[0] !~ Patient.name.given[3]
YamlReferenceImplTest	testEquality	170	unsupported		Unsupported operator: !~	group: 6.1.4. !~ (Not Equivalent) - * string: the strings must be the same while ignoring case and normalizing whitespace.	'ab c' !~ 'Ab  C'
YamlReferenceImplTest	testEquality	171	unsupported		Unsupported operator: !~	group: 6.1.4. !~ (Not Equivalent) - decimal: values must be equal, comparison is done on values rounded\nto the precision of the least precise operand.\nTrailing zeroes are ignored in  determining precision.\n	1.001 !~ 01.0012
YamlReferenceImplTest	testExistence	1	passed			** empty coll	Functions.attrempty.empty()
YamlReferenceImplTest	testExistence	2	passed			** null is not empty	Patient.name.given[0].empty()
YamlReferenceImplTest	testExistence	3	passed			** not empty coll	Functions.attrsingle.empty()
YamlReferenceImplTest	testExistence	4	passed			** empty nothing	Functions.nothing.empty()
YamlReferenceImplTest	testExistence	5	passed			** not true is false	Functions.attrtrue.not()
YamlReferenceImplTest	testExistence	6	passed			** not false is true	Functions.attrfalse.not()
YamlReferenceImplTest	testExistence	7	passed			** true with double not is true	Functions.attrtrue.not().not()
YamlReferenceImplTest	testExistence	8	passed			** empty coll with not() is empty coll	Functions.attrempty.not()
YamlReferenceImplTest	testExistence	9	passed			** multi-valued coll with not() is invalid	Functions.attrdouble.not()
YamlReferenceImplTest	testExistence	10	passed			** not nothing is empty coll	Functions.nothing.not()
YamlReferenceImplTest	testExistence	11	excluded:failure	null: Support for null values within FHIR elements		** error for collection that is not a singleton	Functions.collWithNullAndTrue.not()
YamlReferenceImplTest	testExistence	12	passed			** exists for not empty coll should return true	Functions.coll1.exists()
YamlReferenceImplTest	testExistence	13	passed			** exists for empty coll should return false	Functions.attrempty.exists()
YamlReferenceImplTest	testExistence	14	passed			** exists for null should return true	Patient.name.given[0].exists()
YamlReferenceImplTest	testExistence	15	passed			** exists for undefined coll should return false	Functions.nothing.exists()
YamlReferenceImplTest	testExistence	16	passed			** exists with criteria should work	Functions.coll1.coll2.attr.exists($this > 0)
YamlReferenceImplTest	testExistence	17	passed			** exists with criteria should work for empty filtered coll	Functions.coll1.coll2.attr.exists($this < 0)
YamlReferenceImplTest	testExistence	18	passed			** exists with criteria should work for not exists coll 	Functions.nothing.attr.exists($this < 0)
YamlReferenceImplTest	testExistence	19	passed			** all for empty coll is true	Functions.attrempty.all($this > 0)
YamlReferenceImplTest	testExistence	20	passed			** all for non-exists coll is true	Functions.nothing.all($this > 0)
YamlReferenceImplTest	testExistence	21	passed			** all for evaluations is true	Functions.coll1.coll2.attr.all($this > 0)
YamlReferenceImplTest	testExistence	22	passed			** not all evaluations is true	Functions.coll1.coll2.attr.all($this = 5)
YamlReferenceImplTest	testExistence	23	unsupported		$index is not supported	** the ability to use $index in the expression	(0|1|2|3).all($this = $index)
YamlReferenceImplTest	testExistence	24	passed			** all for a collection with a null value that has an id	Patient.name.given[0].all(id = 'Jacomus1Id')
YamlReferenceImplTest	testExistence	25	passed			** allTrue for empty coll is true	Functions.attrempty.allTrue()
YamlReferenceImplTest	testExistence	26	passed			** allTrue for coll with all true items is true	Functions.coll1.colltrue.attr.allTrue()
YamlReferenceImplTest	testExistence	27	passed			** allTrue for coll with false value	Functions.coll1.collwithfalse.attr.allTrue()
YamlReferenceImplTest	testExistence	28	passed			** allTrue for non exists coll is true	Functions.nothing.allTrue()
YamlReferenceImplTest	testExistence	29	excluded:failure	null: Support for null values within FHIR elements		** allTrue for null values in the collection raise an error	Functions.collWithNullAndTrue.allTrue()
YamlReferenceImplTest	testExistence	30	passed			** anyTrue for empty coll is false	Functions.attrempty.anyTrue()
YamlReferenceImplTest	testExistence	31	passed			** anyTrue for non-exists coll is false	Functions.nothing.anyTrue()
YamlReferenceImplTest	testExistence	32	excluded:failure	null: Support for null values within FHIR elements		** anyTrue for null values in the collection raise an error	Functions.collWithNullAndTrue.anyTrue()
YamlReferenceImplTest	testExistence	33	passed			** anyTrue for coll with true value	Functions.coll1.collwithfalse.attr.anyTrue()
YamlReferenceImplTest	testExistence	34	passed			** anyTrue for coll with all false values	Functions.coll1.collfalse.attr.anyTrue()
YamlReferenceImplTest	testExistence	35	passed			** allFalse for empty coll is true	Functions.attrempty.allFalse()
YamlReferenceImplTest	testExistence	36	passed			** allFalse for non-exists coll is true	Functions.nothing.allFalse()
YamlReferenceImplTest	testExistence	37	excluded:failure	null: Support for null values within FHIR elements		** allFalse for null values in the collection raise an error	Functions.collWithNullAndTrue.allFalse()
YamlReferenceImplTest	testExistence	38	passed			** allFalse for coll with all false items is true	Functions.coll1.collfalse.attr.allFalse()
YamlReferenceImplTest	testExistence	39	passed			** allFalse for coll with true value	Functions.coll1.collwithfalse.attr.allFalse()
YamlReferenceImplTest	testExistence	40	passed			** anyFalse for empty coll is false	Functions.attrempty.anyFalse()
YamlReferenceImplTest	testExistence	41	passed			** anyFalse for non-exists coll is false	Functions.nothing.anyFalse()
YamlReferenceImplTest	testExistence	42	excluded:failure	null: Support for null values within FHIR elements		** anyFalse for null values in the collection raise an error	Functions.collWithNullAndTrue.anyFalse()
YamlReferenceImplTest	testExistence	43	passed			** anyFalse for coll with false value	Functions.coll1.collwithfalse.attr.anyFalse()
YamlReferenceImplTest	testExistence	44	passed			** anyFalse for coll with all true values	Functions.coll1.colltrue.attr.anyFalse()
YamlReferenceImplTest	testExistence	45	unsupported		Unsupported function: subsetOf	** if input coll is empty - result of subset is true	Functions.attrempty.subsetOf(Functions.attrdouble)
YamlReferenceImplTest	testExistence	46	unsupported		Unsupported function: subsetOf	** if argument coll is empty - result of subset is false	Functions.attrdouble.subsetOf(Functions.attrempty)
YamlReferenceImplTest	testExistence	47	unsupported		Unsupported function: subsetOf	** if argument coll isnt exists -  result of subset is false	Functions.attrdouble.subsetOf(Functions.attrempty)
YamlReferenceImplTest	testExistence	48	unsupported		Unsupported function: subsetOf	** subsetOf for collections with null values (1)	Functions.collWith2NullAndTrue.subsetOf(Functions.collWithNullAndTrue)
YamlReferenceImplTest	testExistence	49	unsupported		Unsupported function: subsetOf	** subsetOf for collections with null values (2)	Functions.collWithNullAndTrue.subsetOf(Functions.collWith2NullAndTrue)
YamlReferenceImplTest	testExistence	50	unsupported		Unsupported function: subsetOf	** if input coll is subset of argument coll	Functions.attrdouble.subsetOf(Functions.coll1[0].coll2.attr)
YamlReferenceImplTest	testExistence	51	unsupported		Unsupported function: subsetOf	** if input coll isnt subset of argument coll	Functions.coll1[0].coll2.attr.subsetOf(Functions.attrdouble)
YamlReferenceImplTest	testExistence	52	unsupported		Unsupported function: subsetOf	** should treat objects with different key orders as equivalent	Functions.subsetTestC1.subsetOf(Functions.subsetTestC2)
YamlReferenceImplTest	testExistence	53	unsupported		Unsupported function: subsetOf	** subsetOf for a large collections 1	(1 | 2 | 3 | 4 | 5).subsetOf(1 | 2 | 3 | 4 | 6 | 7)
YamlReferenceImplTest	testExistence	54	unsupported		Unsupported function: subsetOf	** subsetOf for a large collections 2	(1 | 2 | 3 | 4 | 5).subsetOf(1 | 2 | 3 | 4 | 5 | 6)
YamlReferenceImplTest	testExistence	55	unsupported		Unsupported function: supersetOf	** if argumet coll is empty - result of superset is true	Functions.attrdouble.supersetOf(Functions.attrempty)
YamlReferenceImplTest	testExistence	56	unsupported		Unsupported function: supersetOf	** if input coll is empty - result of superset is false	Functions.attrempty.supersetOf(Functions.attrdouble)
YamlReferenceImplTest	testExistence	57	unsupported		Unsupported function: supersetOf	** if input coll isnt exists and argument is non-empty, result of superset is false	Functions.attrempty.supersetOf(Functions.attrdouble)
YamlReferenceImplTest	testExistence	58	unsupported		Unsupported function: supersetOf	** if argument coll is superset of input coll result is true	Functions.coll1[0].coll2.attr.supersetOf(Functions.attrdouble)
YamlReferenceImplTest	testExistence	59	unsupported		Unsupported function: supersetOf	** if argument coll isnt superset of input coll result is false	Functions.attrdouble.supersetOf(Functions.coll1[0].coll2.attr)
YamlReferenceImplTest	testExistence	60	unsupported		Unsupported function: supersetOf	** supersetOf for collections with null values (1)	Functions.collWith2NullAndTrue.supersetOf(Functions.collWithNullAndTrue)
YamlReferenceImplTest	testExistence	61	unsupported		Unsupported function: supersetOf	** supersetOf for collections with null values (2)	Functions.collWithNullAndTrue.supersetOf(Functions.collWith2NullAndTrue)
YamlReferenceImplTest	testExistence	62	passed			** isDistinct of empty input coll is true	Functions.attrempty.isDistinct()
YamlReferenceImplTest	testExistence	63	passed			** isDistinct of non-exists coll is true	Functions.nothing.isDistinct()
YamlReferenceImplTest	testExistence	64	passed			** isDistinct of distinct coll	Functions.coll1.coll2.attr.isDistinct()
YamlReferenceImplTest	testExistence	65	passed			** isDistinct of coll with same values	Functions.coll1.collfalse.attr.isDistinct()
YamlReferenceImplTest	testExistence	66	passed			** isDistinct of a large collection 1	(1 | 2 | 3 | 4 | 5 | 6 | 7).combine(7).isDistinct()
YamlReferenceImplTest	testExistence	67	passed			** isDistinct of a large collection 2	(1 | 2 | 3 | 4 | 5 | 6 | 7).isDistinct()
YamlReferenceImplTest	testExistence	68	passed			** isDistinct of coll with nodes that have null values	(Patient.name.given[0]).combine(Patient.name.given[3]).isDistinct()
YamlReferenceImplTest	testExistence	69	passed			** distinct of empty coll is empty	Functions.attrempty.distinct()
YamlReferenceImplTest	testExistence	70	passed			** distinct of non-exists coll is empty	Functions.nothing.distinct()
YamlReferenceImplTest	testExistence	71	passed			** distinct of coll with same values	Functions.coll1[2].colltrue.attr.distinct()
YamlReferenceImplTest	testExistence	72	passed			** distinct of coll with different values	Functions.coll1[0].coll2.attr.distinct()
YamlReferenceImplTest	testExistence	73	unsupported		Unsupported equality for complex types	** should not depend on the order of properties in an object	Functions.objects.distinct()
YamlReferenceImplTest	testExistence	74	excluded:error	#2418: Bundles are not supported		** should ignore null properties	Bundle.entry.distinct() = (Bundle.entry[0] | Bundle.entry[2])
YamlReferenceImplTest	testExistence	75	excluded:failure	null: Indefinite calendar duration equality in distinct() deduplication		** should use year-to-month conversion factor (https://hl7.org/fhirpath/#equals)	(1 year).combine(12 months).distinct()
YamlReferenceImplTest	testExistence	76	passed			** should compare quantities for equality (https://hl7.org/fhirpath/#equals)	(3 'min').combine(180 seconds).distinct()
YamlReferenceImplTest	testExistence	77	passed			** should work with nodes that have null values	Patient.name.given.distinct().count()
YamlReferenceImplTest	testExistence	78	passed			** count	Functions.coll1.count()
YamlReferenceImplTest	testExistence	79	passed			** nested count	Functions.coll1.coll2.count()
YamlReferenceImplTest	testExistence	80	passed			** nested count (alternative)	Functions.coll1[0].coll2.count()
YamlReferenceImplTest	testExistence	81	passed			** 0 if nothing	Functions.ups.count()
YamlReferenceImplTest	testExtension	1	excluded:failure	#437: ID on primitive types not supported		group: Extension and id for primitive types - ** id for primitive type	Functions.attrtrue.id = 'someid'
YamlReferenceImplTest	testExtension	2	excluded:pass	#437: extensions on primitive types not supported		group: Extension and id for primitive types - ** expression with extension for primitive type 1	Patient.birthDate.extension.where(url = '').empty()
YamlReferenceImplTest	testExtension	3	passed			group: Extension and id for primitive types - ** expression with extension for primitive type 2	Patient.birthDate.extension .where(url = 'http://hl7.org/fhir/StructureDefinition/patient-birthTime') .valueDateTime.toDateTime() = @1974-12-25T14:35:45-05:00
YamlReferenceImplTest	testExtension	4	excluded:unsupported	#437: extensions on primitive types not supported	Representation of this path is not supported: choice element 'value' (do you need to use ofType?)	group: Extension and id for primitive types - ** expression with extension for primitive type 3	Patient.birthDate.extension .where(url = 'http://hl7.org/fhir/StructureDefinition/patient-birthTime') .value = @1974-12-25T14:35:45-05:00
YamlReferenceImplTest	testExtension	5	passed			group: Additional functions - ** empty url	Patient.birthDate.extension('').empty()
YamlReferenceImplTest	testExtension	6	passed			group: Additional functions - ** empty input collection	Patient.birthDate1 .extension('http://hl7.org/fhir/StructureDefinition/patient-birthTime').empty()
YamlReferenceImplTest	testExtension	7	passed			group: Additional functions - ** expression with extension() for primitive type (without using FHIR model data)	Patient.birthDate.extension('http://hl7.org/fhir/StructureDefinition/patient-birthTime') .valueDateTime.toDateTime() = @1974-12-25T14:35:45-05:00
YamlReferenceImplTest	testExtension	8	passed			group: Additional functions - ** expression with extension() for primitive type (without using FHIR model data) when only extension is present	Patient.communication.preferred.extension('test').exists()
YamlReferenceImplTest	testExtension	9	excluded:unsupported	#437: extensions on primitive types not supported	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	group: Additional functions - ** expression with extension() for primitive type (using FHIR model data) when only extension is present	Patient.communication.preferred.extension('test').value.id
YamlReferenceImplTest	testExtension	10	excluded:unsupported	#437: extensions on primitive types not supported	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	group: Additional functions - ** type of a primitive type "id" (using R4 model data)	Patient.communication.preferred.extension('test').value.id is System.String
YamlReferenceImplTest	testExtension	11	excluded:unsupported	#437: extensions on primitive types not supported	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	group: Additional functions - ** type of a primitive type "id" (using DSTU2 model data)	Patient.communication.preferred.extension('test').value.id is FHIR.id
YamlReferenceImplTest	testExtension	12	excluded:error	#2418: Bundles are not supported		group: Additional functions - ** expression with extension() for an item of an array of primitive types when associated value is null	Bundle.entry.resource.name[0].given[0].extension('http://hl7.org/fhir/StructureDefinition/display').value
YamlReferenceImplTest	testExtension	13	excluded:error	#2418: Bundles are not supported		group: Additional functions - ** expression with extension() for an item of an array of primitive types when associated value is not null	Bundle.entry.resource.name[0].given[1].extension('http://hl7.org/fhir/StructureDefinition/display').value
YamlReferenceImplTest	testExtension	14	excluded:unsupported	#437: extensions on primitive types not supported	Representation of this path is not supported: choice element 'value' (do you need to use ofType?)	group: Additional functions - ** expression with extension() for the null item of an array of primitive types when it has a non-null extension	Patient.name.given[0].extension('http://hl7.org/fhir/StructureDefinition/display').value = 'Jacomus1'
YamlReferenceImplTest	testExtension	15	excluded:unsupported	#437: extensions on primitive types not supported	Representation of this path is not supported: choice element 'value' (do you need to use ofType?)	group: Additional functions - ** expression with extension() when the list of primitive types is shorter than the list of extensions	Patient.name.given[3].extension('http://hl7.org/fhir/StructureDefinition/display').value = 'Jacomus2'
YamlReferenceImplTest	testExtension	16	excluded:failure	#437: ID on primitive types not supported		group: Additional functions - ** expression with extension() when the list of values doesn't exists but the list of extensions does	Patient.address[1].id = 'someId'
YamlReferenceImplTest	testExtension	17	passed			group: Additional functions - ** count nodes without value, but with extensions	Patient.name.given.count() = 4
YamlReferenceImplTest	testExtension	18	passed			group: Additional functions - ** exclude null values from the result	Patient.name.given
YamlReferenceImplTest	testExtension	19	excluded:unsupported	#437: extensions on primitive types not supported	Representation of this path is not supported: choice element 'value' (do you need to use ofType?)	group: Additional functions - ** expression with extension() for primitive type (using FHIR model data)	Patient.birthDate.extension('http://hl7.org/fhir/StructureDefinition/patient-birthTime') .value = @1974-12-25T14:35:45-05:00
YamlReferenceImplTest	testExtension	20	excluded:failure	#437: extensions on primitive types not supported		group: Additional functions - ** value of extension of extension (using FHIR model data)	Functions.attrtrue.extension('url1').extension('url2').value = 'someuri'
YamlReferenceImplTest	testExtension	21	excluded:failure	#437: extensions on primitive types not supported		group: Additional functions - ** id of extension of extension	Functions.attrtrue.extension('url1').extension('url2').id = 'someid2'
YamlReferenceImplTest	testFactory	1	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a base64Binary without an extension	%factory.base64Binary('ZHNmZHNm').where($this is base64Binary) .decode('base64')
YamlReferenceImplTest	testFactory	2	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a base64Binary with an extension	%factory.base64Binary('ZHNmZHNm', %factory.Extension('someExt', 'someString')).extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	3	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a base64Binary may throw an exception	%factory.base64Binary('dsfdsf')
YamlReferenceImplTest	testFactory	4	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a boolean without an extension	%factory.boolean(true).where($this is boolean)
YamlReferenceImplTest	testFactory	5	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a boolean with an extension	%factory.boolean('false', %factory.Extension('someExt', 'someString')).where($this is boolean and $this = false) .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	6	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a boolean may throw an exception	%factory.boolean('dsfdsf')
YamlReferenceImplTest	testFactory	7	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a canonical without an extension	%factory.canonical('someUrl').where($this is canonical)
YamlReferenceImplTest	testFactory	8	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a canonical with an extension	%factory.canonical('someUrl', %factory.Extension('someExt', 'someString')).where($this is canonical and $this = 'someUrl') .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	9	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a canonical may throw an exception	%factory.canonical('some Url')
YamlReferenceImplTest	testFactory	10	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a code without an extension	%factory.code('some code').where($this is code)
YamlReferenceImplTest	testFactory	11	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a code with an extension	%factory.code('some code', %factory.Extension('someExt', 'someString')).where($this is code and $this = 'some code') .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	12	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a code may throw an exception	%factory.code('some code ')
YamlReferenceImplTest	testFactory	13	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a date without an extension	%factory.date('2024-01-01').where($this is date) = @2024-01-01
YamlReferenceImplTest	testFactory	14	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a date with an extension	%factory.date('2024-01-01', %factory.Extension('someExt', 'someString')).where($this is date and $this = @2024-01-01) .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	15	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a date may throw an exception	%factory.date('something')
YamlReferenceImplTest	testFactory	16	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a dateTime without an extension	%factory.dateTime('2024-01-01T01:01:01').where($this is dateTime) = @2024-01-01T01:01:01
YamlReferenceImplTest	testFactory	17	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a dateTime with an extension	%factory.dateTime('2024-01-01T01:01:01', %factory.Extension('someExt', 'someString')).where($this is dateTime and $this = @2024-01-01T01:01:01) .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	18	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a dateTime may throw an exception	%factory.dateTime('something')
YamlReferenceImplTest	testFactory	19	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a decimal without an extension	%factory.decimal(1.1).where($this is decimal)
YamlReferenceImplTest	testFactory	20	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a decimal with an extension	%factory.decimal('1.1', %factory.Extension('someExt', 'someString')) .where($this is decimal and $this = 1.1).extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	21	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a decimal may throw an exception	%factory.decimal('sdfds')
YamlReferenceImplTest	testFactory	22	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create an id without an extension	%factory.id('someId-123').where($this is id) = 'someId-123'
YamlReferenceImplTest	testFactory	23	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create an id with an extension	%factory.id('someId-123', %factory.Extension('someExt', 'someString')).where($this is id and $this = 'someId-123') .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	24	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating an id may throw an exception	%factory.id('someId 123')
YamlReferenceImplTest	testFactory	25	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create an instant without an extension	%factory.instant('2024-01-01T01:01:01+01:00').where($this is instant) = @2024-01-01T01:01:01+01:00
YamlReferenceImplTest	testFactory	26	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create an instant with an extension	%factory.instant('2024-01-01T01:01:01+01:00', %factory.Extension('someExt', 'someString')).where($this is instant and $this = @2024-01-01T01:01:01+01:00) .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	27	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating an instant may throw an exception	%factory.instant('something')
YamlReferenceImplTest	testFactory	28	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create an integer without an extension	%factory.integer(134).where($this is integer)
YamlReferenceImplTest	testFactory	29	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create an integer with an extension	%factory.integer(-134, %factory.Extension('someExt', 'someString')) .where($this is integer and $this = -134) .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	30	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating an integer may throw an exception	%factory.integer(1.1)
YamlReferenceImplTest	testFactory	31	excluded:error	#2425: Custom factory not supported		group: primitives - ** create an integer64 without an extension	%factory.integer64(134).where($this is integer64)
YamlReferenceImplTest	testFactory	32	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create an integer64 with an extension	%factory.integer64(134, %factory.Extension('someExt', 'someString')) .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	33	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating an integer64 may throw an exception	%factory.integer64(1.1)
YamlReferenceImplTest	testFactory	34	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a markdown without an extension	%factory.markdown(' md md ').where($this is markdown)
YamlReferenceImplTest	testFactory	35	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a markdown with an extension	%factory.markdown(' md md ', %factory.Extension('someExt', 'someString')) .where($this is markdown and $this = ' md md ') .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	36	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating an markdown may throw an exception	%factory.markdown('')
YamlReferenceImplTest	testFactory	37	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create an oid without an extension	%factory.oid('urn:oid:1.2.3').where($this is oid)
YamlReferenceImplTest	testFactory	38	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create an oid with an extension	%factory.oid('urn:oid:1.2.3', %factory.Extension('someExt', 'someString')) .where($this is oid and $this = 'urn:oid:1.2.3') .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	39	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating an oid may throw an exception	%factory.oid('urn:oid:3.2.3')
YamlReferenceImplTest	testFactory	40	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a string without an extension	%factory.string('some string').where($this is string)
YamlReferenceImplTest	testFactory	41	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a string with an extension	%factory.string('some string', %factory.Extension('someExt', 'someString')) .where($this is string and $this = 'some string') .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	42	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a string may throw an exception	%factory.string('')
YamlReferenceImplTest	testFactory	43	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a positiveInt without an extension	%factory.positiveInt(134).where($this is positiveInt)
YamlReferenceImplTest	testFactory	44	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a positiveInt with an extension	%factory.positiveInt(134, %factory.Extension('someExt', 'someString')) .where($this is positiveInt and $this = 134) .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	45	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a positiveInt may throw an exception	%factory.positiveInt(0)
YamlReferenceImplTest	testFactory	46	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a time without an extension	%factory.time('10:00').where($this is time) =  @T10:00
YamlReferenceImplTest	testFactory	47	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a time with an extension	%factory.time('10:00', %factory.Extension('someExt', 'someString')) .where($this is time and $this = @T10:00) .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	48	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a time may throw an exception	%factory.time('something')
YamlReferenceImplTest	testFactory	49	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a unsignedInt without an extension	%factory.unsignedInt(134).where($this is unsignedInt)
YamlReferenceImplTest	testFactory	50	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a unsignedInt with an extension	%factory.unsignedInt(0, %factory.Extension('someExt', 'someString')) .where($this is unsignedInt and $this = 0) .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	51	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a unsignedInt may throw an exception	%factory.unsignedInt(-1)
YamlReferenceImplTest	testFactory	52	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a uri without an extension	%factory.uri('/something').where($this is uri)
YamlReferenceImplTest	testFactory	53	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a uri with an extension	%factory.uri('', %factory.Extension('someExt', 'someString')) .where($this is uri and $this = '') .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	54	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a uri may throw an exception	%factory.uri(123)
YamlReferenceImplTest	testFactory	55	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a url without an extension	%factory.url('/something').where($this is url)
YamlReferenceImplTest	testFactory	56	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a url with an extension	%factory.url('', %factory.Extension('someExt', 'someString')) .where($this is url and $this = '') .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	57	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a url may throw an exception	%factory.url(123)
YamlReferenceImplTest	testFactory	58	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a uuid without an extension	%factory.uuid('urn:uuid:c757873d-ec9a-4326-a141-556f43239520') .where($this is uuid)
YamlReferenceImplTest	testFactory	59	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** create a uuid with an extension	%factory.uuid('urn:uuid:c757873d-ec9a-4326-a141-556f43239520', %factory.Extension('someExt', 'someString')).where($this is uuid and $this = 'urn:uuid:c757873d-ec9a-4326-a141-556f43239520') .extension('someExt').value = 'someString'
YamlReferenceImplTest	testFactory	60	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: primitives - ** creating a uuid may throw an exception	%factory.uuid('something')
YamlReferenceImplTest	testFactory	61	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Extension - ** create an extension of the string data type	%factory.Extension('someExt', 'someString').where($this is Extension and $this.valueString is string).value = 'someString'
YamlReferenceImplTest	testFactory	62	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Extension - ** create an extension of the integer data type	%factory.Extension('someExt', 11).where($this is Extension and $this.valueInteger is integer).value = 11
YamlReferenceImplTest	testFactory	63	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Extension - ** create an extension of the decimal data type	%factory.Extension('someExt', 11.1).where($this is Extension and $this.valueDecimal is decimal).value = 11.1
YamlReferenceImplTest	testFactory	64	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Extension - ** create an extension of the code data type	%factory.Extension('someExt', %factory.code('some code')).where($this is Extension and $this.valueCode is code).value = 'some code'
YamlReferenceImplTest	testFactory	65	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Identifier - ** created an Identifier	%factory.Identifier('someSystem', 'someValue', 'someUse', %factory .create(CodeableConcept)).where($this.system = 'someSystem' and $this.value = 'someValue' and $this.use = 'someUse' and $this.type is CodeableConcept) is Identifier
YamlReferenceImplTest	testFactory	66	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Identifier - ** created an Identifier without system	%factory.Identifier({}, 'someValue', 'someUse', %factory .create(CodeableConcept)).where($this.system.empty() and $this.value = 'someValue' and $this.use = 'someUse' and $this.type is CodeableConcept) is Identifier
YamlReferenceImplTest	testFactory	67	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Identifier - ** creating an Identifier may throw an exception	%factory.Identifier(1, 'someValue', 'someUse', %factory .create(CodeableConcept))
YamlReferenceImplTest	testFactory	68	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: HumanName - ** created a HumanName	%factory.HumanName('Smith', 'Julia' | 'A', {}, {}, 'Julia Smith').where( family = 'Smith' and given = ('Julia' | 'A') and prefix.empty() and suffix.empty() and text = 'Julia Smith' and use.empty() ) is HumanName
YamlReferenceImplTest	testFactory	69	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: HumanName - ** creating a HumanName may throw an exception	%factory.HumanName(1, 'Julia' | 'A', {}, {}, 'Julia Smith')
YamlReferenceImplTest	testFactory	70	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: ContactPoint - ** created a ContactPoint	%factory.ContactPoint('email', 'coyote@acme.com', 'work').where( system = 'email' and value = 'coyote@acme.com' and use = 'work' ) is ContactPoint
YamlReferenceImplTest	testFactory	71	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: ContactPoint - ** creating a ContactPoint may throw an exception	%factory.ContactPoint(1, 'coyote@acme.com', 'work')
YamlReferenceImplTest	testFactory	72	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Address - ** created an Address	%factory.Address('5 Nowhere Road' | 'second line', 'SomeCity', 'EW', '0000', {}, 'home', 'physical').where(line = ('5 Nowhere Road' | 'second line') and city = 'SomeCity' and state = 'EW' and postalCode = '0000' and country.empty() and use = 'home' and type = 'physical') is Address
YamlReferenceImplTest	testFactory	73	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Address - ** creating an Address may throw an exception	%factory.Address(1 | 'second line', 'SomeCity', 'EW', '0000', {}, 'home', 'physical')
YamlReferenceImplTest	testFactory	74	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Quantity - ** created a Quantity using a string value	%factory.Quantity('http://unitsofmeasure.org', 'mg/dL', '5.03', 'mg/dL').where(system = 'http://unitsofmeasure.org' and code = 'mg/dL') = 5.03 'mg/dL'
YamlReferenceImplTest	testFactory	75	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Quantity - ** created a Quantity using a number value	%factory.Quantity('http://unitsofmeasure.org', 'mg/dL', 5.03, 'mg/dL') = 5.03 'mg/dL'
YamlReferenceImplTest	testFactory	76	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Quantity - ** created a Quantity without system	%factory.Quantity({}, 'mg/dL', 5.03, 'mg/dL').where(system.empty()) is Quantity
YamlReferenceImplTest	testFactory	77	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Quantity - ** creating a Quantity may throw an exception	%factory.Quantity(1, 'mg/dL', 5.03, 'mg/dL')
YamlReferenceImplTest	testFactory	78	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Coding - ** created a Coding	%factory.Coding('http://loinc.org', '1234-5', 'An example test', '1.02').where(system = 'http://loinc.org' and code = '1234-5' and display = 'An example test' and version = '1.02') is Coding
YamlReferenceImplTest	testFactory	79	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: Coding - ** creating a Coding may throw an exception	%factory.Coding(1, '1234-5', 'An example test', '1.02')
YamlReferenceImplTest	testFactory	80	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: CodeableConcept - ** created a CodeableConcept	%factory.CodeableConcept(%factory.Coding('system1', '1') | %factory .Coding('system2', '2'), 'Example Test').where(coding.code = '1' | '2' and text = 'Example Test') is CodeableConcept
YamlReferenceImplTest	testFactory	81	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: CodeableConcept - ** creating a CodeableConcept may throw an exception	%factory.CodeableConcept(1 | %factory.Coding('system2', '2'), 'Example Test')
YamlReferenceImplTest	testFactory	82	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: create - ** the created instance has the correct data type	%factory.create(integer) is FHIR.integer
YamlReferenceImplTest	testFactory	83	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: withExtension - ** adding an extension to an existing list of extensions	%factory.withExtension(%factory.integer(134, %factory.Extension( 'someExt1', 'someString')), 'someExt2', 1).extension.value = 'someString' | 1
YamlReferenceImplTest	testFactory	84	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: withExtension - ** create an instance with no data and add an extension later	%factory.withExtension(%factory.integer({}), 'someExt', 1).extension( 'someExt').value = 1
YamlReferenceImplTest	testFactory	85	excluded:unsupported	#2425: Custom factory not supported	Type factory is not supported	group: withProperty - ** adding a property to an instance	%factory.withProperty(%factory.integer(134, %factory.Extension( 'someExt1', 'someString')), 'id', 'someId').where(extension.value = 'someString').id = 'someId'
YamlReferenceImplTest	testFhirQuantity	1	passed			group: standard tests from fhir-r4 - ** test	1.toQuantity() = 1 '1'
YamlReferenceImplTest	testFhirQuantity	2	passed			group: standard tests from fhir-r4 - ** test	1.0.toQuantity() = 1.0 '1'
YamlReferenceImplTest	testFhirQuantity	3	passed			group: standard tests from fhir-r4 - ** test	'1'.toQuantity()
YamlReferenceImplTest	testFhirQuantity	4	passed			group: standard tests from fhir-r4 - ** test	'1 day'.toQuantity() = 1 day
YamlReferenceImplTest	testFhirQuantity	5	passed			group: standard tests from fhir-r4 - ** test	'1 day'.toQuantity() = 1 'd'
YamlReferenceImplTest	testFhirQuantity	6	passed			group: standard tests from fhir-r4 - ** test	'1 \\'wk\\''.toQuantity() = 1 week
YamlReferenceImplTest	testFhirQuantity	7	unsupported		Unsupported operator: ~	group: standard tests from fhir-r4 - ** test	'1.0'.toQuantity() ~ 1 '1'
YamlReferenceImplTest	testFhirQuantity	8	excluded:error	#2163: Implicit conversion of polymorphic types not supported		group: Mapping from FHIR Quantity to FHIRPath System.Quantity - After converting 'a' to year it is equal to year	QuestionnaireResponse.item[0].answer.value = 2 year
YamlReferenceImplTest	testFhirQuantity	9	excluded:error	null: Non-conformant input fixture: answer is not an array		group: Mapping from FHIR Quantity to FHIRPath System.Quantity - After converting 'a' to year it is equivalent to 'a'	QuestionnaireResponse.item[0].answer.value ~ 2 'a'
YamlReferenceImplTest	testFhirQuantity	10	excluded:error	#2163: Implicit conversion of polymorphic types not supported		group: Mapping from FHIR Quantity to FHIRPath System.Quantity - After converting 'a' to year it isn't equal to 'a'	QuestionnaireResponse.item[0].answer.value != 2 'a'
YamlReferenceImplTest	testFhirQuantity	11	excluded:error	#2163: Implicit conversion of polymorphic types not supported		group: Mapping from FHIR Quantity to FHIRPath System.Quantity - After converting 'min' to minutes it is equal to minutes	QuestionnaireResponse.item[1].answer.value = 3 minutes
YamlReferenceImplTest	testFhirQuantity	12	excluded:error	null: Non-conformant input fixture: answer is not an array		group: Mapping from FHIR Quantity to FHIRPath System.Quantity - Unable to convert from non-UCUM system	QuestionnaireResponse.item[2].answer.value.toQuantity()
YamlReferenceImplTest	testFhirQuantity	13	excluded:error	null: Non-conformant input fixture: answer is not an array		group: Mapping from FHIR Quantity to FHIRPath System.Quantity - Error when a comparator is present and there is a need to convert	QuestionnaireResponse.item[3].answer.value.toQuantity()
YamlReferenceImplTest	testFhirQuantity	14	excluded:error	#2163: Implicit conversion of polymorphic types not supported		group: Mapping from FHIR Quantity to FHIRPath System.Quantity - Can access the comparator field when there isn't a need to convert	QuestionnaireResponse.item[3].answer.value.comparator
YamlReferenceImplTest	testFhirR4	1	passed			group: Miscellaneous accessor tests - ** Extract birthDate	birthDate
YamlReferenceImplTest	testFhirR4	2	passed			group: Miscellaneous accessor tests - ** patient telecom types	telecom.use
YamlReferenceImplTest	testFhirR4	3	passed			group: Tests ported from the Java Unit Tests - ** testSimple	name.given
YamlReferenceImplTest	testFhirR4	4	passed			group: Tests ported from the Java Unit Tests - ** testSimpleNone	name.suffix
YamlReferenceImplTest	testFhirR4	5	excluded:failure	#2153: Tick quotes identifiers not supported		group: Tests ported from the Java Unit Tests - ** testEscapedIdentifier	name.`given`
YamlReferenceImplTest	testFhirR4	6	excluded:failure	#2153: Tick quotes identifiers not supported		group: Tests ported from the Java Unit Tests - ** testSimpleBackTick1	`Patient`.name.`given`
YamlReferenceImplTest	testFhirR4	7	passed			group: Tests ported from the Java Unit Tests - ** testSimpleWithContext	Patient.name.given
YamlReferenceImplTest	testFhirR4	8	excluded:unsupported	#2163: Implicit conversion of polymorphic types not supported	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	group: testObservations - ** testPolymorphismA	Observation.value.unit
YamlReferenceImplTest	testFhirR4	9	passed			group: testObservations - ** testPolymorphismIsA	Observation.value.is(Quantity)
YamlReferenceImplTest	testFhirR4	10	passed			group: testObservations - ** testPolymorphismIsA	Observation.value is Quantity
YamlReferenceImplTest	testFhirR4	11	passed			group: testObservations - ** testPolymorphismIsB	Observation.value.is(Period).not()
YamlReferenceImplTest	testFhirR4	12	passed			group: testObservations - ** testPolymorphismAsA	Observation.value.as(Quantity).unit
YamlReferenceImplTest	testFhirR4	13	passed			group: testObservations - ** testPolymorphismAsAFunction	(Observation.value as Quantity).unit
YamlReferenceImplTest	testFhirR4	14	passed			group: testObservations - ** testPolymorphismAsBFunction	Observation.value.as(Period).start
YamlReferenceImplTest	testFhirR4	15	unsupported		Unsupported function: substring	group: testDollar - ** testDollarThis1	Patient.name.given.where(substring($this.length()-3) = 'out')
YamlReferenceImplTest	testFhirR4	16	unsupported		Unsupported function: substring	group: testDollar - ** testDollarThis2	Patient.name.given.where(substring($this.length()-3) = 'ter')
YamlReferenceImplTest	testFhirR4	17	unsupported		Unsupported function: skip	group: testDollar - ** testDollarOrderAllowed	Patient.name.skip(1).given
YamlReferenceImplTest	testFhirR4	18	unsupported		Unsupported function: skip	group: testDollar - ** testDollarOrderAllowedA	Patient.name.skip(3).given
YamlReferenceImplTest	testFhirR4	19	passed			group: testLiterals - ** testLiteralTrue	Patient.name.exists() = true
YamlReferenceImplTest	testFhirR4	20	passed			group: testLiterals - ** testLiteralFalse	Patient.name.empty() = false
YamlReferenceImplTest	testFhirR4	21	passed			group: testLiterals - ** testLiteralString	Patient.name.given.first() = 'Peter'
YamlReferenceImplTest	testFhirR4	22	passed			group: testLiterals - ** testLiteralInteger1	1.convertsToInteger()
YamlReferenceImplTest	testFhirR4	23	passed			group: testLiterals - ** testLiteralInteger0	0.convertsToInteger()
YamlReferenceImplTest	testFhirR4	24	passed			group: testLiterals - ** testLiteralIntegerNegative1	(-1).convertsToInteger()
YamlReferenceImplTest	testFhirR4	25	passed			group: testLiterals - ** testLiteralIntegerNegative1Invalid	-1.convertsToInteger()
YamlReferenceImplTest	testFhirR4	26	passed			group: testLiterals - ** testLiteralIntegerMax	2147483647.convertsToInteger()
YamlReferenceImplTest	testFhirR4	27	passed			group: testLiterals - ** testLiteralString	'test'.convertsToString()
YamlReferenceImplTest	testFhirR4	28	passed			group: testLiterals - ** testLiteralStringEscapes	'\\\\\\/\\f\\r\\n\\t\\"\\`\\'\\u002a'.convertsToString()
YamlReferenceImplTest	testFhirR4	29	passed			group: testLiterals - ** testLiteralBooleanTrue	true.convertsToBoolean()
YamlReferenceImplTest	testFhirR4	30	passed			group: testLiterals - ** testLiteralBooleanFalse	false.convertsToBoolean()
YamlReferenceImplTest	testFhirR4	31	passed			group: testLiterals - ** testLiteralDecimal10	1.0.convertsToDecimal()
YamlReferenceImplTest	testFhirR4	32	passed			group: testLiterals - ** testLiteralDecimal01	0.1.convertsToDecimal()
YamlReferenceImplTest	testFhirR4	33	passed			group: testLiterals - ** testLiteralDecimal00	0.0.convertsToDecimal()
YamlReferenceImplTest	testFhirR4	34	passed			group: testLiterals - ** testLiteralDecimalNegative01	(-0.1).convertsToDecimal()
YamlReferenceImplTest	testFhirR4	35	passed			group: testLiterals - ** testLiteralDecimalNegative01Invalid	-0.1.convertsToDecimal()
YamlReferenceImplTest	testFhirR4	36	passed			group: testLiterals - ** testLiteralDecimalMax	1234567890987654321.0.convertsToDecimal()
YamlReferenceImplTest	testFhirR4	37	excluded:error	#2144: Decimal literal exceeded maximum scale supported		group: testLiterals - ** testLiteralDecimalStep	0.00000001.convertsToDecimal()
YamlReferenceImplTest	testFhirR4	38	passed			group: testLiterals - ** testLiteralDateTimeYear	@2015T.is(DateTime)
YamlReferenceImplTest	testFhirR4	39	passed			group: testLiterals - ** testLiteralDateTimeMonth	@2015-02T.is(DateTime)
YamlReferenceImplTest	testFhirR4	40	passed			group: testLiterals - ** testLiteralDateTimeDay	@2015-02-04T.is(DateTime)
YamlReferenceImplTest	testFhirR4	41	passed			group: testLiterals - ** testLiteralDateTimeHour	@2015-02-04T14.is(DateTime)
YamlReferenceImplTest	testFhirR4	42	passed			group: testLiterals - ** testLiteralDateTimeMinute	@2015-02-04T14:34.is(DateTime)
YamlReferenceImplTest	testFhirR4	43	passed			group: testLiterals - ** testLiteralDateTimeSecond	@2015-02-04T14:34:28.is(DateTime)
YamlReferenceImplTest	testFhirR4	44	passed			group: testLiterals - ** testLiteralDateTimeMillisecond	@2015-02-04T14:34:28.123.is(DateTime)
YamlReferenceImplTest	testFhirR4	45	passed			group: testLiterals - ** testLiteralDateTimeUTC	@2015-02-04T14:34:28Z.is(DateTime)
YamlReferenceImplTest	testFhirR4	46	passed			group: testLiterals - ** testLiteralDateTimeTimezoneOffset	@2015-02-04T14:34:28+10:00.is(DateTime)
YamlReferenceImplTest	testFhirR4	47	passed			group: testLiterals - ** testLiteralTimeHour	@T14.is(Time)
YamlReferenceImplTest	testFhirR4	48	passed			group: testLiterals - ** testLiteralTimeMinute	@T14:34.is(Time)
YamlReferenceImplTest	testFhirR4	49	passed			group: testLiterals - ** testLiteralTimeSecond	@T14:34:28.is(Time)
YamlReferenceImplTest	testFhirR4	50	passed			group: testLiterals - ** testLiteralTimeMillisecond	@T14:34:28.123.is(Time)
YamlReferenceImplTest	testFhirR4	51	passed			group: testLiterals - ** testLiteralQuantityDecimal	10.1 'mg'.convertsToQuantity()
YamlReferenceImplTest	testFhirR4	52	passed			group: testLiterals - ** testLiteralQuantityInteger	10 'mg'.convertsToQuantity()
YamlReferenceImplTest	testFhirR4	53	passed			group: testLiterals - ** testLiteralQuantityDay	4 days.convertsToQuantity()
YamlReferenceImplTest	testFhirR4	54	passed			group: testLiterals - ** testLiteralIntegerNotEqual	-3 != 3
YamlReferenceImplTest	testFhirR4	55	passed			group: testLiterals - ** testLiteralIntegerEqual	Patient.name.given.count() = 5
YamlReferenceImplTest	testFhirR4	56	passed			group: testLiterals - ** testPolarityPrecedence	-Patient.name.given.count() = -5
YamlReferenceImplTest	testFhirR4	57	passed			group: testLiterals - ** testLiteralIntegerGreaterThan	Patient.name.given.count() > -3
YamlReferenceImplTest	testFhirR4	58	passed			group: testLiterals - ** testLiteralIntegerCountNotEqual	Patient.name.given.count() != 0
YamlReferenceImplTest	testFhirR4	59	passed			group: testLiterals - ** testLiteralIntegerLessThanTrue	1 < 2
YamlReferenceImplTest	testFhirR4	60	passed			group: testLiterals - ** testLiteralIntegerLessThanFalse	1 < -2
YamlReferenceImplTest	testFhirR4	61	passed			group: testLiterals - ** testLiteralIntegerLessThanPolarityTrue	+1 < +2
YamlReferenceImplTest	testFhirR4	62	passed			group: testLiterals - ** testLiteralIntegerLessThanPolarityFalse	-1 < 2
YamlReferenceImplTest	testFhirR4	63	excluded:unsupported	#2163: Implicit conversion of polymorphic types not supported	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	group: testLiterals - ** testLiteralDecimalGreaterThanNonZeroTrue	Observation.value.value > 180.0
YamlReferenceImplTest	testFhirR4	64	excluded:unsupported	#2163: Implicit conversion of polymorphic types not supported	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	group: testLiterals - ** testLiteralDecimalGreaterThanZeroTrue	Observation.value.value > 0.0
YamlReferenceImplTest	testFhirR4	65	excluded:unsupported	#2163: Implicit conversion of polymorphic types not supported	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	group: testLiterals - ** testLiteralDecimalGreaterThanIntegerTrue	Observation.value.value > 0
YamlReferenceImplTest	testFhirR4	66	excluded:unsupported	#2163: Implicit conversion of polymorphic types not supported	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	group: testLiterals - ** testLiteralDecimalLessThanInteger	Observation.value.value < 190
YamlReferenceImplTest	testFhirR4	67	unsupported		Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	group: testLiterals - ** testLiteralDecimalLessThanInvalid	Observation.value.value < 'test'
YamlReferenceImplTest	testFhirR4	68	passed			group: testLiterals - ** testDateEqual	Patient.birthDate = @1974-12-25
YamlReferenceImplTest	testFhirR4	69	passed			group: testLiterals - ** testDateNotEqual	Patient.birthDate != @1974-12-25T12:34:00
YamlReferenceImplTest	testFhirR4	70	passed			group: testLiterals - ** testDateNotEqualTimeSecond	Patient.birthDate != @T12:14:15
YamlReferenceImplTest	testFhirR4	71	passed			group: testLiterals - ** testDateNotEqualTimeMinute	Patient.birthDate != @T12:14
YamlReferenceImplTest	testFhirR4	72	unsupported		Unsupported function: today	group: testLiterals - ** testDateNotEqualToday	Patient.birthDate < today()
YamlReferenceImplTest	testFhirR4	73	unsupported		Unsupported function: now	group: testLiterals - ** testDateTimeGreaterThanDate	now() > Patient.birthDate
YamlReferenceImplTest	testFhirR4	74	passed			group: testLiterals - ** testLiteralDateTimeTZGreater	@2017-11-05T01:30:00.0-04:00 > @2017-11-05T01:15:00.0-05:00
YamlReferenceImplTest	testFhirR4	75	passed			group: testLiterals - ** testLiteralDateTimeTZLess	@2017-11-05T01:30:00.0-04:00 < @2017-11-05T01:15:00.0-05:00
YamlReferenceImplTest	testFhirR4	76	passed			group: testLiterals - ** testLiteralDateTimeTZEqualFalse	@2017-11-05T01:30:00.0-04:00 = @2017-11-05T01:15:00.0-05:00
YamlReferenceImplTest	testFhirR4	77	passed			group: testLiterals - ** testLiteralDateTimeTZEqualTrue	@2017-11-05T01:30:00.0-04:00 = @2017-11-05T00:30:00.0-05:00
YamlReferenceImplTest	testFhirR4	78	passed			group: testLiterals - ** testLiteralUnicode	Patient.name.given.first() = 'P\\u0065ter'
YamlReferenceImplTest	testFhirR4	79	passed			group: testLiterals - ** testCollectionNotEmpty	Patient.name.given.empty().not()
YamlReferenceImplTest	testFhirR4	80	passed			group: testLiterals - ** testCollectionNotEqualEmpty	Patient.name.given != {}
YamlReferenceImplTest	testFhirR4	81	passed			group: testLiterals - ** testExpressions	Patient.name.select(given | family).distinct()
YamlReferenceImplTest	testFhirR4	82	passed			group: testLiterals - ** testExpressionsEqual	Patient.name.given.count() = 1 + 4
YamlReferenceImplTest	testFhirR4	83	passed			group: testLiterals - ** testNotEmpty	Patient.name.empty().not()
YamlReferenceImplTest	testFhirR4	84	passed			group: testLiterals - ** testEmpty	Patient.link.empty()
YamlReferenceImplTest	testFhirR4	85	passed			group: testLiterals - ** testLiteralNotTrue	true.not() = false
YamlReferenceImplTest	testFhirR4	86	passed			group: testLiterals - ** testLiteralNotFalse	false.not() = true
YamlReferenceImplTest	testFhirR4	87	passed			group: testLiterals - ** testIntegerBooleanNotFalse	(1).not() = false
YamlReferenceImplTest	testFhirR4	88	excluded:failure	null: Ambiguous expression		group: testLiterals - ** testNotInvalid	(1|2).not() = false
YamlReferenceImplTest	testFhirR4	89	passed			group: testTypes - ** testStringYearConvertsToDate	'2015'.convertsToDate()
YamlReferenceImplTest	testFhirR4	90	passed			group: testTypes - ** testStringMonthConvertsToDate	'2015-02'.convertsToDate()
YamlReferenceImplTest	testFhirR4	91	passed			group: testTypes - ** testStringDayConvertsToDate	'2015-02-04'.convertsToDate()
YamlReferenceImplTest	testFhirR4	92	passed			group: testTypes - ** testStringYearConvertsToDateTime	'2015'.convertsToDateTime()
YamlReferenceImplTest	testFhirR4	93	passed			group: testTypes - ** testStringMonthConvertsToDateTime	'2015-02'.convertsToDateTime()
YamlReferenceImplTest	testFhirR4	94	passed			group: testTypes - ** testStringDayConvertsToDateTime	'2015-02-04'.convertsToDateTime()
YamlReferenceImplTest	testFhirR4	95	passed			group: testTypes - ** testStringHourConvertsToDateTime	'2015-02-04T14'.convertsToDateTime()
YamlReferenceImplTest	testFhirR4	96	passed			group: testTypes - ** testStringMinuteConvertsToDateTime	'2015-02-04T14:34'.convertsToDateTime()
YamlReferenceImplTest	testFhirR4	97	passed			group: testTypes - ** testStringSecondConvertsToDateTime	'2015-02-04T14:34:28'.convertsToDateTime()
YamlReferenceImplTest	testFhirR4	98	passed			group: testTypes - ** testStringMillisecondConvertsToDateTime	'2015-02-04T14:34:28.123'.convertsToDateTime()
YamlReferenceImplTest	testFhirR4	99	passed			group: testTypes - ** testStringUTCConvertsToDateTime	'2015-02-04T14:34:28Z'.convertsToDateTime()
YamlReferenceImplTest	testFhirR4	100	passed			group: testTypes - ** testStringTZConvertsToDateTime	'2015-02-04T14:34:28+10:00'.convertsToDateTime()
YamlReferenceImplTest	testFhirR4	101	passed			group: testTypes - ** testStringHourConvertsToTime	'14'.convertsToTime()
YamlReferenceImplTest	testFhirR4	102	passed			group: testTypes - ** testStringMinuteConvertsToTime	'14:34'.convertsToTime()
YamlReferenceImplTest	testFhirR4	103	passed			group: testTypes - ** testStringSecondConvertsToTime	'14:34:28'.convertsToTime()
YamlReferenceImplTest	testFhirR4	104	passed			group: testTypes - ** testStringMillisecondConvertsToTime	'14:34:28.123'.convertsToTime()
YamlReferenceImplTest	testFhirR4	105	passed			group: testTypes - ** testIntegerLiteralConvertsToInteger	1.convertsToInteger()
YamlReferenceImplTest	testFhirR4	106	passed			group: testTypes - ** testIntegerLiteralIsInteger	1.is(Integer)
YamlReferenceImplTest	testFhirR4	107	passed			group: testTypes - ** testIntegerLiteralIsSystemInteger	1.is(System.Integer)
YamlReferenceImplTest	testFhirR4	108	passed			group: testTypes - ** testStringLiteralConvertsToInteger	'1'.convertsToInteger()
YamlReferenceImplTest	testFhirR4	109	passed			group: testTypes - ** testStringLiteralConvertsToIntegerFalse	'a'.convertsToInteger().not()
YamlReferenceImplTest	testFhirR4	110	passed			group: testTypes - ** testStringDecimalConvertsToIntegerFalse	'1.0'.convertsToInteger().not()
YamlReferenceImplTest	testFhirR4	111	passed			group: testTypes - ** testStringLiteralIsNotInteger	'1'.is(Integer).not()
YamlReferenceImplTest	testFhirR4	112	passed			group: testTypes - ** testBooleanLiteralConvertsToInteger	true.convertsToInteger()
YamlReferenceImplTest	testFhirR4	113	passed			group: testTypes - ** testBooleanLiteralIsNotInteger	true.is(Integer).not()
YamlReferenceImplTest	testFhirR4	114	passed			group: testTypes - ** testDateIsNotInteger	@2013-04-05.is(Integer).not()
YamlReferenceImplTest	testFhirR4	115	passed			group: testTypes - ** testIntegerLiteralToInteger	1.toInteger() = 1
YamlReferenceImplTest	testFhirR4	116	passed			group: testTypes - ** testStringIntegerLiteralToInteger	'1'.toInteger() = 1
YamlReferenceImplTest	testFhirR4	117	passed			group: testTypes - ** testDecimalLiteralToInteger	'1.1'.toInteger() = {}
YamlReferenceImplTest	testFhirR4	118	passed			group: testTypes - ** testDecimalLiteralToIntegerIsEmpty	'1.1'.toInteger().empty()
YamlReferenceImplTest	testFhirR4	119	passed			group: testTypes - ** testBooleanLiteralToInteger	true.toInteger() = 1
YamlReferenceImplTest	testFhirR4	120	passed			group: testTypes - ** testIntegerLiteralConvertsToDecimal	1.convertsToDecimal()
YamlReferenceImplTest	testFhirR4	121	passed			group: testTypes - ** testIntegerLiteralIsNotDecimal	1.is(Decimal).not()
YamlReferenceImplTest	testFhirR4	122	passed			group: testTypes - ** testDecimalLiteralConvertsToDecimal	1.0.convertsToDecimal()
YamlReferenceImplTest	testFhirR4	123	passed			group: testTypes - ** testStringIntegerLiteralConvertsToDecimal	'1'.convertsToDecimal()
YamlReferenceImplTest	testFhirR4	124	passed			group: testTypes - ** testStringIntegerLiteralIsNotDecimal	'1'.is(Decimal).not()
YamlReferenceImplTest	testFhirR4	125	passed			group: testTypes - ** testStringLiteralConvertsToDecimalFalse	'1.a'.convertsToDecimal().not()
YamlReferenceImplTest	testFhirR4	126	passed			group: testTypes - ** testStringDecimalLiteralConvertsToDecimal	'1.0'.convertsToDecimal()
YamlReferenceImplTest	testFhirR4	127	passed			group: testTypes - ** testStringDecimalLiteralIsNotDecimal	'1.0'.is(Decimal).not()
YamlReferenceImplTest	testFhirR4	128	passed			group: testTypes - ** testBooleanLiteralConvertsToDecimal	true.convertsToDecimal()
YamlReferenceImplTest	testFhirR4	129	passed			group: testTypes - ** testBooleanLiteralIsNotDecimal	true.is(Decimal).not()
YamlReferenceImplTest	testFhirR4	130	passed			group: testTypes - ** testIntegerLiteralToDecimal	1.toDecimal() = 1.0
YamlReferenceImplTest	testFhirR4	131	unsupported		Unsupported operator: ~	group: testTypes - ** testIntegerLiteralToDeciamlEquivalent	1.toDecimal() ~ 1.0
YamlReferenceImplTest	testFhirR4	132	passed			group: testTypes - ** testDecimalLiteralToDecimal	1.0.toDecimal() = 1.0
YamlReferenceImplTest	testFhirR4	133	passed			group: testTypes - ** testDecimalLiteralToDecimalEqual	'1.1'.toDecimal() = 1.1
YamlReferenceImplTest	testFhirR4	134	passed			group: testTypes - ** testBooleanLiteralToDecimal	true.toDecimal() = 1
YamlReferenceImplTest	testFhirR4	135	passed			group: testTypes - ** testIntegerLiteralConvertsToQuantity	1.convertsToQuantity()
YamlReferenceImplTest	testFhirR4	136	passed			group: testTypes - ** testIntegerLiteralIsNotQuantity	1.is(Quantity).not()
YamlReferenceImplTest	testFhirR4	137	passed			group: testTypes - ** testDecimalLiteralConvertsToQuantity	1.0.convertsToQuantity()
YamlReferenceImplTest	testFhirR4	138	passed			group: testTypes - ** testDecimalLiteralIsNotQuantity	1.0.is(System.Quantity).not()
YamlReferenceImplTest	testFhirR4	139	passed			group: testTypes - ** testStringIntegerLiteralConvertsToQuantity	'1'.convertsToQuantity()
YamlReferenceImplTest	testFhirR4	140	passed			group: testTypes - ** testStringIntegerLiteralIsNotQuantity	'1'.is(System.Quantity).not()
YamlReferenceImplTest	testFhirR4	141	passed			group: testTypes - ** testStringQuantityLiteralConvertsToQuantity	'1 day'.convertsToQuantity()
YamlReferenceImplTest	testFhirR4	142	passed			group: testTypes - ** testStringQuantityWeekConvertsToQuantity	'1 \\'wk\\''.convertsToQuantity()
YamlReferenceImplTest	testFhirR4	143	passed			group: testTypes - ** testStringQuantityWeekConvertsToQuantityFalse	'1 wk'.convertsToQuantity().not()
YamlReferenceImplTest	testFhirR4	144	passed			group: testTypes - ** testStringDecimalLiteralConvertsToQuantityFalse	'1.a'.convertsToQuantity().not()
YamlReferenceImplTest	testFhirR4	145	passed			group: testTypes - ** testStringDecimalLiteralConvertsToQuantity	'1.0'.convertsToQuantity()
YamlReferenceImplTest	testFhirR4	146	passed			group: testTypes - ** testStringDecimalLiteralIsNotSystemQuantity	'1.0'.is(System.Quantity).not()
YamlReferenceImplTest	testFhirR4	147	passed			group: testTypes - ** testBooleanLiteralConvertsToQuantity	true.convertsToQuantity()
YamlReferenceImplTest	testFhirR4	148	passed			group: testTypes - ** testBooleanLiteralIsNotSystemQuantity	true.is(System.Quantity).not()
YamlReferenceImplTest	testFhirR4	149	passed			group: testTypes - ** testIntegerLiteralToQuantity	1.toQuantity() = 1 '1'
YamlReferenceImplTest	testFhirR4	150	passed			group: testTypes - ** testDecimalLiteralToQuantity	1.0.toQuantity() = 1.0 '1'
YamlReferenceImplTest	testFhirR4	151	passed			group: testTypes - ** testStringIntegerLiteralToQuantity	'1'.toQuantity()
YamlReferenceImplTest	testFhirR4	152	passed			group: testTypes - ** testStringQuantityLiteralToQuantity	'1 day'.toQuantity() = 1 day
YamlReferenceImplTest	testFhirR4	153	passed			group: testTypes - ** testStringQuantityWeekLiteralToQuantity	'1 \\'wk\\''.toQuantity() = 1 'wk'
YamlReferenceImplTest	testFhirR4	154	unsupported		Unsupported operator: ~	group: testTypes - ** testStringDecimalLiteralToQuantity	'1.0'.toQuantity() ~ 1 '1'
YamlReferenceImplTest	testFhirR4	155	passed			group: testTypes - ** testIntegerLiteralConvertsToBoolean	1.convertsToBoolean()
YamlReferenceImplTest	testFhirR4	156	passed			group: testTypes - ** testIntegerLiteralConvertsToBooleanFalse	2.convertsToBoolean()
YamlReferenceImplTest	testFhirR4	157	passed			group: testTypes - ** testNegativeIntegerLiteralConvertsToBooleanFalse	(-1).convertsToBoolean()
YamlReferenceImplTest	testFhirR4	158	passed			group: testTypes - ** testIntegerLiteralFalseConvertsToBoolean	0.convertsToBoolean()
YamlReferenceImplTest	testFhirR4	159	passed			group: testTypes - ** testDecimalLiteralConvertsToBoolean	1.0.convertsToBoolean()
YamlReferenceImplTest	testFhirR4	160	passed			group: testTypes - ** testStringTrueLiteralConvertsToBoolean	'true'.convertsToBoolean()
YamlReferenceImplTest	testFhirR4	161	passed			group: testTypes - ** testStringFalseLiteralConvertsToBoolean	'false'.convertsToBoolean()
YamlReferenceImplTest	testFhirR4	162	passed			group: testTypes - ** testStringFalseLiteralAlsoConvertsToBoolean	'False'.convertsToBoolean()
YamlReferenceImplTest	testFhirR4	163	passed			group: testTypes - ** testTrueLiteralConvertsToBoolean	true.convertsToBoolean()
YamlReferenceImplTest	testFhirR4	164	passed			group: testTypes - ** testFalseLiteralConvertsToBoolean	false.convertsToBoolean()
YamlReferenceImplTest	testFhirR4	165	passed			group: testTypes - ** testIntegerLiteralToBoolean	1.toBoolean()
YamlReferenceImplTest	testFhirR4	166	passed			group: testTypes - ** testIntegerLiteralToBooleanEmpty	2.toBoolean()
YamlReferenceImplTest	testFhirR4	167	passed			group: testTypes - ** testIntegerLiteralToBooleanFalse	0.toBoolean()
YamlReferenceImplTest	testFhirR4	168	passed			group: testTypes - ** testStringTrueToBoolean	'true'.toBoolean()
YamlReferenceImplTest	testFhirR4	169	passed			group: testTypes - ** testStringFalseToBoolean	'false'.toBoolean()
YamlReferenceImplTest	testFhirR4	170	passed			group: testTypes - ** testIntegerLiteralConvertsToString	1.convertsToString()
YamlReferenceImplTest	testFhirR4	171	passed			group: testTypes - ** testIntegerLiteralIsNotString	1.is(String).not()
YamlReferenceImplTest	testFhirR4	172	passed			group: testTypes - ** testNegativeIntegerLiteralConvertsToString	(-1).convertsToString()
YamlReferenceImplTest	testFhirR4	173	passed			group: testTypes - ** testDecimalLiteralConvertsToString	1.0.convertsToString()
YamlReferenceImplTest	testFhirR4	174	passed			group: testTypes - ** testStringLiteralConvertsToString	'true'.convertsToString()
YamlReferenceImplTest	testFhirR4	175	passed			group: testTypes - ** testBooleanLiteralConvertsToString	true.convertsToString()
YamlReferenceImplTest	testFhirR4	176	passed			group: testTypes - ** testQuantityLiteralConvertsToString	1 'wk'.convertsToString()
YamlReferenceImplTest	testFhirR4	177	passed			group: testTypes - ** testIntegerLiteralToString	1.toString()
YamlReferenceImplTest	testFhirR4	178	passed			group: testTypes - ** testNegativeIntegerLiteralToString	(-1).toString()
YamlReferenceImplTest	testFhirR4	179	passed			group: testTypes - ** testStringLiteralToString	'true'.toString()
YamlReferenceImplTest	testFhirR4	180	passed			group: testTypes - ** testBooleanLiteralToString	true.toString()
YamlReferenceImplTest	testFhirR4	181	passed			group: testTypes - ** testQuantityLiteralWkToString	1 'wk'.toString()
YamlReferenceImplTest	testFhirR4	182	passed			group: testAll - ** testAllTrue1	Patient.name.select(given.exists()).allTrue()
YamlReferenceImplTest	testFhirR4	183	passed			group: testAll - ** testAllTrue2	Patient.name.select(period.exists()).allTrue()
YamlReferenceImplTest	testFhirR4	184	passed			group: testAll - ** testAllTrue3	Patient.name.all(given.exists())
YamlReferenceImplTest	testFhirR4	185	passed			group: testAll - ** testAllTrue4	Patient.name.all(period.exists())
YamlReferenceImplTest	testFhirR4	186	unsupported		Unsupported function: subsetOf	group: testSubSetOf - ** testSubSetOf1	Patient.name.first().subsetOf($this.name)
YamlReferenceImplTest	testFhirR4	187	unsupported		Unsupported function: subsetOf	group: testSubSetOf - ** testSubSetOf2	Patient.name.subsetOf($this.name.first()).not()
YamlReferenceImplTest	testFhirR4	188	unsupported		Unsupported function: supersetOf	group: testSuperSetOf - ** testSuperSetOf1	Patient.name.first().supersetOf($this.name).not()
YamlReferenceImplTest	testFhirR4	189	unsupported		Unsupported function: supersetOf	group: testSuperSetOf - ** testSuperSetOf2	Patient.name.supersetOf($this.name.first())
YamlReferenceImplTest	testFhirR4	190	passed			group: testQuantity - ** testQuantity1	4.0000 'g' = 4000.0 'mg'
YamlReferenceImplTest	testFhirR4	191	unsupported		Unsupported operator: ~	group: testQuantity - ** testQuantity2	4 'g' ~ 4000 'mg'
YamlReferenceImplTest	testFhirR4	192	passed			group: testQuantity - ** testQuantity3	4 'g' != 4040 'mg'
YamlReferenceImplTest	testFhirR4	193	unsupported		Unsupported operator: ~	group: testQuantity - ** testQuantity4	4 'g' ~ 4040 'mg'
YamlReferenceImplTest	testFhirR4	194	excluded:failure	#2494: Unsupported comparison above second calendar durations		group: testQuantity - ** testQuantity5	7 days = 1 week
YamlReferenceImplTest	testFhirR4	195	excluded:failure	#2494: Unsupported comparison above second calendar durations		group: testQuantity - ** testQuantity7	6 days < 1 week
YamlReferenceImplTest	testFhirR4	196	excluded:failure	#2494: Unsupported comparison above second calendar durations		group: testQuantity - ** testQuantity8	8 days > 1 week
YamlReferenceImplTest	testFhirR4	197	excluded:unsupported	#2168: incorrect multiplication/division of quantity literals	Quantity arithmetic operations are not yet supported	group: testQuantity - ** testQuantity9	2.0 'cm' * 2.0 'm' = 0.040 'm2'
YamlReferenceImplTest	testFhirR4	198	excluded:unsupported	#2168: incorrect multiplication/division of quantity literals	Quantity arithmetic operations are not yet supported	group: testQuantity - ** testQuantity10	4.0 'g' / 2.0 'm' = 2 'g/m'
YamlReferenceImplTest	testFhirR4	199	unsupported		Quantity arithmetic operations are not yet supported	group: testQuantity - ** testQuantity11	1.0 'm' / 1.0 'm' = 1 '1'
YamlReferenceImplTest	testFhirR4	200	unsupported		Unsupported function: iif	group: testCollectionBoolean - ** testCollectionBoolean2	iif({}, true, false)
YamlReferenceImplTest	testFhirR4	201	unsupported		Unsupported function: iif	group: testCollectionBoolean - ** testCollectionBoolean3	iif(true, true, false)
YamlReferenceImplTest	testFhirR4	202	unsupported		Unsupported function: iif	group: testCollectionBoolean - ** testCollectionBoolean4	iif({} | true, true, false)
YamlReferenceImplTest	testFhirR4	203	excluded:unsupported	null: iff() with different true/false types not supported	Unsupported function: iif	group: testCollectionBoolean - ** testCollectionBoolean5	iif(true, true, 1/0)
YamlReferenceImplTest	testFhirR4	204	excluded:unsupported	null: iff() with different true/false types not supported	Unsupported function: iif	group: testCollectionBoolean - ** testCollectionBoolean6	iif(false, 1/0, true)
YamlReferenceImplTest	testFhirR4	205	passed			group: testDistinct - ** testDistinct1	(1 | 2 | 3).isDistinct()
YamlReferenceImplTest	testFhirR4	206	unsupported		Unsupported function: descendants	group: testDistinct - ** testDistinct2	Questionnaire.descendants().linkId.isDistinct()
YamlReferenceImplTest	testFhirR4	207	unsupported		Unsupported function: descendants	group: testDistinct - ** testDistinct3	Questionnaire.descendants().linkId.select(substring(0,1)).isDistinct().not()
YamlReferenceImplTest	testFhirR4	208	passed			group: testDistinct - ** testDistinct4	(1 | 2 | 3).distinct()
YamlReferenceImplTest	testFhirR4	209	unsupported		Unsupported function: descendants	group: testDistinct - ** testDistinct5	Questionnaire.descendants().linkId.distinct().count()
YamlReferenceImplTest	testFhirR4	210	unsupported		Unsupported function: descendants	group: testDistinct - ** testDistinct6	Questionnaire.descendants().linkId.select(substring(0,1)).distinct().count()
YamlReferenceImplTest	testFhirR4	211	passed			group: testCount - ** testCount1	Patient.name.count()
YamlReferenceImplTest	testFhirR4	212	passed			group: testCount - ** testCount2	Patient.name.count() = 3
YamlReferenceImplTest	testFhirR4	213	passed			group: testCount - ** testCount3	Patient.name.first().count()
YamlReferenceImplTest	testFhirR4	214	passed			group: testCount - ** testCount4	Patient.name.first().count() = 1
YamlReferenceImplTest	testFhirR4	215	passed			group: testWhere - ** testWhere1	Patient.name.count() = 3
YamlReferenceImplTest	testFhirR4	216	passed			group: testWhere - ** testWhere2	Patient.name.where(given = 'Jim').count() = 1
YamlReferenceImplTest	testFhirR4	217	passed			group: testWhere - ** testWhere3	Patient.name.where(given = 'X').count() = 0
YamlReferenceImplTest	testFhirR4	218	passed			group: testWhere - ** testWhere4	Patient.name.where($this.given = 'Jim').count() = 1
YamlReferenceImplTest	testFhirR4	219	passed			group: testSelect - ** testSelect1	Patient.name.select(given).count() = 5
YamlReferenceImplTest	testFhirR4	220	passed			group: testSelect - ** testSelect2	Patient.name.select(given | family).count() = 7
YamlReferenceImplTest	testFhirR4	221	passed			group: testRepeat - ** testRepeat1	ValueSet.expansion.repeat(contains).count() = 10
YamlReferenceImplTest	testFhirR4	222	passed			group: testRepeat - ** testRepeat2	Questionnaire.repeat(item).code.count() = 11
YamlReferenceImplTest	testFhirR4	223	unsupported		Unsupported function: descendants	group: testRepeat - ** testRepeat3	Questionnaire.descendants().code.count() = 23
YamlReferenceImplTest	testFhirR4	224	unsupported		Unsupported function: children	group: testRepeat - ** testRepeat4	Questionnaire.children().code.count() = 2
YamlReferenceImplTest	testFhirR4	225	excluded:error	null: Parse error		group: testAggregate - ** testAggregate1	(1|2|3|4|5|6|7|8|9).aggregate($this+$total, 0) = 45
YamlReferenceImplTest	testFhirR4	226	excluded:error	null: Parse error		group: testAggregate - ** testAggregate2	(1|2|3|4|5|6|7|8|9).aggregate($this+$total, 2) = 47
YamlReferenceImplTest	testFhirR4	227	excluded:error	null: Parse error		group: testAggregate - ** testAggregate3	(1|2|3|4|5|6|7|8|9).aggregate(iif($total.empty(), $this, iif($this < $total, $this, $total))) = 1
YamlReferenceImplTest	testFhirR4	228	excluded:error	null: Parse error		group: testAggregate - ** testAggregate4	(1|2|3|4|5|6|7|8|9).aggregate(iif($total.empty(), $this, iif($this > $total, $this, $total))) = 9
YamlReferenceImplTest	testFhirR4	229	passed			group: testIndexer - ** testIndexer1	Patient.name[0].given = 'Peter' | 'James'
YamlReferenceImplTest	testFhirR4	230	passed			group: testIndexer - ** testIndexer2	Patient.name[1].given = 'Jim'
YamlReferenceImplTest	testFhirR4	231	unsupported		Unsupported function: single	group: testSingle - ** testSingle1	Patient.name.first().single().exists()
YamlReferenceImplTest	testFhirR4	232	unsupported		Unsupported function: single	group: testSingle - ** testSingle2	Patient.name.single().exists()
YamlReferenceImplTest	testFhirR4	233	passed			group: testFirstLast - ** testFirstLast1	Patient.name.first().given = 'Peter' | 'James'
YamlReferenceImplTest	testFhirR4	234	unsupported		Unsupported function: last	group: testFirstLast - ** testFirstLast2	Patient.name.last().given = 'Peter' | 'James'
YamlReferenceImplTest	testFhirR4	235	unsupported		Unsupported function: tail	group: testTail - ** testTail1	(0 | 1 | 2).tail() = 1 | 2
YamlReferenceImplTest	testFhirR4	236	unsupported		Unsupported function: tail	group: testTail - ** testTail2	Patient.name.tail().given = 'Jim' | 'Peter' | 'James'
YamlReferenceImplTest	testFhirR4	237	unsupported		Unsupported function: skip	group: testSkip - ** testSkip1	(0 | 1 | 2).skip(1) = 1 | 2
YamlReferenceImplTest	testFhirR4	238	unsupported		Unsupported function: skip	group: testSkip - ** testSkip2	(0 | 1 | 2).skip(2) = 2
YamlReferenceImplTest	testFhirR4	239	unsupported		Unsupported function: skip	group: testSkip - ** testSkip3	Patient.name.skip(1).given.trace('test') = 'Jim' | 'Peter' | 'James'
YamlReferenceImplTest	testFhirR4	240	unsupported		Unsupported function: skip	group: testSkip - ** testSkip4	Patient.name.skip(3).given.exists() = false
YamlReferenceImplTest	testFhirR4	241	unsupported		Unsupported function: take	group: testTake - ** testTake1	(0 | 1 | 2).take(1) = 0
YamlReferenceImplTest	testFhirR4	242	unsupported		Unsupported function: take	group: testTake - ** testTake2	(0 | 1 | 2).take(2) = 0 | 1
YamlReferenceImplTest	testFhirR4	243	unsupported		Unsupported function: take	group: testTake - ** testTake3	Patient.name.take(1).given = 'Peter' | 'James'
YamlReferenceImplTest	testFhirR4	244	unsupported		Unsupported function: take	group: testTake - ** testTake4	Patient.name.take(2).given = 'Peter' | 'James' | 'Jim'
YamlReferenceImplTest	testFhirR4	245	unsupported		Unsupported function: take	group: testTake - ** testTake5	Patient.name.take(3).given.count() = 5
YamlReferenceImplTest	testFhirR4	246	unsupported		Unsupported function: take	group: testTake - ** testTake6	Patient.name.take(4).given.count() = 5
YamlReferenceImplTest	testFhirR4	247	unsupported		Unsupported function: take	group: testTake - ** testTake7	Patient.name.take(0).given.exists() = false
YamlReferenceImplTest	testFhirR4	248	unsupported		Unsupported function: iif	group: testIif - ** testIif1	iif(Patient.name.exists(), 'named', 'unnamed') = 'named'
YamlReferenceImplTest	testFhirR4	249	unsupported		Unsupported function: iif	group: testIif - ** testIif2	iif(Patient.name.empty(), 'unnamed', 'named') = 'named'
YamlReferenceImplTest	testFhirR4	250	unsupported		Unsupported function: iif	group: testIif - ** testIif3	iif(true, true, (1 | 2).toString())
YamlReferenceImplTest	testFhirR4	251	unsupported		Unsupported function: iif	group: testIif - ** testIif4	iif(false, (1 | 2).toString(), true)
YamlReferenceImplTest	testFhirR4	252	passed			group: testToInteger - ** testToInteger1	'1'.toInteger() = 1
YamlReferenceImplTest	testFhirR4	253	passed			group: testToInteger - ** testToInteger2	'-1'.toInteger() = -1
YamlReferenceImplTest	testFhirR4	254	passed			group: testToInteger - ** testToInteger3	'0'.toInteger() = 0
YamlReferenceImplTest	testFhirR4	255	passed			group: testToInteger - ** testToInteger4	'0.0'.toInteger().empty()
YamlReferenceImplTest	testFhirR4	256	passed			group: testToInteger - ** testToInteger5	'st'.toInteger().empty()
YamlReferenceImplTest	testFhirR4	257	passed			group: testToDecimal - ** testToDecimal1	'1'.toDecimal() = 1
YamlReferenceImplTest	testFhirR4	258	passed			group: testToDecimal - ** testToDecimal2	'-1'.toInteger() = -1
YamlReferenceImplTest	testFhirR4	259	passed			group: testToDecimal - ** testToDecimal3	'0'.toDecimal() = 0
YamlReferenceImplTest	testFhirR4	260	passed			group: testToDecimal - ** testToDecimal4	'0.0'.toDecimal() = 0.0
YamlReferenceImplTest	testFhirR4	261	passed			group: testToDecimal - ** testToDecimal5	'st'.toDecimal().empty()
YamlReferenceImplTest	testFhirR4	262	passed			group: testToString - ** testToString1	1.toString() = '1'
YamlReferenceImplTest	testFhirR4	263	passed			group: testToString - ** testToString2	'-1'.toInteger() = -1
YamlReferenceImplTest	testFhirR4	264	passed			group: testToString - ** testToString3	0.toString() = '0'
YamlReferenceImplTest	testFhirR4	265	passed			group: testToString - ** testToString5	@2014-12-14.toString() = '2014-12-14'
YamlReferenceImplTest	testFhirR4	266	unsupported		Unsupported function: upper	group: testCase - ** testCase1	't'.upper() = 'T'
YamlReferenceImplTest	testFhirR4	267	unsupported		Unsupported function: lower	group: testCase - ** testCase2	't'.lower() = 't'
YamlReferenceImplTest	testFhirR4	268	unsupported		Unsupported function: upper	group: testCase - ** testCase3	'T'.upper() = 'T'
YamlReferenceImplTest	testFhirR4	269	unsupported		Unsupported function: lower	group: testCase - ** testCase4	'T'.lower() = 't'
YamlReferenceImplTest	testFhirR4	270	unsupported		Unsupported function: toChars	group: testToChars - ** testToChars1	't2'.toChars() = 't' | '2'
YamlReferenceImplTest	testFhirR4	271	unsupported		Unsupported function: substring	group: testSubstring - ** testSubstring1	'12345'.substring(2) = '345'
YamlReferenceImplTest	testFhirR4	272	unsupported		Unsupported function: substring	group: testSubstring - ** testSubstring2	'12345'.substring(2,1) = '3'
YamlReferenceImplTest	testFhirR4	273	unsupported		Unsupported function: substring	group: testSubstring - ** testSubstring3	'12345'.substring(2,5) = '345'
YamlReferenceImplTest	testFhirR4	274	unsupported		Unsupported function: substring	group: testSubstring - ** testSubstring4	'12345'.substring(25).empty()
YamlReferenceImplTest	testFhirR4	275	unsupported		Unsupported function: substring	group: testSubstring - ** testSubstring5	'12345'.substring(-1).empty()
YamlReferenceImplTest	testFhirR4	276	passed			group: testStartsWith - ** testStartsWith1	'12345'.startsWith('2') = false
YamlReferenceImplTest	testFhirR4	277	passed			group: testStartsWith - ** testStartsWith2	'12345'.startsWith('1') = true
YamlReferenceImplTest	testFhirR4	278	passed			group: testStartsWith - ** testStartsWith3	'12345'.startsWith('12') = true
YamlReferenceImplTest	testFhirR4	279	passed			group: testStartsWith - ** testStartsWith4	'12345'.startsWith('13') = false
YamlReferenceImplTest	testFhirR4	280	passed			group: testStartsWith - ** testStartsWith5	'12345'.startsWith('12345') = true
YamlReferenceImplTest	testFhirR4	281	passed			group: testStartsWith - ** testStartsWith6	'12345'.startsWith('123456') = false
YamlReferenceImplTest	testFhirR4	282	passed			group: testStartsWith - ** testStartsWith7	'12345'.startsWith('') = true
YamlReferenceImplTest	testFhirR4	283	passed			group: testEndsWith - ** testEndsWith1	'12345'.endsWith('2') = false
YamlReferenceImplTest	testFhirR4	284	passed			group: testEndsWith - ** testEndsWith2	'12345'.endsWith('5') = true
YamlReferenceImplTest	testFhirR4	285	passed			group: testEndsWith - ** testEndsWith3	'12345'.endsWith('45') = true
YamlReferenceImplTest	testFhirR4	286	passed			group: testEndsWith - ** testEndsWith4	'12345'.endsWith('35') = false
YamlReferenceImplTest	testFhirR4	287	passed			group: testEndsWith - ** testEndsWith5	'12345'.endsWith('12345') = true
YamlReferenceImplTest	testFhirR4	288	passed			group: testEndsWith - ** testEndsWith6	'12345'.endsWith('012345') = false
YamlReferenceImplTest	testFhirR4	289	passed			group: testEndsWith - ** testEndsWith7	'12345'.endsWith('') = true
YamlReferenceImplTest	testFhirR4	290	passed			group: testContainsString - ** testContainsString1	'12345'.contains('6') = false
YamlReferenceImplTest	testFhirR4	291	passed			group: testContainsString - ** testContainsString2	'12345'.contains('5') = true
YamlReferenceImplTest	testFhirR4	292	passed			group: testContainsString - ** testContainsString3	'12345'.contains('45') = true
YamlReferenceImplTest	testFhirR4	293	passed			group: testContainsString - ** testContainsString4	'12345'.contains('35') = false
YamlReferenceImplTest	testFhirR4	294	passed			group: testContainsString - ** testContainsString5	'12345'.contains('12345') = true
YamlReferenceImplTest	testFhirR4	295	passed			group: testContainsString - ** testContainsString6	'12345'.contains('012345') = false
YamlReferenceImplTest	testFhirR4	296	passed			group: testContainsString - ** testContainsString7	'12345'.contains('') = true
YamlReferenceImplTest	testFhirR4	297	unsupported		Unsupported function: length	group: testLength - ** testLength1	'123456'.length() = 6
YamlReferenceImplTest	testFhirR4	298	unsupported		Unsupported function: length	group: testLength - ** testLength2	'12345'.length() = 5
YamlReferenceImplTest	testFhirR4	299	unsupported		Unsupported function: length	group: testLength - ** testLength3	'123'.length() = 3
YamlReferenceImplTest	testFhirR4	300	unsupported		Unsupported function: length	group: testLength - ** testLength4	'1'.length() = 1
YamlReferenceImplTest	testFhirR4	301	unsupported		Unsupported function: length	group: testLength - ** testLength5	''.length() = 0
YamlReferenceImplTest	testFhirR4	302	passed			group: testTrace - ** testTrace1	name.given.trace('test').count() = 5
YamlReferenceImplTest	testFhirR4	303	passed			group: testTrace - ** testTrace2	name.trace('test', given).count() = 3
YamlReferenceImplTest	testFhirR4	304	unsupported		Unsupported function: today	group: testToday - ** testToday1	Patient.birthDate < today()
YamlReferenceImplTest	testFhirR4	305	unsupported		Unsupported function: today	group: testToday - ** testToday2	today().toString().length() = 10
YamlReferenceImplTest	testFhirR4	306	unsupported		Unsupported function: now	group: testNow - ** testNow1	Patient.birthDate < now()
YamlReferenceImplTest	testFhirR4	307	unsupported		Unsupported function: now	group: testNow - ** testNow2	now().toString().length() > 10
YamlReferenceImplTest	testFhirR4	308	passed			group: testEquality - ** testEquality1	1 = 1
YamlReferenceImplTest	testFhirR4	309	passed			group: testEquality - ** testEquality2	{} = {}
YamlReferenceImplTest	testFhirR4	310	passed			group: testEquality - ** testEquality3	true = {}
YamlReferenceImplTest	testFhirR4	311	passed			group: testEquality - ** testEquality4	(1) = (1)
YamlReferenceImplTest	testFhirR4	312	passed			group: testEquality - ** testEquality5	(1 | 2) = (1 | 2)
YamlReferenceImplTest	testFhirR4	313	passed			group: testEquality - ** testEquality6	(1 | 2 | 3) = (1 | 2 | 3)
YamlReferenceImplTest	testFhirR4	314	passed			group: testEquality - ** testEquality8	1 = 2
YamlReferenceImplTest	testFhirR4	315	passed			group: testEquality - ** testEquality9	'a' = 'a'
YamlReferenceImplTest	testFhirR4	316	passed			group: testEquality - ** testEquality10	'a' = 'A'
YamlReferenceImplTest	testFhirR4	317	passed			group: testEquality - ** testEquality11	'a' = 'b'
YamlReferenceImplTest	testFhirR4	318	passed			group: testEquality - ** testEquality12	1.1 = 1.1
YamlReferenceImplTest	testFhirR4	319	passed			group: testEquality - ** testEquality13	1.1 = 1.2
YamlReferenceImplTest	testFhirR4	320	passed			group: testEquality - ** testEquality14	1.10 = 1.1
YamlReferenceImplTest	testFhirR4	321	passed			group: testEquality - ** testEquality15	0 = 0
YamlReferenceImplTest	testFhirR4	322	passed			group: testEquality - ** testEquality16	0.0 = 0
YamlReferenceImplTest	testFhirR4	323	passed			group: testEquality - ** testEquality17	@2012-04-15 = @2012-04-15
YamlReferenceImplTest	testFhirR4	324	passed			group: testEquality - ** testEquality18	@2012-04-15 = @2012-04-16
YamlReferenceImplTest	testFhirR4	325	passed			group: testEquality - ** testEquality19	@2012-04-15 = @2012-04-15T10:00:00
YamlReferenceImplTest	testFhirR4	326	passed			group: testEquality - ** testEquality20	@2012-04-15T15:00:00 = @2012-04-15T10:00:00
YamlReferenceImplTest	testFhirR4	327	passed			group: testEquality - ** testEquality21	@2012-04-15T15:30:31 = @2012-04-15T15:30:31.0
YamlReferenceImplTest	testFhirR4	328	passed			group: testEquality - ** testEquality22	@2012-04-15T15:30:31 = @2012-04-15T15:30:31.1
YamlReferenceImplTest	testFhirR4	329	passed			group: testEquality - ** testEquality24	@2012-04-15T15:00:00+02:00 = @2012-04-15T16:00:00+03:00
YamlReferenceImplTest	testFhirR4	330	excluded:unsupported	#2420: Unsupported equality for complex types	Unsupported equality for complex types	group: testEquality - ** testEquality25	name = name
YamlReferenceImplTest	testFhirR4	331	unsupported		Unsupported function: take	group: testEquality - ** testEquality26	name.take(2) = name.take(2).first() | name.take(2).last()
YamlReferenceImplTest	testFhirR4	332	unsupported		Unsupported function: take	group: testEquality - ** testEquality27	name.take(2) = name.take(2).last() | name.take(2).first()
YamlReferenceImplTest	testFhirR4	333	excluded:unsupported	#2163: Implicit conversion of polymorphic types not supported	Representation of this path is not supported: choice element 'value' (do you need to use ofType?)	group: testEquality - ** testEquality28	Observation.value = 185 '[lb_av]'
YamlReferenceImplTest	testFhirR4	334	passed			group: testNEquality - ** testNEquality1	1 != 1
YamlReferenceImplTest	testFhirR4	335	passed			group: testNEquality - ** testNEquality2	{} != {}
YamlReferenceImplTest	testFhirR4	336	passed			group: testNEquality - ** testNEquality3	1 != 2
YamlReferenceImplTest	testFhirR4	337	passed			group: testNEquality - ** testNEquality4	'a' != 'a'
YamlReferenceImplTest	testFhirR4	338	passed			group: testNEquality - ** testNEquality5	'a' != 'b'
YamlReferenceImplTest	testFhirR4	339	passed			group: testNEquality - ** testNEquality6	1.1 != 1.1
YamlReferenceImplTest	testFhirR4	340	passed			group: testNEquality - ** testNEquality7	1.1 != 1.2
YamlReferenceImplTest	testFhirR4	341	passed			group: testNEquality - ** testNEquality8	1.10 != 1.1
YamlReferenceImplTest	testFhirR4	342	passed			group: testNEquality - ** testNEquality9	0 != 0
YamlReferenceImplTest	testFhirR4	343	passed			group: testNEquality - ** testNEquality10	0.0 != 0
YamlReferenceImplTest	testFhirR4	344	passed			group: testNEquality - ** testNEquality11	@2012-04-15 != @2012-04-15
YamlReferenceImplTest	testFhirR4	345	passed			group: testNEquality - ** testNEquality12	@2012-04-15 != @2012-04-16
YamlReferenceImplTest	testFhirR4	346	passed			group: testNEquality - ** testNEquality13	@2012-04-15 != @2012-04-15T10:00:00
YamlReferenceImplTest	testFhirR4	347	passed			group: testNEquality - ** testNEquality14	@2012-04-15T15:00:00 != @2012-04-15T10:00:00
YamlReferenceImplTest	testFhirR4	348	passed			group: testNEquality - ** testNEquality15	@2012-04-15T15:30:31 != @2012-04-15T15:30:31.0
YamlReferenceImplTest	testFhirR4	349	passed			group: testNEquality - ** testNEquality16	@2012-04-15T15:30:31 != @2012-04-15T15:30:31.1
YamlReferenceImplTest	testFhirR4	350	passed			group: testNEquality - ** testNEquality18	@2012-04-15T15:00:00+02:00 != @2012-04-15T16:00:00+03:00
YamlReferenceImplTest	testFhirR4	351	excluded:unsupported	#2420: Unsupported equality for complex types	Unsupported equality for complex types	group: testNEquality - ** testNEquality19	name != name
YamlReferenceImplTest	testFhirR4	352	unsupported		Unsupported function: take	group: testNEquality - ** testNEquality20	name.take(2) != name.take(2).first() | name.take(2).last()
YamlReferenceImplTest	testFhirR4	353	unsupported		Unsupported function: take	group: testNEquality - ** testNEquality21	name.take(2) != name.take(2).last() | name.take(2).first()
YamlReferenceImplTest	testFhirR4	354	excluded:error	#2144: Decimal literal exceeded maximum scale supported		group: testNEquality - ** testNEquality22	1.2 / 1.8 != 0.6666667
YamlReferenceImplTest	testFhirR4	355	passed			group: testNEquality - ** testNEquality23	1.2 / 1.8 != 0.67
YamlReferenceImplTest	testFhirR4	356	excluded:unsupported	#2163: Implicit conversion of polymorphic types not supported	Representation of this path is not supported: choice element 'value' (do you need to use ofType?)	group: testNEquality - ** testNEquality24	Observation.value != 185 'kg'
YamlReferenceImplTest	testFhirR4	357	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent1	1 ~ 1
YamlReferenceImplTest	testFhirR4	358	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent2	{} ~ {}
YamlReferenceImplTest	testFhirR4	359	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent4	1 ~ 2
YamlReferenceImplTest	testFhirR4	360	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent5	'a' ~ 'a'
YamlReferenceImplTest	testFhirR4	361	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent6	'a' ~ 'A'
YamlReferenceImplTest	testFhirR4	362	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent7	'a' ~ 'b'
YamlReferenceImplTest	testFhirR4	363	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent8	1.1 ~ 1.1
YamlReferenceImplTest	testFhirR4	364	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent9	1.1 ~ 1.2
YamlReferenceImplTest	testFhirR4	365	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent10	1.10 ~ 1.1
YamlReferenceImplTest	testFhirR4	366	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent11	1.2 / 1.8 ~ 0.67
YamlReferenceImplTest	testFhirR4	367	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent12	0 ~ 0
YamlReferenceImplTest	testFhirR4	368	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent13	0.0 ~ 0
YamlReferenceImplTest	testFhirR4	369	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent14	@2012-04-15 ~ @2012-04-15
YamlReferenceImplTest	testFhirR4	370	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent15	@2012-04-15 ~ @2012-04-16
YamlReferenceImplTest	testFhirR4	371	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent16	@2012-04-15 ~ @2012-04-15T10:00:00
YamlReferenceImplTest	testFhirR4	372	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent17	@2012-04-15T15:30:31 ~ @2012-04-15T15:30:31.0
YamlReferenceImplTest	testFhirR4	373	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent18	@2012-04-15T15:30:31 ~ @2012-04-15T15:30:31.1
YamlReferenceImplTest	testFhirR4	374	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent19	name ~ name
YamlReferenceImplTest	testFhirR4	375	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent20	name.take(2).given ~ name.take(2).first().given | name.take(2).last().given
YamlReferenceImplTest	testFhirR4	376	unsupported		Unsupported operator: ~	group: testEquivalent - ** testEquivalent22	Observation.value ~ 185 '[lb_av]'
YamlReferenceImplTest	testFhirR4	377	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent1	1 !~ 1
YamlReferenceImplTest	testFhirR4	378	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent2	{} !~ {}
YamlReferenceImplTest	testFhirR4	379	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent4	1 !~ 2
YamlReferenceImplTest	testFhirR4	380	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent5	'a' !~ 'a'
YamlReferenceImplTest	testFhirR4	381	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent6	'a' !~ 'A'
YamlReferenceImplTest	testFhirR4	382	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent7	'a' !~ 'b'
YamlReferenceImplTest	testFhirR4	383	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent8	1.1 !~ 1.1
YamlReferenceImplTest	testFhirR4	384	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent9	1.1 !~ 1.2
YamlReferenceImplTest	testFhirR4	385	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent10	1.10 !~ 1.1
YamlReferenceImplTest	testFhirR4	386	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent11	0 !~ 0
YamlReferenceImplTest	testFhirR4	387	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent12	0.0 !~ 0
YamlReferenceImplTest	testFhirR4	388	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent13	1.2 / 1.8 !~ 0.6
YamlReferenceImplTest	testFhirR4	389	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent14	@2012-04-15 !~ @2012-04-15
YamlReferenceImplTest	testFhirR4	390	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent15	@2012-04-15 !~ @2012-04-16
YamlReferenceImplTest	testFhirR4	391	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent16	@2012-04-15 !~ @2012-04-15T10:00:00
YamlReferenceImplTest	testFhirR4	392	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent17	@2012-04-15T15:30:31 !~ @2012-04-15T15:30:31.0
YamlReferenceImplTest	testFhirR4	393	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent18	@2012-04-15T15:30:31 !~ @2012-04-15T15:30:31.1
YamlReferenceImplTest	testFhirR4	394	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent20	name.take(2).given !~ name.take(2).first().given | name.take(2).last().given
YamlReferenceImplTest	testFhirR4	395	unsupported		Unsupported operator: !~	group: testNotEquivalent - ** testNotEquivalent22	Observation.value !~ 185 'kg'
YamlReferenceImplTest	testFhirR4	396	passed			group: testLessThan - ** testLessThan1	1 < 2
YamlReferenceImplTest	testFhirR4	397	passed			group: testLessThan - ** testLessThan2	1.0 < 1.2
YamlReferenceImplTest	testFhirR4	398	passed			group: testLessThan - ** testLessThan3	'a' < 'b'
YamlReferenceImplTest	testFhirR4	399	passed			group: testLessThan - ** testLessThan4	'A' < 'a'
YamlReferenceImplTest	testFhirR4	400	passed			group: testLessThan - ** testLessThan5	@2014-12-12 < @2014-12-13
YamlReferenceImplTest	testFhirR4	401	passed			group: testLessThan - ** testLessThan6	@2014-12-13T12:00:00 < @2014-12-13T12:00:01
YamlReferenceImplTest	testFhirR4	402	passed			group: testLessThan - ** testLessThan7	@T12:00:00 < @T14:00:00
YamlReferenceImplTest	testFhirR4	403	passed			group: testLessThan - ** testLessThan8	1 < 1
YamlReferenceImplTest	testFhirR4	404	passed			group: testLessThan - ** testLessThan9	1.0 < 1.0
YamlReferenceImplTest	testFhirR4	405	passed			group: testLessThan - ** testLessThan10	'a' < 'a'
YamlReferenceImplTest	testFhirR4	406	passed			group: testLessThan - ** testLessThan11	'A' < 'A'
YamlReferenceImplTest	testFhirR4	407	passed			group: testLessThan - ** testLessThan12	@2014-12-12 < @2014-12-12
YamlReferenceImplTest	testFhirR4	408	passed			group: testLessThan - ** testLessThan13	@2014-12-13T12:00:00 < @2014-12-13T12:00:00
YamlReferenceImplTest	testFhirR4	409	passed			group: testLessThan - ** testLessThan14	@T12:00:00 < @T12:00:00
YamlReferenceImplTest	testFhirR4	410	passed			group: testLessThan - ** testLessThan15	2 < 1
YamlReferenceImplTest	testFhirR4	411	passed			group: testLessThan - ** testLessThan16	1.1 < 1.0
YamlReferenceImplTest	testFhirR4	412	passed			group: testLessThan - ** testLessThan17	'b' < 'a'
YamlReferenceImplTest	testFhirR4	413	passed			group: testLessThan - ** testLessThan18	'B' < 'A'
YamlReferenceImplTest	testFhirR4	414	passed			group: testLessThan - ** testLessThan19	@2014-12-13 < @2014-12-12
YamlReferenceImplTest	testFhirR4	415	passed			group: testLessThan - ** testLessThan20	@2014-12-13T12:00:01 < @2014-12-13T12:00:00
YamlReferenceImplTest	testFhirR4	416	passed			group: testLessThan - ** testLessThan21	@T12:00:01 < @T12:00:00
YamlReferenceImplTest	testFhirR4	417	excluded:error	#2163: Implicit conversion of polymorphic types not supported		group: testLessThan - ** testLessThan22	Observation.value < 200 '[lb_av]'
YamlReferenceImplTest	testFhirR4	418	passed			group: testLessThan - ** testLessThan23	@2018-03 < @2018-03-01
YamlReferenceImplTest	testFhirR4	419	passed			group: testLessThan - ** testLessThan24	@2018-03-01T10 < @2018-03-01T10:30
YamlReferenceImplTest	testFhirR4	420	passed			group: testLessThan - ** testLessThan25	@T10 < @T10:30
YamlReferenceImplTest	testFhirR4	421	passed			group: testLessThan - ** testLessThan26	@2018-03-01T10:30:00 < @2018-03-01T10:30:00.0
YamlReferenceImplTest	testFhirR4	422	passed			group: testLessThan - ** testLessThan27	@T10:30:00 < @T10:30:00.0
YamlReferenceImplTest	testFhirR4	423	passed			group: testLessOrEqual - ** testLessOrEqual1	1 <= 2
YamlReferenceImplTest	testFhirR4	424	passed			group: testLessOrEqual - ** testLessOrEqual2	1.0 <= 1.2
YamlReferenceImplTest	testFhirR4	425	passed			group: testLessOrEqual - ** testLessOrEqual3	'a' <= 'b'
YamlReferenceImplTest	testFhirR4	426	passed			group: testLessOrEqual - ** testLessOrEqual4	'A' <= 'a'
YamlReferenceImplTest	testFhirR4	427	passed			group: testLessOrEqual - ** testLessOrEqual5	@2014-12-12 <= @2014-12-13
YamlReferenceImplTest	testFhirR4	428	passed			group: testLessOrEqual - ** testLessOrEqual6	@2014-12-13T12:00:00 <= @2014-12-13T12:00:01
YamlReferenceImplTest	testFhirR4	429	passed			group: testLessOrEqual - ** testLessOrEqual7	@T12:00:00 <= @T14:00:00
YamlReferenceImplTest	testFhirR4	430	passed			group: testLessOrEqual - ** testLessOrEqual8	1 <= 1
YamlReferenceImplTest	testFhirR4	431	passed			group: testLessOrEqual - ** testLessOrEqual9	1.0 <= 1.0
YamlReferenceImplTest	testFhirR4	432	passed			group: testLessOrEqual - ** testLessOrEqual10	'a' <= 'a'
YamlReferenceImplTest	testFhirR4	433	passed			group: testLessOrEqual - ** testLessOrEqual11	'A' <= 'A'
YamlReferenceImplTest	testFhirR4	434	passed			group: testLessOrEqual - ** testLessOrEqual12	@2014-12-12 <= @2014-12-12
YamlReferenceImplTest	testFhirR4	435	passed			group: testLessOrEqual - ** testLessOrEqual13	@2014-12-13T12:00:00 <= @2014-12-13T12:00:00
YamlReferenceImplTest	testFhirR4	436	passed			group: testLessOrEqual - ** testLessOrEqual14	@T12:00:00 <= @T12:00:00
YamlReferenceImplTest	testFhirR4	437	passed			group: testLessOrEqual - ** testLessOrEqual15	2 <= 1
YamlReferenceImplTest	testFhirR4	438	passed			group: testLessOrEqual - ** testLessOrEqual16	1.1 <= 1.0
YamlReferenceImplTest	testFhirR4	439	passed			group: testLessOrEqual - ** testLessOrEqual17	'b' <= 'a'
YamlReferenceImplTest	testFhirR4	440	passed			group: testLessOrEqual - ** testLessOrEqual18	'B' <= 'A'
YamlReferenceImplTest	testFhirR4	441	passed			group: testLessOrEqual - ** testLessOrEqual19	@2014-12-13 <= @2014-12-12
YamlReferenceImplTest	testFhirR4	442	passed			group: testLessOrEqual - ** testLessOrEqual20	@2014-12-13T12:00:01 <= @2014-12-13T12:00:00
YamlReferenceImplTest	testFhirR4	443	passed			group: testLessOrEqual - ** testLessOrEqual21	@T12:00:01 <= @T12:00:00
YamlReferenceImplTest	testFhirR4	444	excluded:error	#2163: Implicit conversion of polymorphic types not supported		group: testLessOrEqual - ** testLessOrEqual22	Observation.value <= 200 '[lb_av]'
YamlReferenceImplTest	testFhirR4	445	passed			group: testLessOrEqual - ** testLessOrEqual23	@2018-03 <= @2018-03-01
YamlReferenceImplTest	testFhirR4	446	passed			group: testLessOrEqual - ** testLessOrEqual24	@2018-03-01T10 <= @2018-03-01T10:30
YamlReferenceImplTest	testFhirR4	447	passed			group: testLessOrEqual - ** testLessOrEqual25	@T10 <= @T10:30
YamlReferenceImplTest	testFhirR4	448	passed			group: testLessOrEqual - ** testLessOrEqual26	@2018-03-01T10:30:00  <= @2018-03-01T10:30:00.0
YamlReferenceImplTest	testFhirR4	449	passed			group: testLessOrEqual - ** testLessOrEqual27	@T10:30:00 <= @T10:30:00.0
YamlReferenceImplTest	testFhirR4	450	passed			group: testGreatorOrEqual - ** testGreatorOrEqual1	1 >= 2
YamlReferenceImplTest	testFhirR4	451	passed			group: testGreatorOrEqual - ** testGreatorOrEqual2	1.0 >= 1.2
YamlReferenceImplTest	testFhirR4	452	passed			group: testGreatorOrEqual - ** testGreatorOrEqual3	'a' >= 'b'
YamlReferenceImplTest	testFhirR4	453	passed			group: testGreatorOrEqual - ** testGreatorOrEqual4	'A' >= 'a'
YamlReferenceImplTest	testFhirR4	454	passed			group: testGreatorOrEqual - ** testGreatorOrEqual5	@2014-12-12 >= @2014-12-13
YamlReferenceImplTest	testFhirR4	455	passed			group: testGreatorOrEqual - ** testGreatorOrEqual6	@2014-12-13T12:00:00 >= @2014-12-13T12:00:01
YamlReferenceImplTest	testFhirR4	456	passed			group: testGreatorOrEqual - ** testGreatorOrEqual7	@T12:00:00 >= @T14:00:00
YamlReferenceImplTest	testFhirR4	457	passed			group: testGreatorOrEqual - ** testGreatorOrEqual8	1 >= 1
YamlReferenceImplTest	testFhirR4	458	passed			group: testGreatorOrEqual - ** testGreatorOrEqual9	1.0 >= 1.0
YamlReferenceImplTest	testFhirR4	459	passed			group: testGreatorOrEqual - ** testGreatorOrEqual10	'a' >= 'a'
YamlReferenceImplTest	testFhirR4	460	passed			group: testGreatorOrEqual - ** testGreatorOrEqual11	'A' >= 'A'
YamlReferenceImplTest	testFhirR4	461	passed			group: testGreatorOrEqual - ** testGreatorOrEqual12	@2014-12-12 >= @2014-12-12
YamlReferenceImplTest	testFhirR4	462	passed			group: testGreatorOrEqual - ** testGreatorOrEqual13	@2014-12-13T12:00:00 >= @2014-12-13T12:00:00
YamlReferenceImplTest	testFhirR4	463	passed			group: testGreatorOrEqual - ** testGreatorOrEqual14	@T12:00:00 >= @T12:00:00
YamlReferenceImplTest	testFhirR4	464	passed			group: testGreatorOrEqual - ** testGreatorOrEqual15	2 >= 1
YamlReferenceImplTest	testFhirR4	465	passed			group: testGreatorOrEqual - ** testGreatorOrEqual16	1.1 >= 1.0
YamlReferenceImplTest	testFhirR4	466	passed			group: testGreatorOrEqual - ** testGreatorOrEqual17	'b' >= 'a'
YamlReferenceImplTest	testFhirR4	467	passed			group: testGreatorOrEqual - ** testGreatorOrEqual18	'B' >= 'A'
YamlReferenceImplTest	testFhirR4	468	passed			group: testGreatorOrEqual - ** testGreatorOrEqual19	@2014-12-13 >= @2014-12-12
YamlReferenceImplTest	testFhirR4	469	passed			group: testGreatorOrEqual - ** testGreatorOrEqual20	@2014-12-13T12:00:01 >= @2014-12-13T12:00:00
YamlReferenceImplTest	testFhirR4	470	passed			group: testGreatorOrEqual - ** testGreatorOrEqual21	@T12:00:01 >= @T12:00:00
YamlReferenceImplTest	testFhirR4	471	excluded:error	#2163: Implicit conversion of polymorphic types not supported		group: testGreatorOrEqual - ** testGreatorOrEqual22	Observation.value >= 100 '[lb_av]'
YamlReferenceImplTest	testFhirR4	472	passed			group: testGreatorOrEqual - ** testGreatorOrEqual23	@2018-03 >= @2018-03-01
YamlReferenceImplTest	testFhirR4	473	passed			group: testGreatorOrEqual - ** testGreatorOrEqual24	@2018-03-01T10 >= @2018-03-01T10:30
YamlReferenceImplTest	testFhirR4	474	passed			group: testGreatorOrEqual - ** testGreatorOrEqual25	@T10 >= @T10:30
YamlReferenceImplTest	testFhirR4	475	passed			group: testGreatorOrEqual - ** testGreatorOrEqual26	@2018-03-01T10:30:00 >= @2018-03-01T10:30:00.0
YamlReferenceImplTest	testFhirR4	476	passed			group: testGreatorOrEqual - ** testGreatorOrEqual27	@T10:30:00 >= @T10:30:00.0
YamlReferenceImplTest	testFhirR4	477	passed			group: testGreaterThan - ** testGreaterThan1	1 > 2
YamlReferenceImplTest	testFhirR4	478	passed			group: testGreaterThan - ** testGreaterThan2	1.0 > 1.2
YamlReferenceImplTest	testFhirR4	479	passed			group: testGreaterThan - ** testGreaterThan3	'a' > 'b'
YamlReferenceImplTest	testFhirR4	480	passed			group: testGreaterThan - ** testGreaterThan4	'A' > 'a'
YamlReferenceImplTest	testFhirR4	481	passed			group: testGreaterThan - ** testGreaterThan5	@2014-12-12 > @2014-12-13
YamlReferenceImplTest	testFhirR4	482	passed			group: testGreaterThan - ** testGreaterThan6	@2014-12-13T12:00:00 > @2014-12-13T12:00:01
YamlReferenceImplTest	testFhirR4	483	passed			group: testGreaterThan - ** testGreaterThan7	@T12:00:00 > @T14:00:00
YamlReferenceImplTest	testFhirR4	484	passed			group: testGreaterThan - ** testGreaterThan8	1 > 1
YamlReferenceImplTest	testFhirR4	485	passed			group: testGreaterThan - ** testGreaterThan9	1.0 > 1.0
YamlReferenceImplTest	testFhirR4	486	passed			group: testGreaterThan - ** testGreaterThan10	'a' > 'a'
YamlReferenceImplTest	testFhirR4	487	passed			group: testGreaterThan - ** testGreaterThan11	'A' > 'A'
YamlReferenceImplTest	testFhirR4	488	passed			group: testGreaterThan - ** testGreaterThan12	@2014-12-12 > @2014-12-12
YamlReferenceImplTest	testFhirR4	489	passed			group: testGreaterThan - ** testGreaterThan13	@2014-12-13T12:00:00 > @2014-12-13T12:00:00
YamlReferenceImplTest	testFhirR4	490	passed			group: testGreaterThan - ** testGreaterThan14	@T12:00:00 > @T12:00:00
YamlReferenceImplTest	testFhirR4	491	passed			group: testGreaterThan - ** testGreaterThan15	2 > 1
YamlReferenceImplTest	testFhirR4	492	passed			group: testGreaterThan - ** testGreaterThan16	1.1 > 1.0
YamlReferenceImplTest	testFhirR4	493	passed			group: testGreaterThan - ** testGreaterThan17	'b' > 'a'
YamlReferenceImplTest	testFhirR4	494	passed			group: testGreaterThan - ** testGreaterThan18	'B' > 'A'
YamlReferenceImplTest	testFhirR4	495	passed			group: testGreaterThan - ** testGreaterThan19	@2014-12-13 > @2014-12-12
YamlReferenceImplTest	testFhirR4	496	passed			group: testGreaterThan - ** testGreaterThan20	@2014-12-13T12:00:01 > @2014-12-13T12:00:00
YamlReferenceImplTest	testFhirR4	497	passed			group: testGreaterThan - ** testGreaterThan21	@T12:00:01 > @T12:00:00
YamlReferenceImplTest	testFhirR4	498	excluded:error	#2163: Implicit conversion of polymorphic types not supported		group: testGreaterThan - ** testGreaterThan22	Observation.value > 100 '[lb_av]'
YamlReferenceImplTest	testFhirR4	499	passed			group: testGreaterThan - ** testGreaterThan23	@2018-03 > @2018-03-01
YamlReferenceImplTest	testFhirR4	500	passed			group: testGreaterThan - ** testGreaterThan24	@2018-03-01T10 > @2018-03-01T10:30
YamlReferenceImplTest	testFhirR4	501	passed			group: testGreaterThan - ** testGreaterThan25	@T10 > @T10:30
YamlReferenceImplTest	testFhirR4	502	passed			group: testGreaterThan - ** testGreaterThan26	@2018-03-01T10:30:00 > @2018-03-01T10:30:00.0
YamlReferenceImplTest	testFhirR4	503	passed			group: testGreaterThan - ** testGreaterThan27	@T10:30:00 > @T10:30:00.0
YamlReferenceImplTest	testFhirR4	504	passed			group: testUnion - ** testUnion1	(1 | 2 | 3).count() = 3
YamlReferenceImplTest	testFhirR4	505	passed			group: testUnion - ** testUnion2	(1 | 2 | 2).count() = 2
YamlReferenceImplTest	testFhirR4	506	passed			group: testUnion - ** testUnion3	(1|1).count() = 1
YamlReferenceImplTest	testFhirR4	507	passed			group: testUnion - ** testUnion4	1.union(2).union(3).count() = 3
YamlReferenceImplTest	testFhirR4	508	passed			group: testUnion - ** testUnion5	1.union(2.union(3)).count() = 3
YamlReferenceImplTest	testFhirR4	509	passed			group: testUnion - ** testUnion6	(1 | 2).combine(2).count() = 3
YamlReferenceImplTest	testFhirR4	510	passed			group: testUnion - ** testUnion7	1.combine(1).count() = 2
YamlReferenceImplTest	testFhirR4	511	passed			group: testUnion - ** testUnion8	1.combine(1).union(2).count() = 2
YamlReferenceImplTest	testFhirR4	512	unsupported		Unsupported function: intersect	group: testIntersect - ** testIntersect1	(1 | 2 | 3).intersect(2 | 4) = 2
YamlReferenceImplTest	testFhirR4	513	unsupported		Unsupported function: intersect	group: testIntersect - ** testIntersect2	(1 | 2).intersect(4).empty()
YamlReferenceImplTest	testFhirR4	514	unsupported		Unsupported function: intersect	group: testIntersect - ** testIntersect3	(1 | 2).intersect({}).empty()
YamlReferenceImplTest	testFhirR4	515	unsupported		Unsupported function: intersect	group: testIntersect - ** testIntersect4	1.combine(1).intersect(1).count() = 1
YamlReferenceImplTest	testFhirR4	516	unsupported		Unsupported function: exclude	group: testExclude - ** testExclude1	(1 | 2 | 3).exclude(2 | 4) = 1 | 3
YamlReferenceImplTest	testFhirR4	517	unsupported		Unsupported function: exclude	group: testExclude - ** testExclude2	(1 | 2).exclude(4) = 1 | 2
YamlReferenceImplTest	testFhirR4	518	unsupported		Unsupported function: exclude	group: testExclude - ** testExclude3	(1 | 2).exclude({}) = 1 | 2
YamlReferenceImplTest	testFhirR4	519	unsupported		Unsupported function: exclude	group: testExclude - ** testExclude4	1.combine(1).exclude(2).count() = 2
YamlReferenceImplTest	testFhirR4	520	passed			group: testIn - ** testIn1	1 in (1 | 2 | 3)
YamlReferenceImplTest	testFhirR4	521	passed			group: testIn - ** testIn2	1 in (2 | 3)
YamlReferenceImplTest	testFhirR4	522	passed			group: testIn - ** testIn3	'a' in ('a' | 'c' | 'd')
YamlReferenceImplTest	testFhirR4	523	passed			group: testIn - ** testIn4	'b' in ('a' | 'c' | 'd')
YamlReferenceImplTest	testFhirR4	524	passed			group: testContainsCollection - ** testContainsCollection1	(1 | 2 | 3) contains 1
YamlReferenceImplTest	testFhirR4	525	passed			group: testContainsCollection - ** testContainsCollection2	(2 | 3) contains 1
YamlReferenceImplTest	testFhirR4	526	passed			group: testContainsCollection - ** testContainsCollection3	('a' | 'c' | 'd') contains 'a'
YamlReferenceImplTest	testFhirR4	527	passed			group: testContainsCollection - ** testContainsCollection4	('a' | 'c' | 'd') contains 'b'
YamlReferenceImplTest	testFhirR4	528	passed			group: testBooleanLogicAnd - ** testBooleanLogicAnd1	(true and true) = true
YamlReferenceImplTest	testFhirR4	529	passed			group: testBooleanLogicAnd - ** testBooleanLogicAnd2	(true and false) = false
YamlReferenceImplTest	testFhirR4	530	passed			group: testBooleanLogicAnd - ** testBooleanLogicAnd3	(true and {}).empty()
YamlReferenceImplTest	testFhirR4	531	passed			group: testBooleanLogicAnd - ** testBooleanLogicAnd4	(false and true) = false
YamlReferenceImplTest	testFhirR4	532	passed			group: testBooleanLogicAnd - ** testBooleanLogicAnd5	(false and false) = false
YamlReferenceImplTest	testFhirR4	533	passed			group: testBooleanLogicAnd - ** testBooleanLogicAnd6	(false and {}) = false
YamlReferenceImplTest	testFhirR4	534	passed			group: testBooleanLogicAnd - ** testBooleanLogicAnd7	({} and true).empty()
YamlReferenceImplTest	testFhirR4	535	passed			group: testBooleanLogicAnd - ** testBooleanLogicAnd8	({} and false) = false
YamlReferenceImplTest	testFhirR4	536	passed			group: testBooleanLogicAnd - ** testBooleanLogicAnd9	({} and {}).empty()
YamlReferenceImplTest	testFhirR4	537	passed			group: testBooleanLogicOr - ** testBooleanLogicOr1	(true or true) = true
YamlReferenceImplTest	testFhirR4	538	passed			group: testBooleanLogicOr - ** testBooleanLogicOr2	(true or false) = true
YamlReferenceImplTest	testFhirR4	539	passed			group: testBooleanLogicOr - ** testBooleanLogicOr3	(true or {}) = true
YamlReferenceImplTest	testFhirR4	540	passed			group: testBooleanLogicOr - ** testBooleanLogicOr4	(false or true) = true
YamlReferenceImplTest	testFhirR4	541	passed			group: testBooleanLogicOr - ** testBooleanLogicOr5	(false or false) = false
YamlReferenceImplTest	testFhirR4	542	passed			group: testBooleanLogicOr - ** testBooleanLogicOr6	(false or {}).empty()
YamlReferenceImplTest	testFhirR4	543	passed			group: testBooleanLogicOr - ** testBooleanLogicOr7	({} or true) = true
YamlReferenceImplTest	testFhirR4	544	passed			group: testBooleanLogicOr - ** testBooleanLogicOr8	({} or false).empty()
YamlReferenceImplTest	testFhirR4	545	passed			group: testBooleanLogicOr - ** testBooleanLogicOr9	({} or {}).empty()
YamlReferenceImplTest	testFhirR4	546	passed			group: testBooleanLogicXOr - ** testBooleanLogicXOr1	(true xor true) = false
YamlReferenceImplTest	testFhirR4	547	passed			group: testBooleanLogicXOr - ** testBooleanLogicXOr2	(true xor false) = true
YamlReferenceImplTest	testFhirR4	548	passed			group: testBooleanLogicXOr - ** testBooleanLogicXOr3	(true xor {}).empty()
YamlReferenceImplTest	testFhirR4	549	passed			group: testBooleanLogicXOr - ** testBooleanLogicXOr4	(false xor true) = true
YamlReferenceImplTest	testFhirR4	550	passed			group: testBooleanLogicXOr - ** testBooleanLogicXOr5	(false xor false) = false
YamlReferenceImplTest	testFhirR4	551	passed			group: testBooleanLogicXOr - ** testBooleanLogicXOr6	(false xor {}).empty()
YamlReferenceImplTest	testFhirR4	552	passed			group: testBooleanLogicXOr - ** testBooleanLogicXOr7	({} xor true).empty()
YamlReferenceImplTest	testFhirR4	553	passed			group: testBooleanLogicXOr - ** testBooleanLogicXOr8	({} xor false).empty()
YamlReferenceImplTest	testFhirR4	554	passed			group: testBooleanLogicXOr - ** testBooleanLogicXOr9	({} xor {}).empty()
YamlReferenceImplTest	testFhirR4	555	passed			group: testBooleanImplies - ** testBooleanImplies1	(true implies true) = true
YamlReferenceImplTest	testFhirR4	556	passed			group: testBooleanImplies - ** testBooleanImplies2	(true implies false) = false
YamlReferenceImplTest	testFhirR4	557	passed			group: testBooleanImplies - ** testBooleanImplies3	(true implies {}).empty()
YamlReferenceImplTest	testFhirR4	558	passed			group: testBooleanImplies - ** testBooleanImplies4	(false implies true) = true
YamlReferenceImplTest	testFhirR4	559	passed			group: testBooleanImplies - ** testBooleanImplies5	(false implies false) = true
YamlReferenceImplTest	testFhirR4	560	passed			group: testBooleanImplies - ** testBooleanImplies6	(false implies {}) = true
YamlReferenceImplTest	testFhirR4	561	passed			group: testBooleanImplies - ** testBooleanImplies7	({} implies true) = true
YamlReferenceImplTest	testFhirR4	562	passed			group: testBooleanImplies - ** testBooleanImplies8	({} implies false).empty()
YamlReferenceImplTest	testFhirR4	563	passed			group: testBooleanImplies - ** testBooleanImplies9	({} implies {}).empty()
YamlReferenceImplTest	testFhirR4	564	passed			group: testPlus - ** testPlus1	1 + 1 = 2
YamlReferenceImplTest	testFhirR4	565	passed			group: testPlus - ** testPlus2	1 + 0 = 1
YamlReferenceImplTest	testFhirR4	566	passed			group: testPlus - ** testPlus3	1.2 + 1.8 = 3.0
YamlReferenceImplTest	testFhirR4	567	passed			group: testPlus - ** testPlus4	'a'+'b' = 'ab'
YamlReferenceImplTest	testFhirR4	568	unsupported		Unsupported operator: &	group: testConcatenate - ** testConcatenate1	'a' & 'b' = 'ab'
YamlReferenceImplTest	testFhirR4	569	unsupported		Unsupported operator: &	group: testConcatenate - ** testConcatenate2	'1' & {} = '1'
YamlReferenceImplTest	testFhirR4	570	unsupported		Unsupported operator: &	group: testConcatenate - ** testConcatenate3	{} & 'b' = 'b'
YamlReferenceImplTest	testFhirR4	571	unsupported		Unsupported operator: &	group: testConcatenate - ** testConcatenate4	(1 | 2 | 3) & 'b' = '1,2,3b'
YamlReferenceImplTest	testFhirR4	572	passed			group: testMinus - ** testMinus1	1 - 1 = 0
YamlReferenceImplTest	testFhirR4	573	passed			group: testMinus - ** testMinus2	1 - 0 = 1
YamlReferenceImplTest	testFhirR4	574	passed			group: testMinus - ** testMinus3	1.8 - 1.2 = 0.6
YamlReferenceImplTest	testFhirR4	575	unsupported		Operation not supported on String: -	group: testMinus - ** testMinus4	'a'-'b' = 'ab'
YamlReferenceImplTest	testFhirR4	576	passed			group: testMultiply - ** testMultiply1	1.2 * 1.8 = 2.16
YamlReferenceImplTest	testFhirR4	577	passed			group: testMultiply - ** testMultiply2	1 * 1 = 1
YamlReferenceImplTest	testFhirR4	578	passed			group: testMultiply - ** testMultiply3	1 * 0 = 0
YamlReferenceImplTest	testFhirR4	579	passed			group: testDivide - ** testDivide1	1 / 1 = 1
YamlReferenceImplTest	testFhirR4	580	passed			group: testDivide - ** testDivide2	4 / 2 = 2
YamlReferenceImplTest	testFhirR4	581	passed			group: testDivide - ** testDivide3	4.0 / 2.0 = 2.0
YamlReferenceImplTest	testFhirR4	582	passed			group: testDivide - ** testDivide4	1 / 2 = 0.5
YamlReferenceImplTest	testFhirR4	583	excluded:error	#2144: Decimal literal exceeded maximum scale supported		group: testDivide - ** testDivide5	1.2 / 1.8 = 0.66666667
YamlReferenceImplTest	testFhirR4	584	passed			group: testDivide - ** testDivide6	1 / 0
YamlReferenceImplTest	testFhirR4	585	unsupported		Unsupported operator: div	group: testDiv - ** testDiv1	1 div 1 = 1
YamlReferenceImplTest	testFhirR4	586	unsupported		Unsupported operator: div	group: testDiv - ** testDiv2	4 div 2 = 2
YamlReferenceImplTest	testFhirR4	587	unsupported		Unsupported operator: div	group: testDiv - ** testDiv3	5 div 2 = 2
YamlReferenceImplTest	testFhirR4	588	unsupported		Unsupported operator: div	group: testDiv - ** testDiv4	2.2 div 1.8 = 1
YamlReferenceImplTest	testFhirR4	589	unsupported		Unsupported operator: div	group: testDiv - ** testDiv5	5 div 0
YamlReferenceImplTest	testFhirR4	590	passed			group: testMod - ** testMod1	1 mod 1 = 0
YamlReferenceImplTest	testFhirR4	591	passed			group: testMod - ** testMod2	4 mod 2 = 0
YamlReferenceImplTest	testFhirR4	592	passed			group: testMod - ** testMod3	5 mod 2 = 1
YamlReferenceImplTest	testFhirR4	593	passed			group: testMod - ** testMod4	2.2 mod 1.8 = 0.4
YamlReferenceImplTest	testFhirR4	594	passed			group: testMod - ** testMod5	5 mod 0
YamlReferenceImplTest	testFhirR4	595	unsupported		Unsupported function: round	group: testRound - ** testRound1	1.round() = 1
YamlReferenceImplTest	testFhirR4	596	unsupported		Unsupported function: sqrt	group: testSqrt - ** testSqrt1	81.sqrt() = 9.0
YamlReferenceImplTest	testFhirR4	597	unsupported		Unsupported function: sqrt	group: testSqrt - ** testSqrt2	(-1).sqrt()
YamlReferenceImplTest	testFhirR4	598	unsupported		Unsupported function: abs	group: testAbs - ** testAbs1	(-5).abs() = 5
YamlReferenceImplTest	testFhirR4	599	unsupported		Unsupported function: abs	group: testAbs - ** testAbs2	(-5.5).abs() = 5.5
YamlReferenceImplTest	testFhirR4	600	unsupported		Quantity math operations are not supported yet	group: testAbs - ** testAbs3	(-5.5 'mg').abs() = 5.5 'mg'
YamlReferenceImplTest	testFhirR4	601	unsupported		Unsupported function: ceiling	group: testCeiling - ** testCeiling1	1.ceiling() = 1
YamlReferenceImplTest	testFhirR4	602	unsupported		Unsupported function: ceiling	group: testCeiling - ** testCeiling2	(-1.1).ceiling() = -1
YamlReferenceImplTest	testFhirR4	603	unsupported		Unsupported function: ceiling	group: testCeiling - ** testCeiling3	1.1.ceiling() = 2
YamlReferenceImplTest	testFhirR4	604	unsupported		Unsupported function: exp	group: testExp - ** testExp1	0.exp() = 1
YamlReferenceImplTest	testFhirR4	605	unsupported		Unsupported function: exp	group: testExp - ** testExp2	(-0.0).exp() = 1
YamlReferenceImplTest	testFhirR4	606	unsupported		Unsupported function: floor	group: testFloor - ** testFloor1	1.floor() = 1
YamlReferenceImplTest	testFhirR4	607	unsupported		Unsupported function: floor	group: testFloor - ** testFloor2	2.1.floor() = 2
YamlReferenceImplTest	testFhirR4	608	unsupported		Unsupported function: floor	group: testFloor - ** testFloor3	(-2.1).floor() = -3
YamlReferenceImplTest	testFhirR4	609	unsupported		Unsupported function: ln	group: testLn - ** testLn1	1.ln() = 0.0
YamlReferenceImplTest	testFhirR4	610	unsupported		Unsupported function: ln	group: testLn - ** testLn2	1.0.ln() = 0.0
YamlReferenceImplTest	testFhirR4	611	unsupported		Unsupported function: log	group: testLog - ** testLog1	16.log(2) = 4.0
YamlReferenceImplTest	testFhirR4	612	unsupported		Unsupported function: log	group: testLog - ** testLog2	100.0.log(10.0) = 2.0
YamlReferenceImplTest	testFhirR4	613	unsupported		Unsupported function: power	group: testPower - ** testPower1	2.power(3) = 8
YamlReferenceImplTest	testFhirR4	614	unsupported		Unsupported function: power	group: testPower - ** testPower2	2.5.power(2) = 6.25
YamlReferenceImplTest	testFhirR4	615	unsupported		Unsupported function: power	group: testPower - ** testPower3	(-1).power(0.5)
YamlReferenceImplTest	testFhirR4	616	unsupported		Unsupported function: truncate	group: testTruncate - ** testTruncate1	101.truncate() = 101
YamlReferenceImplTest	testFhirR4	617	excluded:error	#2144: Decimal literal exceeded maximum scale supported		group: testTruncate - ** testTruncate2	1.00000001.truncate() = 1
YamlReferenceImplTest	testFhirR4	618	unsupported		Unsupported function: truncate	group: testTruncate - ** testTruncate3	(-1.56).truncate() = -1
YamlReferenceImplTest	testFhirR4	619	passed			group: testPrecedence - ** test unary precedence	-1.convertsToInteger()
YamlReferenceImplTest	testFhirR4	620	passed			group: testPrecedence - ** testPrecedence2	1+2*3+4 = 11
YamlReferenceImplTest	testFhirR4	621	excluded:error	null: 'is' operator precedence differs from fhirpath.js		group: testPrecedence - ** testPrecedence3	1 > 2 is Boolean
YamlReferenceImplTest	testFhirR4	622	excluded:error	null: Operator precedence of is/as causes union type errors		group: testPrecedence - ** testPrecedence4	1 | 1 is Integer
YamlReferenceImplTest	testFhirR4	623	passed			group: testVariables - ** testVariables3	%ucum = 'http://unitsofmeasure.org'
YamlReferenceImplTest	testFhirR4	624	passed			group: testExtension - ** testExtension1	Patient.birthDate.extension('http://hl7.org/fhir/StructureDefinition/patient-birthTime').exists()
YamlReferenceImplTest	testFhirR4	625	excluded:pass	#437: Extensions on primitive types not supported		group: testExtension - ** testExtension3	Patient.birthDate.extension('http://hl7.org/fhir/StructureDefinition/patient-birthTime1').empty()
YamlReferenceImplTest	testFhirR4	626	passed			group: testType - ** testType1	1.type().namespace = 'System'
YamlReferenceImplTest	testFhirR4	627	passed			group: testType - ** testType2	1.type().name = 'Integer'
YamlReferenceImplTest	testFhirR4	628	passed			group: testType - ** testType3	true.type().namespace = 'System'
YamlReferenceImplTest	testFhirR4	629	passed			group: testType - ** testType4	true.type().name = 'Boolean'
YamlReferenceImplTest	testFhirR4	630	passed			group: testType - ** testType5	true.is(Boolean)
YamlReferenceImplTest	testFhirR4	631	passed			group: testType - ** testType6	true.is(System.Boolean)
YamlReferenceImplTest	testFhirR4	632	passed			group: testType - ** testType7	true is Boolean
YamlReferenceImplTest	testFhirR4	633	passed			group: testType - ** testType8	true is System.Boolean
YamlReferenceImplTest	testFhirR4	634	passed			group: testType - ** testType9	Patient.active.type().namespace = 'FHIR'
YamlReferenceImplTest	testFhirR4	635	passed			group: testType - ** testType10	Patient.active.type().name = 'boolean'
YamlReferenceImplTest	testFhirR4	636	passed			group: testType - ** testType11	Patient.active.is(boolean)
YamlReferenceImplTest	testFhirR4	637	excluded:failure	#2804: FHIR and System type namespace distinction not preserved		group: testType - ** testType12	Patient.active.is(Boolean).not()
YamlReferenceImplTest	testFhirR4	638	passed			group: testType - ** testType13	Patient.active.is(FHIR.boolean)
YamlReferenceImplTest	testFhirR4	639	excluded:failure	#2804: FHIR and System type namespace distinction not preserved		group: testType - ** testType14	Patient.active.is(System.Boolean).not()
YamlReferenceImplTest	testFhirR4	640	passed			group: testType - ** testType15	Patient.type().namespace = 'FHIR'
YamlReferenceImplTest	testFhirR4	641	passed			group: testType - ** testType16	Patient.type().name = 'Patient'
YamlReferenceImplTest	testFhirR4	642	passed			group: testType - ** testType17	Patient.is(Patient)
YamlReferenceImplTest	testFhirR4	643	passed			group: testType - ** testType18	Patient.is(FHIR.Patient)
YamlReferenceImplTest	testFhirR4	644	passed			group: testType - ** testType19	Patient.is(FHIR.`Patient`)
YamlReferenceImplTest	testFhirR4	645	passed			group: testType - ** testType20	Patient.ofType(Patient).type().name
YamlReferenceImplTest	testFhirR4	646	passed			group: testType - ** testType21	Patient.ofType(FHIR.Patient).type().name
YamlReferenceImplTest	testFhirR4	647	passed			group: testType - ** testType23	Patient.ofType(FHIR.`Patient`).type().name
YamlReferenceImplTest	testFilteringAndProjection	1	passed			** filter coll of numbers	Functions.coll1.coll2.attr.where($this > 2)
YamlReferenceImplTest	testFilteringAndProjection	2	passed			** filter coll with empty coll result	Functions.coll1.coll2.attr.where($this = 0)
YamlReferenceImplTest	testFilteringAndProjection	3	unsupported		$index is not supported	** the ability to use $index in the expression	Functions.coll1.coll2.attr.where($index > 2)
YamlReferenceImplTest	testFilteringAndProjection	4	passed			** filter empty coll	Functions.attrempty.where($this > 0)
YamlReferenceImplTest	testFilteringAndProjection	5	passed			** filter non-exists coll	Functions.nothing.where($this < 0)
YamlReferenceImplTest	testFilteringAndProjection	6	passed			** where for a collection with a null value that has an id	Patient.name.given.where(id = 'Jacomus1Id').count()
YamlReferenceImplTest	testFilteringAndProjection	7	passed			** simple select	Functions.coll1.coll2.select(attr)
YamlReferenceImplTest	testFilteringAndProjection	8	passed			** select 2	Functions.coll1.select(colltrue | collfalse).attr
YamlReferenceImplTest	testFilteringAndProjection	9	passed			** select 3	Functions.coll1.select(colltrue.attr | collfalse.attr)
YamlReferenceImplTest	testFilteringAndProjection	10	passed			** select on empty coll is empty	Functions.attrempty.select(nothing)
YamlReferenceImplTest	testFilteringAndProjection	11	unsupported		$index is not supported	** the ability to use $index in the expression	Functions.coll1.coll2.select(attr + $index)
YamlReferenceImplTest	testFilteringAndProjection	12	unsupported		Unsupported operator: &	** select() function for collection with null elements	Patient.name.given.select($this & $this.id)
YamlReferenceImplTest	testFilteringAndProjection	13	passed			** should not result in an infinite loop 1	Functions.coll1.colltrue.repeat(true)
YamlReferenceImplTest	testFilteringAndProjection	14	passed			** should not result in an infinite loop 2	(1 | 2).repeat('item')
YamlReferenceImplTest	testFilteringAndProjection	15	excluded:failure	null: Indefinite calendar duration equality in repeat() deduplication		** should use year-to-month conversion factor (https://hl7.org/fhirpath/#equals)	(1 year).combine(12 months).repeat($this)
YamlReferenceImplTest	testFilteringAndProjection	16	passed			** should compare quantities for equality (https://hl7.org/fhirpath/#equals)	(3 'min').combine(180 seconds).repeat($this)
YamlReferenceImplTest	testFilteringAndProjection	17	passed			** find all attrs	Functions.repeat(repeatingAttr).count()
YamlReferenceImplTest	testFilteringAndProjection	18	passed			** find all repeatingAttr.a values	Functions.repeat(repeatingAttr).a
YamlReferenceImplTest	testFilteringAndProjection	19	passed			** find all true values in nested coll	Functions.coll1.colltrue.repeat(attr)
YamlReferenceImplTest	testFilteringAndProjection	20	passed			** find non-exists value	Functions.coll1.repeat(nothing)
YamlReferenceImplTest	testFilteringAndProjection	21	passed			** repeat() function for collection with null elements	Patient.name.given.repeat(id)
YamlReferenceImplTest	testFilteringAndProjection	22	passed			** empty input coll	Functions.attrempty.ofType(String)
YamlReferenceImplTest	testFilteringAndProjection	23	excluded:failure	null: Test runner does not support representation of mixed collections in arbitrary objects		** string	heteroattr.ofType(String)
YamlReferenceImplTest	testFilteringAndProjection	24	excluded:failure	null: Test runner does not support representation of mixed collections in arbitrary objects		** integer	heteroattr.ofType(Integer)
YamlReferenceImplTest	testFilteringAndProjection	25	excluded:failure	null: Test runner does not support representation of mixed collections in arbitrary objects		** decimal	heteroattr.ofType(Decimal)
YamlReferenceImplTest	testFilteringAndProjection	26	excluded:failure	null: Test runner does not support representation of mixed collections in arbitrary objects		** boolean	heteroattr.ofType(Boolean)
YamlReferenceImplTest	testFilteringAndProjection	27	passed			** object (not fhir)	heteroattr.ofType(Object)
YamlReferenceImplTest	testHasValue	1	unsupported		Unsupported function: hasValue	group: no model specified - collection only contains one string	Patient.name.given[0].hasValue()
YamlReferenceImplTest	testHasValue	2	unsupported		Unsupported function: hasValue	group: no model specified - collection only contains one non-primitive	Patient.identifier.hasValue()
YamlReferenceImplTest	testHasValue	3	unsupported		Unsupported function: hasValue	group: no model specified - collection contains array of strings	Patient.name.given.hasValue()
YamlReferenceImplTest	testHasValue	4	unsupported		Unsupported function: hasValue	group: no model specified - collection contains object	Patient.name.hasValue()
YamlReferenceImplTest	testHasValue	5	unsupported		Unsupported function: hasValue	group: no model specified - collection only contains an element without a value but with an extension	Patient.birthDate.hasValue()
YamlReferenceImplTest	testHasValue	6	unsupported		Unsupported function: hasValue	group: no model specified - collection only contains an element of a non-FHIR primitive type	'5'.hasValue()
YamlReferenceImplTest	testHasValue	7	unsupported		Unsupported function: hasValue	group: model specified - collection only contains one string	Patient.name.given[0].hasValue()
YamlReferenceImplTest	testHasValue	8	unsupported		Unsupported function: hasValue	group: model specified - collection contains array of strings	Patient.name.given.hasValue()
YamlReferenceImplTest	testHasValue	9	unsupported		Unsupported function: hasValue	group: model specified - collection only contains one string	Patient.name.given[0].hasValue()
YamlReferenceImplTest	testHasValue	10	unsupported		Unsupported function: hasValue	group: model specified - collection contains array of strings	Patient.name.given.hasValue()
YamlReferenceImplTest	testHasValue	11	unsupported		Unsupported function: hasValue	group: model specified - collection only contains one string	Patient.name.given[0].hasValue()
YamlReferenceImplTest	testHasValue	12	unsupported		Unsupported function: hasValue	group: model specified - collection contains array of strings	Patient.name.given.hasValue()
YamlReferenceImplTest	testHasValue	13	unsupported		Unsupported function: hasValue	group: model specified - collection only contains one string	Patient.name.given[0].hasValue()
YamlReferenceImplTest	testHasValue	14	unsupported		Unsupported function: hasValue	group: model specified - collection contains array of strings	Patient.name.given.hasValue()
YamlReferenceImplTest	testHasValue	15	unsupported		Unsupported function: hasValue	group: model specified - collection only contains an element without a value but with an extension	Patient.birthDate.hasValue()
YamlReferenceImplTest	testHasValue	16	unsupported		Unsupported function: hasValue	group: model specified - collection only contains an element of a non-FHIR primitive type	'5'.hasValue()
YamlReferenceImplTest	testLiterals	1	passed				{}
YamlReferenceImplTest	testLiterals	2	passed				'a\\\\b\\'\\"\\`\\r\\n\\t\\u0065'
YamlReferenceImplTest	testLiterals	3	passed				"a\\\\b\\'\\"\\`\\r\\n\\t\\u0065"
YamlReferenceImplTest	testLiterals	4	excluded:failure	null: Using ` to wrap a string is not allowed by grammar			`a\\\\b\\'\\"\\`\\r\\n\\t\\u0065`
YamlReferenceImplTest	testLiterals	5	passed				2 'mo'
YamlReferenceImplTest	testLiterals	6	passed				2 years
YamlReferenceImplTest	testLiterals	7	passed				(2 years).value
YamlReferenceImplTest	testLiterals	8	passed				@2019
YamlReferenceImplTest	testLiterals	9	passed				@2019-02-03T01:00Z = @2019-02-02T21:00-04:00
YamlReferenceImplTest	testLiterals	10	passed				@2019-02-03T02:00Z = @2019-02-02T21:00-04:00
YamlReferenceImplTest	testLiterals	11	passed				-7
YamlReferenceImplTest	testLiterals	12	passed				-7.3
YamlReferenceImplTest	testLiterals	13	passed				+7
YamlReferenceImplTest	testLiterals	14	passed				(-7).combine(3)
YamlReferenceImplTest	testLiterals	15	passed				-7.combine(3)
YamlReferenceImplTest	testLiterals	16	passed				-((7).combine(3))
YamlReferenceImplTest	testLiterals	17	passed				-true
YamlReferenceImplTest	testLiterals	18	passed				-'zzz'
YamlReferenceImplTest	testMath5	1	unsupported		Unsupported function: abs	** Can take the absolute value of a number	Math.d2.abs()
YamlReferenceImplTest	testMath5	2	unsupported		Unsupported function: abs	** Empty result when taking the absolute value of empty collection	Math.n2.abs()
YamlReferenceImplTest	testMath5	3	unsupported		Unsupported function: abs	** Empty result when taking the absolute value of null	Math.arrWithNullsAndVals[0].abs()
YamlReferenceImplTest	testMath5	4	unsupported		Unsupported function: abs	** Error taking the absolute value due to too many input parameters	Math.n3.abs(n4)
YamlReferenceImplTest	testMath5	5	unsupported		Unsupported function: abs	** Error taking the absolute value if the input collection contains multiple items	Math.arr.abs()
YamlReferenceImplTest	testMath5	6	unsupported		Unsupported function: ceiling	** Can return a number equal to the input	Math.n1.ceiling()
YamlReferenceImplTest	testMath5	7	unsupported		Unsupported function: ceiling	** Can round a number upward to its nearest integer	Math.d1.ceiling()
YamlReferenceImplTest	testMath5	8	unsupported		Unsupported function: ceiling	** Can round a number upward to its nearest integer	Math.d2.ceiling()
YamlReferenceImplTest	testMath5	9	unsupported		Unsupported function: ceiling	** Empty result when rounding a number upward to its nearest integer from empty collection	Math.n2.ceiling()
YamlReferenceImplTest	testMath5	10	unsupported		Unsupported function: ceiling	** Empty result when rounding a number upward to its nearest integer from null	Math.arrWithNullsAndVals[0].ceiling()
YamlReferenceImplTest	testMath5	11	unsupported		Unsupported function: ceiling	** Error rounding a number due to too many input parameters	Math.n3.ceiling(n4)
YamlReferenceImplTest	testMath5	12	unsupported		Unsupported function: ceiling	** Error rounding a number if the input collection contains multiple items	Math.arr.ceiling()
YamlReferenceImplTest	testMath5	13	unsupported		Unsupported function: exp	** Can raise e to the input number power	Math.n0.exp()
YamlReferenceImplTest	testMath5	14	unsupported		Unsupported function: exp	** Empty result for empty degree	Math.n2.exp()
YamlReferenceImplTest	testMath5	15	unsupported		Unsupported function: exp	** Empty result for null	Math.arrWithNullsAndVals[0].exp()
YamlReferenceImplTest	testMath5	16	unsupported		Unsupported function: exp	** Error exponentiation due to too many input parameters	Math.n3.exp(n4)
YamlReferenceImplTest	testMath5	17	unsupported		Unsupported function: exp	** Error exponentiation if the input collection contains multiple items	Math.arr.exp()
YamlReferenceImplTest	testMath5	18	unsupported		Unsupported function: floor	** Can return a number equal to the input	Math.n1.floor()
YamlReferenceImplTest	testMath5	19	unsupported		Unsupported function: floor	** Can round a number downward to its nearest integer	Math.d1.floor()
YamlReferenceImplTest	testMath5	20	unsupported		Unsupported function: floor	** Can round a number downward to its nearest integer	Math.d2.floor()
YamlReferenceImplTest	testMath5	21	unsupported		Unsupported function: floor	** Empty result when rounding a number downward to its nearest integer from empty collection	Math.n2.floor()
YamlReferenceImplTest	testMath5	22	unsupported		Unsupported function: floor	** Empty result when rounding a number downward to its nearest integer from null	Math.arrWithNullsAndVals[0].floor()
YamlReferenceImplTest	testMath5	23	unsupported		Unsupported function: floor	** Error rounding a number due to too many input parameters	Math.n3.floor(n4)
YamlReferenceImplTest	testMath5	24	unsupported		Unsupported function: floor	** Error rounding a number if the input collection contains multiple items	Math.arr.floor()
YamlReferenceImplTest	testMath5	25	unsupported		Unsupported function: ln	** Can take the natural logarithm of the number	Math.n1.ln()
YamlReferenceImplTest	testMath5	26	unsupported		Unsupported function: ln	** Empty result when taking logarithm from empty collection	Math.n2.ln()
YamlReferenceImplTest	testMath5	27	unsupported		Unsupported function: ln	** Empty result when taking logarithm from null	Math.arrWithNullsAndVals[0].ln()
YamlReferenceImplTest	testMath5	28	unsupported		Unsupported function: ln	** Error taking logarithm due to too many input parameters	Math.n3.ln(n4)
YamlReferenceImplTest	testMath5	29	unsupported		Unsupported function: ln	** Error taking logarithm if the input collection contains multiple items	Math.arr.ln()
YamlReferenceImplTest	testMath5	30	unsupported		Unsupported function: log	** Can take the logarithm of the number with a given base	Math.n4.log(2)
YamlReferenceImplTest	testMath5	31	unsupported		Unsupported function: log	** Empty result when taking logarithm from empty collection	Math.n2.log(8)
YamlReferenceImplTest	testMath5	32	unsupported		Unsupported function: log	** Empty result when taking logarithm with empty base	Math.n3.log(n2)
YamlReferenceImplTest	testMath5	33	unsupported		Unsupported function: log	** Empty result when taking logarithm from null	Math.arrWithNullsAndVals[0].log(2)
YamlReferenceImplTest	testMath5	34	unsupported		Unsupported function: log	** Empty result when taking logarithm with base=null	Math.n4.log(%context.arrWithNullsAndVals[0])
YamlReferenceImplTest	testMath5	35	unsupported		Unsupported function: log	** Error taking logarithm if the input collection contains multiple items	Math.arr.log(8)
YamlReferenceImplTest	testMath5	36	passed			** Error taking logarithm due to too many input parameters	Math.n3.log([3, 5])
YamlReferenceImplTest	testMath5	37	unsupported		Unsupported function: power	** Can raise input number to the power of given degree	Math.n4.power(2)
YamlReferenceImplTest	testMath5	38	unsupported		Unsupported function: power	** Empty result if the power cannot be represented	n6.power(1.5)
YamlReferenceImplTest	testMath5	39	unsupported		Unsupported function: power	** Empty result if the power cannot be represented	Math.n6.power(0.5)
YamlReferenceImplTest	testMath5	40	unsupported		Unsupported function: power	** Empty result when raising empty collection to a power	Math.n2.power(8)
YamlReferenceImplTest	testMath5	41	unsupported		Unsupported function: power	** Empty result when raising collection to the empty power	Math.n3.power(n2)
YamlReferenceImplTest	testMath5	42	unsupported		Unsupported function: power	** Empty result when raising null to a power	Math.arrWithNullsAndVals[0].power(2)
YamlReferenceImplTest	testMath5	43	unsupported		Unsupported function: power	** Empty result when raising a number to the null power	Math.n4.power(%context.arrWithNullsAndVals[0])
YamlReferenceImplTest	testMath5	44	unsupported		Unsupported function: power	** Error raising to a power if the input collection contains multiple items	Math.arr.power(8)
YamlReferenceImplTest	testMath5	45	passed			** Error raising to a power due to too many input parameters	Math.n3.power([3, 5])
YamlReferenceImplTest	testMath5	46	unsupported		Unsupported function: round	** Can round number with a given accuracy	Math.d3.round(2)
YamlReferenceImplTest	testMath5	47	unsupported		Unsupported function: round	** Can round a number to the nearest integer if a given accuracy is empty	Math.d2.round(n2)
YamlReferenceImplTest	testMath5	48	unsupported		Unsupported function: round	** Can round a number to the nearest integer if accuracy is not specified	Math.d2.round()
YamlReferenceImplTest	testMath5	49	unsupported		Unsupported function: round	** Empty result when rounding empty number	Math.n2.round(n3)
YamlReferenceImplTest	testMath5	50	unsupported		Unsupported function: round	** Empty result when rounding null	Math.arrWithNullsAndVals[0].round(2)
YamlReferenceImplTest	testMath5	51	unsupported		Unsupported function: round	** Can round a number to the nearest integer if a given accuracy is null	Math.d2.round(%context.arrWithNullsAndVals[0])
YamlReferenceImplTest	testMath5	52	unsupported		Unsupported function: round	** Error rounding if the input collection contains multiple items	Math.arr.round(8)
YamlReferenceImplTest	testMath5	53	passed			** Error rounding due to too many input parameters	Math.n3.round([3, 5])
YamlReferenceImplTest	testMath5	54	unsupported		Unsupported function: sqrt	** Can take square root	Math.n5.sqrt()
YamlReferenceImplTest	testMath5	55	unsupported		Unsupported function: sqrt	** Empty result when taking square root of a negative number	Math.d2.sqrt()
YamlReferenceImplTest	testMath5	56	unsupported		Unsupported function: sqrt	** Empty result when taking square root of an empty collection	Math.n2.sqrt()
YamlReferenceImplTest	testMath5	57	unsupported		Unsupported function: sqrt	** Empty result when taking square root of null	Math.arrWithNullsAndVals[0].sqrt()
YamlReferenceImplTest	testMath5	58	unsupported		Unsupported function: sqrt	** Error taking square root due to too many input parameters	Math.n3.sqrt(n4)
YamlReferenceImplTest	testMath5	59	unsupported		Unsupported function: sqrt	** Error taking square root if the input collection contains multiple items	Math.arr.sqrt()
YamlReferenceImplTest	testMath5	60	unsupported		Unsupported function: truncate	** Can return the integer part of the number	Math.d1.truncate()
YamlReferenceImplTest	testMath5	61	unsupported		Unsupported function: truncate	** Empty result when taking integer part from empty collection	Math.n2.truncate()
YamlReferenceImplTest	testMath5	62	unsupported		Unsupported function: truncate	** Empty result when taking integer part from null	Math.arrWithNullsAndVals[0].truncate()
YamlReferenceImplTest	testMath5	63	unsupported		Unsupported function: truncate	** Error taking integer part due to too many input parameters	Math.n3.truncate(n4)
YamlReferenceImplTest	testMath5	64	unsupported		Unsupported function: truncate	** Error taking integer part if the input collection contains multiple items	Math.arr.truncate()
YamlReferenceImplTest	testMath6	1	passed			** Can multiply two numbers	n1 * n2
YamlReferenceImplTest	testMath6	2	passed			** Can multiply two number literals	2 * 4
YamlReferenceImplTest	testMath6	3	passed			** Error multiplying string (1)	s5 * n2
YamlReferenceImplTest	testMath6	4	passed			** Error multiplying string (2)	n2 * s5
YamlReferenceImplTest	testMath6	5	unsupported		Operation not supported on String: *	** Error multiplying string (3)	s5 * s5
YamlReferenceImplTest	testMath6	6	passed			** Error multiplying multi-valued collection (1)	n1 * a3
YamlReferenceImplTest	testMath6	7	passed			** Error multiplying multi-valued collection (2)	a3 * n1
YamlReferenceImplTest	testMath6	8	passed			** Empty result multiplying empty collection (1)	n1 * n4
YamlReferenceImplTest	testMath6	9	passed			** Empty result multiplying empty collection (2)	n4 * n1
YamlReferenceImplTest	testMath6	10	passed			** Empty result when one of the operands is null	n1 * i
YamlReferenceImplTest	testMath6	11	unsupported		Quantity arithmetic operations are not yet supported	** Can multiply two quantities	2 'kg' * 5 'm' = 10 'kg.m'
YamlReferenceImplTest	testMath6	12	unsupported		Quantity arithmetic operations are not yet supported	** Can multiply a quantity by a number	2 'kg' * 5 = 10 'kg'
YamlReferenceImplTest	testMath6	13	unsupported		Quantity arithmetic operations are not yet supported	** Can multiply a number by a quantity	2 * 5 'kg' = 10 'kg'
YamlReferenceImplTest	testMath6	14	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands has a special unit	1 'B' * 2
YamlReferenceImplTest	testMath6	15	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands is a calendar duration quantity greater than seconds (1)	1 'm' * 1 year
YamlReferenceImplTest	testMath6	16	unsupported		Quantity arithmetic operations are not yet supported	** Can multiply a quantity by a calendar duration quantity less than or equal to seconds (1)	1 'm' * (1 year).toQuantity('seconds') = 31536000 'm.s'
YamlReferenceImplTest	testMath6	17	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands is a calendar duration quantity greater than seconds (2)	1 year * 1 year
YamlReferenceImplTest	testMath6	18	unsupported		Quantity arithmetic operations are not yet supported	** Can multiply a quantity by a calendar duration quantity less than or equal to seconds (2)	(1 year).toQuantity('s') * (1 year).toQuantity('seconds') = 994519296000000 's2'
YamlReferenceImplTest	testMath6	19	passed			** Can divide two numbers	n2/n1
YamlReferenceImplTest	testMath6	20	passed			** Can divide two number literals	5/2
YamlReferenceImplTest	testMath6	21	passed			** Empty result when one of the operands is null	n1 / i
YamlReferenceImplTest	testMath6	22	unsupported		Quantity arithmetic operations are not yet supported	** Can divide two quantities	10 'kg' / 5 'm' = 2 'kg/m'
YamlReferenceImplTest	testMath6	23	unsupported		Quantity arithmetic operations are not yet supported	** Can divide a quantity by a number	20 'kg' / 5 = 4 'kg'
YamlReferenceImplTest	testMath6	24	unsupported		Quantity arithmetic operations are not yet supported	** Can divide a number by a quantity	20 / 5 'kg' = 4 '1/kg'
YamlReferenceImplTest	testMath6	25	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands has a special unit	1 'B' / 2
YamlReferenceImplTest	testMath6	26	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands is a calendar duration quantity greater than seconds (1)	1 'mo' / 1 year
YamlReferenceImplTest	testMath6	27	unsupported		Quantity arithmetic operations are not yet supported	** Can divide a UCUM duration quantity by a calendar duration quantity less than or equal to seconds	1 'mo' / (1 year).toQuantity('seconds') = 0.08339041095890411
YamlReferenceImplTest	testMath6	28	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands is a calendar duration quantity greater than seconds (2)	1 year / 1 'mo'
YamlReferenceImplTest	testMath6	29	unsupported		Quantity arithmetic operations are not yet supported	** Can divide a calendar duration quantity less than or equal to seconds by a UCUM duration quantity	(1 year).toQuantity('seconds') / 1 'mo' = 11.991786447638603
YamlReferenceImplTest	testMath6	30	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands is a calendar duration quantity greater than seconds (3)	3 minutes / 1 's'
YamlReferenceImplTest	testMath6	31	unsupported		Quantity arithmetic operations are not yet supported	** Allow division of a UCUM duration quantity	3 's' / 3 milliseconds = 1000
YamlReferenceImplTest	testMath6	32	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands is a calendar duration quantity greater than seconds (4)	1 'kg' / 3 months
YamlReferenceImplTest	testMath6	33	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands is a calendar duration quantity greater than seconds (5)	1 / 3 months
YamlReferenceImplTest	testMath6	34	unsupported		Quantity arithmetic operations are not yet supported	** Can divide a UCUM quantity by a calendar duration quantity less than or equal to seconds	1 'kg' / (3 months).toQuantity('seconds') = 0.0001286008230452675 'g/s'
YamlReferenceImplTest	testMath6	35	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when a calendar duration quantity divided by zero	1 year / 0 months
YamlReferenceImplTest	testMath6	36	unsupported		Quantity arithmetic operations are not yet supported	** Can divide when both operands are calendar duration quantities	3 month / 2 year = 1 / 8
YamlReferenceImplTest	testMath6	37	unsupported		Quantity arithmetic operations are not yet supported	** Can divide a calendar duration quantity by a number	1 year / 2 = 6 months
YamlReferenceImplTest	testMath6	38	excluded:error	null: Mixed collections not supported		** Can divide System.Quantity (converted from FHIR.Quantity) by System.Quantity	Observation.value / 185 months = 1
YamlReferenceImplTest	testMath6	39	excluded:error	null: Mixed collections not supported		** Empty result when one of the operands is a calendar duration quantity greater than seconds (6)	Observation.value / 185 'mo'
YamlReferenceImplTest	testMath6	40	passed			** Can add two numbers	n1 + n2
YamlReferenceImplTest	testMath6	41	passed			** Can add two number literals	2+3
YamlReferenceImplTest	testMath6	42	unsupported		Quantity arithmetic operations are not yet supported	** Can add two quantity literals	3 'm' + 3 'cm' = 303 'cm'
YamlReferenceImplTest	testMath6	43	unsupported		Quantity arithmetic operations are not yet supported	** Can add a number to a quantity	3 '1' + 2 = 5 '1'
YamlReferenceImplTest	testMath6	44	unsupported		Quantity arithmetic operations are not yet supported	** Can add a quantity to a number	3 + 2 '1' = 5 '1'
YamlReferenceImplTest	testMath6	45	passed			** Error adding array of numbers	n1 + a3
YamlReferenceImplTest	testMath6	46	passed			** Error adding non-matching types	n1 + s5
YamlReferenceImplTest	testMath6	47	passed			** Error adding non-matching types (2)	s5 + n1
YamlReferenceImplTest	testMath6	48	passed			** Empty result if one or both operands is empty (1b)	s5 + n4
YamlReferenceImplTest	testMath6	49	passed			** Empty result if one or both operands is empty (2)	n4 + n1
YamlReferenceImplTest	testMath6	50	passed			** Empty result if one or both operands is empty (3)	n4 + n4
YamlReferenceImplTest	testMath6	51	passed			** Empty result when one of the operands is null	n1 + i
YamlReferenceImplTest	testMath6	52	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands has a special unit	1 'B' + 1 'B'
YamlReferenceImplTest	testMath6	53	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands is a calendar duration quantity greater than seconds	1 'a' + 1 year
YamlReferenceImplTest	testMath6	54	unsupported		Quantity arithmetic operations are not yet supported	** Can add a calendar duration quantity less than or equal to seconds to a UCUM time-valued quantity	1 'a' + (1 year).toQuantity('seconds') = 63093600 seconds
YamlReferenceImplTest	testMath6	55	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands is a calendar duration quantity greater than seconds	1 year + 1 'a'
YamlReferenceImplTest	testMath6	56	unsupported		Quantity arithmetic operations are not yet supported	** Can add a UCUM time-valued quantity to a calendar duration quantity less than or equal to seconds	(1 year).toQuantity('seconds') + 1 'a' = 63093600 seconds
YamlReferenceImplTest	testMath6	57	unsupported		Quantity arithmetic operations are not yet supported	** Changing the places of the terms may change the sum because of the year/month conversion factor	(1 year + 6 months).toQuantity('seconds') + 1 'a' - (2.5 days).toQuantity('seconds') = 1 'a' + (6 months).toQuantity('seconds') + (1 year).toQuantity('seconds')
YamlReferenceImplTest	testMath6	58	unsupported		Quantity arithmetic operations are not yet supported	** Changing the places of the terms does not change the sum	(1 year).toQuantity('seconds') + 6 'mo' + 1 'a' = 1 'a' + 6 'mo' + (1 year).toQuantity('seconds')
YamlReferenceImplTest	testMath6	59	unsupported		Quantity arithmetic operations are not yet supported	** Can add a calendar duration quantity to a calendar duration quantity	2 years + 6 months = 2.5 year
YamlReferenceImplTest	testMath6	60	unsupported		Quantity arithmetic operations are not yet supported	** Can add a UCUM time-valued quantity to an UCUM time-valued quantity	2 'a' + 1 'a' = 3 'a'
YamlReferenceImplTest	testMath6	61	passed			** Can subtract two numbers	n1 - n2
YamlReferenceImplTest	testMath6	62	passed			** Can subtract two number literals	5-3
YamlReferenceImplTest	testMath6	63	passed			** Empty result when one of the operands is empty	n1 - n4
YamlReferenceImplTest	testMath6	64	passed			** Empty result when one of the operands is null	n1 - i
YamlReferenceImplTest	testMath6	65	unsupported		Quantity arithmetic operations are not yet supported	** Can subtract two quantity literals	3 'm' - 3 'cm' = 297 'cm'
YamlReferenceImplTest	testMath6	66	unsupported		Quantity arithmetic operations are not yet supported	** Can subtract a quantity from a number	3 - 2 '1' = 1 '1'
YamlReferenceImplTest	testMath6	67	unsupported		Quantity arithmetic operations are not yet supported	** Can subtract a number from a quantity	3 '1' - 2 = 1 '1'
YamlReferenceImplTest	testMath6	68	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands has a special unit	1 'B' - 1 'B'
YamlReferenceImplTest	testMath6	69	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands is a calendar duration quantity greater than seconds (1)	1 'a' - 1 year
YamlReferenceImplTest	testMath6	70	unsupported		Quantity arithmetic operations are not yet supported	** Can subtract a calendar duration quantity less than or equal to seconds from a UCUM time-valued quantity	1 'a' - (1 year).toQuantity('seconds') = 21600 seconds
YamlReferenceImplTest	testMath6	71	unsupported		Quantity arithmetic operations are not yet supported	** Empty result when one of the operands is a calendar duration quantity greater than seconds (1)	1 year - 1 'a'
YamlReferenceImplTest	testMath6	72	unsupported		Quantity arithmetic operations are not yet supported	** Can subtract a UCUM time-valued quantity from a calendar duration quantity less than or equal to seconds	(1 year).toQuantity('seconds') - 1 'a' = -21600 seconds
YamlReferenceImplTest	testMath6	73	unsupported		Quantity arithmetic operations are not yet supported	** Can subtract a calendar duration quantity from a calendar duration quantity	3 year - 1 year = 2 year
YamlReferenceImplTest	testMath6	74	unsupported		Quantity arithmetic operations are not yet supported	** Can subtract a UCUM time-valued quantity from an UCUM time-valued quantity	3 'a' - 1 'a' = 2 'a'
YamlReferenceImplTest	testMath6	75	unsupported		Unsupported operator: div	** Can div two numbers	n2 div n1
YamlReferenceImplTest	testMath6	76	unsupported		Unsupported operator: div	** Empty result when one of the operands is null	n1 div i
YamlReferenceImplTest	testMath6	77	unsupported		Unsupported operator: div	** Error truncated divide array of numbers	MathTestData.n1 div MathTestData.a3
YamlReferenceImplTest	testMath6	78	unsupported		Unsupported operator: div	** Error truncated divide non-number	MathTestData.n1 div MathTestData.s5
YamlReferenceImplTest	testMath6	79	passed			** Can mod two numbers	n2 mod n1
YamlReferenceImplTest	testMath6	80	passed			** Empty result when one of the operands is null	n1 mod i
YamlReferenceImplTest	testMath6	81	passed			** Error reminder of truncated division array of numbers	MathTestData.n1 mod MathTestData.a3
YamlReferenceImplTest	testMath6	82	passed			** Error reminder of truncated division non-number	MathTestData.n1 mod MathTestData.s5
YamlReferenceImplTest	testMath6	83	unsupported		Unsupported operator: &	** Can concatenate two strings	s5 & s6
YamlReferenceImplTest	testMath6	84	passed			** Can concatenate two strings with +	s5 + s6
YamlReferenceImplTest	testMath6	85	unsupported		Unsupported operator: &	** Empty collection in concatenation is empty string (1)	n4 & s5
YamlReferenceImplTest	testMath6	86	unsupported		Unsupported operator: &	** Empty collection in concatenation is empty string (2)	s5 & n4
YamlReferenceImplTest	testMath6	87	unsupported		Unsupported operator: &	** Empty collection in concatenation is empty string (3)	n4 & n4
YamlReferenceImplTest	testMath6	88	passed			** Empty collection in concatenation using +	n4 + s5
YamlReferenceImplTest	testMath6	89	passed			** Null in concatenation is treated as an empty collection (1)	Patient.name.given[0] + 'c'
YamlReferenceImplTest	testMath6	90	passed			** Null in concatenation is treated as an empty collection (2)	'c' + Patient.name.given[0]
YamlReferenceImplTest	testMath6	91	unsupported		Unsupported operator: &	** Null in concatenation is treated as an empty collection (3)	Patient.name.given[0] & 'c'
YamlReferenceImplTest	testMath6	92	unsupported		Unsupported operator: &	** Null in concatenation is treated as an empty collection (4)	'c' & Patient.name.given[0]
YamlReferenceImplTest	testMath6	93	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2018 + 1 'a' = @2019
YamlReferenceImplTest	testMath6	94	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2018 + 1 year
YamlReferenceImplTest	testMath6	95	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016 + 364 days
YamlReferenceImplTest	testMath6	96	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016 - 364 days
YamlReferenceImplTest	testMath6	97	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016 + 365 days
YamlReferenceImplTest	testMath6	98	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016 - 365 days
YamlReferenceImplTest	testMath6	99	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016 + 11 months
YamlReferenceImplTest	testMath6	100	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016 + 12 months
YamlReferenceImplTest	testMath6	101	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2018 + 2 years
YamlReferenceImplTest	testMath6	102	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2018-02 + 1 'mo'
YamlReferenceImplTest	testMath6	103	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2018-02 + 1 month
YamlReferenceImplTest	testMath6	104	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2018-02 + 2 months
YamlReferenceImplTest	testMath6	105	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-01 + 29 days
YamlReferenceImplTest	testMath6	106	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-01 + 29 'd'
YamlReferenceImplTest	testMath6	107	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-01 + 30 days
YamlReferenceImplTest	testMath6	108	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-01 + 30 'd'
YamlReferenceImplTest	testMath6	109	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-01 + 1 hour
YamlReferenceImplTest	testMath6	110	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2018-01-03 + 2 years
YamlReferenceImplTest	testMath6	111	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2018-02-18T12:23:45-05:00 + 2 years
YamlReferenceImplTest	testMath6	112	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-02-29 + 1 month
YamlReferenceImplTest	testMath6	113	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-02-29 + 1 year
YamlReferenceImplTest	testMath6	114	excluded:unsupported	null: Unsupported dateTime arithmetics operator (Quantity + Date)	Quantity arithmetic operations are not yet supported	group:  6.7 Date/Time Arithmetic	1 year + @2016-02-29
YamlReferenceImplTest	testMath6	115	passed			group:  6.7 Date/Time Arithmetic	@2016-02-29 + @2016-02-29
YamlReferenceImplTest	testMath6	116	unsupported		Quantity arithmetic operations are not yet supported	group:  6.7 Date/Time Arithmetic	1 year - @2016-02-29
YamlReferenceImplTest	testMath6	117	unsupported		Quantity arithmetic operations are not yet supported	group:  6.7 Date/Time Arithmetic	1 year - 6 months - 1 year + 18 months = 1 year
YamlReferenceImplTest	testMath6	118	unsupported		Quantity arithmetic operations are not yet supported	group:  6.7 Date/Time Arithmetic	1 year * 2 = 2 year
YamlReferenceImplTest	testMath6	119	unsupported		Quantity arithmetic operations are not yet supported	group:  6.7 Date/Time Arithmetic	2 * 5 months = 10 months
YamlReferenceImplTest	testMath6	120	unsupported		Quantity arithmetic operations are not yet supported	group:  6.7 Date/Time Arithmetic	1 year / 6 months = 2
YamlReferenceImplTest	testMath6	121	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-01-31 + 1 month
YamlReferenceImplTest	testMath6	122	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-01-31 + 1 month + 1 day
YamlReferenceImplTest	testMath6	123	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-02-28 + 1 week
YamlReferenceImplTest	testMath6	124	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-02-28 + 1 'wk'
YamlReferenceImplTest	testMath6	125	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-02-28 + 1 day
YamlReferenceImplTest	testMath6	126	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-02-28 + 2 days
YamlReferenceImplTest	testMath6	127	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-02-28 - 2 days
YamlReferenceImplTest	testMath6	128	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-01-01 + 1 month
YamlReferenceImplTest	testMath6	129	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-01-01 + 24 hours
YamlReferenceImplTest	testMath6	130	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-01-01 + 47 hours
YamlReferenceImplTest	testMath6	131	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2018-02-18T12 + 59 minutes = @2018-02-18T12
YamlReferenceImplTest	testMath6	132	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2018-02-18T12 + 60 minutes = @2018-02-18T13
YamlReferenceImplTest	testMath6	133	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2018-02-18T12:00 + 59 seconds = @2018-02-18T12:00
YamlReferenceImplTest	testMath6	134	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2018-02-18T12:00 + 60 seconds = @2018-02-18T12:01
YamlReferenceImplTest	testMath6	135	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@T09:45:23 + 2 years
YamlReferenceImplTest	testMath6	136	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T09:45Z + 120 seconds = @T04:47-05:00
YamlReferenceImplTest	testMath6	137	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T09:45Z + 59 seconds = @T09:45Z
YamlReferenceImplTest	testMath6	138	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T09:45:23Z + 2 minutes = @T04:47:23.000-05:00
YamlReferenceImplTest	testMath6	139	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T23:59:23Z + 2 minutes = @T00:01:23Z
YamlReferenceImplTest	testMath6	140	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T23:59:23-05:00 + 2 minutes = @T00:01:23-05:00
YamlReferenceImplTest	testMath6	141	passed			group:  6.7 Date/Time Arithmetic	@T23:59:23-05:00 + 2 days
YamlReferenceImplTest	testMath6	142	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T23:59:23-05:00 + 5 milliseconds = @T23:59:23.005-05:00
YamlReferenceImplTest	testMath6	143	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T23:59:23-05:00 + 1.5 seconds = @T23:59:24.500-05:00
YamlReferenceImplTest	testMath6	144	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T23:59:23-05:00 + 1.5 's' = @T23:59:24.500-05:00
YamlReferenceImplTest	testMath6	145	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T23:59:23-05:00 - 1.5 's' = @T23:59:21.500-05:00
YamlReferenceImplTest	testMath6	146	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T23:59:23-05:00 + 1000 milliseconds = @T23:59:24-05:00
YamlReferenceImplTest	testMath6	147	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T23:59:23.005-05:00 + 995 milliseconds = @T23:59:24-05:00
YamlReferenceImplTest	testMath6	148	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T23:59:23.005-05:00 + 1990 milliseconds = @T23:59:24.995-05:00
YamlReferenceImplTest	testMath6	149	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-02-29 + 1.5 year
YamlReferenceImplTest	testMath6	150	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-02-28 + 1.5 weeks
YamlReferenceImplTest	testMath6	151	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-02-28 - 1.5 weeks
YamlReferenceImplTest	testMath6	152	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-02-28 + 1.5 'wk'
YamlReferenceImplTest	testMath6	153	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	@2016-02-28 + 1.5 day
YamlReferenceImplTest	testMath6	154	excluded:error	null: Parse error		group:  6.7 Date/Time Arithmetic	@T23:59:23-05:00 + 1.5 hours = @T00:59:23-05:00
YamlReferenceImplTest	testMath6	155	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	Patient.birthDate - 1 day = @1974-12-24
YamlReferenceImplTest	testMath6	156	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic	Patient.birthDate + 1 day = @1974-12-26
YamlReferenceImplTest	testMath6	157	unsupported		Unsupported dateTime arithmetic	group:  6.7 Date/Time Arithmetic - ** Do not convert a resource node value to Date without model	Patient.birthDate + 1 day = @1974-12-26
YamlReferenceImplTest	testPaths	1	excluded:unsupported	null: Mixed collections not supported	Representation of this path is not supported: choice element 'value' (do you need to use ofType?)	group: Paths with choice types - Observation.value with an R5 FHIR model	Observation.value
YamlReferenceImplTest	testPaths	2	excluded:unsupported	null: Mixed collections not supported	Representation of this path is not supported: choice element 'value' (do you need to use ofType?)	group: Paths with choice types - Observation.value with an R4 FHIR model	Observation.value
YamlReferenceImplTest	testPaths	3	excluded:unsupported	null: Mixed collections not supported	Representation of this path is not supported: choice element 'value' (do you need to use ofType?)	group: Paths with choice types - Observation.value with an STU3 FHIR model	Observation.value
YamlReferenceImplTest	testPaths	4	excluded:unsupported	null: Mixed collections not supported	Representation of this path is not supported: choice element 'value' (do you need to use ofType?)	group: Paths with choice types - Observation.value with an DSTU2 FHIR model	Observation.value
YamlReferenceImplTest	testPaths	5	excluded:unsupported	null: Mixed collections not supported	Representation of this path is not supported: choice element 'value' (do you need to use ofType?)	group: Paths with choice types - Observation.value without a model	Observation.value
YamlReferenceImplTest	testPaths	6	excluded:error	#2406: Contained resources not supported		group: Paths with choice types - Observation.value contained in another resource (1)	Observation.contained[0].value
YamlReferenceImplTest	testPaths	7	excluded:error	#2406: Contained resources not supported		group: Paths with choice types - Observation.value contained in another resource (2)	Observation.contained.value
YamlReferenceImplTest	testPaths	8	excluded:error	#2406: Contained resources not supported		group: Paths with choice types - Type of Observation contained in another resource	Observation.contained[0] is Observation
YamlReferenceImplTest	testPaths	9	unsupported		Unsupported function: children	group: Paths with choice types - Getting choice type fields via children()	Observation.children().value
YamlReferenceImplTest	testPaths	10	unsupported		Unsupported function: children	group: Paths with choice types - Getting choice type fields via children() (2)	Observation.children().children().value
YamlReferenceImplTest	testPaths	11	unsupported		Unsupported function: descendants	group: Paths with choice types - Getting choice type fields via descendants()	Observation.descendants().value
YamlReferenceImplTest	testPaths	12	unsupported		Unsupported function: descendants	group: Paths with choice types - Getting choice type fields via descendants() and where()	Observation.descendants().where(resourceType = 'Observation').value
YamlReferenceImplTest	testPaths	13	excluded:error	#2406: Contained resources not supported		group: Paths with choice types - QR with where()	contained.where(resourceType = 'QuestionnaireResponse').item.where(linkId = '1').answer.value
YamlReferenceImplTest	testPaths	14	excluded:error	#2406: Contained resources not supported		group: Paths with choice types - QR with descendants() and where() (1)	contained.where(resourceType = 'QuestionnaireResponse').descendants().where(linkId = '1').answer.value
YamlReferenceImplTest	testPaths	15	excluded:error	#2406: Contained resources not supported		group: Paths with choice types - QR with descendants() and where() (2)	contained.where(resourceType = 'QuestionnaireResponse').descendants().where(linkId = '1.0').answer.value
YamlReferenceImplTest	testPaths	16	excluded:error	#2406: Contained resources not supported		group: Paths with choice types - QR with descendants() and where() (3)	contained.where(resourceType = 'QuestionnaireResponse').descendants().where(linkId = '1.1').answer.value
YamlReferenceImplTest	testPaths	17	excluded:error	#2406: Contained resources not supported		group: Paths with choice types - QR with item.answer.value	contained.where(resourceType = 'QuestionnaireResponse').item.answer.value
YamlReferenceImplTest	testPaths	18	excluded:error	#2406: Contained resources not supported		group: Paths with choice types - QR with item.item.answer.value	contained.where(resourceType = 'QuestionnaireResponse').item.item.answer.value
YamlReferenceImplTest	testPaths	19	excluded:error	#2406: Contained resources not supported		group: Paths with choice types - QR with item.answer.item.answer.value	contained.where(resourceType = 'QuestionnaireResponse').item.answer.item.answer.value
YamlReferenceImplTest	testPaths	20	excluded:error	#2406: Contained resources not supported		group: Paths with choice types - QR with item.answer.item.answer.value (STU3)	contained.where(resourceType = 'QuestionnaireResponse').item.answer.item.answer.value
YamlReferenceImplTest	testPaths	21	excluded:failure	null: Custom fields not supported within FHIR resources		group: Paths with choice types - Access a custom field starting with a capital letter (1)	CustomField = 'test'
YamlReferenceImplTest	testPaths	22	excluded:failure	null: Custom fields not supported within FHIR resources		group: Paths with choice types - Access a custom field starting with a capital letter (2)	Observation.CustomField = 'test'
YamlReferenceImplTest	testRepeat	1	passed				Questionnaire.repeat(item).linkId
YamlReferenceImplTest	testRepeat	2	passed				Questionnaire.combine(Questionnaire).repeat(item).linkId
YamlReferenceImplTest	testSimple	1	passed				Patient.name.family
YamlReferenceImplTest	testSimple	2	passed				name.family
YamlReferenceImplTest	testSimple	3	excluded:failure	#2153: Tick quotes identifiers not supported			name.`family`
YamlReferenceImplTest	testSimple	4	passed			mapcat arrays	Patient.name.given
YamlReferenceImplTest	testSimple	5	passed				Patient.id
YamlReferenceImplTest	testSimple	6	passed				id
YamlReferenceImplTest	testSimple	7	passed				Encounter.id
YamlReferenceImplTest	testSimple	8	passed			access by index	Patient.address[0].use
YamlReferenceImplTest	testSimple	9	passed			access by index	Patient.name[0].family
YamlReferenceImplTest	testSimple	10	passed			access by index unexisting	Patient.name[1].family
YamlReferenceImplTest	testSimple	11	passed			access by index	Patient.name[2].family
YamlReferenceImplTest	testSimple	12	passed			access by index unexisting	Patient.name[3].family
YamlReferenceImplTest	testSimple	13	passed			access by index	Patient.name[1].given[0]
YamlReferenceImplTest	testSimple	14	passed				Patient.managingOrganization.reference
YamlReferenceImplTest	testSimple	15	passed				Patient.name.exists()
YamlReferenceImplTest	testSimple	16	excluded:error	null: Boolean evaluation of collections			Patient.name.exists(given)
YamlReferenceImplTest	testSimple	17	passed				Patient.ups.exists()
YamlReferenceImplTest	testSimple	18	passed				Patient.name.empty()
YamlReferenceImplTest	testSimple	19	passed				Patient.ups.empty()
YamlReferenceImplTest	testSimple	20	passed			count	Patient.name.given.count()
YamlReferenceImplTest	testSimple	21	passed			count	Patient.name.given.count() = 5
YamlReferenceImplTest	testSimple	22	passed				Patient.name.where(use ='official').family
YamlReferenceImplTest	testSimple	23	passed				Patient.name.where(use ='official').given
YamlReferenceImplTest	testSimple	24	passed				'a' | 'b'
YamlReferenceImplTest	testSimple	25	passed				Patient.name.select(given)
YamlReferenceImplTest	testSimple	26	passed				Patient.name.given | Patient.name.family
YamlReferenceImplTest	testSimple	27	passed				Patient.name.select(given | family)
YamlReferenceImplTest	testSimple	28	passed				Patient.name.select(given.union(family))
YamlReferenceImplTest	testSimple	29	passed				Patient.name.select(given.combine(family))
YamlReferenceImplTest	testSimple	30	unsupported		Unsupported function: subsetOf		Patient.name.select(('James').subsetOf(given))
YamlReferenceImplTest	testSimple	31	unsupported		Unsupported function: supersetOf		Patient.name.select(('Peter' | 'James').supersetOf(given))
YamlReferenceImplTest	testSimple	32	unsupported		Unsupported function: intersect		Patient.name.select(('Peter' | 'James' | 'something').intersect(given | family))
YamlReferenceImplTest	testSimple	33	passed				Patient.name.trace('tracing').given[0]
YamlReferenceImplTest	testSimple	34	unsupported		Unsupported function: single		Patient.name.given.select($this.single())
YamlReferenceImplTest	testSimple	35	passed				Patient.name.given.select($this.contains('Ja'))
YamlReferenceImplTest	testSimple	36	excluded:failure	null: length without parentheses treated as child traversal			Patient.name.given.select($this.length) = Patient.name.given.select(length)
YamlReferenceImplTest	testSimple	37	excluded:failure	null: Unsupported comparison between typed complex value and expected structure			03) 5555 6473, system=phone}]=(Patient.telecom.where($this.use = 'work')
YamlReferenceImplTest	testSimple	38	passed				identifier.where($this.type.coding.code = 'MR').value
YamlReferenceImplTest	testSimple	39	passed				identifier.where($this.type.coding.code = 'Z').value
YamlReferenceImplTest	testSimple	40	passed			trace(name) doesn't affect the context of the following subexpressions	name.given.combine(name.family) = name.trace('n').given.combine(name.family)
YamlReferenceImplTest	testSimple	41	passed			trace(name, projection) doesn't affect the context of the following subexpressions	name.given.combine(name.family) = name.trace('m', family).given.combine(name.family)
YamlReferenceImplTest	testSimple	42	passed			Parenthesized expression	(2).combine(3)
YamlReferenceImplTest	testSimple	43	excluded:error	null: Unsupported type runner feature: expression maps		Evaluating expression for a part of a resource	FAIL: {base=QuestionnaireResponse.item, expression=answer.value = 2 year}
YamlReferenceImplTest	testSimple	44	excluded:failure	#2395: type() test expects old spec format without baseType		type() function for the null element of a collection	Patient.name.given.type()
YamlReferenceImplTest	testSimple	45	passed			ofType() function for the null element of a collection (1)	Patient.name.given.ofType(string)[0]
YamlReferenceImplTest	testSimple	46	passed			ofType() function for the null element of a collection (2)	Patient.name.given.ofType(string)
YamlReferenceImplTest	testStringManipulation	1	unsupported		Unsupported function: indexOf	** index of substring (contains)	Functions.str.attr.indexOf('string')
YamlReferenceImplTest	testStringManipulation	2	unsupported		Unsupported function: indexOf	** index of substring (isnt contains)	Functions.str.attr.indexOf('qwerty')
YamlReferenceImplTest	testStringManipulation	3	unsupported		Unsupported function: indexOf	** index of substring with zero length	Functions.str.attr.indexOf('')
YamlReferenceImplTest	testStringManipulation	4	unsupported		Unsupported function: indexOf	** indexOf on empty input collection	Functions.str.empty.indexOf('')
YamlReferenceImplTest	testStringManipulation	5	unsupported		Unsupported function: indexOf	** indexOf on a singleton with null value	Patient.name.given[0].indexOf('Peter')
YamlReferenceImplTest	testStringManipulation	6	unsupported		Unsupported function: indexOf	** index of empty substring	Functions.str.empty.indexOf(EmptySubstring)
YamlReferenceImplTest	testStringManipulation	7	unsupported		Unsupported function: substring	** substring with length	Functions.str.attr.substring(5, 3)
YamlReferenceImplTest	testStringManipulation	8	unsupported		Unsupported function: substring	** substring without length	Functions.str.attr.substring(5)
YamlReferenceImplTest	testStringManipulation	9	unsupported		Unsupported function: substring	** substring with outside index	Functions.str.attr.substring(55)
YamlReferenceImplTest	testStringManipulation	10	unsupported		Unsupported function: substring	** substring with empty index	Functions.str.attr.substring(EmptyStart)
YamlReferenceImplTest	testStringManipulation	11	unsupported		Unsupported function: substring	** substring with outside length	Functions.str.attr.substring(5, 55)
YamlReferenceImplTest	testStringManipulation	12	unsupported		Unsupported function: substring	** substring with empty length	Functions.str.attr.substring(5, EmptyLength)
YamlReferenceImplTest	testStringManipulation	13	unsupported		Unsupported function: substring	** substring on empty input collection	Functions.str.empty.substring(0)
YamlReferenceImplTest	testStringManipulation	14	unsupported		Unsupported function: substring	** substring on a singleton with null value	Patient.name.given[0].substring(0)
YamlReferenceImplTest	testStringManipulation	15	passed			** start with	Functions.str.attr.startsWith('test')
YamlReferenceImplTest	testStringManipulation	16	passed			** not start with	Functions.str.attr.startsWith('jest')
YamlReferenceImplTest	testStringManipulation	17	passed			** starts with prefix with zero length	Functions.str.attr.startsWith('')
YamlReferenceImplTest	testStringManipulation	18	passed			** startsWith on empty input collection	Functions.str.empty.startsWith('')
YamlReferenceImplTest	testStringManipulation	19	passed			** start with empty prefix	Functions.str.attr.startsWith(EmptyString)
YamlReferenceImplTest	testStringManipulation	20	passed			** startsWith on a singleton with null value	Patient.name.given[0].startsWith('P')
YamlReferenceImplTest	testStringManipulation	21	passed			** end with	Functions.str.attr.endsWith('ing')
YamlReferenceImplTest	testStringManipulation	22	passed			** not end with	Functions.str.attr.endsWith('lala')
YamlReferenceImplTest	testStringManipulation	23	passed			** end with suffix with zero length	Functions.str.attr.endsWith('')
YamlReferenceImplTest	testStringManipulation	24	passed			** endsWith on empty input collection	Functions.str.empty.endsWith('')
YamlReferenceImplTest	testStringManipulation	25	passed			** end with empty substring	Functions.str.attr.endsWith(EmptyString)
YamlReferenceImplTest	testStringManipulation	26	passed			** endsWith on a singleton with null value	Patient.name.given[0].endsWith('P')
YamlReferenceImplTest	testStringManipulation	27	passed			** contains	Functions.str.attr.contains('tri')
YamlReferenceImplTest	testStringManipulation	28	passed			** not contains	Functions.str.attr.contains('lala')
YamlReferenceImplTest	testStringManipulation	29	passed			** contains empty string	Functions.str.attr.contains('')
YamlReferenceImplTest	testStringManipulation	30	passed			** contains on empty input collection	Functions.str.empty.contains('P')
YamlReferenceImplTest	testStringManipulation	31	passed			** contains on a singleton with null value	Patient.name.given[0].contains('P')
YamlReferenceImplTest	testStringManipulation	32	unsupported		Unsupported function: upper	** upper 1	'abcdefg'.upper()
YamlReferenceImplTest	testStringManipulation	33	unsupported		Unsupported function: upper	** upper 2	'AbCdefg'.upper()
YamlReferenceImplTest	testStringManipulation	34	unsupported		Unsupported function: upper	** upper on empty input collection	Functions.str.empty.upper()
YamlReferenceImplTest	testStringManipulation	35	unsupported		Unsupported function: upper	** upper on a singleton with null value	Patient.name.given[0].upper()
YamlReferenceImplTest	testStringManipulation	36	unsupported		Unsupported function: lower	** lower 1	'ABCDEFG'.lower()
YamlReferenceImplTest	testStringManipulation	37	unsupported		Unsupported function: lower	** lower 2	'aBcDEFG'.lower()
YamlReferenceImplTest	testStringManipulation	38	unsupported		Unsupported function: lower	** lower on empty input collection	Functions.str.empty.lower()
YamlReferenceImplTest	testStringManipulation	39	unsupported		Unsupported function: lower	** lower on a singleton with null value	Patient.name.given[0].lower()
YamlReferenceImplTest	testStringManipulation	40	unsupported		Unsupported function: replace	** replace	Functions.str.attr.replace('string', 'lala')
YamlReferenceImplTest	testStringManipulation	41	unsupported		Unsupported function: replace	** replace multiple	Functions.str.attr.replace('t', 'FHIR')
YamlReferenceImplTest	testStringManipulation	42	unsupported		Unsupported function: replace	** replace for empty string	Functions.str.attr.replace('t', '')
YamlReferenceImplTest	testStringManipulation	43	unsupported		Unsupported function: replace	** replace is not the same as replaceMatches	Functions.str.attr2.replace('\\\\s', 'a')
YamlReferenceImplTest	testStringManipulation	44	unsupported		Unsupported function: replace	** surround	Functions.str.attr.replace('', 'x')
YamlReferenceImplTest	testStringManipulation	45	unsupported		Unsupported function: replace	** replace on empty input collection	Functions.str.empty.replace('', 'x')
YamlReferenceImplTest	testStringManipulation	46	unsupported		Unsupported function: replace	** replace with empty collection in pattern	Functions.str.attr.replace(EmptyPattern, '')
YamlReferenceImplTest	testStringManipulation	47	unsupported		Unsupported function: replace	** replace with empty collection in substitution	Functions.str.attr.replace('', EmptySubstitution)
YamlReferenceImplTest	testStringManipulation	48	unsupported		Unsupported function: replace	** replace on a singleton with null value	Patient.name.given[0].replace('a', 'b')
YamlReferenceImplTest	testStringManipulation	49	unsupported		Unsupported function: matches	** matches	Functions.str.attr.matches('t.+')
YamlReferenceImplTest	testStringManipulation	50	unsupported		Unsupported function: matches	** not matches	Functions.str.attr.matches('asd.+')
YamlReferenceImplTest	testStringManipulation	51	unsupported		Unsupported function: matches	** matches on empty input collection	Functions.str.empty.matches('.*')
YamlReferenceImplTest	testStringManipulation	52	unsupported		Unsupported function: matches	** matches with empty collection in regex	Functions.str.attr.matches(EmptyRegex)
YamlReferenceImplTest	testStringManipulation	53	unsupported		Unsupported function: matches	** matches with dotAll	'first line\nsecond line'.matches('line.second')
YamlReferenceImplTest	testStringManipulation	54	unsupported		Unsupported function: matches	** matches on a singleton with null value	Patient.name.given[0].matches('.*')
YamlReferenceImplTest	testStringManipulation	55	unsupported		Unsupported function: matches	** matches with unicode character class escapes	'Taupō'.matches('^\\\\p{Lu}\\\\p{Ll}*$')
YamlReferenceImplTest	testStringManipulation	56	unsupported		Unsupported function: replaceMatches	** replaceMatches	Functions.str.attr.replaceMatches('test', 'match')
YamlReferenceImplTest	testStringManipulation	57	unsupported		Unsupported function: replaceMatches	** replaceMatches 3	Functions.str.date.replaceMatches('\\\\b(\\\\d{1,2})/(\\\\d{1,2})/(\\\\d{2,4})\\\\b', '$2-$1-$3')
YamlReferenceImplTest	testStringManipulation	58	unsupported		Unsupported function: replaceMatches	** replaceMatches replaces each match	Functions.str.dateRange.replaceMatches('\\\\b(\\\\d{1,2})/(\\\\d{1,2})/(\\\\d{2,4})\\\\b', '$2-$1-$3')
YamlReferenceImplTest	testStringManipulation	59	unsupported		Unsupported function: replaceMatches	** replaceMatches on empty input collection	Functions.str.empty.replaceMatches('(.*)', '$1')
YamlReferenceImplTest	testStringManipulation	60	unsupported		Unsupported function: replaceMatches	** replaceMatches with empty collection in regex	Functions.str.attr.replaceMatches(EmptyRegex, 'aaa')
YamlReferenceImplTest	testStringManipulation	61	unsupported		Unsupported function: replaceMatches	** replaceMatches with empty input collection in substitution	Functions.str.attr.replaceMatches('', EmptySubstitution)
YamlReferenceImplTest	testStringManipulation	62	unsupported		Unsupported function: replaceMatches	** replaceMatches on a singleton with null value	Patient.name.given[0].replaceMatches('(.*)', '$1')
YamlReferenceImplTest	testStringManipulation	63	unsupported		Unsupported function: replaceMatches	** replaceMatches with a unicode character class escape	'Taupō'.replaceMatches('(\\\\p{Lu})', 'city: $1')
YamlReferenceImplTest	testStringManipulation	64	unsupported		Unsupported function: length	** length	Functions.str.attr.length()
YamlReferenceImplTest	testStringManipulation	65	unsupported		Unsupported function: length	** length of empty coll	Functions.str.empty.length()
YamlReferenceImplTest	testStringManipulation	66	unsupported		Unsupported function: length	** length on a singleton with null value	Patient.name.given[0].length()
YamlReferenceImplTest	testStringManipulation	67	unsupported		Unsupported function: toChars	** list of characters in the input string	'abc'.toChars()
YamlReferenceImplTest	testStringManipulation	68	unsupported		Unsupported function: toChars	** toChars on empty input collection	Functions.str.empty.toChars()
YamlReferenceImplTest	testStringManipulation	69	unsupported		Unsupported function: toChars	** toChars on a singleton with null value	Patient.name.given[0].toChars()
YamlReferenceImplTest	testStringManipulation	70	unsupported		Unsupported function: split	** split string on delimiter	'a,b,c'.split(',')
YamlReferenceImplTest	testStringManipulation	71	unsupported		Unsupported function: split	** split string on delimiter	'a,,c'.split(',')
YamlReferenceImplTest	testStringManipulation	72	unsupported		Unsupported function: split	** split on a singleton with null value	Patient.name.given[0].split(',')
YamlReferenceImplTest	testStringManipulation	73	excluded:error	null: Parse error		** join string with separator	('a'|'b'|'c').join(',')
YamlReferenceImplTest	testStringManipulation	74	excluded:error	null: Parse error		** join string with separator	('a'|'b'|'c').join()
YamlReferenceImplTest	testStringManipulation	75	excluded:pass	null: Parse error		** join string with date throws error	('a'|'2023-05-28'.toDate()|'c').join(',')
YamlReferenceImplTest	testStringManipulation	76	passed			** join on empty input collection	Functions.str.empty.join(',')
YamlReferenceImplTest	testStringManipulation	77	passed			** join on a collection with null values	Patient.name.given.join(',')
YamlReferenceImplTest	testStringManipulation	78	unsupported		Unsupported function: trim	** trim down a string	'   this is a string   '.trim()
YamlReferenceImplTest	testStringManipulation	79	unsupported		Unsupported function: split	** trim down a string null	'a ,, c'.split(',').select(trim())
YamlReferenceImplTest	testStringManipulation	80	unsupported		Unsupported function: trim	** trim on a singleton with null value	Patient.name.given[0].trim()
YamlReferenceImplTest	testStringManipulation	81	unsupported		Unsupported function: encode	** encode urlbase64	'http://example.org/fhir'.encode('base64')
YamlReferenceImplTest	testStringManipulation	82	unsupported		Unsupported function: encode	** encode on a singleton with null value	Patient.name.given[0].encode('base64')
YamlReferenceImplTest	testStringManipulation	83	unsupported		Unsupported function: decode	** decode	'aHR0cDovL2V4YW1wbGUub3JnL2ZoaXI='.decode('base64')
YamlReferenceImplTest	testStringManipulation	84	unsupported		Unsupported function: decode	** decode on a singleton with null value	Patient.name.given[0].decode('base64')
YamlReferenceImplTest	testStringManipulation	85	unsupported		Unsupported function: encode	** testEncodeDecode 1e	'test'.encode('base64')
YamlReferenceImplTest	testStringManipulation	86	unsupported		Unsupported function: encode	** testEncodeDecode 2e	'test'.encode('hex')
YamlReferenceImplTest	testStringManipulation	87	unsupported		Unsupported function: encode	** testEncodeDecode 3e	'subjects?_d'.encode('base64')
YamlReferenceImplTest	testStringManipulation	88	unsupported		Unsupported function: encode	** testEncodeDecode 4e	'subjects?_d'.encode('urlbase64')
YamlReferenceImplTest	testStringManipulation	89	unsupported		Unsupported function: decode	** testEncodeDecode 1u	'dGVzdA=='.decode('base64')
YamlReferenceImplTest	testStringManipulation	90	unsupported		Unsupported function: decode	** testEncodeDecode 2u	'74657374'.decode('hex')
YamlReferenceImplTest	testStringManipulation	91	unsupported		Unsupported function: decode	** testEncodeDecode 3u	'c3ViamVjdHM/X2Q='.decode('base64')
YamlReferenceImplTest	testStringManipulation	92	unsupported		Unsupported function: decode	** testEncodeDecode 4u	'c3ViamVjdHM_X2Q='.decode('urlbase64')
YamlReferenceImplTest	testStringManipulation	93	unsupported		Unsupported function: encode	extra test that results with a `+` in the encoded string	'subjects>_d'.encode('base64')
YamlReferenceImplTest	testStringManipulation	94	unsupported		Unsupported function: decode	** uneven number of chars, so throws an error	'6B6C6D6E6'.decode('hex')
YamlReferenceImplTest	testStringManipulation	95	unsupported		Unsupported function: trim	** "123456".trim().length() = 6	'123456'.trim().length() = 6
YamlReferenceImplTest	testStringManipulation	96	unsupported		Unsupported function: trim	** "123 456".trim().length() = 7	'123 456'.trim().length() = 7
YamlReferenceImplTest	testStringManipulation	97	unsupported		Unsupported function: trim	** " 123456 ".trim().length() = 6	' 123456 '.trim().length() = 6
YamlReferenceImplTest	testStringManipulation	98	unsupported		Unsupported function: trim	** "   ".trim().length() = 0	'   '.trim().length() = 0
YamlReferenceImplTest	testStringManipulation	99	unsupported		Unsupported function: split	** 'Peter,James,Jim,Peter,James'.split(',').count() = 5	'Peter,James,Jim,Peter,James'.split(',').count() = 5
YamlReferenceImplTest	testStringManipulation	100	passed			** name.given.join(',')	name.given.join(',')
YamlReferenceImplTest	testSubsetting	1	passed			** [index]	Functions.coll1[1].coll2[0].attr
YamlReferenceImplTest	testSubsetting	2	passed			** [big index]	Functions.coll1[100].coll2[0].attr
YamlReferenceImplTest	testSubsetting	3	unsupported		Unsupported function: single	** single	Functions.attrsingle.single()
YamlReferenceImplTest	testSubsetting	4	unsupported		Unsupported function: single	** single on nothing	Functions.ups.single()
YamlReferenceImplTest	testSubsetting	5	unsupported		Unsupported function: single	** single on empty	Functions.attrempty.single()
YamlReferenceImplTest	testSubsetting	6	unsupported		Unsupported function: single	** single on many	Functions.attrdouble.single()
YamlReferenceImplTest	testSubsetting	7	unsupported		Unsupported function: single	** single for collection with null values (1)	Functions.collWithNullsAndTrue[0].single()
YamlReferenceImplTest	testSubsetting	8	unsupported		Unsupported function: single	** single for collection with null values (2)	Functions.collWithNullsAndTrue.single()
YamlReferenceImplTest	testSubsetting	9	passed			** first	Functions.attrdouble.first()
YamlReferenceImplTest	testSubsetting	10	excluded:failure	#437: Primitive attributes		** first for collection with null values	Functions.collWithNullsAndTrue.first().id
YamlReferenceImplTest	testSubsetting	11	passed			** first nothing	Functions.nothing.first()
YamlReferenceImplTest	testSubsetting	12	unsupported		Unsupported function: last	** last	Functions.attrdouble.last()
YamlReferenceImplTest	testSubsetting	13	unsupported		Unsupported function: last	** last (alternative)	Functions.attrsingle.last()
YamlReferenceImplTest	testSubsetting	14	unsupported		Unsupported function: last	** last on empty	Functions.attrempty.last()
YamlReferenceImplTest	testSubsetting	15	unsupported		Unsupported function: last	** last for collection with null values	Functions.collWithNullsAndTrue.last().id
YamlReferenceImplTest	testSubsetting	16	unsupported		Unsupported function: last	** last nothing	Functions.nothing.last()
YamlReferenceImplTest	testSubsetting	17	unsupported		Unsupported function: tail	** tail	Functions.attrdouble.tail()
YamlReferenceImplTest	testSubsetting	18	unsupported		Unsupported function: tail	** tail on one	Functions.attrsingle.tail()
YamlReferenceImplTest	testSubsetting	19	unsupported		Unsupported function: tail	** tail on empty	Functions.attrempty.tail()
YamlReferenceImplTest	testSubsetting	20	unsupported		Unsupported function: tail	** tail nothing	Functions.nothing.tail()
YamlReferenceImplTest	testSubsetting	21	unsupported		Unsupported function: tail	** tail (alternative)	Functions.coll1.coll2.attr.tail()
YamlReferenceImplTest	testSubsetting	22	unsupported		Unsupported function: tail	** tail for collection with null values	Functions.collWithNullsAndTrue.tail()[1].id
YamlReferenceImplTest	testSubsetting	23	unsupported		Unsupported function: skip	** skip	Functions.attrdouble.skip(1)
YamlReferenceImplTest	testSubsetting	24	unsupported		Unsupported function: skip	** skip 2	Functions.attrsingle.skip(1)
YamlReferenceImplTest	testSubsetting	25	unsupported		Unsupported function: skip	** skip 3	Functions.coll1.coll2.attr.skip(3)
YamlReferenceImplTest	testSubsetting	26	unsupported		Unsupported function: skip	** skip 4	Functions.coll1.coll2.attr.skip(4)
YamlReferenceImplTest	testSubsetting	27	unsupported		Unsupported function: skip	** skip 5	Functions.coll1.coll2.attr.skip(5)
YamlReferenceImplTest	testSubsetting	28	unsupported		Unsupported function: skip	** skip 6	Functions.coll1.coll2.attr.skip(6)
YamlReferenceImplTest	testSubsetting	29	unsupported		Unsupported function: skip	** skip for collection with null values	Functions.collWithNullsAndTrue.skip(2).id
YamlReferenceImplTest	testSubsetting	30	unsupported		Unsupported function: take	** take	Functions.attrdouble.take(1)
YamlReferenceImplTest	testSubsetting	31	unsupported		Unsupported function: take	** take 2	Functions.attrdouble.take(2)
YamlReferenceImplTest	testSubsetting	32	unsupported		Unsupported function: take	** take more then has	Functions.attrsingle.take(2)
YamlReferenceImplTest	testSubsetting	33	unsupported		Unsupported function: take	** take on empty	Functions.attrempty.take(1)
YamlReferenceImplTest	testSubsetting	34	unsupported		Unsupported function: take	** take nothing	Functions.nothing.take(2)
YamlReferenceImplTest	testSubsetting	35	unsupported		Unsupported function: take	** take 3	Functions.coll1.coll2.attr.take(3)
YamlReferenceImplTest	testSubsetting	36	unsupported		Unsupported function: take	** take 4	Functions.coll1.coll2.attr.take(4)
YamlReferenceImplTest	testSubsetting	37	unsupported		Unsupported function: take	** take 5	Functions.coll1.coll2.attr.take(5)
YamlReferenceImplTest	testSubsetting	38	unsupported		Unsupported function: take	** take for collection with null values	Functions.collWithNullsAndTrue.take(1).id
YamlReferenceImplTest	testSubsetting	39	unsupported		Unsupported function: intersect	** should not depend on the order of properties in an object	Functions.objects.group1.intersect(Functions.objects.group2)
YamlReferenceImplTest	testSubsetting	40	excluded:error	#2418: Bundles are not supported		** should ignore null properties	Bundle.entry.intersect(Bundle.entry) = (Bundle.entry[0] | Bundle.entry[2])
YamlReferenceImplTest	testSubsetting	41	unsupported		Unsupported function: intersect	** should use year-to-month conversion factor (https://hl7.org/fhirpath/#equals)	(1 year).combine(12 months).intersect(12 months)
YamlReferenceImplTest	testSubsetting	42	unsupported		Unsupported function: intersect	** should use year-to-month conversion factor for large collections	(1 year | 2 year | 3 year | 4 year | 5 year | 6 year).combine(12 months).intersect(12 months)
YamlReferenceImplTest	testSubsetting	43	unsupported		Unsupported function: intersect	** should compare quantities for equality (https://hl7.org/fhirpath/#equals)	(3 'min').combine(180 seconds).intersect(180 seconds)
YamlReferenceImplTest	testSubsetting	44	unsupported		Unsupported function: intersect	** should compare quantities for equality for large collections	(3 'min' | 4 'min' | 5 'min' | 6 'min' | 7 'min').combine(180 seconds).intersect(180 seconds)
YamlReferenceImplTest	testSubsetting	45	unsupported		Unsupported function: intersect	** intersect for collection with null values	Functions.collWithNullsAndTrue[0].intersect(Functions.collWithNullsAndTrue[2]).id
YamlReferenceImplTest	testSubsetting	46	unsupported		Unsupported function: exclude	** with large collection 1	(1 | 2 | 3 | 5 | 6 | 7).exclude(2 | 4) = 1 | 3 | 5 | 6 | 7
YamlReferenceImplTest	testSubsetting	47	unsupported		Unsupported function: exclude	** with large collection 2	(1 | 2 | 3 | 5 | 6 | 7).exclude(4) = 1 | 2 | 3 | 5 | 6 | 7
YamlReferenceImplTest	testSubsetting	48	unsupported		Unsupported function: exclude	** with large collection 3	(1 | 2 | 3 | 4 | 5 | 6 | 7).exclude({}) = 1 | 2 | 3 | 4 | 5 | 6 | 7
YamlReferenceImplTest	testSubsetting	49	unsupported		Unsupported function: exclude	** with large collection 4	1.combine(1 | 3 | 4 | 5 | 6 | 7).exclude(2).count() = 7
YamlReferenceImplTest	testSubsetting	50	unsupported		Unsupported function: exclude	** exclude for collection with null values	(Functions.collWithNullsAndTrue[0] | Functions.collWithNullsAndTrue[1]).exclude(Functions.collWithNullsAndTrue[1]).id
YamlReferenceImplTest	testTreeNavigation	1	unsupported		Unsupported function: children	** children	ch.children()
YamlReferenceImplTest	testTreeNavigation	2	unsupported		Unsupported function: children	** children() on a collection with null elements does not throw an error	name.given.children()
YamlReferenceImplTest	testTreeNavigation	3	unsupported		Unsupported function: descendants	** descendants	ch.descendants()
YamlReferenceImplTest	testTreeNavigation	4	unsupported		Unsupported function: descendants	** descendants	desc.descendants()
YamlReferenceImplTest	testTreeNavigation	5	unsupported		Unsupported function: descendants	** descendants() on a collection with null elements does not throw an error	name.given.descendants()
YamlReferenceImplTest	testTypes	1	excluded:error	null: 'is' operator precedence differs from fhirpath.js		** is boolean	1 > 2 is Boolean
YamlReferenceImplTest	testTypes	2	passed			** values is integer	coll.a is Integer
YamlReferenceImplTest	testTypes	3	passed			** values isnt Patient type	coll.all($this is Patient)
YamlReferenceImplTest	testTypes	4	passed			** is Quantity	1 year is Quantity
YamlReferenceImplTest	testTypes	5	passed			** is System.Quantity	1 year is System.Quantity
YamlReferenceImplTest	testTypes	6	excluded:failure	#2524: Type hierarchy checking not yet implemented		** Observation is DomainResource	Observation is DomainResource
YamlReferenceImplTest	testTypes	7	excluded:failure	#2524: Type hierarchy checking not yet implemented		** Observation is Resource	Observation is Resource
YamlReferenceImplTest	testTypes	8	passed			** valueQuantity is FHIR.Quantity	Observation.value is FHIR.Quantity
YamlReferenceImplTest	testTypes	9	excluded:failure	#2524: Type hierarchy checking not yet implemented		** valueQuantity is Element	Observation.value is Element
YamlReferenceImplTest	testTypes	10	excluded:unsupported	null: 'is' operator on fields accessed through polymorphic collections	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	** valueQuantity.value is decimal	Observation.value.value is decimal
YamlReferenceImplTest	testTypes	11	excluded:unsupported	null: 'is' operator on fields accessed through polymorphic collections	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	** valueQuantity.value is Element	Observation.value.value is Element
YamlReferenceImplTest	testTypes	12	excluded:unsupported	null: 'is' operator on fields accessed through polymorphic collections	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	** valueQuantity.system is uri	Observation.value.system is uri
YamlReferenceImplTest	testTypes	13	excluded:unsupported	null: 'is' operator on fields accessed through polymorphic collections	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	** valueQuantity.unit is string	Observation.value.unit is string
YamlReferenceImplTest	testTypes	14	excluded:unsupported	null: 'is' operator on fields accessed through polymorphic collections	Direct traversal of polymorphic collections is not supported. Please use 'ofType()' to specify the type of element to traverse.	** valueQuantity.unit is not decimal	Observation.value.unit is decimal
YamlReferenceImplTest	testTypes	15	passed			** Patient.contact is BackboneElement	Patient.contact is BackboneElement
YamlReferenceImplTest	testTypes	16	excluded:failure	#2524: Type hierarchy checking not yet implemented		** Patient.contact is Element	Patient.contact is Element
YamlReferenceImplTest	testTypes	17	passed			** Patient.contact.name is HumanName	Patient.contact.name is HumanName
YamlReferenceImplTest	testTypes	18	passed			** Patient.contact.name.given is string	Patient.contact.name.given is string
YamlReferenceImplTest	testTypes	19	passed			** Extension.valueString is string	Patient.contact.name.family.extension('http://hl7.org/fhir/StructureDefinition/humanname-own-prefix').value is string
YamlReferenceImplTest	testTypes	20	passed			** Extension.url is System.String	Patient.contact.name.family.extension('http://hl7.org/fhir/StructureDefinition/humanname-own-prefix').url is System.String
YamlReferenceImplTest	testTypes	21	passed			** Patient.id is System.String (R5)	Patient.id is System.String
YamlReferenceImplTest	testTypes	22	passed			** Patient.id is System.String (R4)	Patient.id is System.String
YamlReferenceImplTest	testTypes	23	passed			** Patient.id is id (DSTU2)	Patient.id is id
YamlReferenceImplTest	testTypes	24	passed			** Patient.id is id (STU3)	Patient.id is id
YamlReferenceImplTest	testTypes	25	passed			** Patient.deceased is boolean	Patient.deceased is boolean
YamlReferenceImplTest	testTypes	26	excluded:failure	#437: 'is' operator with extensions on primitive types not supported		** "is" for the null element of a collection	Patient.name.given[3] is FHIR.string
YamlReferenceImplTest	testTypes	27	excluded:failure	#2524: Type hierarchy checking not yet implemented		** StructureDefinition.snapshot.element.type is Element	StructureDefinition.snapshot.element.type is Element
YamlReferenceImplTest	testTypes	28	passed			** StructureDefinition.snapshot.element.type.code is uri	StructureDefinition.snapshot.element.type.code is uri
YamlReferenceImplTest	testTypes	29	passed			** The type of a valid but missing property is empty	Patient.photo is Attachment
YamlReferenceImplTest	testTypes	30	passed			** is(Quantity)	1 day.is(Quantity)
YamlReferenceImplTest	testTypes	31	passed			** is(System.Quantity)	1 day.is(System.Quantity)
YamlReferenceImplTest	testTypes	32	excluded:failure	#437: Primitive elements with extensions but no value not supported		** is() for the null element of a collection	Patient.name.given[3].is(FHIR.string)
YamlReferenceImplTest	testTypes	33	excluded:failure	null: Mixed-type collections handled as empty instead of error		** more than one item in the input collection for "as"	mixedTypeVals as Integer
YamlReferenceImplTest	testTypes	34	excluded:failure	null: Mixed-type collections handled as empty instead of error		** as integer	mixedTypeVals.select($this as Integer)
YamlReferenceImplTest	testTypes	35	passed			** as Quantity	1 year as Quantity
YamlReferenceImplTest	testTypes	36	passed			** as System.Quantity	1 year as System.Quantity
YamlReferenceImplTest	testTypes	37	excluded:error	null: Non-conformant input fixture: answer is not an array		** as FHIR.Quantity	QuestionnaireResponse.item.select(answer.value as FHIR.Quantity).count()
YamlReferenceImplTest	testTypes	38	passed			** "as" for the null element of a collection	(Patient.name.given[3] as FHIR.string).count() = 1
YamlReferenceImplTest	testTypes	39	excluded:failure	#2804: FHIR and System type namespace distinction not preserved		** "as FHIR.Quantity" returns the empty collection for the System.Quantity	1 year as FHIR.Quantity
YamlReferenceImplTest	testTypes	40	excluded:failure	null: Mixed-type collections handled as empty instead of error		** more than one item in the input collection for "as()"	mixedTypeVals.as(Integer)
YamlReferenceImplTest	testTypes	41	excluded:failure	null: Mixed-type collections handled as empty instead of error		** as(Integer)	mixedTypeVals.select($this.as(Integer))
YamlReferenceImplTest	testTypes	42	passed			** as(Quantity)	1 year.as(Quantity)
YamlReferenceImplTest	testTypes	43	passed			** as(System.Quantity)	1 year.as(System.Quantity)
YamlReferenceImplTest	testTypes	44	excluded:error	null: Non-conformant input fixture: answer is not an array		** as FHIR.Quantity	QuestionnaireResponse.item.select(answer.value.as(FHIR.Quantity)).count()
YamlReferenceImplTest	testTypes	45	excluded:failure	#2804: FHIR and System type namespace distinction not preserved		** "as(FHIR.Quantity)" returns the empty collection for the System.Quantity	1 year.as(FHIR.Quantity)
YamlReferenceImplTest	testTypes	46	passed			** as() for the null element of a collection	Patient.name.given[3].as(FHIR.string).count() = 1
YamlReferenceImplTest	testTypes	47	unsupported		Unsupported function: children	** as Coding	Questionnaire.children().select(code as Coding).count() = 2
YamlReferenceImplTest	testTypes	48	unsupported		Unsupported function: children	** as Element	Questionnaire.children().select(code as Element).count() = 2
YamlReferenceImplTest	testTypes	49	unsupported		Unsupported function: children	** "as Quantity" found nothing	Questionnaire.children().select(code as Quantity).count() = 0
YamlReferenceImplTest	testTypes	50	unsupported		Unsupported function: descendants	** descendants() returns resource nodes with the correct data types (1)	descendants().where($this is Identifier and system = 'urn:oid:1.2.36.146.595.217.0.1').use = 'usual'
YamlReferenceImplTest	testUtilityFunctions	1	passed			group: 5.9.1. trace(name : string) : collection - ** trace	coll.attr.trace('coll')
YamlReferenceImplTest	testUtilityFunctions	2	passed			group: 5.9.1. trace(name : string) : collection - ** trace for collection with null values	Patient.name.given.trace('coll')[0].id
YamlReferenceImplTest	testUtilityFunctions	3	unsupported		Unsupported function: now	group: now(): DateTime - ** should return the same date as today()	now().toString().substring(0,10) = today().toString()
YamlReferenceImplTest	testUtilityFunctions	4	unsupported		Unsupported function: now	group: now(): DateTime - ** should return the same time as timeOfDay()	now().toString().substring(11,12) = timeOfDay().toString()
YamlReferenceImplTest	testUtilityFunctions	5	unsupported		Unsupported function: timeOfDay	group: timeOfDay(): Time - ** should return the full time	timeOfDay().toString().length() = 12
YamlReferenceImplTest	testUtilityFunctions	6	unsupported		Unsupported function: timeOfDay	group: timeOfDay(): Time - ** should return type System.Time	timeOfDay().is(System.Time)
YamlReferenceImplTest	testVariables	1	passed			group: Standard variables - %resource (FHIR extension)	%resource.n1
YamlReferenceImplTest	testVariables	2	passed			group: Standard variables - %context	%context.n1
YamlReferenceImplTest	testVariables	3	excluded:failure	#2424: Unsupported context definition within view runner		group: Standard variables - %context	%context.n1
YamlReferenceImplTest	testVariables	4	passed			group: Standard variables - %ucum	%ucum
YamlReferenceImplTest	testVariables	5	passed			group: Environment variables	%a - 1
YamlReferenceImplTest	testVariables	6	passed			group: Environment variables - Empty variable	%a
YamlReferenceImplTest	testVariables	7	passed			group: Environment variables - Null variable	%a
YamlReferenceImplTest	testVariables	8	passed			group: Environment variables - Undefined variable	%a
YamlReferenceImplTest	testVariables	9	passed			group: Environment variables - Back-quoted variable name	%`a.b() - 1` - 2
YamlReferenceImplTest	testVariables	10	passed			group: Environment variables - Single-quoted variable name	%'a.b() - 1' - 3
YamlFhirPathTest	testDateTimeComparison	1	passed			Equality (=) - Year precision - same year equals	Observation.component.where(code.coding.code = 'year-2023').value.ofType(dateTime) = component.where(code.coding.code = 'year-2023-same').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	2	passed			Equality (=) - Year precision - different year not equals	Observation.component.where(code.coding.code = 'year-2023').value.ofType(dateTime) = component.where(code.coding.code = 'year-2022').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	3	passed			Equality (=) - Year vs Year-Month precision - returns empty	Observation.component.where(code.coding.code = 'year-2023').value.ofType(dateTime) = component.where(code.coding.code = 'month-2023-03').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	4	passed			Equality (=) - Month precision - same month equals	Observation.component.where(code.coding.code = 'month-2023-03').value.ofType(dateTime) = component.where(code.coding.code = 'month-2023-03-same').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	5	passed			Equality (=) - Month precision - different month not equals	Observation.component.where(code.coding.code = 'month-2023-03').value.ofType(dateTime) = component.where(code.coding.code = 'month-2023-04').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	6	passed			Equality (=) - Month vs Day precision - returns empty	Observation.component.where(code.coding.code = 'month-2023-03').value.ofType(dateTime) = component.where(code.coding.code = 'day-2023-03-15').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	7	passed			Equality (=) - Day precision - same day equals	Observation.component.where(code.coding.code = 'day-2023-03-15').value.ofType(dateTime) = component.where(code.coding.code = 'day-2023-03-15-same').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	8	passed			Equality (=) - Day precision - different day not equals	Observation.component.where(code.coding.code = 'day-2023-03-15').value.ofType(dateTime) = component.where(code.coding.code = 'day-2023-03-16').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	9	passed			Equality (=) - Day vs Second precision - returns empty	Observation.component.where(code.coding.code = 'day-2023-03-15').value.ofType(dateTime) = component.where(code.coding.code = 'second-2023-03-15T10:30:00').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	10	passed			Equality (=) - Second precision - same second equals	Observation.component.where(code.coding.code = 'second-2023-03-15T10:30:00').value.ofType(dateTime) = component.where(code.coding.code = 'second-2023-03-15T10:30:00-same').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	11	passed			Equality (=) - Second precision - different second not equals	Observation.component.where(code.coding.code = 'second-2023-03-15T10:30:00').value.ofType(dateTime) = component.where(code.coding.code = 'second-2023-03-15T10:30:01').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	12	passed			Equality (=) - Second vs Millisecond precision - same second with zero milliseconds equals	Observation.component.where(code.coding.code = 'second-2023-03-15T10:30:00').value.ofType(dateTime) = component.where(code.coding.code = 'second-with-zero-ms').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	13	passed			Equality (=) - Second vs Millisecond precision - different values not equal	Observation.component.where(code.coding.code = 'second-2023-03-15T10:30:00').value.ofType(dateTime) = component.where(code.coding.code = 'millisecond-2023-03-15T10:30:00.123').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	14	passed			Equality (=) - Millisecond precision - same millisecond equals	Observation.component.where(code.coding.code = 'millisecond-2023-03-15T10:30:00.123').value.ofType(dateTime) = component.where(code.coding.code = 'millisecond-2023-03-15T10:30:00.123-same').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	15	passed			Equality (=) - Millisecond precision - different millisecond not equals	Observation.component.where(code.coding.code = 'millisecond-2023-03-15T10:30:00.123').value.ofType(dateTime) = component.where(code.coding.code = 'millisecond-2023-03-15T10:30:00.124').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	16	passed			Equality (=) - Cross-timezone - same instant equals	Observation.component.where(code.coding.code = 'tz-utc').value.ofType(dateTime) = component.where(code.coding.code = 'tz-plus4').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	17	passed			Equality (=) - Cross-timezone - different instant not equals	Observation.component.where(code.coding.code = 'tz-utc').value.ofType(dateTime) = component.where(code.coding.code = 'tz-plus4-different').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	18	passed			Inequality (!=) - Same DateTime precision - not equal	Observation.effective.ofType(dateTime) != component.where(code.coding.code = 'same-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	19	passed			Inequality (!=) - Different precision - returns empty	Observation.effective.ofType(dateTime) != component.where(code.coding.code = 'day-2023-03-15').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	20	passed			Inequality (!=) - Different dates - same precision	Observation.effective.ofType(dateTime) != component.where(code.coding.code = 'different-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	21	passed			Less Than (<) - DateTime comparison - first before second	Observation.effective.ofType(dateTime) < component.where(code.coding.code = 'later-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	22	passed			Less Than (<) - DateTime comparison - first after second	Observation.effective.ofType(dateTime) < component.where(code.coding.code = 'earlier-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	23	passed			Less Than (<) - Date comparison - overlapping ranges (different precision)	Observation.effective.ofType(dateTime) < component.where(code.coding.code = 'month-2023-03').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	24	passed			Less Than (<) - Date comparison - same dates	Observation.effective.ofType(dateTime) < component.where(code.coding.code = 'same-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	25	passed			Less Than or Equal (<=) - DateTime comparison - first before second	Observation.effective.ofType(dateTime) <= component.where(code.coding.code = 'later-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	26	passed			Less Than or Equal (<=) - DateTime comparison - same dates	Observation.effective.ofType(dateTime) <= component.where(code.coding.code = 'same-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	27	passed			Less Than or Equal (<=) - DateTime comparison - first after second	Observation.effective.ofType(dateTime) <= component.where(code.coding.code = 'earlier-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	28	passed			Less Than or Equal (<=) - Date comparison - overlapping ranges (different precision)	Observation.effective.ofType(dateTime) <= component.where(code.coding.code = 'month-2023-03').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	29	passed			Greater Than (>) - DateTime comparison - first after second	Observation.effective.ofType(dateTime) > component.where(code.coding.code = 'earlier-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	30	passed			Greater Than (>) - DateTime comparison - first before second	Observation.effective.ofType(dateTime) > component.where(code.coding.code = 'later-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	31	passed			Greater Than (>) - DateTime comparison - same dates	Observation.effective.ofType(dateTime) > component.where(code.coding.code = 'same-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	32	passed			Greater Than (>) - Date comparison - overlapping ranges (different precision)	Observation.effective.ofType(dateTime) > component.where(code.coding.code = 'month-2023-03').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	33	passed			Greater Than or Equal (>=) - DateTime comparison - first after second	Observation.effective.ofType(dateTime) >= component.where(code.coding.code = 'earlier-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	34	passed			Greater Than or Equal (>=) - DateTime comparison - same dates	Observation.effective.ofType(dateTime) >= component.where(code.coding.code = 'same-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	35	passed			Greater Than or Equal (>=) - DateTime comparison - first before second	Observation.effective.ofType(dateTime) >= component.where(code.coding.code = 'later-datetime').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	36	passed			Greater Than or Equal (>=) - Date comparison - overlapping ranges (different precision)	Observation.effective.ofType(dateTime) >= component.where(code.coding.code = 'month-2023-03').value.ofType(dateTime)
YamlFhirPathTest	testDateTimeComparison	37	passed			Compare DateTime with Instant - DateTime with seconds vs issued (Instant) comparison	Observation.effective.ofType(dateTime) = issued
YamlFhirPathTest	testMath	1	passed			Polarity operator - ** Integer negation **	-10
YamlFhirPathTest	testMath	2	passed			Polarity operator - ** Integer positive **	+7
YamlFhirPathTest	testMath	3	passed			Polarity operator - ** Decimal negation **	-10.23
YamlFhirPathTest	testMath	4	passed			Polarity operator - ** Integer positive **	+7.3
YamlFhirPathTest	testSearchParams	1	passed				Resource.id
YamlFhirPathTest	testSearchParams	2	passed				Resource.id
YamlFhirPathTest	testSearchParams	3	passed				Resource.language
YamlFhirPathTest	testSearchParams	4	passed				Resource.meta.lastUpdated
YamlFhirPathTest	testSearchParams	5	passed				Resource.meta.profile
YamlFhirPathTest	testSearchParams	6	passed				Resource.meta.security
YamlFhirPathTest	testSearchParams	7	passed				Resource.meta.source
YamlFhirPathTest	testSearchParams	8	passed				Resource.meta.tag
YamlFhirPathTest	testSearchParams	9	passed				Account.guarantor.party
YamlFhirPathTest	testSearchParams	10	passed				Account.identifier | AdverseEvent.identifier | AllergyIntolerance.identifier | Appointment.identifier | AppointmentResponse.identifier | Basic.identifier | BodyStructure.identifier | CarePlan.identifier | CareTeam.identifier | ChargeItem.identifier | Claim.identifier | ClaimResponse.identifier | ClinicalImpression.identifier | Communication.identifier | CommunicationRequest.identifier | Composition.identifier | Condition.identifier | Consent.identifier | Contract.identifier | Coverage.identifier | CoverageEligibilityRequest.identifier | CoverageEligibilityResponse.identifier | DetectedIssue.identifier | DeviceRequest.identifier | DeviceUsage.identifier | DiagnosticReport.identifier | DocumentReference.identifier | Encounter.identifier | EnrollmentRequest.identifier | EpisodeOfCare.identifier | ExplanationOfBenefit.identifier | FamilyMemberHistory.identifier | Flag.identifier | Goal.identifier | GuidanceResponse.identifier | ImagingSelection.identifier | ImagingStudy.identifier | Immunization.identifier | ImmunizationEvaluation.identifier | ImmunizationRecommendation.identifier | Invoice.identifier | List.identifier | MeasureReport.identifier | Medication.identifier | MedicationAdministration.identifier | MedicationDispense.identifier | MedicationRequest.identifier | MedicationStatement.identifier | MolecularSequence.identifier | NutritionIntake.identifier | NutritionOrder.identifier | Observation.identifier | Person.identifier | Procedure.identifier | QuestionnaireResponse.identifier | RelatedPerson.identifier | RequestOrchestration.identifier | ResearchSubject.identifier | RiskAssessment.identifier | ServiceRequest.identifier | Specimen.identifier | SupplyDelivery.identifier | SupplyRequest.identifier | Task.identifier | VisionPrescription.identifier
YamlFhirPathTest	testSearchParams	11	passed				Account.name
YamlFhirPathTest	testSearchParams	12	passed				Account.owner
YamlFhirPathTest	testSearchParams	13	passed				Account.subject.where(resolve() is Patient) | AdverseEvent.subject.where(resolve() is Patient) | AllergyIntolerance.patient | Appointment.participant.actor.where(resolve() is Patient) | Appointment.subject.where(resolve() is Patient) | AppointmentResponse.actor.where(resolve() is Patient) | AuditEvent.patient | Basic.subject.where(resolve() is Patient) | BodyStructure.patient | CarePlan.subject.where(resolve() is Patient) | CareTeam.subject.where(resolve() is Patient) | ChargeItem.subject.where(resolve() is Patient) | Claim.patient | ClaimResponse.patient | ClinicalImpression.subject.where(resolve() is Patient) | Communication.subject.where(resolve() is Patient) | CommunicationRequest.subject.where(resolve() is Patient) | Composition.subject.where(resolve() is Patient) | Condition.subject.where(resolve() is Patient) | Consent.subject.where(resolve() is Patient) | Contract.subject.where(resolve() is Patient) | Coverage.beneficiary | CoverageEligibilityRequest.patient | CoverageEligibilityResponse.patient | DetectedIssue.subject.where(resolve() is Patient) | DeviceRequest.subject.where(resolve() is Patient) | DeviceUsage.patient | DiagnosticReport.subject.where(resolve() is Patient) | DocumentReference.subject.where(resolve() is Patient) | Encounter.subject.where(resolve() is Patient) | EnrollmentRequest.candidate | EpisodeOfCare.patient | ExplanationOfBenefit.patient | FamilyMemberHistory.patient | Flag.subject.where(resolve() is Patient) | Goal.subject.where(resolve() is Patient) | GuidanceResponse.subject.where(resolve() is Patient) | ImagingSelection.subject.where(resolve() is Patient) | ImagingStudy.subject.where(resolve() is Patient) | Immunization.patient | ImmunizationEvaluation.patient | ImmunizationRecommendation.patient | Invoice.subject.where(resolve() is Patient) | List.subject.where(resolve() is Patient) | MeasureReport.subject.where(resolve() is Patient) | MedicationAdministration.subject.where(resolve() is Patient) | MedicationDispense.subject.where(resolve() is Patient) | MedicationRequest.subject.where(resolve() is Patient) | MedicationStatement.subject.where(resolve() is Patient) | MolecularSequence.subject.where(resolve() is Patient) | NutritionIntake.subject.where(resolve() is Patient) | NutritionOrder.subject.where(resolve() is Patient) | Observation.subject.where(resolve() is Patient) | Person.link.target.where(resolve() is Patient) | Procedure.subject.where(resolve() is Patient) | Provenance.patient | QuestionnaireResponse.subject.where(resolve() is Patient) | RelatedPerson.patient | RequestOrchestration.subject.where(resolve() is Patient) | ResearchSubject.subject.where(resolve() is Patient) | RiskAssessment.subject.where(resolve() is Patient) | ServiceRequest.subject.where(resolve() is Patient) | Specimen.subject.where(resolve() is Patient) | SupplyDelivery.patient | SupplyRequest.deliverFor | Task.for.where(resolve() is Patient) | VisionPrescription.patient
YamlFhirPathTest	testSearchParams	14	passed				Account.servicePeriod
YamlFhirPathTest	testSearchParams	15	passed				Account.relatedAccount.account
YamlFhirPathTest	testSearchParams	16	passed				Account.status
YamlFhirPathTest	testSearchParams	17	passed				Account.subject
YamlFhirPathTest	testSearchParams	18	passed				Account.type | AllergyIntolerance.type | Composition.type | Coverage.type | DocumentReference.type | Encounter.type | EpisodeOfCare.type | Invoice.type | MedicationDispense.type | MolecularSequence.type | Specimen.type
YamlFhirPathTest	testSearchParams	19	passed				ActivityDefinition.relatedArtifact.where(type='composed-of').resource | EventDefinition.relatedArtifact.where(type='composed-of').resource | EvidenceVariable.relatedArtifact.where(type='composed-of').resource | Library.relatedArtifact.where(type='composed-of').resource | Measure.relatedArtifact.where(type='composed-of').resource | PlanDefinition.relatedArtifact.where(type='composed-of').resource
YamlFhirPathTest	testSearchParams	20	passed				(ActivityDefinition.useContext.value.ofType(CodeableConcept)) | (ActorDefinition.useContext.value.ofType(CodeableConcept)) | (CapabilityStatement.useContext.value.ofType(CodeableConcept)) | (ChargeItemDefinition.useContext.value.ofType(CodeableConcept)) | (Citation.useContext.value.ofType(CodeableConcept)) | (CodeSystem.useContext.value.ofType(CodeableConcept)) | (CompartmentDefinition.useContext.value.ofType(CodeableConcept)) | (ConceptMap.useContext.value.ofType(CodeableConcept)) | (ConditionDefinition.useContext.value.ofType(CodeableConcept)) | (EventDefinition.useContext.value.ofType(CodeableConcept)) | (Evidence.useContext.value.ofType(CodeableConcept)) | (EvidenceReport.useContext.value.ofType(CodeableConcept)) | (EvidenceVariable.useContext.value.ofType(CodeableConcept)) | (ExampleScenario.useContext.value.ofType(CodeableConcept)) | (GraphDefinition.useContext.value.ofType(CodeableConcept)) | (ImplementationGuide.useContext.value.ofType(CodeableConcept)) | (Library.useContext.value.ofType(CodeableConcept)) | (Measure.useContext.value.ofType(CodeableConcept)) | (MessageDefinition.useContext.value.ofType(CodeableConcept)) | (NamingSystem.useContext.value.ofType(CodeableConcept)) | (OperationDefinition.useContext.value.ofType(CodeableConcept)) | (PlanDefinition.useContext.value.ofType(CodeableConcept)) | (Questionnaire.useContext.value.ofType(CodeableConcept)) | (Requirements.useContext.value.ofType(CodeableConcept)) | (SearchParameter.useContext.value.ofType(CodeableConcept)) | (StructureDefinition.useContext.value.ofType(CodeableConcept)) | (StructureMap.useContext.value.ofType(CodeableConcept)) | (TerminologyCapabilities.useContext.value.ofType(CodeableConcept)) | (TestScript.useContext.value.ofType(CodeableConcept)) | (ValueSet.useContext.value.ofType(CodeableConcept))
YamlFhirPathTest	testSearchParams	21	passed				(ActivityDefinition.useContext.value.ofType(Quantity)) | (ActivityDefinition.useContext.value.ofType(Range)) | (ActorDefinition.useContext.value.ofType(Quantity)) | (ActorDefinition.useContext.value.ofType(Range)) | (CapabilityStatement.useContext.value.ofType(Quantity)) | (CapabilityStatement.useContext.value.ofType(Range)) | (ChargeItemDefinition.useContext.value.ofType(Quantity)) | (ChargeItemDefinition.useContext.value.ofType(Range)) | (Citation.useContext.value.ofType(Quantity)) | (Citation.useContext.value.ofType(Range)) | (CodeSystem.useContext.value.ofType(Quantity)) | (CodeSystem.useContext.value.ofType(Range)) | (CompartmentDefinition.useContext.value.ofType(Quantity)) | (CompartmentDefinition.useContext.value.ofType(Range)) | (ConceptMap.useContext.value.ofType(Quantity)) | (ConceptMap.useContext.value.ofType(Range)) | (ConditionDefinition.useContext.value.ofType(Quantity)) | (ConditionDefinition.useContext.value.ofType(Range)) | (EventDefinition.useContext.value.ofType(Quantity)) | (EventDefinition.useContext.value.ofType(Range)) | (Evidence.useContext.value.ofType(Quantity)) | (Evidence.useContext.value.ofType(Range)) | (EvidenceReport.useContext.value.ofType(Quantity)) | (EvidenceReport.useContext.value.ofType(Range)) | (EvidenceVariable.useContext.value.ofType(Quantity)) | (EvidenceVariable.useContext.value.ofType(Range)) | (ExampleScenario.useContext.value.ofType(Quantity)) | (ExampleScenario.useContext.value.ofType(Range)) | (GraphDefinition.useContext.value.ofType(Quantity)) | (GraphDefinition.useContext.value.ofType(Range)) | (ImplementationGuide.useContext.value.ofType(Quantity)) | (ImplementationGuide.useContext.value.ofType(Range)) | (Library.useContext.value.ofType(Quantity)) | (Library.useContext.value.ofType(Range)) | (Measure.useContext.value.ofType(Quantity)) | (Measure.useContext.value.ofType(Range)) | (MessageDefinition.useContext.value.ofType(Quantity)) | (MessageDefinition.useContext.value.ofType(Range)) | (NamingSystem.useContext.value.ofType(Quantity)) | (NamingSystem.useContext.value.ofType(Range)) | (OperationDefinition.useContext.value.ofType(Quantity)) | (OperationDefinition.useContext.value.ofType(Range)) | (PlanDefinition.useContext.value.ofType(Quantity)) | (PlanDefinition.useContext.value.ofType(Range)) | (Questionnaire.useContext.value.ofType(Quantity)) | (Questionnaire.useContext.value.ofType(Range)) | (Requirements.useContext.value.ofType(Quantity)) | (Requirements.useContext.value.ofType(Range)) | (SearchParameter.useContext.value.ofType(Quantity)) | (SearchParameter.useContext.value.ofType(Range)) | (StructureDefinition.useContext.value.ofType(Quantity)) | (StructureDefinition.useContext.value.ofType(Range)) | (StructureMap.useContext.value.ofType(Quantity)) | (StructureMap.useContext.value.ofType(Range)) | (TerminologyCapabilities.useContext.value.ofType(Quantity)) | (TerminologyCapabilities.useContext.value.ofType(Range)) | (TestScript.useContext.value.ofType(Quantity)) | (TestScript.useContext.value.ofType(Range)) | (ValueSet.useContext.value.ofType(Quantity)) | (ValueSet.useContext.value.ofType(Range))
YamlFhirPathTest	testSearchParams	22	passed				ActivityDefinition.useContext.code | ActorDefinition.useContext.code | CapabilityStatement.useContext.code | ChargeItemDefinition.useContext.code | Citation.useContext.code | CodeSystem.useContext.code | CompartmentDefinition.useContext.code | ConceptMap.useContext.code | ConditionDefinition.useContext.code | EventDefinition.useContext.code | Evidence.useContext.code | EvidenceReport.useContext.code | EvidenceVariable.useContext.code | ExampleScenario.useContext.code | GraphDefinition.useContext.code | ImplementationGuide.useContext.code | Library.useContext.code | Measure.useContext.code | MessageDefinition.useContext.code | NamingSystem.useContext.code | OperationDefinition.useContext.code | PlanDefinition.useContext.code | Questionnaire.useContext.code | Requirements.useContext.code | SearchParameter.useContext.code | StructureDefinition.useContext.code | StructureMap.useContext.code | TerminologyCapabilities.useContext.code | TestScript.useContext.code | ValueSet.useContext.code
YamlFhirPathTest	testSearchParams	23	passed				ActivityDefinition.date | ActorDefinition.date | CapabilityStatement.date | ChargeItemDefinition.date | Citation.date | CodeSystem.date | CompartmentDefinition.date | ConceptMap.date | ConditionDefinition.date | EventDefinition.date | Evidence.date | EvidenceVariable.date | ExampleScenario.date | GraphDefinition.date | ImplementationGuide.date | Library.date | Measure.date | MessageDefinition.date | NamingSystem.date | OperationDefinition.date | PlanDefinition.date | Questionnaire.date | Requirements.date | SearchParameter.date | StructureDefinition.date | StructureMap.date | SubscriptionTopic.date | TerminologyCapabilities.date | TestScript.date | ValueSet.date
YamlFhirPathTest	testSearchParams	24	passed				ActivityDefinition.relatedArtifact.where(type='depends-on').resource | ActivityDefinition.library | EventDefinition.relatedArtifact.where(type='depends-on').resource | EvidenceVariable.relatedArtifact.where(type='depends-on').resource | Library.relatedArtifact.where(type='depends-on').resource | Measure.relatedArtifact.where(type='depends-on').resource | Measure.library | PlanDefinition.relatedArtifact.where(type='depends-on').resource | PlanDefinition.library
YamlFhirPathTest	testSearchParams	25	passed				ActivityDefinition.relatedArtifact.where(type='derived-from').resource | CodeSystem.relatedArtifact.where(type='derived-from').resource | ConceptMap.relatedArtifact.where(type='derived-from').resource | EventDefinition.relatedArtifact.where(type='derived-from').resource | EvidenceVariable.relatedArtifact.where(type='derived-from').resource | Library.relatedArtifact.where(type='derived-from').resource | Measure.relatedArtifact.where(type='derived-from').resource | NamingSystem.relatedArtifact.where(type='derived-from').resource | PlanDefinition.relatedArtifact.where(type='derived-from').resource | ValueSet.relatedArtifact.where(type='derived-from').resource
YamlFhirPathTest	testSearchParams	26	passed				ActivityDefinition.description | ActorDefinition.description | CapabilityStatement.description | ChargeItemDefinition.description | Citation.description | CodeSystem.description | CompartmentDefinition.description | ConceptMap.description | ConditionDefinition.description | EventDefinition.description | Evidence.description | EvidenceVariable.description | GraphDefinition.description | ImplementationGuide.description | Library.description | Measure.description | MessageDefinition.description | NamingSystem.description | OperationDefinition.description | PlanDefinition.description | Questionnaire.description | Requirements.description | SearchParameter.description | StructureDefinition.description | StructureMap.description | TerminologyCapabilities.description | TestScript.description | ValueSet.description
YamlFhirPathTest	testSearchParams	27	passed				ActivityDefinition.effectivePeriod | ChargeItemDefinition.applicability.effectivePeriod | Citation.effectivePeriod | CodeSystem.effectivePeriod | ConceptMap.effectivePeriod | EventDefinition.effectivePeriod | Library.effectivePeriod | Measure.effectivePeriod | NamingSystem.effectivePeriod | PlanDefinition.effectivePeriod | Questionnaire.effectivePeriod | ValueSet.effectivePeriod
YamlFhirPathTest	testSearchParams	28	passed				ActivityDefinition.identifier | ActorDefinition.identifier | CapabilityStatement.identifier | ChargeItemDefinition.identifier | Citation.identifier | CodeSystem.identifier | ConceptMap.identifier | ConditionDefinition.identifier | EventDefinition.identifier | Evidence.identifier | EvidenceReport.identifier | EvidenceVariable.identifier | ExampleScenario.identifier | GraphDefinition.identifier | ImplementationGuide.identifier | Library.identifier | Measure.identifier | MedicationKnowledge.identifier | MessageDefinition.identifier | NamingSystem.identifier | ObservationDefinition.identifier | OperationDefinition.identifier | PlanDefinition.identifier | Questionnaire.identifier | Requirements.identifier | SearchParameter.identifier | SpecimenDefinition.identifier | StructureDefinition.identifier | StructureMap.identifier | SubscriptionTopic.identifier | TerminologyCapabilities.identifier | TestPlan.identifier | TestScript.identifier | ValueSet.identifier
YamlFhirPathTest	testSearchParams	29	passed				ActivityDefinition.jurisdiction | ActorDefinition.jurisdiction | CapabilityStatement.jurisdiction | ChargeItemDefinition.jurisdiction | Citation.jurisdiction | CodeSystem.jurisdiction | ConceptMap.jurisdiction | ConditionDefinition.jurisdiction | EventDefinition.jurisdiction | ExampleScenario.jurisdiction | GraphDefinition.jurisdiction | ImplementationGuide.jurisdiction | Library.jurisdiction | Measure.jurisdiction | MessageDefinition.jurisdiction | NamingSystem.jurisdiction | OperationDefinition.jurisdiction | PlanDefinition.jurisdiction | Questionnaire.jurisdiction | Requirements.jurisdiction | SearchParameter.jurisdiction | StructureDefinition.jurisdiction | StructureMap.jurisdiction | TerminologyCapabilities.jurisdiction | TestScript.jurisdiction | ValueSet.jurisdiction
YamlFhirPathTest	testSearchParams	30	passed				ActivityDefinition.kind
YamlFhirPathTest	testSearchParams	31	passed				ActivityDefinition.name | CapabilityStatement.name | Citation.name | CodeSystem.name | CompartmentDefinition.name | ConceptMap.name | ConditionDefinition.name | EventDefinition.name | EvidenceVariable.name | ExampleScenario.name | GraphDefinition.name | ImplementationGuide.name | Library.name | Measure.name | MessageDefinition.name | NamingSystem.name | OperationDefinition.name | PlanDefinition.name | Questionnaire.name | Requirements.name | SearchParameter.name | StructureDefinition.name | StructureMap.name | TerminologyCapabilities.name | TestScript.name | ValueSet.name
YamlFhirPathTest	testSearchParams	32	passed				ActivityDefinition.relatedArtifact.where(type='predecessor').resource | CodeSystem.relatedArtifact.where(type='predecessor').resource | ConceptMap.relatedArtifact.where(type='predecessor').resource | EventDefinition.relatedArtifact.where(type='predecessor').resource | EvidenceVariable.relatedArtifact.where(type='predecessor').resource | Library.relatedArtifact.where(type='predecessor').resource | Measure.relatedArtifact.where(type='predecessor').resource | NamingSystem.relatedArtifact.where(type='predecessor').resource | PlanDefinition.relatedArtifact.where(type='predecessor').resource | ValueSet.relatedArtifact.where(type='predecessor').resource
YamlFhirPathTest	testSearchParams	33	passed				ActivityDefinition.publisher | ActorDefinition.publisher | CapabilityStatement.publisher | ChargeItemDefinition.publisher | Citation.publisher | CodeSystem.publisher | CompartmentDefinition.publisher | ConceptMap.publisher | ConditionDefinition.publisher | EventDefinition.publisher | Evidence.publisher | EvidenceReport.publisher | EvidenceVariable.publisher | ExampleScenario.publisher | GraphDefinition.publisher | ImplementationGuide.publisher | Library.publisher | Measure.publisher | MessageDefinition.publisher | NamingSystem.publisher | OperationDefinition.publisher | PlanDefinition.publisher | Questionnaire.publisher | Requirements.publisher | SearchParameter.publisher | StructureDefinition.publisher | StructureMap.publisher | TerminologyCapabilities.publisher | TestScript.publisher | ValueSet.publisher
YamlFhirPathTest	testSearchParams	34	passed				ActivityDefinition.status | ActorDefinition.status | CapabilityStatement.status | ChargeItemDefinition.status | Citation.status | CodeSystem.status | CompartmentDefinition.status | ConceptMap.status | ConditionDefinition.status | EventDefinition.status | Evidence.status | EvidenceReport.status | EvidenceVariable.status | ExampleScenario.status | GraphDefinition.status | ImplementationGuide.status | Library.status | Measure.status | MedicationKnowledge.status | MessageDefinition.status | NamingSystem.status | ObservationDefinition.status | OperationDefinition.status | PlanDefinition.status | Questionnaire.status | Requirements.status | SearchParameter.status | SpecimenDefinition.status | StructureDefinition.status | StructureMap.status | SubscriptionTopic.status | TerminologyCapabilities.status | TestPlan.status | TestScript.status | ValueSet.status
YamlFhirPathTest	testSearchParams	35	passed				ActivityDefinition.relatedArtifact.where(type='successor').resource | EventDefinition.relatedArtifact.where(type='successor').resource | EvidenceVariable.relatedArtifact.where(type='successor').resource | Library.relatedArtifact.where(type='successor').resource | Measure.relatedArtifact.where(type='successor').resource | PlanDefinition.relatedArtifact.where(type='successor').resource
YamlFhirPathTest	testSearchParams	36	passed				ActivityDefinition.title | ActorDefinition.title | CapabilityStatement.title | ChargeItemDefinition.title | Citation.title | CodeSystem.title | ConceptMap.title | ConditionDefinition.title | EventDefinition.title | Evidence.title | EvidenceVariable.title | ImplementationGuide.title | Library.title | Measure.title | MessageDefinition.title | ObservationDefinition.title | OperationDefinition.title | PlanDefinition.title | Questionnaire.title | Requirements.title | SpecimenDefinition.title | StructureDefinition.title | StructureMap.title | SubscriptionTopic.title | TerminologyCapabilities.title | TestScript.title | ValueSet.title
YamlFhirPathTest	testSearchParams	37	passed				ActivityDefinition.topic | CodeSystem.topic | ConceptMap.topic | EventDefinition.topic | Library.topic | Measure.topic | NamingSystem.topic | PlanDefinition.topic | ValueSet.topic
YamlFhirPathTest	testSearchParams	38	passed				ActivityDefinition.url | ActorDefinition.url | CapabilityStatement.url | ChargeItemDefinition.url | Citation.url | CodeSystem.url | CompartmentDefinition.url | ConceptMap.url | ConditionDefinition.url | EventDefinition.url | Evidence.url | EvidenceReport.url | EvidenceVariable.url | ExampleScenario.url | GraphDefinition.url | ImplementationGuide.url | Library.url | Measure.url | MessageDefinition.url | NamingSystem.url | ObservationDefinition.url | OperationDefinition.url | PlanDefinition.url | Questionnaire.url | Requirements.url | SearchParameter.url | SpecimenDefinition.url | StructureDefinition.url | StructureMap.url | SubscriptionTopic.url | TerminologyCapabilities.url | TestPlan.url | TestScript.url | ValueSet.url
YamlFhirPathTest	testSearchParams	39	passed				ActivityDefinition.version | ActorDefinition.version | CapabilityStatement.version | ChargeItemDefinition.version | Citation.version | CodeSystem.version | CompartmentDefinition.version | ConceptMap.version | ConditionDefinition.version | EventDefinition.version | Evidence.version | EvidenceVariable.version | ExampleScenario.version | GraphDefinition.version | ImplementationGuide.version | Library.version | Measure.version | MessageDefinition.version | NamingSystem.version | OperationDefinition.version | PlanDefinition.version | Questionnaire.version | Requirements.version | SearchParameter.version | StructureDefinition.version | StructureMap.version | SubscriptionTopic.version | TerminologyCapabilities.version | TestScript.version | ValueSet.version
YamlFhirPathTest	testSearchParams	40	passed				ActivityDefinition.useContext | ActorDefinition.useContext | CapabilityStatement.useContext | ChargeItemDefinition.useContext | Citation.useContext | CodeSystem.useContext | CompartmentDefinition.useContext | ConceptMap.useContext | ConditionDefinition.useContext | EventDefinition.useContext | Evidence.useContext | EvidenceReport.useContext | EvidenceVariable.useContext | ExampleScenario.useContext | GraphDefinition.useContext | ImplementationGuide.useContext | Library.useContext | Measure.useContext | MessageDefinition.useContext | NamingSystem.useContext | OperationDefinition.useContext | PlanDefinition.useContext | Questionnaire.useContext | Requirements.useContext | SearchParameter.useContext | StructureDefinition.useContext | StructureMap.useContext | TerminologyCapabilities.useContext | TestScript.useContext | ValueSet.useContext
YamlFhirPathTest	testSearchParams	41	passed				ActivityDefinition.useContext | ActorDefinition.useContext | CapabilityStatement.useContext | ChargeItemDefinition.useContext | Citation.useContext | CodeSystem.useContext | CompartmentDefinition.useContext | ConceptMap.useContext | ConditionDefinition.useContext | EventDefinition.useContext | Evidence.useContext | EvidenceReport.useContext | EvidenceVariable.useContext | ExampleScenario.useContext | GraphDefinition.useContext | ImplementationGuide.useContext | Library.useContext | Measure.useContext | MessageDefinition.useContext | NamingSystem.useContext | OperationDefinition.useContext | PlanDefinition.useContext | Questionnaire.useContext | Requirements.useContext | SearchParameter.useContext | StructureDefinition.useContext | StructureMap.useContext | TerminologyCapabilities.useContext | TestScript.useContext | ValueSet.useContext
YamlFhirPathTest	testSearchParams	42	passed				ActorDefinition.type
YamlFhirPathTest	testSearchParams	43	passed				AdministrableProductDefinition.device
YamlFhirPathTest	testSearchParams	44	passed				AdministrableProductDefinition.administrableDoseForm
YamlFhirPathTest	testSearchParams	45	passed				AdministrableProductDefinition.formOf
YamlFhirPathTest	testSearchParams	46	passed				AdministrableProductDefinition.identifier
YamlFhirPathTest	testSearchParams	47	passed				AdministrableProductDefinition.ingredient
YamlFhirPathTest	testSearchParams	48	passed				AdministrableProductDefinition.producedFrom
YamlFhirPathTest	testSearchParams	49	passed				AdministrableProductDefinition.routeOfAdministration.code
YamlFhirPathTest	testSearchParams	50	passed				AdministrableProductDefinition.status
YamlFhirPathTest	testSearchParams	51	passed				AdministrableProductDefinition.routeOfAdministration.targetSpecies.code
YamlFhirPathTest	testSearchParams	52	passed				AdverseEvent.actuality
YamlFhirPathTest	testSearchParams	53	passed				AdverseEvent.category
YamlFhirPathTest	testSearchParams	54	passed				AdverseEvent.code | AllergyIntolerance.code | AllergyIntolerance.reaction.substance | AuditEvent.code | Basic.code | ChargeItem.code | Condition.code | DetectedIssue.code | DeviceRequest.code.concept | DiagnosticReport.code | FamilyMemberHistory.condition.code | ImagingSelection.status | List.code | Medication.code | MedicationAdministration.medication.concept | MedicationDispense.medication.concept | MedicationRequest.medication.concept | MedicationStatement.medication.concept | NutritionIntake.code | Observation.code | Procedure.code | RequestOrchestration.code | Task.code
YamlFhirPathTest	testSearchParams	55	passed				AdverseEvent.occurrence.ofType(dateTime) | AdverseEvent.occurrence.ofType(Period) | AdverseEvent.occurrence.ofType(Timing) | AllergyIntolerance.recordedDate | (start | requestedPeriod.start).first() | AuditEvent.recorded | CarePlan.period | ClinicalImpression.date | Composition.date | Consent.date | DiagnosticReport.effective.ofType(dateTime) | DiagnosticReport.effective.ofType(Period) | DocumentReference.date | Encounter.actualPeriod | EpisodeOfCare.period | FamilyMemberHistory.date | Flag.period | (Immunization.occurrence.ofType(dateTime)) | ImmunizationEvaluation.date | ImmunizationRecommendation.date | Invoice.date | List.date | MeasureReport.date | NutritionIntake.occurrence.ofType(dateTime) | NutritionIntake.occurrence.ofType(Period) | Observation.effective.ofType(dateTime) | Observation.effective.ofType(Period) | Observation.effective.ofType(Timing) | Observation.effective.ofType(instant) | Procedure.occurrence.ofType(dateTime) | Procedure.occurrence.ofType(Period) | Procedure.occurrence.ofType(Timing) | ResearchSubject.period | (RiskAssessment.occurrence.ofType(dateTime)) | SupplyRequest.authoredOn
YamlFhirPathTest	testSearchParams	56	passed				AdverseEvent.location
YamlFhirPathTest	testSearchParams	57	passed				AdverseEvent.recorder
YamlFhirPathTest	testSearchParams	58	passed				AdverseEvent.resultingEffect
YamlFhirPathTest	testSearchParams	59	passed				AdverseEvent.seriousness
YamlFhirPathTest	testSearchParams	60	passed				AdverseEvent.status
YamlFhirPathTest	testSearchParams	61	passed				AdverseEvent.study
YamlFhirPathTest	testSearchParams	62	passed				AdverseEvent.subject
YamlFhirPathTest	testSearchParams	63	passed				(AdverseEvent.suspectEntity.instance as Reference)
YamlFhirPathTest	testSearchParams	64	passed				AllergyIntolerance.category
YamlFhirPathTest	testSearchParams	65	passed				AllergyIntolerance.clinicalStatus
YamlFhirPathTest	testSearchParams	66	passed				AllergyIntolerance.criticality
YamlFhirPathTest	testSearchParams	67	passed				AllergyIntolerance.lastOccurrence
YamlFhirPathTest	testSearchParams	68	passed				AllergyIntolerance.reaction.manifestation.concept
YamlFhirPathTest	testSearchParams	69	passed				AllergyIntolerance.reaction.manifestation.reference
YamlFhirPathTest	testSearchParams	70	passed				AllergyIntolerance.participant.actor
YamlFhirPathTest	testSearchParams	71	passed				AllergyIntolerance.reaction.exposureRoute
YamlFhirPathTest	testSearchParams	72	passed				AllergyIntolerance.reaction.severity
YamlFhirPathTest	testSearchParams	73	passed				AllergyIntolerance.verificationStatus
YamlFhirPathTest	testSearchParams	74	passed				Appointment.participant.actor
YamlFhirPathTest	testSearchParams	75	passed				Appointment.appointmentType
YamlFhirPathTest	testSearchParams	76	passed				Appointment.basedOn
YamlFhirPathTest	testSearchParams	77	passed				Appointment.participant.actor.where(resolve() is Group) | Appointment.subject.where(resolve() is Group)
YamlFhirPathTest	testSearchParams	78	passed				Appointment.participant.actor.where(resolve() is Location)
YamlFhirPathTest	testSearchParams	79	passed				Appointment.participant.status
YamlFhirPathTest	testSearchParams	80	passed				Appointment.participant.actor.where(resolve() is Practitioner)
YamlFhirPathTest	testSearchParams	81	passed				Appointment.reason.concept
YamlFhirPathTest	testSearchParams	82	passed				Appointment.reason.reference
YamlFhirPathTest	testSearchParams	83	passed				requestedPeriod
YamlFhirPathTest	testSearchParams	84	passed				Appointment.serviceCategory
YamlFhirPathTest	testSearchParams	85	passed				Appointment.serviceType.concept
YamlFhirPathTest	testSearchParams	86	passed				Appointment.serviceType.reference
YamlFhirPathTest	testSearchParams	87	passed				Appointment.slot
YamlFhirPathTest	testSearchParams	88	passed				Appointment.specialty
YamlFhirPathTest	testSearchParams	89	passed				Appointment.status
YamlFhirPathTest	testSearchParams	90	passed				Appointment.subject
YamlFhirPathTest	testSearchParams	91	passed				Appointment.supportingInformation
YamlFhirPathTest	testSearchParams	92	passed				AppointmentResponse.actor
YamlFhirPathTest	testSearchParams	93	passed				AppointmentResponse.appointment
YamlFhirPathTest	testSearchParams	94	passed				AppointmentResponse.actor.where(resolve() is Group)
YamlFhirPathTest	testSearchParams	95	passed				AppointmentResponse.actor.where(resolve() is Location)
YamlFhirPathTest	testSearchParams	96	passed				AppointmentResponse.participantStatus
YamlFhirPathTest	testSearchParams	97	passed				AppointmentResponse.actor.where(resolve() is Practitioner)
YamlFhirPathTest	testSearchParams	98	passed				ArtifactAssessment.date
YamlFhirPathTest	testSearchParams	99	passed				ArtifactAssessment.identifier
YamlFhirPathTest	testSearchParams	100	passed				AuditEvent.action
YamlFhirPathTest	testSearchParams	101	passed				AuditEvent.agent.who
YamlFhirPathTest	testSearchParams	102	passed				AuditEvent.agent.role
YamlFhirPathTest	testSearchParams	103	passed				AuditEvent.basedOn
YamlFhirPathTest	testSearchParams	104	passed				AuditEvent.category
YamlFhirPathTest	testSearchParams	105	passed				AuditEvent.encounter | CarePlan.encounter | ChargeItem.encounter | Claim.item.encounter | ClinicalImpression.encounter | Communication.encounter | CommunicationRequest.encounter | Composition.encounter | Condition.encounter | DeviceRequest.encounter | DiagnosticReport.encounter | EncounterHistory.encounter | ExplanationOfBenefit.item.encounter | Flag.encounter | ImagingStudy.encounter | List.encounter | MedicationDispense.encounter | MedicationStatement.encounter | NutritionIntake.encounter | NutritionOrder.encounter | Observation.encounter | Procedure.encounter | Provenance.encounter | QuestionnaireResponse.encounter | RequestOrchestration.encounter | RiskAssessment.encounter | ServiceRequest.encounter | Task.encounter | VisionPrescription.encounter
YamlFhirPathTest	testSearchParams	106	passed				AuditEvent.entity.what
YamlFhirPathTest	testSearchParams	107	passed				AuditEvent.entity.role
YamlFhirPathTest	testSearchParams	108	passed				AuditEvent.outcome.code
YamlFhirPathTest	testSearchParams	109	passed				AuditEvent.agent.policy
YamlFhirPathTest	testSearchParams	110	passed				AuditEvent.authorization | AuditEvent.agent.authorization
YamlFhirPathTest	testSearchParams	111	passed				AuditEvent.source.observer
YamlFhirPathTest	testSearchParams	112	passed				Basic.author
YamlFhirPathTest	testSearchParams	113	passed				Basic.created
YamlFhirPathTest	testSearchParams	114	passed				Basic.subject
YamlFhirPathTest	testSearchParams	115	passed				BiologicallyDerivedProduct.biologicalSourceEvent
YamlFhirPathTest	testSearchParams	116	passed				BiologicallyDerivedProduct.productCode
YamlFhirPathTest	testSearchParams	117	passed				BiologicallyDerivedProduct.collection.collector
YamlFhirPathTest	testSearchParams	118	passed				BiologicallyDerivedProduct.identifier
YamlFhirPathTest	testSearchParams	119	passed				BiologicallyDerivedProduct.productCategory
YamlFhirPathTest	testSearchParams	120	passed				BiologicallyDerivedProduct.productStatus
YamlFhirPathTest	testSearchParams	121	passed				BiologicallyDerivedProduct.request
YamlFhirPathTest	testSearchParams	122	passed				BiologicallyDerivedProduct.identifier
YamlFhirPathTest	testSearchParams	123	passed				BiologicallyDerivedProductDispense.identifier
YamlFhirPathTest	testSearchParams	124	passed				BiologicallyDerivedProductDispense.patient
YamlFhirPathTest	testSearchParams	125	passed				BiologicallyDerivedProductDispense.performer.actor
YamlFhirPathTest	testSearchParams	126	passed				BiologicallyDerivedProductDispense.product
YamlFhirPathTest	testSearchParams	127	passed				BiologicallyDerivedProductDispense.status
YamlFhirPathTest	testSearchParams	128	passed				BodyStructure.excludedStructure.structure
YamlFhirPathTest	testSearchParams	129	passed				BodyStructure.includedStructure.structure
YamlFhirPathTest	testSearchParams	130	passed				BodyStructure.morphology
YamlFhirPathTest	testSearchParams	131	passed				Bundle.entry[0].resource as Composition
YamlFhirPathTest	testSearchParams	132	passed				Bundle.identifier
YamlFhirPathTest	testSearchParams	133	passed				Bundle.entry[0].resource as MessageHeader
YamlFhirPathTest	testSearchParams	134	passed				Bundle.timestamp
YamlFhirPathTest	testSearchParams	135	passed				Bundle.type
YamlFhirPathTest	testSearchParams	136	passed				CapabilityStatement.fhirVersion
YamlFhirPathTest	testSearchParams	137	passed				CapabilityStatement.format
YamlFhirPathTest	testSearchParams	138	passed				CapabilityStatement.implementationGuide
YamlFhirPathTest	testSearchParams	139	passed				CapabilityStatement.rest.mode
YamlFhirPathTest	testSearchParams	140	passed				CapabilityStatement.rest.resource.type
YamlFhirPathTest	testSearchParams	141	passed				CapabilityStatement.rest.resource.profile
YamlFhirPathTest	testSearchParams	142	passed				CapabilityStatement.rest.security.service
YamlFhirPathTest	testSearchParams	143	passed				CapabilityStatement.software.name
YamlFhirPathTest	testSearchParams	144	passed				CapabilityStatement.rest.resource.supportedProfile
YamlFhirPathTest	testSearchParams	145	passed				CarePlan.activity.plannedActivityReference
YamlFhirPathTest	testSearchParams	146	passed				CarePlan.basedOn
YamlFhirPathTest	testSearchParams	147	passed				CarePlan.careTeam
YamlFhirPathTest	testSearchParams	148	passed				CarePlan.category
YamlFhirPathTest	testSearchParams	149	passed				CarePlan.addresses.reference
YamlFhirPathTest	testSearchParams	150	passed				CarePlan.custodian
YamlFhirPathTest	testSearchParams	151	passed				CarePlan.goal
YamlFhirPathTest	testSearchParams	152	passed				CarePlan.instantiatesCanonical
YamlFhirPathTest	testSearchParams	153	passed				CarePlan.instantiatesUri
YamlFhirPathTest	testSearchParams	154	passed				CarePlan.intent
YamlFhirPathTest	testSearchParams	155	passed				CarePlan.partOf
YamlFhirPathTest	testSearchParams	156	passed				CarePlan.replaces
YamlFhirPathTest	testSearchParams	157	passed				CarePlan.status
YamlFhirPathTest	testSearchParams	158	passed				CarePlan.subject
YamlFhirPathTest	testSearchParams	159	passed				CareTeam.category
YamlFhirPathTest	testSearchParams	160	passed				CareTeam.name | CareTeam.extension('http://hl7.org/fhir/StructureDefinition/careteam-alias').value
YamlFhirPathTest	testSearchParams	161	passed				CareTeam.participant.member
YamlFhirPathTest	testSearchParams	162	passed				CareTeam.status
YamlFhirPathTest	testSearchParams	163	passed				CareTeam.subject
YamlFhirPathTest	testSearchParams	164	passed				ChargeItem.account
YamlFhirPathTest	testSearchParams	165	passed				ChargeItem.enteredDate
YamlFhirPathTest	testSearchParams	166	passed				ChargeItem.enterer
YamlFhirPathTest	testSearchParams	167	passed				ChargeItem.totalPriceComponent.factor
YamlFhirPathTest	testSearchParams	168	passed				ChargeItem.occurrence.ofType(dateTime) | ChargeItem.occurrence.ofType(Period) | ChargeItem.occurrence.ofType(Timing)
YamlFhirPathTest	testSearchParams	169	passed				ChargeItem.performer.actor
YamlFhirPathTest	testSearchParams	170	passed				ChargeItem.performer.function
YamlFhirPathTest	testSearchParams	171	passed				ChargeItem.performingOrganization
YamlFhirPathTest	testSearchParams	172	passed				ChargeItem.totalPriceComponent.amount
YamlFhirPathTest	testSearchParams	173	passed				ChargeItem.quantity
YamlFhirPathTest	testSearchParams	174	passed				ChargeItem.requestingOrganization
YamlFhirPathTest	testSearchParams	175	passed				ChargeItem.service.reference
YamlFhirPathTest	testSearchParams	176	passed				ChargeItem.status
YamlFhirPathTest	testSearchParams	177	passed				ChargeItem.subject
YamlFhirPathTest	testSearchParams	178	passed				(Citation.classification.type)
YamlFhirPathTest	testSearchParams	179	passed				(Citation.classification.classifier)
YamlFhirPathTest	testSearchParams	180	passed				Citation.classification
YamlFhirPathTest	testSearchParams	181	passed				Claim.careTeam.provider
YamlFhirPathTest	testSearchParams	182	passed				Claim.created
YamlFhirPathTest	testSearchParams	183	passed				Claim.item.detail.udi
YamlFhirPathTest	testSearchParams	184	passed				Claim.enterer
YamlFhirPathTest	testSearchParams	185	passed				Claim.facility
YamlFhirPathTest	testSearchParams	186	passed				Claim.insurer
YamlFhirPathTest	testSearchParams	187	passed				Claim.item.udi
YamlFhirPathTest	testSearchParams	188	passed				Claim.payee.party
YamlFhirPathTest	testSearchParams	189	passed				Claim.priority
YamlFhirPathTest	testSearchParams	190	passed				Claim.procedure.udi
YamlFhirPathTest	testSearchParams	191	passed				Claim.provider
YamlFhirPathTest	testSearchParams	192	passed				Claim.status
YamlFhirPathTest	testSearchParams	193	passed				Claim.item.detail.subDetail.udi
YamlFhirPathTest	testSearchParams	194	passed				Claim.use
YamlFhirPathTest	testSearchParams	195	passed				ClaimResponse.created
YamlFhirPathTest	testSearchParams	196	passed				ClaimResponse.disposition
YamlFhirPathTest	testSearchParams	197	passed				ClaimResponse.insurer
YamlFhirPathTest	testSearchParams	198	passed				ClaimResponse.outcome
YamlFhirPathTest	testSearchParams	199	passed				ClaimResponse.payment.date
YamlFhirPathTest	testSearchParams	200	passed				ClaimResponse.request
YamlFhirPathTest	testSearchParams	201	passed				ClaimResponse.requestor
YamlFhirPathTest	testSearchParams	202	passed				ClaimResponse.status
YamlFhirPathTest	testSearchParams	203	passed				ClaimResponse.use
YamlFhirPathTest	testSearchParams	204	passed				ClinicalImpression.finding.item.concept
YamlFhirPathTest	testSearchParams	205	passed				ClinicalImpression.finding.item.reference
YamlFhirPathTest	testSearchParams	206	passed				ClinicalImpression.performer
YamlFhirPathTest	testSearchParams	207	passed				ClinicalImpression.previous
YamlFhirPathTest	testSearchParams	208	passed				ClinicalImpression.problem
YamlFhirPathTest	testSearchParams	209	passed				ClinicalImpression.status
YamlFhirPathTest	testSearchParams	210	passed				ClinicalImpression.subject
YamlFhirPathTest	testSearchParams	211	passed				ClinicalImpression.supportingInfo
YamlFhirPathTest	testSearchParams	212	passed				ClinicalUseDefinition.contraindication.diseaseSymptomProcedure.concept
YamlFhirPathTest	testSearchParams	213	passed				ClinicalUseDefinition.contraindication.diseaseSymptomProcedure.reference
YamlFhirPathTest	testSearchParams	214	passed				ClinicalUseDefinition.undesirableEffect.symptomConditionEffect.concept
YamlFhirPathTest	testSearchParams	215	passed				ClinicalUseDefinition.undesirableEffect.symptomConditionEffect.reference
YamlFhirPathTest	testSearchParams	216	passed				ClinicalUseDefinition.identifier
YamlFhirPathTest	testSearchParams	217	passed				ClinicalUseDefinition.indication.diseaseSymptomProcedure.concept
YamlFhirPathTest	testSearchParams	218	passed				ClinicalUseDefinition.indication.diseaseSymptomProcedure.reference
YamlFhirPathTest	testSearchParams	219	passed				ClinicalUseDefinition.interaction.type
YamlFhirPathTest	testSearchParams	220	excluded:error	null: Unsupported resources types (post r4)			ClinicalUseDefinition.subject.where(resolve() is MedicinalProductDefinition)
YamlFhirPathTest	testSearchParams	221	passed				ClinicalUseDefinition.status
YamlFhirPathTest	testSearchParams	222	passed				ClinicalUseDefinition.subject
YamlFhirPathTest	testSearchParams	223	passed				ClinicalUseDefinition.type
YamlFhirPathTest	testSearchParams	224	passed				CodeSystem.concept.code
YamlFhirPathTest	testSearchParams	225	passed				CodeSystem.content
YamlFhirPathTest	testSearchParams	226	passed				CodeSystem.concept.designation.language
YamlFhirPathTest	testSearchParams	227	passed				CodeSystem.supplements
YamlFhirPathTest	testSearchParams	228	passed				CodeSystem.url
YamlFhirPathTest	testSearchParams	229	passed				Communication.basedOn
YamlFhirPathTest	testSearchParams	230	passed				Communication.category
YamlFhirPathTest	testSearchParams	231	passed				Communication.instantiatesCanonical
YamlFhirPathTest	testSearchParams	232	passed				Communication.instantiatesUri
YamlFhirPathTest	testSearchParams	233	passed				Communication.medium
YamlFhirPathTest	testSearchParams	234	passed				Communication.partOf
YamlFhirPathTest	testSearchParams	235	passed				Communication.received
YamlFhirPathTest	testSearchParams	236	passed				Communication.recipient
YamlFhirPathTest	testSearchParams	237	passed				Communication.sender
YamlFhirPathTest	testSearchParams	238	passed				Communication.sent
YamlFhirPathTest	testSearchParams	239	passed				Communication.status
YamlFhirPathTest	testSearchParams	240	passed				Communication.subject
YamlFhirPathTest	testSearchParams	241	passed				Communication.topic
YamlFhirPathTest	testSearchParams	242	passed				CommunicationRequest.authoredOn
YamlFhirPathTest	testSearchParams	243	passed				CommunicationRequest.basedOn
YamlFhirPathTest	testSearchParams	244	passed				CommunicationRequest.category
YamlFhirPathTest	testSearchParams	245	passed				CommunicationRequest.groupIdentifier
YamlFhirPathTest	testSearchParams	246	passed				CommunicationRequest.informationProvider
YamlFhirPathTest	testSearchParams	247	passed				CommunicationRequest.medium
YamlFhirPathTest	testSearchParams	248	passed				CommunicationRequest.occurrence.ofType(dateTime) | CommunicationRequest.occurrence.ofType(Period)
YamlFhirPathTest	testSearchParams	249	passed				CommunicationRequest.priority
YamlFhirPathTest	testSearchParams	250	passed				CommunicationRequest.recipient
YamlFhirPathTest	testSearchParams	251	passed				CommunicationRequest.replaces
YamlFhirPathTest	testSearchParams	252	passed				CommunicationRequest.requester
YamlFhirPathTest	testSearchParams	253	passed				CommunicationRequest.status
YamlFhirPathTest	testSearchParams	254	passed				CommunicationRequest.subject
YamlFhirPathTest	testSearchParams	255	passed				CompartmentDefinition.code
YamlFhirPathTest	testSearchParams	256	passed				CompartmentDefinition.resource.code
YamlFhirPathTest	testSearchParams	257	passed				Composition.attester.party
YamlFhirPathTest	testSearchParams	258	passed				Composition.author
YamlFhirPathTest	testSearchParams	259	passed				Composition.category
YamlFhirPathTest	testSearchParams	260	passed				Composition.section.entry
YamlFhirPathTest	testSearchParams	261	passed				Composition.event.detail.concept
YamlFhirPathTest	testSearchParams	262	passed				Composition.event.detail.reference
YamlFhirPathTest	testSearchParams	263	passed				Composition.event.period
YamlFhirPathTest	testSearchParams	264	passed				Composition.relatesTo.resourceReference
YamlFhirPathTest	testSearchParams	265	passed				Composition.section.code
YamlFhirPathTest	testSearchParams	266	passed				Composition.section.text | Composition.section.section.text
YamlFhirPathTest	testSearchParams	267	passed				Composition.status
YamlFhirPathTest	testSearchParams	268	passed				Composition.subject
YamlFhirPathTest	testSearchParams	269	passed				Composition.title
YamlFhirPathTest	testSearchParams	270	passed				Composition.url
YamlFhirPathTest	testSearchParams	271	passed				Composition.version
YamlFhirPathTest	testSearchParams	272	passed				Composition.section
YamlFhirPathTest	testSearchParams	273	passed				ConceptMap.property.uri
YamlFhirPathTest	testSearchParams	274	passed				ConceptMap.group.unmapped.otherMap
YamlFhirPathTest	testSearchParams	275	passed				ConceptMap.group.element.code
YamlFhirPathTest	testSearchParams	276	passed				ConceptMap.group.source
YamlFhirPathTest	testSearchParams	277	passed				(ConceptMap.sourceScope as canonical)
YamlFhirPathTest	testSearchParams	278	passed				(ConceptMap.sourceScope as uri)
YamlFhirPathTest	testSearchParams	279	passed				ConceptMap.group.element.target.code
YamlFhirPathTest	testSearchParams	280	passed				ConceptMap.group.target
YamlFhirPathTest	testSearchParams	281	passed				(ConceptMap.targetScope as canonical)
YamlFhirPathTest	testSearchParams	282	passed				(ConceptMap.targetScope as uri)
YamlFhirPathTest	testSearchParams	283	passed				Condition.abatement.ofType(Age) | Condition.abatement.ofType(Range)
YamlFhirPathTest	testSearchParams	284	passed				Condition.abatement.ofType(dateTime) | Condition.abatement.ofType(Period)
YamlFhirPathTest	testSearchParams	285	passed				Condition.abatement.ofType(string)
YamlFhirPathTest	testSearchParams	286	passed				Condition.bodySite
YamlFhirPathTest	testSearchParams	287	passed				Condition.category
YamlFhirPathTest	testSearchParams	288	passed				Condition.clinicalStatus
YamlFhirPathTest	testSearchParams	289	passed				Condition.evidence.concept
YamlFhirPathTest	testSearchParams	290	passed				Condition.evidence.reference
YamlFhirPathTest	testSearchParams	291	passed				Condition.onset.ofType(Age) | Condition.onset.ofType(Range)
YamlFhirPathTest	testSearchParams	292	passed				Condition.onset.ofType(dateTime) | Condition.onset.ofType(Period)
YamlFhirPathTest	testSearchParams	293	passed				Condition.onset.ofType(string)
YamlFhirPathTest	testSearchParams	294	passed				Condition.participant.actor
YamlFhirPathTest	testSearchParams	295	passed				Condition.participant.function
YamlFhirPathTest	testSearchParams	296	passed				Condition.recordedDate
YamlFhirPathTest	testSearchParams	297	passed				Condition.severity
YamlFhirPathTest	testSearchParams	298	passed				Condition.stage.summary
YamlFhirPathTest	testSearchParams	299	passed				Condition.subject
YamlFhirPathTest	testSearchParams	300	passed				Condition.verificationStatus
YamlFhirPathTest	testSearchParams	301	passed				Consent.provision.action
YamlFhirPathTest	testSearchParams	302	passed				Consent.provision.actor.reference
YamlFhirPathTest	testSearchParams	303	passed				Consent.category
YamlFhirPathTest	testSearchParams	304	passed				Consent.controller
YamlFhirPathTest	testSearchParams	305	passed				Consent.provision.data.reference
YamlFhirPathTest	testSearchParams	306	passed				Consent.grantee
YamlFhirPathTest	testSearchParams	307	passed				Consent.manager
YamlFhirPathTest	testSearchParams	308	passed				Consent.provision.period
YamlFhirPathTest	testSearchParams	309	passed				Consent.provision.purpose
YamlFhirPathTest	testSearchParams	310	passed				Consent.provision.securityLabel
YamlFhirPathTest	testSearchParams	311	passed				Consent.sourceReference
YamlFhirPathTest	testSearchParams	312	passed				Consent.status
YamlFhirPathTest	testSearchParams	313	passed				Consent.subject
YamlFhirPathTest	testSearchParams	314	passed				Consent.verification.verified
YamlFhirPathTest	testSearchParams	315	passed				Consent.verification.verificationDate
YamlFhirPathTest	testSearchParams	316	passed				Contract.authority
YamlFhirPathTest	testSearchParams	317	passed				Contract.domain
YamlFhirPathTest	testSearchParams	318	passed				Contract.instantiatesUri
YamlFhirPathTest	testSearchParams	319	passed				Contract.issued
YamlFhirPathTest	testSearchParams	320	passed				Contract.signer.party
YamlFhirPathTest	testSearchParams	321	passed				Contract.status
YamlFhirPathTest	testSearchParams	322	passed				Contract.subject
YamlFhirPathTest	testSearchParams	323	passed				Contract.url
YamlFhirPathTest	testSearchParams	324	passed				Coverage.beneficiary
YamlFhirPathTest	testSearchParams	325	passed				Coverage.class.type
YamlFhirPathTest	testSearchParams	326	passed				Coverage.class.value
YamlFhirPathTest	testSearchParams	327	passed				Coverage.dependent
YamlFhirPathTest	testSearchParams	328	passed				Coverage.insurer
YamlFhirPathTest	testSearchParams	329	passed				Coverage.paymentBy.party
YamlFhirPathTest	testSearchParams	330	passed				Coverage.policyHolder
YamlFhirPathTest	testSearchParams	331	passed				Coverage.status
YamlFhirPathTest	testSearchParams	332	passed				Coverage.subscriber
YamlFhirPathTest	testSearchParams	333	passed				Coverage.subscriberId
YamlFhirPathTest	testSearchParams	334	passed				CoverageEligibilityRequest.created
YamlFhirPathTest	testSearchParams	335	passed				CoverageEligibilityRequest.enterer
YamlFhirPathTest	testSearchParams	336	passed				CoverageEligibilityRequest.facility
YamlFhirPathTest	testSearchParams	337	passed				CoverageEligibilityRequest.provider
YamlFhirPathTest	testSearchParams	338	passed				CoverageEligibilityRequest.status
YamlFhirPathTest	testSearchParams	339	passed				CoverageEligibilityResponse.created
YamlFhirPathTest	testSearchParams	340	passed				CoverageEligibilityResponse.disposition
YamlFhirPathTest	testSearchParams	341	passed				CoverageEligibilityResponse.insurer
YamlFhirPathTest	testSearchParams	342	passed				CoverageEligibilityResponse.outcome
YamlFhirPathTest	testSearchParams	343	passed				CoverageEligibilityResponse.request
YamlFhirPathTest	testSearchParams	344	passed				CoverageEligibilityResponse.requestor
YamlFhirPathTest	testSearchParams	345	passed				CoverageEligibilityResponse.status
YamlFhirPathTest	testSearchParams	346	passed				DetectedIssue.author
YamlFhirPathTest	testSearchParams	347	passed				DetectedIssue.category
YamlFhirPathTest	testSearchParams	348	passed				DetectedIssue.identified.ofType(dateTime) | DetectedIssue.identified.ofType(Period)
YamlFhirPathTest	testSearchParams	349	passed				DetectedIssue.implicated
YamlFhirPathTest	testSearchParams	350	passed				DetectedIssue.status
YamlFhirPathTest	testSearchParams	351	passed				DetectedIssue.subject
YamlFhirPathTest	testSearchParams	352	passed				Device.biologicalSourceEvent
YamlFhirPathTest	testSearchParams	353	passed				Device.definition.concept
YamlFhirPathTest	testSearchParams	354	passed				Device.definition.reference
YamlFhirPathTest	testSearchParams	355	passed				Device.name.value | Device.type.coding.display | Device.type.text
YamlFhirPathTest	testSearchParams	356	passed				Device.expirationDate
YamlFhirPathTest	testSearchParams	357	passed				Device.identifier
YamlFhirPathTest	testSearchParams	358	passed				Device.location
YamlFhirPathTest	testSearchParams	359	passed				Device.lotNumber
YamlFhirPathTest	testSearchParams	360	passed				Device.manufactureDate
YamlFhirPathTest	testSearchParams	361	passed				Device.manufacturer
YamlFhirPathTest	testSearchParams	362	passed				Device.modelNumber
YamlFhirPathTest	testSearchParams	363	passed				Device.owner
YamlFhirPathTest	testSearchParams	364	passed				Device.parent
YamlFhirPathTest	testSearchParams	365	passed				Device.serialNumber | Device.identifier.where(type='SNO')
YamlFhirPathTest	testSearchParams	366	passed				Device.conformsTo.specification
YamlFhirPathTest	testSearchParams	367	passed				Device.status
YamlFhirPathTest	testSearchParams	368	passed				Device.type
YamlFhirPathTest	testSearchParams	369	passed				Device.udiCarrier.carrierHRF
YamlFhirPathTest	testSearchParams	370	passed				Device.udiCarrier.deviceIdentifier
YamlFhirPathTest	testSearchParams	371	passed				Device.url
YamlFhirPathTest	testSearchParams	372	passed				Device.version.value
YamlFhirPathTest	testSearchParams	373	passed				Device
YamlFhirPathTest	testSearchParams	374	passed				Device.conformsTo
YamlFhirPathTest	testSearchParams	375	passed				DeviceAssociation.device
YamlFhirPathTest	testSearchParams	376	passed				DeviceAssociation.identifier
YamlFhirPathTest	testSearchParams	377	passed				DeviceAssociation.operation.operator
YamlFhirPathTest	testSearchParams	378	passed				DeviceAssociation.subject.where(resolve() is Patient)
YamlFhirPathTest	testSearchParams	379	passed				DeviceAssociation.status
YamlFhirPathTest	testSearchParams	380	passed				DeviceAssociation.subject.where(resolve() is Patient)
YamlFhirPathTest	testSearchParams	381	passed				DeviceDefinition.deviceName.name | DeviceDefinition.classification.type.coding.display | DeviceDefinition.classification.type.text
YamlFhirPathTest	testSearchParams	382	passed				DeviceDefinition.identifier
YamlFhirPathTest	testSearchParams	383	passed				DeviceDefinition.manufacturer
YamlFhirPathTest	testSearchParams	384	passed				DeviceDefinition.owner
YamlFhirPathTest	testSearchParams	385	passed				DeviceDefinition.conformsTo.specification
YamlFhirPathTest	testSearchParams	386	passed				DeviceDefinition.conformsTo.category
YamlFhirPathTest	testSearchParams	387	passed				DeviceDefinition.conformsTo
YamlFhirPathTest	testSearchParams	388	passed				DeviceDispense.device.concept
YamlFhirPathTest	testSearchParams	389	passed				DeviceDispense.identifier
YamlFhirPathTest	testSearchParams	390	passed				DeviceDispense.subject.where(resolve() is Patient)
YamlFhirPathTest	testSearchParams	391	passed				DeviceDispense.status
YamlFhirPathTest	testSearchParams	392	passed				DeviceDispense.subject
YamlFhirPathTest	testSearchParams	393	passed				DeviceMetric.category
YamlFhirPathTest	testSearchParams	394	passed				DeviceMetric.device
YamlFhirPathTest	testSearchParams	395	passed				DeviceMetric.identifier
YamlFhirPathTest	testSearchParams	396	passed				DeviceMetric.type
YamlFhirPathTest	testSearchParams	397	passed				DeviceRequest.authoredOn
YamlFhirPathTest	testSearchParams	398	passed				DeviceRequest.basedOn
YamlFhirPathTest	testSearchParams	399	passed				DeviceRequest.code.reference
YamlFhirPathTest	testSearchParams	400	passed				(DeviceRequest.occurrence.ofType(dateTime)) | (DeviceRequest.occurrence.ofType(Period))
YamlFhirPathTest	testSearchParams	401	passed				DeviceRequest.groupIdentifier
YamlFhirPathTest	testSearchParams	402	passed				DeviceRequest.instantiatesCanonical
YamlFhirPathTest	testSearchParams	403	passed				DeviceRequest.instantiatesUri
YamlFhirPathTest	testSearchParams	404	passed				DeviceRequest.insurance
YamlFhirPathTest	testSearchParams	405	passed				DeviceRequest.intent
YamlFhirPathTest	testSearchParams	406	passed				DeviceRequest.performer.reference
YamlFhirPathTest	testSearchParams	407	passed				DeviceRequest.performer.concept
YamlFhirPathTest	testSearchParams	408	passed				DeviceRequest.replaces
YamlFhirPathTest	testSearchParams	409	passed				DeviceRequest.requester
YamlFhirPathTest	testSearchParams	410	passed				DeviceRequest.status
YamlFhirPathTest	testSearchParams	411	passed				DeviceRequest.subject
YamlFhirPathTest	testSearchParams	412	passed				DeviceUsage.device.concept
YamlFhirPathTest	testSearchParams	413	passed				DeviceUsage.status
YamlFhirPathTest	testSearchParams	414	passed				DiagnosticReport.basedOn
YamlFhirPathTest	testSearchParams	415	passed				DiagnosticReport.category
YamlFhirPathTest	testSearchParams	416	passed				DiagnosticReport.conclusionCode
YamlFhirPathTest	testSearchParams	417	passed				DiagnosticReport.issued
YamlFhirPathTest	testSearchParams	418	passed				DiagnosticReport.media.link
YamlFhirPathTest	testSearchParams	419	passed				DiagnosticReport.performer
YamlFhirPathTest	testSearchParams	420	passed				DiagnosticReport.result
YamlFhirPathTest	testSearchParams	421	passed				DiagnosticReport.resultsInterpreter
YamlFhirPathTest	testSearchParams	422	passed				DiagnosticReport.specimen
YamlFhirPathTest	testSearchParams	423	passed				DiagnosticReport.status
YamlFhirPathTest	testSearchParams	424	passed				DiagnosticReport.study
YamlFhirPathTest	testSearchParams	425	passed				DiagnosticReport.subject
YamlFhirPathTest	testSearchParams	426	passed				DocumentReference.attester.party
YamlFhirPathTest	testSearchParams	427	passed				DocumentReference.author
YamlFhirPathTest	testSearchParams	428	passed				DocumentReference.basedOn
YamlFhirPathTest	testSearchParams	429	passed				DocumentReference.bodySite.concept
YamlFhirPathTest	testSearchParams	430	passed				DocumentReference.bodySite.reference
YamlFhirPathTest	testSearchParams	431	passed				DocumentReference.category
YamlFhirPathTest	testSearchParams	432	passed				DocumentReference.content.attachment.contentType
YamlFhirPathTest	testSearchParams	433	passed				DocumentReference.context
YamlFhirPathTest	testSearchParams	434	passed				DocumentReference.content.attachment.creation
YamlFhirPathTest	testSearchParams	435	passed				DocumentReference.custodian
YamlFhirPathTest	testSearchParams	436	passed				DocumentReference.description
YamlFhirPathTest	testSearchParams	437	passed				DocumentReference.docStatus
YamlFhirPathTest	testSearchParams	438	passed				DocumentReference.event.concept
YamlFhirPathTest	testSearchParams	439	passed				DocumentReference.event.reference
YamlFhirPathTest	testSearchParams	440	passed				DocumentReference.facilityType
YamlFhirPathTest	testSearchParams	441	passed				(DocumentReference.content.profile.value.ofType(canonical))
YamlFhirPathTest	testSearchParams	442	passed				(DocumentReference.content.profile.value.ofType(Coding))
YamlFhirPathTest	testSearchParams	443	passed				(DocumentReference.content.profile.value.ofType(uri))
YamlFhirPathTest	testSearchParams	444	passed				DocumentReference.content.attachment.language
YamlFhirPathTest	testSearchParams	445	passed				DocumentReference.content.attachment.url
YamlFhirPathTest	testSearchParams	446	passed				DocumentReference.modality
YamlFhirPathTest	testSearchParams	447	passed				DocumentReference.period
YamlFhirPathTest	testSearchParams	448	passed				DocumentReference.relatesTo.target
YamlFhirPathTest	testSearchParams	449	passed				DocumentReference.relatesTo.code
YamlFhirPathTest	testSearchParams	450	passed				DocumentReference.securityLabel
YamlFhirPathTest	testSearchParams	451	passed				DocumentReference.practiceSetting
YamlFhirPathTest	testSearchParams	452	passed				DocumentReference.status
YamlFhirPathTest	testSearchParams	453	passed				DocumentReference.subject
YamlFhirPathTest	testSearchParams	454	passed				DocumentReference.version
YamlFhirPathTest	testSearchParams	455	passed				DocumentReference.relatesTo
YamlFhirPathTest	testSearchParams	456	passed				Encounter.account
YamlFhirPathTest	testSearchParams	457	passed				Encounter.appointment
YamlFhirPathTest	testSearchParams	458	passed				Encounter.basedOn
YamlFhirPathTest	testSearchParams	459	passed				Encounter.careTeam
YamlFhirPathTest	testSearchParams	460	passed				Encounter.class
YamlFhirPathTest	testSearchParams	461	passed				Encounter.actualPeriod.start
YamlFhirPathTest	testSearchParams	462	passed				Encounter.diagnosis.condition.concept
YamlFhirPathTest	testSearchParams	463	passed				Encounter.diagnosis.condition.reference
YamlFhirPathTest	testSearchParams	464	passed				Encounter.actualPeriod.end
YamlFhirPathTest	testSearchParams	465	passed				Encounter.episodeOfCare
YamlFhirPathTest	testSearchParams	466	passed				Encounter.length
YamlFhirPathTest	testSearchParams	467	passed				Encounter.location.location
YamlFhirPathTest	testSearchParams	468	passed				Encounter.partOf
YamlFhirPathTest	testSearchParams	469	passed				Encounter.participant.actor
YamlFhirPathTest	testSearchParams	470	passed				Encounter.participant.type
YamlFhirPathTest	testSearchParams	471	passed				Encounter.participant.actor.where(resolve() is Practitioner)
YamlFhirPathTest	testSearchParams	472	passed				Encounter.reason.value.concept
YamlFhirPathTest	testSearchParams	473	passed				Encounter.reason.value.reference
YamlFhirPathTest	testSearchParams	474	passed				Encounter.serviceProvider
YamlFhirPathTest	testSearchParams	475	passed				Encounter.specialArrangement
YamlFhirPathTest	testSearchParams	476	passed				Encounter.status
YamlFhirPathTest	testSearchParams	477	passed				Encounter.subject
YamlFhirPathTest	testSearchParams	478	passed				Encounter.subjectStatus
YamlFhirPathTest	testSearchParams	479	passed				Encounter.location
YamlFhirPathTest	testSearchParams	480	passed				EncounterHistory.identifier
YamlFhirPathTest	testSearchParams	481	passed				EncounterHistory.subject.where(resolve() is Patient)
YamlFhirPathTest	testSearchParams	482	passed				EncounterHistory.status
YamlFhirPathTest	testSearchParams	483	passed				EncounterHistory.subject
YamlFhirPathTest	testSearchParams	484	passed				Endpoint.connectionType
YamlFhirPathTest	testSearchParams	485	passed				Endpoint.identifier
YamlFhirPathTest	testSearchParams	486	passed				Endpoint.name
YamlFhirPathTest	testSearchParams	487	passed				Endpoint.managingOrganization
YamlFhirPathTest	testSearchParams	488	passed				Endpoint.payload.type
YamlFhirPathTest	testSearchParams	489	passed				Endpoint.status
YamlFhirPathTest	testSearchParams	490	passed				EnrollmentRequest.status
YamlFhirPathTest	testSearchParams	491	passed				EnrollmentRequest.candidate
YamlFhirPathTest	testSearchParams	492	passed				EnrollmentResponse.identifier
YamlFhirPathTest	testSearchParams	493	passed				EnrollmentResponse.request
YamlFhirPathTest	testSearchParams	494	passed				EnrollmentResponse.status
YamlFhirPathTest	testSearchParams	495	passed				EpisodeOfCare.careManager.where(resolve() is Practitioner)
YamlFhirPathTest	testSearchParams	496	passed				EpisodeOfCare.diagnosis.condition.concept
YamlFhirPathTest	testSearchParams	497	passed				EpisodeOfCare.diagnosis.condition.reference
YamlFhirPathTest	testSearchParams	498	passed				EpisodeOfCare.referralRequest
YamlFhirPathTest	testSearchParams	499	passed				EpisodeOfCare.managingOrganization
YamlFhirPathTest	testSearchParams	500	passed				EpisodeOfCare.reason.value.concept
YamlFhirPathTest	testSearchParams	501	passed				EpisodeOfCare.reason.value.reference
YamlFhirPathTest	testSearchParams	502	passed				EpisodeOfCare.status
YamlFhirPathTest	testSearchParams	503	passed				ExplanationOfBenefit.careTeam.provider
YamlFhirPathTest	testSearchParams	504	passed				ExplanationOfBenefit.claim
YamlFhirPathTest	testSearchParams	505	passed				ExplanationOfBenefit.insurance.coverage
YamlFhirPathTest	testSearchParams	506	passed				ExplanationOfBenefit.created
YamlFhirPathTest	testSearchParams	507	passed				ExplanationOfBenefit.item.detail.udi
YamlFhirPathTest	testSearchParams	508	passed				ExplanationOfBenefit.disposition
YamlFhirPathTest	testSearchParams	509	passed				ExplanationOfBenefit.enterer
YamlFhirPathTest	testSearchParams	510	passed				ExplanationOfBenefit.facility
YamlFhirPathTest	testSearchParams	511	passed				ExplanationOfBenefit.item.udi
YamlFhirPathTest	testSearchParams	512	passed				ExplanationOfBenefit.payee.party
YamlFhirPathTest	testSearchParams	513	passed				ExplanationOfBenefit.procedure.udi
YamlFhirPathTest	testSearchParams	514	passed				ExplanationOfBenefit.provider
YamlFhirPathTest	testSearchParams	515	passed				ExplanationOfBenefit.status
YamlFhirPathTest	testSearchParams	516	passed				ExplanationOfBenefit.item.detail.subDetail.udi
YamlFhirPathTest	testSearchParams	517	passed				FamilyMemberHistory.instantiatesCanonical
YamlFhirPathTest	testSearchParams	518	passed				FamilyMemberHistory.instantiatesUri
YamlFhirPathTest	testSearchParams	519	passed				FamilyMemberHistory.relationship
YamlFhirPathTest	testSearchParams	520	passed				FamilyMemberHistory.sex
YamlFhirPathTest	testSearchParams	521	passed				FamilyMemberHistory.status
YamlFhirPathTest	testSearchParams	522	passed				Flag.author
YamlFhirPathTest	testSearchParams	523	passed				Flag.category
YamlFhirPathTest	testSearchParams	524	passed				Flag.status
YamlFhirPathTest	testSearchParams	525	passed				Flag.subject
YamlFhirPathTest	testSearchParams	526	passed				FormularyItem.code
YamlFhirPathTest	testSearchParams	527	passed				FormularyItem.identifier
YamlFhirPathTest	testSearchParams	528	passed				GenomicStudy.analysis.focus
YamlFhirPathTest	testSearchParams	529	passed				GenomicStudy.identifier
YamlFhirPathTest	testSearchParams	530	passed				GenomicStudy.subject.where(resolve() is Patient)
YamlFhirPathTest	testSearchParams	531	passed				GenomicStudy.status
YamlFhirPathTest	testSearchParams	532	passed				GenomicStudy.subject
YamlFhirPathTest	testSearchParams	533	passed				Goal.achievementStatus
YamlFhirPathTest	testSearchParams	534	passed				Goal.addresses
YamlFhirPathTest	testSearchParams	535	passed				Goal.category
YamlFhirPathTest	testSearchParams	536	passed				Goal.description
YamlFhirPathTest	testSearchParams	537	passed				Goal.lifecycleStatus
YamlFhirPathTest	testSearchParams	538	passed				(Goal.start.ofType(date))
YamlFhirPathTest	testSearchParams	539	passed				Goal.subject
YamlFhirPathTest	testSearchParams	540	passed				(Goal.target.due.ofType(date))
YamlFhirPathTest	testSearchParams	541	passed				Goal.target.measure
YamlFhirPathTest	testSearchParams	542	passed				GraphDefinition.start
YamlFhirPathTest	testSearchParams	543	passed				Group.characteristic.code
YamlFhirPathTest	testSearchParams	544	passed				(Group.characteristic.value.ofType(Reference))
YamlFhirPathTest	testSearchParams	545	passed				Group.code
YamlFhirPathTest	testSearchParams	546	passed				Group.characteristic.exclude
YamlFhirPathTest	testSearchParams	547	passed				Group.identifier
YamlFhirPathTest	testSearchParams	548	passed				Group.managingEntity
YamlFhirPathTest	testSearchParams	549	passed				Group.member.entity
YamlFhirPathTest	testSearchParams	550	passed				Group.membership
YamlFhirPathTest	testSearchParams	551	passed				Group.name
YamlFhirPathTest	testSearchParams	552	passed				Group.type
YamlFhirPathTest	testSearchParams	553	passed				(Group.characteristic.value.ofType(CodeableConcept)) | (Group.characteristic.value.ofType(boolean))
YamlFhirPathTest	testSearchParams	554	passed				Group.characteristic
YamlFhirPathTest	testSearchParams	555	passed				GuidanceResponse.requestIdentifier
YamlFhirPathTest	testSearchParams	556	passed				GuidanceResponse.status
YamlFhirPathTest	testSearchParams	557	passed				GuidanceResponse.subject
YamlFhirPathTest	testSearchParams	558	passed				HealthcareService.active
YamlFhirPathTest	testSearchParams	559	passed				HealthcareService.characteristic
YamlFhirPathTest	testSearchParams	560	passed				HealthcareService.communication
YamlFhirPathTest	testSearchParams	561	passed				HealthcareService.coverageArea
YamlFhirPathTest	testSearchParams	562	passed				HealthcareService.eligibility.code
YamlFhirPathTest	testSearchParams	563	passed				HealthcareService.endpoint
YamlFhirPathTest	testSearchParams	564	passed				HealthcareService.identifier
YamlFhirPathTest	testSearchParams	565	passed				HealthcareService.location
YamlFhirPathTest	testSearchParams	566	passed				HealthcareService.name
YamlFhirPathTest	testSearchParams	567	passed				HealthcareService.offeredIn
YamlFhirPathTest	testSearchParams	568	passed				HealthcareService.providedBy
YamlFhirPathTest	testSearchParams	569	passed				HealthcareService.program
YamlFhirPathTest	testSearchParams	570	passed				HealthcareService.category
YamlFhirPathTest	testSearchParams	571	passed				HealthcareService.type
YamlFhirPathTest	testSearchParams	572	passed				HealthcareService.specialty
YamlFhirPathTest	testSearchParams	573	passed				ImagingSelection.basedOn
YamlFhirPathTest	testSearchParams	574	passed				ImagingSelection.bodySite.concept
YamlFhirPathTest	testSearchParams	575	passed				ImagingSelection.bodySite.reference
YamlFhirPathTest	testSearchParams	576	passed				ImagingSelection.derivedFrom
YamlFhirPathTest	testSearchParams	577	passed				ImagingSelection.issued
YamlFhirPathTest	testSearchParams	578	passed				ImagingSelection.status
YamlFhirPathTest	testSearchParams	579	passed				ImagingSelection.studyUid
YamlFhirPathTest	testSearchParams	580	passed				ImagingSelection.subject
YamlFhirPathTest	testSearchParams	581	passed				ImagingStudy.basedOn
YamlFhirPathTest	testSearchParams	582	passed				ImagingStudy.series.bodySite.concept
YamlFhirPathTest	testSearchParams	583	passed				ImagingStudy.series.bodySite.reference
YamlFhirPathTest	testSearchParams	584	passed				ImagingStudy.series.instance.sopClass
YamlFhirPathTest	testSearchParams	585	passed				ImagingStudy.endpoint | ImagingStudy.series.endpoint
YamlFhirPathTest	testSearchParams	586	passed				ImagingStudy.series.instance.uid
YamlFhirPathTest	testSearchParams	587	passed				ImagingStudy.series.modality
YamlFhirPathTest	testSearchParams	588	passed				ImagingStudy.series.performer.actor
YamlFhirPathTest	testSearchParams	589	passed				ImagingStudy.referrer
YamlFhirPathTest	testSearchParams	590	passed				ImagingStudy.series.uid
YamlFhirPathTest	testSearchParams	591	passed				ImagingStudy.started
YamlFhirPathTest	testSearchParams	592	passed				ImagingStudy.status
YamlFhirPathTest	testSearchParams	593	passed				ImagingStudy.subject
YamlFhirPathTest	testSearchParams	594	passed				Immunization.location
YamlFhirPathTest	testSearchParams	595	passed				Immunization.lotNumber
YamlFhirPathTest	testSearchParams	596	passed				Immunization.manufacturer.reference
YamlFhirPathTest	testSearchParams	597	passed				Immunization.performer.actor
YamlFhirPathTest	testSearchParams	598	passed				Immunization.reaction.manifestation.reference
YamlFhirPathTest	testSearchParams	599	passed				Immunization.reaction.date
YamlFhirPathTest	testSearchParams	600	passed				Immunization.reason.concept
YamlFhirPathTest	testSearchParams	601	passed				Immunization.reason.reference
YamlFhirPathTest	testSearchParams	602	passed				Immunization.protocolApplied.series
YamlFhirPathTest	testSearchParams	603	passed				Immunization.status
YamlFhirPathTest	testSearchParams	604	passed				Immunization.statusReason
YamlFhirPathTest	testSearchParams	605	passed				Immunization.protocolApplied.targetDisease
YamlFhirPathTest	testSearchParams	606	passed				Immunization.vaccineCode
YamlFhirPathTest	testSearchParams	607	passed				ImmunizationEvaluation.doseStatus
YamlFhirPathTest	testSearchParams	608	passed				ImmunizationEvaluation.immunizationEvent
YamlFhirPathTest	testSearchParams	609	passed				ImmunizationEvaluation.status
YamlFhirPathTest	testSearchParams	610	passed				ImmunizationEvaluation.targetDisease
YamlFhirPathTest	testSearchParams	611	passed				ImmunizationRecommendation.recommendation.supportingPatientInformation
YamlFhirPathTest	testSearchParams	612	passed				ImmunizationRecommendation.recommendation.forecastStatus
YamlFhirPathTest	testSearchParams	613	passed				ImmunizationRecommendation.recommendation.supportingImmunization
YamlFhirPathTest	testSearchParams	614	passed				ImmunizationRecommendation.recommendation.targetDisease
YamlFhirPathTest	testSearchParams	615	passed				ImmunizationRecommendation.recommendation.vaccineCode
YamlFhirPathTest	testSearchParams	616	passed				ImplementationGuide.dependsOn.uri
YamlFhirPathTest	testSearchParams	617	passed				ImplementationGuide.experimental
YamlFhirPathTest	testSearchParams	618	passed				ImplementationGuide.global.profile
YamlFhirPathTest	testSearchParams	619	passed				ImplementationGuide.definition.resource.reference
YamlFhirPathTest	testSearchParams	620	passed				Ingredient.for
YamlFhirPathTest	testSearchParams	621	passed				Ingredient.function
YamlFhirPathTest	testSearchParams	622	passed				Ingredient.identifier
YamlFhirPathTest	testSearchParams	623	passed				Ingredient.manufacturer.manufacturer
YamlFhirPathTest	testSearchParams	624	passed				Ingredient.role
YamlFhirPathTest	testSearchParams	625	passed				Ingredient.status
YamlFhirPathTest	testSearchParams	626	passed				Ingredient.substance.strength.concentration.ofType(Quantity)
YamlFhirPathTest	testSearchParams	627	passed				Ingredient.substance.strength.presentation.ofType(Quantity)
YamlFhirPathTest	testSearchParams	628	passed				Ingredient.substance.code.reference
YamlFhirPathTest	testSearchParams	629	passed				Ingredient.substance.code.concept
YamlFhirPathTest	testSearchParams	630	passed				Ingredient.substance.code.reference
YamlFhirPathTest	testSearchParams	631	passed				Ingredient.substance.strength.concentration.ofType(Ratio)
YamlFhirPathTest	testSearchParams	632	passed				Ingredient.substance.strength.presentation.ofType(Ratio)
YamlFhirPathTest	testSearchParams	633	passed				InsurancePlan.contact.address
YamlFhirPathTest	testSearchParams	634	passed				InsurancePlan.contact.address.city
YamlFhirPathTest	testSearchParams	635	passed				InsurancePlan.contact.address.country
YamlFhirPathTest	testSearchParams	636	passed				InsurancePlan.contact.address.postalCode
YamlFhirPathTest	testSearchParams	637	passed				InsurancePlan.contact.address.state
YamlFhirPathTest	testSearchParams	638	passed				InsurancePlan.contact.address.use
YamlFhirPathTest	testSearchParams	639	passed				InsurancePlan.administeredBy
YamlFhirPathTest	testSearchParams	640	passed				InsurancePlan.endpoint
YamlFhirPathTest	testSearchParams	641	passed				InsurancePlan.identifier
YamlFhirPathTest	testSearchParams	642	passed				InsurancePlan.name | InsurancePlan.alias
YamlFhirPathTest	testSearchParams	643	passed				InsurancePlan.ownedBy
YamlFhirPathTest	testSearchParams	644	passed				InsurancePlan.name
YamlFhirPathTest	testSearchParams	645	passed				InsurancePlan.status
YamlFhirPathTest	testSearchParams	646	passed				InsurancePlan.type
YamlFhirPathTest	testSearchParams	647	passed				InventoryItem.code
YamlFhirPathTest	testSearchParams	648	passed				InventoryItem.identifier
YamlFhirPathTest	testSearchParams	649	passed				InventoryItem.status
YamlFhirPathTest	testSearchParams	650	passed				InventoryItem.instance.subject
YamlFhirPathTest	testSearchParams	651	passed				InventoryReport.identifier
YamlFhirPathTest	testSearchParams	652	passed				InventoryReport.inventoryListing.item.item.concept
YamlFhirPathTest	testSearchParams	653	passed				InventoryReport.inventoryListing.item.item.reference
YamlFhirPathTest	testSearchParams	654	passed				InventoryReport.status
YamlFhirPathTest	testSearchParams	655	passed				Invoice.account
YamlFhirPathTest	testSearchParams	656	passed				Invoice.issuer
YamlFhirPathTest	testSearchParams	657	passed				Invoice.participant.actor
YamlFhirPathTest	testSearchParams	658	passed				Invoice.participant.role
YamlFhirPathTest	testSearchParams	659	passed				Invoice.recipient
YamlFhirPathTest	testSearchParams	660	passed				Invoice.status
YamlFhirPathTest	testSearchParams	661	passed				Invoice.subject
YamlFhirPathTest	testSearchParams	662	passed				Invoice.totalGross
YamlFhirPathTest	testSearchParams	663	passed				Invoice.totalNet
YamlFhirPathTest	testSearchParams	664	passed				Library.content.contentType
YamlFhirPathTest	testSearchParams	665	passed				Library.type
YamlFhirPathTest	testSearchParams	666	passed				Linkage.author
YamlFhirPathTest	testSearchParams	667	passed				Linkage.item.resource
YamlFhirPathTest	testSearchParams	668	passed				Linkage.item.resource
YamlFhirPathTest	testSearchParams	669	passed				List.emptyReason
YamlFhirPathTest	testSearchParams	670	passed				List.entry.item
YamlFhirPathTest	testSearchParams	671	passed				List.note.text
YamlFhirPathTest	testSearchParams	672	passed				List.source
YamlFhirPathTest	testSearchParams	673	passed				List.status
YamlFhirPathTest	testSearchParams	674	passed				List.subject
YamlFhirPathTest	testSearchParams	675	passed				List.title
YamlFhirPathTest	testSearchParams	676	passed				Location.address
YamlFhirPathTest	testSearchParams	677	passed				Location.address.city
YamlFhirPathTest	testSearchParams	678	passed				Location.address.country
YamlFhirPathTest	testSearchParams	679	passed				Location.address.postalCode
YamlFhirPathTest	testSearchParams	680	passed				Location.address.state
YamlFhirPathTest	testSearchParams	681	passed				Location.address.use
YamlFhirPathTest	testSearchParams	682	passed				Location.characteristic
YamlFhirPathTest	testSearchParams	683	passed				Location.extension('http://hl7.org/fhir/StructureDefinition/location-boundary-geojson').value
YamlFhirPathTest	testSearchParams	684	passed				Location.endpoint
YamlFhirPathTest	testSearchParams	685	passed				Location.identifier
YamlFhirPathTest	testSearchParams	686	passed				Location.name | Location.alias
YamlFhirPathTest	testSearchParams	687	passed				Location.position
YamlFhirPathTest	testSearchParams	688	passed				Location.operationalStatus
YamlFhirPathTest	testSearchParams	689	passed				Location.managingOrganization
YamlFhirPathTest	testSearchParams	690	passed				Location.partOf
YamlFhirPathTest	testSearchParams	691	passed				Location.status
YamlFhirPathTest	testSearchParams	692	passed				Location.type
YamlFhirPathTest	testSearchParams	693	passed				ManufacturedItemDefinition.manufacturedDoseForm
YamlFhirPathTest	testSearchParams	694	passed				ManufacturedItemDefinition.identifier
YamlFhirPathTest	testSearchParams	695	passed				ManufacturedItemDefinition.ingredient
YamlFhirPathTest	testSearchParams	696	passed				ManufacturedItemDefinition.name
YamlFhirPathTest	testSearchParams	697	passed				ManufacturedItemDefinition.status
YamlFhirPathTest	testSearchParams	698	passed				MeasureReport.evaluatedResource
YamlFhirPathTest	testSearchParams	699	passed				MeasureReport.location
YamlFhirPathTest	testSearchParams	700	passed				MeasureReport.measure
YamlFhirPathTest	testSearchParams	701	passed				MeasureReport.period
YamlFhirPathTest	testSearchParams	702	passed				MeasureReport.reporter
YamlFhirPathTest	testSearchParams	703	passed				MeasureReport.status
YamlFhirPathTest	testSearchParams	704	passed				MeasureReport.subject
YamlFhirPathTest	testSearchParams	705	passed				Medication.batch.expirationDate
YamlFhirPathTest	testSearchParams	706	passed				Medication.ingredient.item.reference
YamlFhirPathTest	testSearchParams	707	passed				Medication.ingredient.item.concept
YamlFhirPathTest	testSearchParams	708	passed				Medication.batch.lotNumber
YamlFhirPathTest	testSearchParams	709	passed				Medication.marketingAuthorizationHolder
YamlFhirPathTest	testSearchParams	710	passed				Medication.identifier
YamlFhirPathTest	testSearchParams	711	passed				Medication.status
YamlFhirPathTest	testSearchParams	712	passed				MedicationAdministration.occurence.ofType(dateTime) | MedicationAdministration.occurence.ofType(Period)
YamlFhirPathTest	testSearchParams	713	passed				MedicationAdministration.device.reference
YamlFhirPathTest	testSearchParams	714	passed				MedicationAdministration.encounter | MedicationRequest.encounter
YamlFhirPathTest	testSearchParams	715	passed				MedicationAdministration.medication.reference | MedicationDispense.medication.reference | MedicationRequest.medication.reference | MedicationStatement.medication.reference
YamlFhirPathTest	testSearchParams	716	passed				MedicationAdministration.performer.actor.reference
YamlFhirPathTest	testSearchParams	717	passed				MedicationAdministration.performer.actor.concept
YamlFhirPathTest	testSearchParams	718	passed				MedicationAdministration.reason.reference
YamlFhirPathTest	testSearchParams	719	passed				MedicationAdministration.reason.concept
YamlFhirPathTest	testSearchParams	720	passed				MedicationAdministration.statusReason
YamlFhirPathTest	testSearchParams	721	passed				MedicationAdministration.request
YamlFhirPathTest	testSearchParams	722	passed				MedicationAdministration.status | MedicationDispense.status | MedicationRequest.status | MedicationStatement.status
YamlFhirPathTest	testSearchParams	723	passed				MedicationAdministration.subject
YamlFhirPathTest	testSearchParams	724	passed				MedicationDispense.destination
YamlFhirPathTest	testSearchParams	725	passed				MedicationDispense.location
YamlFhirPathTest	testSearchParams	726	passed				MedicationDispense.performer.actor
YamlFhirPathTest	testSearchParams	727	passed				MedicationDispense.authorizingPrescription
YamlFhirPathTest	testSearchParams	728	passed				MedicationDispense.receiver
YamlFhirPathTest	testSearchParams	729	passed				MedicationDispense.recorded
YamlFhirPathTest	testSearchParams	730	passed				MedicationDispense.substitution.responsibleParty
YamlFhirPathTest	testSearchParams	731	passed				MedicationDispense.subject
YamlFhirPathTest	testSearchParams	732	passed				MedicationDispense.whenHandedOver
YamlFhirPathTest	testSearchParams	733	passed				MedicationDispense.whenPrepared
YamlFhirPathTest	testSearchParams	734	passed				MedicationKnowledge.medicineClassification.classification
YamlFhirPathTest	testSearchParams	735	passed				MedicationKnowledge.medicineClassification.type
YamlFhirPathTest	testSearchParams	736	passed				MedicationKnowledge.code
YamlFhirPathTest	testSearchParams	737	passed				MedicationKnowledge.definitional.doseForm
YamlFhirPathTest	testSearchParams	738	passed				MedicationKnowledge.definitional.ingredient.item.reference
YamlFhirPathTest	testSearchParams	739	passed				MedicationKnowledge.definitional.ingredient.item.concept
YamlFhirPathTest	testSearchParams	740	passed				MedicationKnowledge.monitoringProgram.name
YamlFhirPathTest	testSearchParams	741	passed				MedicationKnowledge.monitoringProgram.type
YamlFhirPathTest	testSearchParams	742	passed				MedicationKnowledge.monograph.source
YamlFhirPathTest	testSearchParams	743	passed				MedicationKnowledge.monograph.type
YamlFhirPathTest	testSearchParams	744	passed				MedicationKnowledge.productType
YamlFhirPathTest	testSearchParams	745	passed				MedicationKnowledge.cost.source
YamlFhirPathTest	testSearchParams	746	passed				MedicationRequest.authoredOn
YamlFhirPathTest	testSearchParams	747	passed				MedicationRequest.category
YamlFhirPathTest	testSearchParams	748	passed				MedicationRequest.dosageInstruction.timing.event | (MedicationRequest.dosageInstruction.timing.repeat.bounds.ofType(Period))
YamlFhirPathTest	testSearchParams	749	passed				MedicationRequest.groupIdentifier
YamlFhirPathTest	testSearchParams	750	passed				MedicationRequest.dispenseRequest.dispenser
YamlFhirPathTest	testSearchParams	751	passed				MedicationRequest.performer
YamlFhirPathTest	testSearchParams	752	passed				MedicationRequest.performerType
YamlFhirPathTest	testSearchParams	753	passed				MedicationRequest.intent
YamlFhirPathTest	testSearchParams	754	passed				MedicationRequest.priority
YamlFhirPathTest	testSearchParams	755	passed				MedicationRequest.requester
YamlFhirPathTest	testSearchParams	756	passed				MedicationRequest.subject
YamlFhirPathTest	testSearchParams	757	passed				MedicationStatement.adherence.code
YamlFhirPathTest	testSearchParams	758	passed				MedicationStatement.category
YamlFhirPathTest	testSearchParams	759	passed				MedicationStatement.effective.ofType(dateTime) | MedicationStatement.effective.ofType(Period)
YamlFhirPathTest	testSearchParams	760	passed				MedicationStatement.informationSource
YamlFhirPathTest	testSearchParams	761	passed				MedicationStatement.subject
YamlFhirPathTest	testSearchParams	762	passed				MedicinalProductDefinition.characteristic.value.ofType(Quantity) | MedicinalProductDefinition.characteristic.value.ofType(CodeableConcept)
YamlFhirPathTest	testSearchParams	763	passed				MedicinalProductDefinition.characteristic.type
YamlFhirPathTest	testSearchParams	764	passed				MedicinalProductDefinition.contact.contact
YamlFhirPathTest	testSearchParams	765	passed				MedicinalProductDefinition.domain
YamlFhirPathTest	testSearchParams	766	passed				MedicinalProductDefinition.identifier
YamlFhirPathTest	testSearchParams	767	passed				MedicinalProductDefinition.ingredient
YamlFhirPathTest	testSearchParams	768	passed				MedicinalProductDefinition.masterFile
YamlFhirPathTest	testSearchParams	769	passed				MedicinalProductDefinition.name.productName
YamlFhirPathTest	testSearchParams	770	passed				MedicinalProductDefinition.name.usage.language
YamlFhirPathTest	testSearchParams	771	passed				MedicinalProductDefinition.classification
YamlFhirPathTest	testSearchParams	772	passed				MedicinalProductDefinition.status
YamlFhirPathTest	testSearchParams	773	passed				MedicinalProductDefinition.type
YamlFhirPathTest	testSearchParams	774	passed				MessageDefinition.category
YamlFhirPathTest	testSearchParams	775	passed				MessageDefinition.event.ofType(Coding)
YamlFhirPathTest	testSearchParams	776	passed				MessageDefinition.focus.code
YamlFhirPathTest	testSearchParams	777	passed				MessageDefinition.parent
YamlFhirPathTest	testSearchParams	778	passed				MessageHeader.author
YamlFhirPathTest	testSearchParams	779	passed				MessageHeader.response.code
YamlFhirPathTest	testSearchParams	780	passed				MessageHeader.destination.name
YamlFhirPathTest	testSearchParams	781	passed				MessageHeader.event.ofType(Coding) | MessageHeader.event.ofType(canonical)
YamlFhirPathTest	testSearchParams	782	passed				MessageHeader.focus
YamlFhirPathTest	testSearchParams	783	passed				MessageHeader.destination.receiver
YamlFhirPathTest	testSearchParams	784	passed				MessageHeader.response.identifier
YamlFhirPathTest	testSearchParams	785	passed				MessageHeader.responsible
YamlFhirPathTest	testSearchParams	786	passed				MessageHeader.sender
YamlFhirPathTest	testSearchParams	787	passed				MessageHeader.source.name
YamlFhirPathTest	testSearchParams	788	passed				MessageHeader.destination.target
YamlFhirPathTest	testSearchParams	789	passed				MolecularSequence.focus
YamlFhirPathTest	testSearchParams	790	passed				MolecularSequence.subject
YamlFhirPathTest	testSearchParams	791	passed				NamingSystem.contact.name
YamlFhirPathTest	testSearchParams	792	passed				NamingSystem.uniqueId.type
YamlFhirPathTest	testSearchParams	793	passed				NamingSystem.kind
YamlFhirPathTest	testSearchParams	794	passed				NamingSystem.uniqueId.period
YamlFhirPathTest	testSearchParams	795	passed				NamingSystem.responsible
YamlFhirPathTest	testSearchParams	796	passed				NamingSystem.contact.telecom
YamlFhirPathTest	testSearchParams	797	passed				NamingSystem.type
YamlFhirPathTest	testSearchParams	798	passed				NamingSystem.uniqueId.value
YamlFhirPathTest	testSearchParams	799	passed				NutritionIntake.consumedItem.nutritionProduct.concept
YamlFhirPathTest	testSearchParams	800	passed				(NutritionIntake.reported as Reference)
YamlFhirPathTest	testSearchParams	801	passed				NutritionIntake.status
YamlFhirPathTest	testSearchParams	802	passed				NutritionIntake.subject
YamlFhirPathTest	testSearchParams	803	passed				NutritionOrder.enteralFormula.additive.type.concept
YamlFhirPathTest	testSearchParams	804	passed				NutritionOrder.dateTime
YamlFhirPathTest	testSearchParams	805	passed				NutritionOrder.enteralFormula.baseFormulaType.concept
YamlFhirPathTest	testSearchParams	806	passed				NutritionOrder.groupIdentifier
YamlFhirPathTest	testSearchParams	807	passed				NutritionOrder.oralDiet.type
YamlFhirPathTest	testSearchParams	808	passed				NutritionOrder.orderer
YamlFhirPathTest	testSearchParams	809	passed				NutritionOrder.status
YamlFhirPathTest	testSearchParams	810	passed				NutritionOrder.subject
YamlFhirPathTest	testSearchParams	811	passed				NutritionOrder.supplement.type.concept
YamlFhirPathTest	testSearchParams	812	passed				NutritionProduct.code
YamlFhirPathTest	testSearchParams	813	passed				NutritionProduct.instance.identifier
YamlFhirPathTest	testSearchParams	814	passed				NutritionProduct.instance.lotNumber
YamlFhirPathTest	testSearchParams	815	passed				NutritionProduct.instance.identifier
YamlFhirPathTest	testSearchParams	816	passed				NutritionProduct.category
YamlFhirPathTest	testSearchParams	817	passed				Observation.basedOn
YamlFhirPathTest	testSearchParams	818	passed				Observation.category
YamlFhirPathTest	testSearchParams	819	passed				Observation.code | Observation.component.code
YamlFhirPathTest	testSearchParams	820	passed				Observation.dataAbsentReason | Observation.component.dataAbsentReason
YamlFhirPathTest	testSearchParams	821	passed				Observation.value.ofType(CodeableConcept) | Observation.component.value.ofType(CodeableConcept)
YamlFhirPathTest	testSearchParams	822	passed				Observation.value.ofType(Quantity) | Observation.value.ofType(SampledData) | Observation.component.value.ofType(Quantity) | Observation.component.value.ofType(SampledData)
YamlFhirPathTest	testSearchParams	823	passed				Observation.component.code
YamlFhirPathTest	testSearchParams	824	passed				Observation.component.dataAbsentReason
YamlFhirPathTest	testSearchParams	825	passed				Observation.component.value.ofType(canonical)
YamlFhirPathTest	testSearchParams	826	passed				Observation.component.value.ofType(CodeableConcept)
YamlFhirPathTest	testSearchParams	827	passed				Observation.component.value.ofType(Quantity) | Observation.component.value.ofType(SampledData)
YamlFhirPathTest	testSearchParams	828	passed				Observation.component.value.ofType(Reference)
YamlFhirPathTest	testSearchParams	829	passed				Observation.dataAbsentReason
YamlFhirPathTest	testSearchParams	830	passed				Observation.derivedFrom
YamlFhirPathTest	testSearchParams	831	passed				Observation.device
YamlFhirPathTest	testSearchParams	832	passed				Observation.focus
YamlFhirPathTest	testSearchParams	833	passed				Observation.hasMember
YamlFhirPathTest	testSearchParams	834	passed				Observation.method
YamlFhirPathTest	testSearchParams	835	passed				Observation.partOf
YamlFhirPathTest	testSearchParams	836	passed				Observation.performer
YamlFhirPathTest	testSearchParams	837	passed				Observation.specimen
YamlFhirPathTest	testSearchParams	838	passed				Observation.status
YamlFhirPathTest	testSearchParams	839	passed				Observation.subject
YamlFhirPathTest	testSearchParams	840	passed				Observation.value.ofType(canonical)
YamlFhirPathTest	testSearchParams	841	passed				Observation.value.ofType(CodeableConcept)
YamlFhirPathTest	testSearchParams	842	passed				Observation.value.ofType(dateTime) | Observation.value.ofType(Period)
YamlFhirPathTest	testSearchParams	843	passed				Observation.value.ofType(markdown) | Observation.value.ofType(CodeableConcept).text
YamlFhirPathTest	testSearchParams	844	passed				Observation.value.ofType(Quantity) | Observation.value.ofType(SampledData)
YamlFhirPathTest	testSearchParams	845	passed				Observation.value.ofType(Reference)
YamlFhirPathTest	testSearchParams	846	passed				Observation
YamlFhirPathTest	testSearchParams	847	passed				Observation
YamlFhirPathTest	testSearchParams	848	passed				Observation
YamlFhirPathTest	testSearchParams	849	passed				Observation
YamlFhirPathTest	testSearchParams	850	passed				Observation | Observation.component
YamlFhirPathTest	testSearchParams	851	passed				Observation | Observation.component
YamlFhirPathTest	testSearchParams	852	passed				Observation.component
YamlFhirPathTest	testSearchParams	853	passed				Observation.component
YamlFhirPathTest	testSearchParams	854	passed				ObservationDefinition.category
YamlFhirPathTest	testSearchParams	855	passed				ObservationDefinition.code
YamlFhirPathTest	testSearchParams	856	passed				ObservationDefinition.experimental
YamlFhirPathTest	testSearchParams	857	passed				ObservationDefinition.method
YamlFhirPathTest	testSearchParams	858	passed				OperationDefinition.base
YamlFhirPathTest	testSearchParams	859	passed				OperationDefinition.code
YamlFhirPathTest	testSearchParams	860	passed				OperationDefinition.inputProfile
YamlFhirPathTest	testSearchParams	861	passed				OperationDefinition.instance
YamlFhirPathTest	testSearchParams	862	passed				OperationDefinition.kind
YamlFhirPathTest	testSearchParams	863	passed				OperationDefinition.outputProfile
YamlFhirPathTest	testSearchParams	864	passed				OperationDefinition.system
YamlFhirPathTest	testSearchParams	865	passed				OperationDefinition.type
YamlFhirPathTest	testSearchParams	866	passed				Organization.active
YamlFhirPathTest	testSearchParams	867	passed				Organization.contact.address
YamlFhirPathTest	testSearchParams	868	passed				Organization.contact.address.city
YamlFhirPathTest	testSearchParams	869	passed				Organization.contact.address.country
YamlFhirPathTest	testSearchParams	870	passed				Organization.contact.address.postalCode
YamlFhirPathTest	testSearchParams	871	passed				Organization.contact.address.state
YamlFhirPathTest	testSearchParams	872	passed				Organization.contact.address.use
YamlFhirPathTest	testSearchParams	873	passed				Organization.endpoint
YamlFhirPathTest	testSearchParams	874	passed				Organization.identifier | Organization.qualification.identifier
YamlFhirPathTest	testSearchParams	875	passed				Organization.name | Organization.alias
YamlFhirPathTest	testSearchParams	876	passed				Organization.partOf
YamlFhirPathTest	testSearchParams	877	passed				Organization.name
YamlFhirPathTest	testSearchParams	878	passed				Organization.type
YamlFhirPathTest	testSearchParams	879	passed				OrganizationAffiliation.active
YamlFhirPathTest	testSearchParams	880	passed				OrganizationAffiliation.period
YamlFhirPathTest	testSearchParams	881	passed				OrganizationAffiliation.contact.telecom.where(system='email')
YamlFhirPathTest	testSearchParams	882	passed				OrganizationAffiliation.endpoint
YamlFhirPathTest	testSearchParams	883	passed				OrganizationAffiliation.identifier
YamlFhirPathTest	testSearchParams	884	passed				OrganizationAffiliation.location
YamlFhirPathTest	testSearchParams	885	passed				OrganizationAffiliation.network
YamlFhirPathTest	testSearchParams	886	passed				OrganizationAffiliation.participatingOrganization
YamlFhirPathTest	testSearchParams	887	passed				OrganizationAffiliation.contact.telecom.where(system='phone')
YamlFhirPathTest	testSearchParams	888	passed				OrganizationAffiliation.organization
YamlFhirPathTest	testSearchParams	889	passed				OrganizationAffiliation.code
YamlFhirPathTest	testSearchParams	890	passed				OrganizationAffiliation.healthcareService
YamlFhirPathTest	testSearchParams	891	passed				OrganizationAffiliation.specialty
YamlFhirPathTest	testSearchParams	892	passed				OrganizationAffiliation.contact.telecom
YamlFhirPathTest	testSearchParams	893	passed				PackagedProductDefinition.packaging.containedItem.item.reference
YamlFhirPathTest	testSearchParams	894	passed				PackagedProductDefinition.packaging.containedItem.item.reference
YamlFhirPathTest	testSearchParams	895	passed				PackagedProductDefinition.packaging.containedItem.item.reference
YamlFhirPathTest	testSearchParams	896	passed				PackagedProductDefinition.identifier
YamlFhirPathTest	testSearchParams	897	passed				PackagedProductDefinition.packaging.containedItem.item.reference
YamlFhirPathTest	testSearchParams	898	passed				PackagedProductDefinition.packaging.containedItem.item.reference
YamlFhirPathTest	testSearchParams	899	passed				PackagedProductDefinition.name
YamlFhirPathTest	testSearchParams	900	passed				PackagedProductDefinition.packaging.containedItem.item.reference
YamlFhirPathTest	testSearchParams	901	passed				PackagedProductDefinition.packaging.containedItem.item.reference
YamlFhirPathTest	testSearchParams	902	passed				PackagedProductDefinition.packageFor
YamlFhirPathTest	testSearchParams	903	passed				PackagedProductDefinition.status
YamlFhirPathTest	testSearchParams	904	passed				Patient.active
YamlFhirPathTest	testSearchParams	905	passed				Patient.address | Person.address | Practitioner.address | RelatedPerson.address
YamlFhirPathTest	testSearchParams	906	passed				Patient.address.city | Person.address.city | Practitioner.address.city | RelatedPerson.address.city
YamlFhirPathTest	testSearchParams	907	passed				Patient.address.country | Person.address.country | Practitioner.address.country | RelatedPerson.address.country
YamlFhirPathTest	testSearchParams	908	passed				Patient.address.postalCode | Person.address.postalCode | Practitioner.address.postalCode | RelatedPerson.address.postalCode
YamlFhirPathTest	testSearchParams	909	passed				Patient.address.state | Person.address.state | Practitioner.address.state | RelatedPerson.address.state
YamlFhirPathTest	testSearchParams	910	passed				Patient.address.use | Person.address.use | Practitioner.address.use | RelatedPerson.address.use
YamlFhirPathTest	testSearchParams	911	passed				Patient.birthDate | Person.birthDate | RelatedPerson.birthDate
YamlFhirPathTest	testSearchParams	912	passed				(Patient.deceased.ofType(dateTime))
YamlFhirPathTest	testSearchParams	913	passed				Patient.deceased.exists() and Patient.deceased != false
YamlFhirPathTest	testSearchParams	914	passed				Patient.telecom.where(system='email') | Person.telecom.where(system='email') | Practitioner.telecom.where(system='email') | PractitionerRole.contact.telecom.where(system='email') | RelatedPerson.telecom.where(system='email')
YamlFhirPathTest	testSearchParams	915	passed				Patient.name.family | Practitioner.name.family
YamlFhirPathTest	testSearchParams	916	passed				Patient.gender | Person.gender | Practitioner.gender | RelatedPerson.gender
YamlFhirPathTest	testSearchParams	917	passed				Patient.generalPractitioner
YamlFhirPathTest	testSearchParams	918	passed				Patient.name.given | Practitioner.name.given
YamlFhirPathTest	testSearchParams	919	passed				Patient.identifier
YamlFhirPathTest	testSearchParams	920	passed				Patient.communication.language
YamlFhirPathTest	testSearchParams	921	passed				Patient.link.other
YamlFhirPathTest	testSearchParams	922	passed				Patient.name
YamlFhirPathTest	testSearchParams	923	passed				Patient.managingOrganization
YamlFhirPathTest	testSearchParams	924	passed				Patient.telecom.where(system='phone') | Person.telecom.where(system='phone') | Practitioner.telecom.where(system='phone') | PractitionerRole.contact.telecom.where(system='phone') | RelatedPerson.telecom.where(system='phone')
YamlFhirPathTest	testSearchParams	925	passed				Patient.name | Person.name | Practitioner.name | RelatedPerson.name
YamlFhirPathTest	testSearchParams	926	passed				Patient.telecom | Person.telecom | Practitioner.telecom | PractitionerRole.contact.telecom | RelatedPerson.telecom
YamlFhirPathTest	testSearchParams	927	passed				PaymentNotice.created
YamlFhirPathTest	testSearchParams	928	passed				PaymentNotice.identifier
YamlFhirPathTest	testSearchParams	929	passed				PaymentNotice.paymentStatus
YamlFhirPathTest	testSearchParams	930	passed				PaymentNotice.reporter
YamlFhirPathTest	testSearchParams	931	passed				PaymentNotice.request
YamlFhirPathTest	testSearchParams	932	passed				PaymentNotice.response
YamlFhirPathTest	testSearchParams	933	passed				PaymentNotice.status
YamlFhirPathTest	testSearchParams	934	passed				PaymentReconciliation.allocation.account
YamlFhirPathTest	testSearchParams	935	passed				PaymentReconciliation.allocation.encounter
YamlFhirPathTest	testSearchParams	936	passed				PaymentReconciliation.created
YamlFhirPathTest	testSearchParams	937	passed				PaymentReconciliation.disposition
YamlFhirPathTest	testSearchParams	938	passed				PaymentReconciliation.identifier
YamlFhirPathTest	testSearchParams	939	passed				PaymentReconciliation.outcome
YamlFhirPathTest	testSearchParams	940	passed				PaymentReconciliation.paymentIssuer
YamlFhirPathTest	testSearchParams	941	passed				PaymentReconciliation.request
YamlFhirPathTest	testSearchParams	942	passed				PaymentReconciliation.requestor
YamlFhirPathTest	testSearchParams	943	passed				PaymentReconciliation.status
YamlFhirPathTest	testSearchParams	944	passed				Permission.status
YamlFhirPathTest	testSearchParams	945	passed				(Person.deceased.ofType(dateTime))
YamlFhirPathTest	testSearchParams	946	passed				Person.deceased.exists() and Person.deceased != false
YamlFhirPathTest	testSearchParams	947	passed				Person.name.family
YamlFhirPathTest	testSearchParams	948	passed				Person.name.given
YamlFhirPathTest	testSearchParams	949	passed				Person.link.target
YamlFhirPathTest	testSearchParams	950	passed				Person.name
YamlFhirPathTest	testSearchParams	951	passed				Person.managingOrganization
YamlFhirPathTest	testSearchParams	952	passed				Person.link.target.where(resolve() is Practitioner)
YamlFhirPathTest	testSearchParams	953	passed				Person.link.target.where(resolve() is RelatedPerson)
YamlFhirPathTest	testSearchParams	954	passed				PlanDefinition.action.definition.ofType(canonical) | PlanDefinition.action.definition.ofType(uri)
YamlFhirPathTest	testSearchParams	955	passed				PlanDefinition.type
YamlFhirPathTest	testSearchParams	956	passed				Practitioner.active
YamlFhirPathTest	testSearchParams	957	passed				Practitioner.communication.language
YamlFhirPathTest	testSearchParams	958	passed				(Practitioner.deceased.ofType(dateTime))
YamlFhirPathTest	testSearchParams	959	passed				Practitioner.deceased.exists() and Practitioner.deceased != false
YamlFhirPathTest	testSearchParams	960	passed				Practitioner.identifier | Practitioner.qualification.identifier
YamlFhirPathTest	testSearchParams	961	passed				Practitioner.name
YamlFhirPathTest	testSearchParams	962	passed				Practitioner.qualification.period
YamlFhirPathTest	testSearchParams	963	passed				PractitionerRole.active
YamlFhirPathTest	testSearchParams	964	passed				PractitionerRole.characteristic
YamlFhirPathTest	testSearchParams	965	passed				PractitionerRole.communication
YamlFhirPathTest	testSearchParams	966	passed				PractitionerRole.period
YamlFhirPathTest	testSearchParams	967	passed				PractitionerRole.endpoint
YamlFhirPathTest	testSearchParams	968	passed				PractitionerRole.identifier
YamlFhirPathTest	testSearchParams	969	passed				PractitionerRole.location
YamlFhirPathTest	testSearchParams	970	passed				PractitionerRole.organization
YamlFhirPathTest	testSearchParams	971	passed				PractitionerRole.practitioner
YamlFhirPathTest	testSearchParams	972	passed				PractitionerRole.code
YamlFhirPathTest	testSearchParams	973	passed				PractitionerRole.healthcareService
YamlFhirPathTest	testSearchParams	974	passed				PractitionerRole.specialty
YamlFhirPathTest	testSearchParams	975	passed				Procedure.basedOn
YamlFhirPathTest	testSearchParams	976	passed				Procedure.category
YamlFhirPathTest	testSearchParams	977	passed				Procedure.instantiatesCanonical
YamlFhirPathTest	testSearchParams	978	passed				Procedure.instantiatesUri
YamlFhirPathTest	testSearchParams	979	passed				Procedure.location
YamlFhirPathTest	testSearchParams	980	passed				Procedure.partOf
YamlFhirPathTest	testSearchParams	981	passed				Procedure.performer.actor
YamlFhirPathTest	testSearchParams	982	passed				Procedure.reason.concept
YamlFhirPathTest	testSearchParams	983	passed				Procedure.reason.reference
YamlFhirPathTest	testSearchParams	984	passed				Procedure.report
YamlFhirPathTest	testSearchParams	985	passed				Procedure.status
YamlFhirPathTest	testSearchParams	986	passed				Procedure.subject
YamlFhirPathTest	testSearchParams	987	passed				Provenance.activity
YamlFhirPathTest	testSearchParams	988	passed				Provenance.agent.who
YamlFhirPathTest	testSearchParams	989	passed				Provenance.agent.role
YamlFhirPathTest	testSearchParams	990	passed				Provenance.agent.type
YamlFhirPathTest	testSearchParams	991	passed				Provenance.basedOn
YamlFhirPathTest	testSearchParams	992	passed				Provenance.entity.what
YamlFhirPathTest	testSearchParams	993	passed				Provenance.location
YamlFhirPathTest	testSearchParams	994	passed				Provenance.recorded
YamlFhirPathTest	testSearchParams	995	passed				Provenance.signature.type
YamlFhirPathTest	testSearchParams	996	passed				Provenance.target
YamlFhirPathTest	testSearchParams	997	passed				(Provenance.occurred.ofType(dateTime))
YamlFhirPathTest	testSearchParams	998	passed				Questionnaire.code | Questionnaire.item.code
YamlFhirPathTest	testSearchParams	999	passed				Questionnaire.item.definition
YamlFhirPathTest	testSearchParams	1000	passed				Questionnaire.item.code
YamlFhirPathTest	testSearchParams	1001	passed				Questionnaire.code
YamlFhirPathTest	testSearchParams	1002	passed				Questionnaire.subjectType
YamlFhirPathTest	testSearchParams	1003	passed				QuestionnaireResponse.author
YamlFhirPathTest	testSearchParams	1004	passed				QuestionnaireResponse.authored
YamlFhirPathTest	testSearchParams	1005	passed				QuestionnaireResponse.basedOn
YamlFhirPathTest	testSearchParams	1006	passed				QuestionnaireResponse.item.where(extension('http://hl7.org/fhir/StructureDefinition/questionnaireresponse-isSubject').exists()).answer.value.ofType(Reference)
YamlFhirPathTest	testSearchParams	1007	passed				QuestionnaireResponse.partOf
YamlFhirPathTest	testSearchParams	1008	passed				QuestionnaireResponse.questionnaire
YamlFhirPathTest	testSearchParams	1009	passed				QuestionnaireResponse.source
YamlFhirPathTest	testSearchParams	1010	passed				QuestionnaireResponse.status
YamlFhirPathTest	testSearchParams	1011	passed				QuestionnaireResponse.subject
YamlFhirPathTest	testSearchParams	1012	passed				RegulatedAuthorization.case.identifier
YamlFhirPathTest	testSearchParams	1013	passed				RegulatedAuthorization.case.type
YamlFhirPathTest	testSearchParams	1014	passed				RegulatedAuthorization.holder
YamlFhirPathTest	testSearchParams	1015	passed				RegulatedAuthorization.identifier
YamlFhirPathTest	testSearchParams	1016	passed				RegulatedAuthorization.region
YamlFhirPathTest	testSearchParams	1017	passed				RegulatedAuthorization.status
YamlFhirPathTest	testSearchParams	1018	passed				RegulatedAuthorization.subject
YamlFhirPathTest	testSearchParams	1019	passed				RelatedPerson.active
YamlFhirPathTest	testSearchParams	1020	passed				RelatedPerson.name.family
YamlFhirPathTest	testSearchParams	1021	passed				RelatedPerson.name.given
YamlFhirPathTest	testSearchParams	1022	passed				RelatedPerson.name
YamlFhirPathTest	testSearchParams	1023	passed				RelatedPerson.relationship
YamlFhirPathTest	testSearchParams	1024	passed				RequestOrchestration.author
YamlFhirPathTest	testSearchParams	1025	passed				RequestOrchestration.authoredOn
YamlFhirPathTest	testSearchParams	1026	passed				RequestOrchestration.basedOn
YamlFhirPathTest	testSearchParams	1027	passed				RequestOrchestration.groupIdentifier
YamlFhirPathTest	testSearchParams	1028	passed				RequestOrchestration.instantiatesCanonical
YamlFhirPathTest	testSearchParams	1029	passed				RequestOrchestration.instantiatesUri
YamlFhirPathTest	testSearchParams	1030	passed				RequestOrchestration.intent
YamlFhirPathTest	testSearchParams	1031	passed				RequestOrchestration.action.participant.actor.ofType(Reference) | RequestOrchestration.action.participant.actor.ofType(canonical)
YamlFhirPathTest	testSearchParams	1032	passed				RequestOrchestration.priority
YamlFhirPathTest	testSearchParams	1033	passed				RequestOrchestration.status
YamlFhirPathTest	testSearchParams	1034	passed				RequestOrchestration.subject
YamlFhirPathTest	testSearchParams	1035	passed				Requirements.actor
YamlFhirPathTest	testSearchParams	1036	passed				Requirements.derivedFrom
YamlFhirPathTest	testSearchParams	1037	passed				ResearchStudy.classifier
YamlFhirPathTest	testSearchParams	1038	passed				ResearchStudy.condition
YamlFhirPathTest	testSearchParams	1039	passed				ResearchStudy.period
YamlFhirPathTest	testSearchParams	1040	passed				ResearchStudy.description
YamlFhirPathTest	testSearchParams	1041	passed				ResearchStudy.recruitment.eligibility
YamlFhirPathTest	testSearchParams	1042	passed				ResearchStudy.focus.concept
YamlFhirPathTest	testSearchParams	1043	passed				ResearchStudy.focus.reference
YamlFhirPathTest	testSearchParams	1044	passed				ResearchStudy.identifier
YamlFhirPathTest	testSearchParams	1045	passed				ResearchStudy.keyword
YamlFhirPathTest	testSearchParams	1046	passed				ResearchStudy.name
YamlFhirPathTest	testSearchParams	1047	passed				ResearchStudy.objective.description
YamlFhirPathTest	testSearchParams	1048	passed				ResearchStudy.objective.type
YamlFhirPathTest	testSearchParams	1049	passed				ResearchStudy.partOf
YamlFhirPathTest	testSearchParams	1050	passed				ResearchStudy.phase
YamlFhirPathTest	testSearchParams	1051	passed				ResearchStudy.protocol
YamlFhirPathTest	testSearchParams	1052	passed				ResearchStudy.recruitment.actualNumber
YamlFhirPathTest	testSearchParams	1053	passed				ResearchStudy.recruitment.targetNumber
YamlFhirPathTest	testSearchParams	1054	passed				ResearchStudy.region
YamlFhirPathTest	testSearchParams	1055	passed				ResearchStudy.site
YamlFhirPathTest	testSearchParams	1056	passed				ResearchStudy.status
YamlFhirPathTest	testSearchParams	1057	passed				ResearchStudy.studyDesign
YamlFhirPathTest	testSearchParams	1058	passed				ResearchStudy.title
YamlFhirPathTest	testSearchParams	1059	passed				ResearchStudy.progressStatus
YamlFhirPathTest	testSearchParams	1060	passed				ResearchStudy.progressStatus
YamlFhirPathTest	testSearchParams	1061	passed				ResearchStudy.progressStatus
YamlFhirPathTest	testSearchParams	1062	passed				ResearchSubject.status
YamlFhirPathTest	testSearchParams	1063	passed				ResearchSubject.study
YamlFhirPathTest	testSearchParams	1064	passed				ResearchSubject.subject
YamlFhirPathTest	testSearchParams	1065	passed				ResearchSubject.progress.subjectState
YamlFhirPathTest	testSearchParams	1066	passed				RiskAssessment.condition
YamlFhirPathTest	testSearchParams	1067	passed				RiskAssessment.method
YamlFhirPathTest	testSearchParams	1068	passed				RiskAssessment.performer
YamlFhirPathTest	testSearchParams	1069	passed				RiskAssessment.prediction.probability.ofType(decimal)
YamlFhirPathTest	testSearchParams	1070	passed				RiskAssessment.prediction.qualitativeRisk
YamlFhirPathTest	testSearchParams	1071	passed				RiskAssessment.subject
YamlFhirPathTest	testSearchParams	1072	passed				Schedule.active
YamlFhirPathTest	testSearchParams	1073	passed				Schedule.actor
YamlFhirPathTest	testSearchParams	1074	passed				Schedule.planningHorizon
YamlFhirPathTest	testSearchParams	1075	passed				Schedule.identifier
YamlFhirPathTest	testSearchParams	1076	passed				Schedule.name
YamlFhirPathTest	testSearchParams	1077	passed				Schedule.serviceCategory
YamlFhirPathTest	testSearchParams	1078	passed				Schedule.serviceType.concept
YamlFhirPathTest	testSearchParams	1079	passed				Schedule.serviceType.reference
YamlFhirPathTest	testSearchParams	1080	passed				Schedule.specialty
YamlFhirPathTest	testSearchParams	1081	passed				SearchParameter.base
YamlFhirPathTest	testSearchParams	1082	passed				SearchParameter.code
YamlFhirPathTest	testSearchParams	1083	passed				SearchParameter.component.definition
YamlFhirPathTest	testSearchParams	1084	passed				SearchParameter.derivedFrom
YamlFhirPathTest	testSearchParams	1085	passed				SearchParameter.target
YamlFhirPathTest	testSearchParams	1086	passed				SearchParameter.type
YamlFhirPathTest	testSearchParams	1087	passed				ServiceRequest.authoredOn
YamlFhirPathTest	testSearchParams	1088	passed				ServiceRequest.basedOn
YamlFhirPathTest	testSearchParams	1089	passed				ServiceRequest.bodySite
YamlFhirPathTest	testSearchParams	1090	passed				ServiceRequest.bodyStructure
YamlFhirPathTest	testSearchParams	1091	passed				ServiceRequest.category
YamlFhirPathTest	testSearchParams	1092	passed				ServiceRequest.code.concept
YamlFhirPathTest	testSearchParams	1093	passed				ServiceRequest.code.reference
YamlFhirPathTest	testSearchParams	1094	passed				ServiceRequest.instantiatesCanonical
YamlFhirPathTest	testSearchParams	1095	passed				ServiceRequest.instantiatesUri
YamlFhirPathTest	testSearchParams	1096	passed				ServiceRequest.intent
YamlFhirPathTest	testSearchParams	1097	passed				ServiceRequest.occurrence.ofType(dateTime) | ServiceRequest.occurrence.ofType(Period) | ServiceRequest.occurrence.ofType(Timing)
YamlFhirPathTest	testSearchParams	1098	passed				ServiceRequest.performer
YamlFhirPathTest	testSearchParams	1099	passed				ServiceRequest.performerType
YamlFhirPathTest	testSearchParams	1100	passed				ServiceRequest.priority
YamlFhirPathTest	testSearchParams	1101	passed				ServiceRequest.replaces
YamlFhirPathTest	testSearchParams	1102	passed				ServiceRequest.requester
YamlFhirPathTest	testSearchParams	1103	passed				ServiceRequest.requisition
YamlFhirPathTest	testSearchParams	1104	passed				ServiceRequest.specimen
YamlFhirPathTest	testSearchParams	1105	passed				ServiceRequest.status
YamlFhirPathTest	testSearchParams	1106	passed				ServiceRequest.subject
YamlFhirPathTest	testSearchParams	1107	passed				Slot.appointmentType
YamlFhirPathTest	testSearchParams	1108	passed				Slot.identifier
YamlFhirPathTest	testSearchParams	1109	passed				Slot.schedule
YamlFhirPathTest	testSearchParams	1110	passed				Slot.serviceCategory
YamlFhirPathTest	testSearchParams	1111	passed				Slot.serviceType.concept
YamlFhirPathTest	testSearchParams	1112	passed				Slot.serviceType.reference
YamlFhirPathTest	testSearchParams	1113	passed				Slot.specialty
YamlFhirPathTest	testSearchParams	1114	passed				Slot.start
YamlFhirPathTest	testSearchParams	1115	passed				Slot.status
YamlFhirPathTest	testSearchParams	1116	passed				Specimen.accessionIdentifier
YamlFhirPathTest	testSearchParams	1117	passed				Specimen.collection.bodySite.reference
YamlFhirPathTest	testSearchParams	1118	passed				Specimen.collection.collected.ofType(dateTime) | Specimen.collection.collected.ofType(Period)
YamlFhirPathTest	testSearchParams	1119	passed				Specimen.collection.collector
YamlFhirPathTest	testSearchParams	1120	passed				Specimen.container.device.where(resolve() is Device)
YamlFhirPathTest	testSearchParams	1121	passed				Specimen.parent
YamlFhirPathTest	testSearchParams	1122	passed				Specimen.collection.procedure
YamlFhirPathTest	testSearchParams	1123	passed				Specimen.status
YamlFhirPathTest	testSearchParams	1124	passed				Specimen.subject
YamlFhirPathTest	testSearchParams	1125	passed				SpecimenDefinition.typeTested.container.type
YamlFhirPathTest	testSearchParams	1126	passed				SpecimenDefinition.experimental
YamlFhirPathTest	testSearchParams	1127	passed				SpecimenDefinition.typeTested.isDerived
YamlFhirPathTest	testSearchParams	1128	passed				SpecimenDefinition.typeCollected
YamlFhirPathTest	testSearchParams	1129	passed				SpecimenDefinition.typeTested.type
YamlFhirPathTest	testSearchParams	1130	passed				StructureDefinition.abstract
YamlFhirPathTest	testSearchParams	1131	passed				StructureDefinition.baseDefinition
YamlFhirPathTest	testSearchParams	1132	passed				StructureDefinition.snapshot.element.base.path | StructureDefinition.differential.element.base.path
YamlFhirPathTest	testSearchParams	1133	passed				StructureDefinition.derivation
YamlFhirPathTest	testSearchParams	1134	passed				StructureDefinition.experimental
YamlFhirPathTest	testSearchParams	1135	passed				StructureDefinition.context.expression
YamlFhirPathTest	testSearchParams	1136	passed				StructureDefinition.context.type
YamlFhirPathTest	testSearchParams	1137	passed				StructureDefinition.keyword
YamlFhirPathTest	testSearchParams	1138	passed				StructureDefinition.kind
YamlFhirPathTest	testSearchParams	1139	passed				StructureDefinition.snapshot.element.path | StructureDefinition.differential.element.path
YamlFhirPathTest	testSearchParams	1140	passed				StructureDefinition.type
YamlFhirPathTest	testSearchParams	1141	passed				StructureDefinition.snapshot.element.binding.valueSet
YamlFhirPathTest	testSearchParams	1142	passed				StructureDefinition.context
YamlFhirPathTest	testSearchParams	1143	passed				Subscription.contact
YamlFhirPathTest	testSearchParams	1144	passed				Subscription.content
YamlFhirPathTest	testSearchParams	1145	passed				Subscription.filterBy.value
YamlFhirPathTest	testSearchParams	1146	passed				Subscription.identifier
YamlFhirPathTest	testSearchParams	1147	passed				Subscription.name
YamlFhirPathTest	testSearchParams	1148	passed				Subscription.managingEntity
YamlFhirPathTest	testSearchParams	1149	passed				Subscription.contentType
YamlFhirPathTest	testSearchParams	1150	passed				Subscription.status
YamlFhirPathTest	testSearchParams	1151	passed				Subscription.topic
YamlFhirPathTest	testSearchParams	1152	passed				Subscription.channelType
YamlFhirPathTest	testSearchParams	1153	passed				Subscription.endpoint
YamlFhirPathTest	testSearchParams	1154	passed				SubscriptionTopic.url | SubscriptionTopic.derivedFrom
YamlFhirPathTest	testSearchParams	1155	passed				SubscriptionTopic.effectivePeriod
YamlFhirPathTest	testSearchParams	1156	passed				SubscriptionTopic.eventTrigger.event
YamlFhirPathTest	testSearchParams	1157	passed				SubscriptionTopic.resourceTrigger.resource | SubscriptionTopic.eventTrigger.resource | SubscriptionTopic.canFilterBy.resource | SubscriptionTopic.notificationShape.resource
YamlFhirPathTest	testSearchParams	1158	passed				SubscriptionTopic.resourceTrigger.description
YamlFhirPathTest	testSearchParams	1159	passed				Substance.category
YamlFhirPathTest	testSearchParams	1160	passed				Substance.code.concept | (Substance.ingredient.substance.ofType(CodeableConcept))
YamlFhirPathTest	testSearchParams	1161	passed				Substance.code.reference
YamlFhirPathTest	testSearchParams	1162	passed				Substance.expiry
YamlFhirPathTest	testSearchParams	1163	passed				Substance.identifier
YamlFhirPathTest	testSearchParams	1164	passed				Substance.quantity
YamlFhirPathTest	testSearchParams	1165	passed				Substance.status
YamlFhirPathTest	testSearchParams	1166	passed				(Substance.ingredient.substance.ofType(Reference))
YamlFhirPathTest	testSearchParams	1167	passed				SubstanceDefinition.classification
YamlFhirPathTest	testSearchParams	1168	passed				SubstanceDefinition.code.code
YamlFhirPathTest	testSearchParams	1169	passed				SubstanceDefinition.domain
YamlFhirPathTest	testSearchParams	1170	passed				SubstanceDefinition.identifier
YamlFhirPathTest	testSearchParams	1171	passed				SubstanceDefinition.name.name
YamlFhirPathTest	testSearchParams	1172	passed				SupplyDelivery.receiver
YamlFhirPathTest	testSearchParams	1173	passed				SupplyDelivery.status
YamlFhirPathTest	testSearchParams	1174	passed				SupplyDelivery.supplier
YamlFhirPathTest	testSearchParams	1175	passed				SupplyRequest.category
YamlFhirPathTest	testSearchParams	1176	passed				SupplyRequest.requester
YamlFhirPathTest	testSearchParams	1177	passed				SupplyRequest.status
YamlFhirPathTest	testSearchParams	1178	passed				SupplyRequest.deliverTo
YamlFhirPathTest	testSearchParams	1179	passed				SupplyRequest.supplier
YamlFhirPathTest	testSearchParams	1180	passed				Task.performer.actor
YamlFhirPathTest	testSearchParams	1181	passed				Task.authoredOn
YamlFhirPathTest	testSearchParams	1182	passed				Task.basedOn
YamlFhirPathTest	testSearchParams	1183	passed				Task.businessStatus
YamlFhirPathTest	testSearchParams	1184	passed				Task.focus
YamlFhirPathTest	testSearchParams	1185	passed				Task.groupIdentifier
YamlFhirPathTest	testSearchParams	1186	passed				Task.intent
YamlFhirPathTest	testSearchParams	1187	passed				Task.lastModified
YamlFhirPathTest	testSearchParams	1188	passed				Task.output.value.ofType(Reference)
YamlFhirPathTest	testSearchParams	1189	passed				Task.owner
YamlFhirPathTest	testSearchParams	1190	passed				Task.partOf
YamlFhirPathTest	testSearchParams	1191	passed				Task.requestedPerformer.concept
YamlFhirPathTest	testSearchParams	1192	passed				Task.executionPeriod
YamlFhirPathTest	testSearchParams	1193	passed				Task.priority
YamlFhirPathTest	testSearchParams	1194	passed				Task.requestedPerformer.reference
YamlFhirPathTest	testSearchParams	1195	passed				Task.requester
YamlFhirPathTest	testSearchParams	1196	passed				Task.status
YamlFhirPathTest	testSearchParams	1197	passed				Task.for
YamlFhirPathTest	testSearchParams	1198	passed				TestPlan.scope
YamlFhirPathTest	testSearchParams	1199	passed				TestReport.identifier
YamlFhirPathTest	testSearchParams	1200	passed				TestReport.issued
YamlFhirPathTest	testSearchParams	1201	passed				TestReport.participant.uri
YamlFhirPathTest	testSearchParams	1202	passed				TestReport.result
YamlFhirPathTest	testSearchParams	1203	passed				TestReport.status
YamlFhirPathTest	testSearchParams	1204	passed				TestReport.tester
YamlFhirPathTest	testSearchParams	1205	passed				TestReport.testScript
YamlFhirPathTest	testSearchParams	1206	passed				TestScript.scope.artifact
YamlFhirPathTest	testSearchParams	1207	passed				TestScript.metadata.capability.description
YamlFhirPathTest	testSearchParams	1208	passed				TestScript.scope
YamlFhirPathTest	testSearchParams	1209	passed				TestScript.scope
YamlFhirPathTest	testSearchParams	1210	passed				Transport.identifier
YamlFhirPathTest	testSearchParams	1211	passed				Transport.status
YamlFhirPathTest	testSearchParams	1212	passed				ValueSet.expansion.contains.code | ValueSet.compose.include.concept.code
YamlFhirPathTest	testSearchParams	1213	passed				ValueSet.expansion.identifier
YamlFhirPathTest	testSearchParams	1214	passed				ValueSet.compose.include.system
YamlFhirPathTest	testSearchParams	1215	passed				VerificationResult.attestation.communicationMethod
YamlFhirPathTest	testSearchParams	1216	passed				VerificationResult.attestation.onBehalfOf
YamlFhirPathTest	testSearchParams	1217	passed				VerificationResult.attestation.who
YamlFhirPathTest	testSearchParams	1218	passed				VerificationResult.primarySource.validationDate
YamlFhirPathTest	testSearchParams	1219	passed				VerificationResult.primarySource.type
YamlFhirPathTest	testSearchParams	1220	passed				VerificationResult.primarySource.who
YamlFhirPathTest	testSearchParams	1221	passed				VerificationResult.status
YamlFhirPathTest	testSearchParams	1222	passed				VerificationResult.statusDate
YamlFhirPathTest	testSearchParams	1223	passed				VerificationResult.target
YamlFhirPathTest	testSearchParams	1224	passed				VerificationResult.validator.organization
YamlFhirPathTest	testSearchParams	1225	passed				VisionPrescription.dateWritten
YamlFhirPathTest	testSearchParams	1226	passed				VisionPrescription.prescriber
YamlFhirPathTest	testSearchParams	1227	passed				VisionPrescription.status
```
