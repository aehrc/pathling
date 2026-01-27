/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.dsl;

import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toCoding;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toDate;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toDateTime;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toQuantity;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toTime;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;

public class CombiningOperatorsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testEmptyCollectionUnion() {
    return builder()
        .withSubject(sb -> sb)
        .group("Empty collection union - type agnostic")
        .testEmpty("{} | {}", "Empty union empty")
        .testEmpty("( {} | {} ) | ( {} | {} )", "Grouped empty unions")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testBooleanUnion() {
    return builder()
        .withSubject(
            sb ->
                sb.boolArray("trueFalseArray", true, false)
                    .boolArray("falseTrueArray", false, true)
                    .boolArray("trueTrueArray", true, true)
                    .boolEmpty("emptyBool"))
        .group("Boolean union - empty collections")
        .testTrue("true | {}", "True union empty literal")
        .testTrue("{} | true", "Empty literal union true")
        .testFalse("false | {}", "False union empty literal")
        .testFalse("{} | false", "Empty literal union false")
        .testTrue("true | emptyBool", "True union empty Boolean")
        .testTrue("emptyBool | true", "Empty Boolean union true")
        .testEmpty("emptyBool | emptyBool", "Empty Boolean union empty Boolean")
        .testTrue("{} | trueTrueArray", "Empty union [true, true] deduplicates to [true]")
        .testTrue("trueTrueArray | {}", "[true, true] union empty deduplicates to [true]")
        .group("Boolean union - single values")
        .testEquals(List.of(true, false), "true | false", "True union false")
        .testEquals(List.of(false, true), "false | true", "False union true")
        .testTrue("true | true", "True union true (deduplication)")
        .testFalse("false | false", "False union false (deduplication)")
        .group("Boolean union - arrays")
        .testEquals(
            List.of(true, false),
            "trueFalseArray | falseTrueArray",
            "Array [true, false] union [false, true] (deduplication)")
        .testEquals(
            List.of(true, false),
            "trueFalseArray | trueFalseArray",
            "Array union itself (deduplication)")
        .group("Boolean union - mixed singleton and array")
        .testEquals(
            List.of(true, false),
            "true | trueFalseArray",
            "True singleton union [true, false] array")
        .testEquals(
            List.of(true, false),
            "trueFalseArray | true",
            "Array [true, false] union true singleton")
        .testEquals(
            List.of(false, true),
            "false | trueFalseArray",
            "False singleton union [true, false] array")
        .testEquals(
            List.of(true, false),
            "trueFalseArray | false",
            "Array [true, false] union false singleton")
        .group("Boolean union - grouped/nested expressions")
        .testEquals(
            List.of(true, false),
            "(true | false) | (false | true)",
            "Grouped: (true | false) | (false | true)")
        .testEquals(
            List.of(true, false),
            "(true | true) | (false | false)",
            "Grouped with duplicates: (true | true) | (false | false)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testIntegerUnion() {
    return builder()
        .withSubject(
            sb ->
                sb.integerArray("oneTwoThree", 1, 2, 3)
                    .integerArray("twoThreeFour", 2, 3, 4)
                    .integerArray("oneOneTwoThree", 1, 1, 2, 3)
                    .integerArray("oneOne", 1, 1)
                    .integerEmpty("emptyInt"))
        .group("Integer union - empty collections")
        .testEquals(1, "1 | {}", "1 union empty literal")
        .testEquals(1, "{} | 1", "Empty literal union 1")
        .testEquals(1, "1 | emptyInt", "1 union empty Integer")
        .testEquals(1, "emptyInt | 1", "Empty Integer union 1")
        .testEmpty("emptyInt | emptyInt", "Empty Integer union empty Integer")
        .testEquals(1, "{} | oneOne", "Empty union [1, 1] deduplicates to [1]")
        .testEquals(1, "oneOne | {}", "[1, 1] union empty deduplicates to [1]")
        .group("Integer union - single values")
        .testEquals(List.of(1, 2), "1 | 2", "1 union 2")
        .testEquals(List.of(2, 1), "2 | 1", "2 union 1")
        .testEquals(1, "1 | 1", "1 union 1 (deduplication)")
        .group("Integer union - arrays")
        .testEquals(
            List.of(1, 2, 3, 4),
            "oneTwoThree | twoThreeFour",
            "Array [1, 2, 3] union [2, 3, 4] (deduplication)")
        .testEquals(
            List.of(1, 2, 3), "oneTwoThree | oneTwoThree", "Array union itself (deduplication)")
        .testEquals(
            List.of(1, 2, 3),
            "oneOneTwoThree | oneTwoThree",
            "Array [1, 1, 2, 3] union [1, 2, 3] (deduplication)")
        .group("Integer union - mixed singleton and array")
        .testEquals(List.of(1, 2, 3), "1 | oneTwoThree", "1 singleton union [1, 2, 3] array")
        .testEquals(List.of(1, 2, 3), "oneTwoThree | 1", "Array [1, 2, 3] union 1 singleton")
        .testEquals(List.of(1, 2, 3, 4), "oneTwoThree | 4", "Array [1, 2, 3] union 4 literal")
        .group("Integer union - grouped/nested expressions")
        .testEquals(List.of(1, 2, 3), "(1 | 2) | (2 | 3)", "Grouped: (1 | 2) | (2 | 3)")
        .testEquals(
            List.of(1, 2), "(1 | 1) | (2 | 2)", "Grouped with duplicates: (1 | 1) | (2 | 2)")
        .testEquals(
            List.of(1, 2, 3, 4), "(1 | 2) | (3 | 4)", "Grouped with literal: (1 | 2) | (3 | 4)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testStringUnion() {
    return builder()
        .withSubject(
            sb ->
                sb.stringArray("abc", "a", "b", "c")
                    .stringArray("bcd", "b", "c", "d")
                    .stringArray("aabbc", "a", "a", "b", "b", "c")
                    .stringArray("aaa", "a", "a", "a")
                    .stringEmpty("emptyString"))
        .group("String union - empty collections")
        .testEquals("a", "'a' | {}", "'a' union empty literal")
        .testEquals("a", "{} | 'a'", "Empty literal union 'a'")
        .testEquals("a", "'a' | emptyString", "'a' union empty String")
        .testEquals("a", "emptyString | 'a'", "Empty String union 'a'")
        .testEmpty("emptyString | emptyString", "Empty String union empty String")
        .testEquals("a", "{} | aaa", "Empty union ['a', 'a', 'a'] deduplicates to ['a']")
        .testEquals("a", "aaa | {}", "['a', 'a', 'a'] union empty deduplicates to ['a']")
        .group("String union - single values")
        .testEquals(List.of("a", "b"), "'a' | 'b'", "'a' union 'b'")
        .testEquals(List.of("b", "a"), "'b' | 'a'", "'b' union 'a'")
        .testEquals("a", "'a' | 'a'", "'a' union 'a' (deduplication)")
        .group("String union - arrays")
        .testEquals(
            List.of("a", "b", "c", "d"),
            "abc | bcd",
            "Array ['a', 'b', 'c'] union ['b', 'c', 'd'] (deduplication)")
        .testEquals(List.of("a", "b", "c"), "abc | abc", "Array union itself (deduplication)")
        .testEquals(
            List.of("a", "b", "c"),
            "aabbc | abc",
            "Array ['a', 'a', 'b', 'b', 'c'] union ['a', 'b', 'c'] (deduplication)")
        .group("String union - mixed singleton and array")
        .testEquals(
            List.of("a", "b", "c"), "'a' | abc", "'a' singleton union ['a', 'b', 'c'] array")
        .testEquals(
            List.of("a", "b", "c"), "abc | 'a'", "Array ['a', 'b', 'c'] union 'a' singleton")
        .testEquals(
            List.of("a", "b", "c", "d"), "abc | 'd'", "Array ['a', 'b', 'c'] union 'd' literal")
        .group("String union - grouped/nested expressions")
        .testEquals(
            List.of("a", "b", "c"),
            "('a' | 'b') | ('b' | 'c')",
            "Grouped: ('a' | 'b') | ('b' | 'c')")
        .testEquals(
            List.of("a", "b"),
            "('a' | 'a') | ('b' | 'b')",
            "Grouped with duplicates: ('a' | 'a') | ('b' | 'b')")
        .testEquals(
            List.of("a", "b", "c", "d"),
            "('a' | 'b') | ('c' | 'd')",
            "Grouped with literal: ('a' | 'b') | ('c' | 'd')")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testDecimalUnion() {
    return builder()
        .withSubject(
            sb ->
                sb.decimalArray("dec123", 1.1, 2.2, 3.3)
                    .decimalArray("dec234", 2.2, 3.3, 4.4)
                    .decimalArray("dec111", 1.1, 1.1)
                    .decimalEmpty("emptyDec"))
        .group("Decimal union - empty collections")
        .testEquals(2.5, "2.5 | {}", "2.5 union empty literal")
        .testEquals(2.5, "{} | 2.5", "Empty literal union 2.5")
        .testEquals(2.5, "2.5 | emptyDec", "2.5 union empty Decimal")
        .testEquals(2.5, "emptyDec | 2.5", "Empty Decimal union 2.5")
        .testEmpty("emptyDec | emptyDec", "Empty Decimal union empty Decimal")
        .testEquals(1.1, "{} | dec111", "Empty union [1.1, 1.1] deduplicates to [1.1]")
        .testEquals(1.1, "dec111 | {}", "[1.1, 1.1] union empty deduplicates to [1.1]")
        .group("Decimal union - single values")
        .testEquals(List.of(2.5, 5.5), "2.5 | 5.5", "2.5 union 5.5")
        .testEquals(List.of(5.5, 2.5), "5.5 | 2.5", "5.5 union 2.5")
        .testEquals(2.5, "2.5 | 2.5", "2.5 union 2.5 (deduplication)")
        .group("Decimal union - precision variations")
        .testEquals(2.5, "2.5 | 2.50", "2.5 union 2.50 (different precision, same value)")
        .testEquals(1.1, "1.1 | 1.10", "1.1 union 1.10 (trailing zeros)")
        .testEquals(1.0, "1.0 | 1.00", "1.0 union 1.00 (trailing zeros after decimal)")
        .group("Decimal union - arrays")
        .testEquals(
            List.of(1.1, 2.2, 3.3, 4.4),
            "dec123 | dec234",
            "Array [1.1, 2.2, 3.3] union [2.2, 3.3, 4.4] (deduplication)")
        .testEquals(List.of(1.1, 2.2, 3.3), "dec123 | dec123", "Array union itself (deduplication)")
        .group("Decimal union - mixed with Integer")
        .testEquals(
            List.of(2.5, 5.0), "2.5 | 5", "Decimal union integer (integer promoted to decimal)")
        .testEquals(
            List.of(5.0, 2.5), "5 | 2.5", "Integer union decimal (integer promoted to decimal)")
        .testEquals(List.of(1.0, 2.0), "1.0 | 2", "1.0 union 2")
        .testEquals(1.0, "1.0 | 1", "1.0 union 1 (should deduplicate)")
        .group("Decimal union - grouped/nested expressions")
        .testEquals(
            List.of(1.1, 2.2, 3.3),
            "(1.1 | 2.2) | (2.2 | 3.3)",
            "Grouped: (1.1 | 2.2) | (2.2 | 3.3)")
        .testEquals(
            List.of(1.1, 2.2),
            "(1.1 | 1.1) | (2.2 | 2.2)",
            "Grouped with duplicates: (1.1 | 1.1) | (2.2 | 2.2)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testQuantityUnion() {
    return builder()
        .withSubject(sb -> sb) // No quantity arrays needed - using literals only
        .group("Quantity union - same dimension different units")
        .testEquals(
            toQuantity("1000 'mg'"), "1000 'mg' | 1 'g'", "1000mg = 1g, first element retained")
        .testEquals(
            List.of(toQuantity("1000 'mg'"), toQuantity("2 'g'")),
            "1000 'mg' | 2 'g'",
            "1000mg != 2g, keep both")
        .testEquals(
            List.of(toQuantity("1000 'mg'"), toQuantity("2000 'mg'"), toQuantity("3 'g'")),
            "(1000 'mg' | 2000 'mg') | (1 'g' | 3 'g')",
            "Complex union with mixed units")
        .group("Quantity union - definite time quantities")
        .testEquals(
            toQuantity("1000 'ms'"), "1000 'ms' | 1 's'", "1000ms = 1s, first element retained")
        .testEquals(
            toQuantity("2 second"),
            "2 second | 2 's'",
            "Calendar second = UCUM second, first element retained")
        .testEquals(
            toQuantity("1 'd'"), "1 'd' | 24 'h'", "Definite durations comparable, keep first")
        .group("Quantity union - indefinite calendar durations")
        .testEquals(
            List.of(toQuantity("1 year"), toQuantity("12 months")),
            "1 year | 12 months",
            "Indefinite durations not comparable, keep both")
        .group("Quantity union - unit '1' with decimals/integers")
        .testEquals(
            toQuantity("1 '1'"),
            "1 '1' | 1.0",
            "Decimal promoted to Quantity('1'), first element retained")
        .testEquals(
            toQuantity("1 '1'"),
            "1.0 '1' | 1.00 '1'",
            "Unitless quantities with different precision, first retained")
        .group("Quantity union - incompatible dimensions")
        .testEquals(
            List.of(toQuantity("1 'cm'"), toQuantity("1 'cm2'")),
            "1 'cm' | 1 'cm2'",
            "Different dimensions (length vs area), keep both")
        .group("Quantity union - empty collections")
        .testEquals(toQuantity("1 'mg'"), "{} | 1 'mg'", "Empty union quantity")
        .testEquals(
            toQuantity("1 'mg'"), "(1 'mg' | 1 'mg') | {}", "Deduplicate then union with empty")
        .testEmpty("{} | {}", "Empty union empty")
        .group("Quantity union - grouped expressions")
        .testEquals(
            List.of(toQuantity("1 'mg'"), toQuantity("2 'mg'"), toQuantity("3 'mg'")),
            "(1 'mg' | 2 'mg') | (2 'mg' | 3 'mg')",
            "Nested union with deduplication")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCodingUnion() {
    return builder()
        .withSubject(
            sb ->
                sb.codingArray(
                        "heartRateDuplicate",
                        "http://loinc.org|8867-4||'Heart rate'",
                        "http://loinc.org|8867-4||'Heart rate'")
                    .codingArray(
                        "vitalSigns",
                        "http://loinc.org|8867-4||'Heart rate'",
                        "http://loinc.org|8480-6||'Systolic blood pressure'")
                    .codingArray(
                        "bloodPressure",
                        "http://loinc.org|8480-6||'Systolic blood pressure'",
                        "http://loinc.org|8462-4||'Diastolic blood pressure'")
                    .codingEmpty("emptyCoding"))
        .group("Coding union - empty collections")
        .testEquals(
            toCoding("http://loinc.org|8867-4||'Heart rate'"),
            "http://loinc.org|8867-4||'Heart rate' | {}",
            "Coding literal union empty")
        .testEquals(
            toCoding("http://loinc.org|8867-4||'Heart rate'"),
            "{} | http://loinc.org|8867-4||'Heart rate'",
            "Empty union coding literal")
        .testEquals(
            toCoding("http://loinc.org|8867-4||'Heart rate'"),
            "http://loinc.org|8867-4||'Heart rate' | emptyCoding",
            "Coding literal union empty Coding")
        .testEquals(
            toCoding("http://loinc.org|8867-4||'Heart rate'"),
            "emptyCoding | http://loinc.org|8867-4||'Heart rate'",
            "Empty Coding union coding literal")
        .testEmpty("emptyCoding | emptyCoding", "Empty Coding union empty Coding")
        .testEquals(
            toCoding("http://loinc.org|8867-4||'Heart rate'"),
            "{} | heartRateDuplicate",
            "Empty union [coding, coding] deduplicates to [coding]")
        .testEquals(
            toCoding("http://loinc.org|8867-4||'Heart rate'"),
            "heartRateDuplicate | {}",
            "[coding, coding] union empty deduplicates to [coding]")
        .group("Coding union - single values")
        .testEquals(
            toCoding("http://loinc.org|8867-4||'Heart rate'"),
            "http://loinc.org|8867-4||'Heart rate' | http://loinc.org|8867-4||'Heart rate'",
            "Identical coding union itself (deduplication)")
        .testEquals(
            List.of(
                toCoding("http://loinc.org|8867-4||'Heart rate'"),
                toCoding("http://loinc.org|8480-6||'Systolic blood pressure'")),
            "http://loinc.org|8867-4||'Heart rate' | http://loinc.org|8480-6||'Systolic blood"
                + " pressure'",
            "Different coding literals union")
        .group("Coding union - arrays")
        .testEquals(
            List.of(
                toCoding("http://loinc.org|8867-4||'Heart rate'"),
                toCoding("http://loinc.org|8480-6||'Systolic blood pressure'")),
            "vitalSigns | vitalSigns",
            "Array union itself (deduplication)")
        .testEquals(
            List.of(
                toCoding("http://loinc.org|8867-4||'Heart rate'"),
                toCoding("http://loinc.org|8480-6||'Systolic blood pressure'"),
                toCoding("http://loinc.org|8462-4||'Diastolic blood pressure'")),
            "vitalSigns | bloodPressure",
            "Arrays with one common element (deduplication)")
        .group("Coding union - mixed singleton and array")
        .testEquals(
            List.of(
                toCoding("http://loinc.org|8867-4||'Heart rate'"),
                toCoding("http://loinc.org|8480-6||'Systolic blood pressure'")),
            "http://loinc.org|8867-4||'Heart rate' | vitalSigns",
            "Singleton union array containing it (deduplication)")
        .testEquals(
            List.of(
                toCoding("http://loinc.org|8867-4||'Heart rate'"),
                toCoding("http://loinc.org|8480-6||'Systolic blood pressure'")),
            "vitalSigns | http://loinc.org|8867-4||'Heart rate'",
            "Array union singleton it contains (deduplication)")
        .testEquals(
            List.of(
                toCoding("http://loinc.org|8867-4||'Heart rate'"),
                toCoding("http://loinc.org|8480-6||'Systolic blood pressure'"),
                toCoding("http://loinc.org|8462-4||'Diastolic blood pressure'")),
            "vitalSigns | http://loinc.org|8462-4||'Diastolic blood pressure'",
            "Array union singleton not in array")
        .group("Coding union - grouped/nested expressions")
        .testEquals(
            List.of(
                toCoding("http://loinc.org|8867-4||'Heart rate'"),
                toCoding("http://loinc.org|8480-6||'Systolic blood pressure'"),
                toCoding("http://loinc.org|8462-4||'Diastolic blood pressure'")),
            "(http://loinc.org|8867-4||'Heart rate' | http://loinc.org|8480-6||'Systolic blood"
                + " pressure') | (http://loinc.org|8480-6||'Systolic blood pressure' |"
                + " http://loinc.org|8462-4||'Diastolic blood pressure')",
            "Grouped union with deduplication")
        .testEquals(
            List.of(
                toCoding("http://loinc.org|8867-4||'Heart rate'"),
                toCoding("http://loinc.org|8480-6||'Systolic blood pressure'")),
            "(http://loinc.org|8867-4||'Heart rate' | http://loinc.org|8867-4||'Heart rate') |"
                + " (http://loinc.org|8480-6||'Systolic blood pressure' |"
                + " http://loinc.org|8480-6||'Systolic blood pressure')",
            "Grouped with duplicates within groups")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTimeUnion() {
    return builder()
        .withSubject(
            sb ->
                sb.time("t1", "12:00")
                    .time("t2", "13:00")
                    .time("t3", "12:00:00")
                    .time("t4", "12:00:00.000")
                    .timeArray("samePrecision1", "12:00", "13:00")
                    .timeArray("samePrecision2", "12:00", "12:00")
                    .timeArray("mixedPrecision", "12:00", "12:00:00")
                    .timeEmpty("emptyTime"))
        .group("Time union - empty collections")
        .testEquals(toTime("12:00"), "@T12:00 | {}", "Time literal union empty")
        .testEquals(toTime("12:00"), "{} | @T12:00", "Empty union time literal")
        .testEquals(toTime("12:00"), "@T12:00 | emptyTime", "Time literal union empty Time")
        .testEquals(toTime("12:00"), "emptyTime | @T12:00", "Empty Time union time literal")
        .testEmpty("emptyTime | emptyTime", "Empty Time union empty Time")
        .testEquals(
            toTime("12:00"),
            "{} | samePrecision2",
            "Empty union [12:00, 12:00] deduplicates to [12:00]")
        .testEquals(
            toTime("12:00"),
            "samePrecision2 | {}",
            "[12:00, 12:00] union empty deduplicates to [12:00]")
        .group("Time union - single values, same precision")
        .testEquals(toTime("12:00"), "@T12:00 | @T12:00", "Same time union itself (deduplication)")
        .testEquals(
            List.of(toTime("12:00"), toTime("13:00")), "@T12:00 | @T13:00", "Different times union")
        .testEquals(toTime("12:00"), "t1 | t1", "Variable union itself (deduplication)")
        .testEquals(
            List.of(toTime("12:00"), toTime("13:00")), "t1 | t2", "Different time variables union")
        .group("Time union - single values, different precision")
        .testEquals(
            List.of(toTime("12:00"), toTime("12:00:00")),
            "t1 | t3",
            "Different precision (12:00 vs 12:00:00), keep both")
        .testEquals(
            List.of(toTime("12:00"), toTime("12:00:00.000")),
            "t1 | t4",
            "Different precision (12:00 vs 12:00:00.000), keep both")
        .testEquals(
            toTime("12:00:00"),
            "t3 | t4",
            "Both fractional seconds precision (12:00:00 vs 12:00:00.000), deduplicate")
        .group("Time union - arrays, same precision")
        .testEquals(
            List.of(toTime("12:00"), toTime("13:00")),
            "samePrecision1 | samePrecision1",
            "Array union itself (deduplication)")
        .testEquals(
            toTime("12:00"),
            "samePrecision2 | samePrecision2",
            "Array [12:00, 12:00] union itself (deduplication)")
        .group("Time union - arrays, mixed precision")
        .testEquals(
            List.of(toTime("12:00"), toTime("12:00:00")),
            "mixedPrecision | mixedPrecision",
            "Array [12:00, 12:00:00] union itself (incomparable, keep both)")
        .group("Time union - mixed singleton and array")
        .testEquals(
            List.of(toTime("12:00"), toTime("13:00")),
            "@T12:00 | samePrecision1",
            "Singleton union array containing it (deduplication)")
        .testEquals(
            List.of(toTime("12:00"), toTime("13:00")),
            "samePrecision1 | @T12:00",
            "Array union singleton it contains (deduplication)")
        .testEquals(
            List.of(toTime("12:00"), toTime("13:00"), toTime("14:00")),
            "samePrecision1 | @T14:00",
            "Array union singleton not in array")
        .group("Time union - grouped/nested expressions")
        .testEquals(
            List.of(toTime("12:00"), toTime("13:00"), toTime("14:00")),
            "(@T12:00 | @T13:00) | (@T13:00 | @T14:00)",
            "Grouped union with deduplication")
        .testEquals(
            List.of(toTime("12:00"), toTime("13:00")),
            "(@T12:00 | @T12:00) | (@T13:00 | @T13:00)",
            "Grouped with duplicates within groups")
        .group("Time union - precision handling in groups")
        .testEquals(
            List.of(toTime("12:00"), toTime("12:00:00"), toTime("13:00")),
            "(@T12:00 | @T12:00:00) | (@T13:00 | @T13:00)",
            "Mixed precision in union (incomparable, keep both 12:00 and 12:00:00)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testDateUnion() {
    return builder()
        .withSubject(
            sb ->
                sb.date("d1", "2020-01-01")
                    .date("d2", "2020-01-02")
                    .date("d3", "2020-01")
                    .date("d4", "2020")
                    .dateArray("samePrecision1", "2020-01-01", "2020-01-02")
                    .dateArray("samePrecision2", "2020-01-01", "2020-01-01")
                    .dateArray("mixedPrecision", "2020-01-01", "2020-01")
                    .dateEmpty("emptyDate"))
        .group("Date union - empty collections")
        .testEquals(toDate("2020-01-01"), "@2020-01-01 | {}", "Date literal union empty")
        .testEquals(toDate("2020-01-01"), "{} | @2020-01-01", "Empty union date literal")
        .testEquals(
            toDate("2020-01-01"), "@2020-01-01 | emptyDate", "Date literal union empty Date")
        .testEquals(
            toDate("2020-01-01"), "emptyDate | @2020-01-01", "Empty Date union date literal")
        .testEmpty("emptyDate | emptyDate", "Empty Date union empty Date")
        .testEquals(
            toDate("2020-01-01"),
            "{} | samePrecision2",
            "Empty union [2020-01-01, 2020-01-01] deduplicates to [2020-01-01]")
        .testEquals(
            toDate("2020-01-01"),
            "samePrecision2 | {}",
            "[2020-01-01, 2020-01-01] union empty deduplicates to [2020-01-01]")
        .group("Date union - single values, same precision")
        .testEquals(
            toDate("2020-01-01"),
            "@2020-01-01 | @2020-01-01",
            "Same date union itself (deduplication)")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01-02")),
            "@2020-01-01 | @2020-01-02",
            "Different dates union")
        .testEquals(toDate("2020-01-01"), "d1 | d1", "Variable union itself (deduplication)")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01-02")),
            "d1 | d2",
            "Different date variables union")
        .group("Date union - single values, different precision")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01")),
            "d1 | d3",
            "Different precision (2020-01-01 vs 2020-01), keep both")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020")),
            "d1 | d4",
            "Different precision (2020-01-01 vs 2020), keep both")
        .testEquals(
            List.of(toDate("2020-01"), toDate("2020")),
            "d3 | d4",
            "Different precision (2020-01 vs 2020), keep both")
        .group("Date union - arrays, same precision")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01-02")),
            "samePrecision1 | samePrecision1",
            "Array union itself (deduplication)")
        .testEquals(
            toDate("2020-01-01"),
            "samePrecision2 | samePrecision2",
            "Array [2020-01-01, 2020-01-01] union itself (deduplication)")
        .group("Date union - arrays, mixed precision")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01")),
            "mixedPrecision | mixedPrecision",
            "Array [2020-01-01, 2020-01] union itself (incomparable, keep both)")
        .group("Date union - mixed singleton and array")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01-02")),
            "@2020-01-01 | samePrecision1",
            "Singleton union array containing it (deduplication)")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01-02")),
            "samePrecision1 | @2020-01-01",
            "Array union singleton it contains (deduplication)")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01-02"), toDate("2020-01-03")),
            "samePrecision1 | @2020-01-03",
            "Array union singleton not in array")
        .group("Date union - grouped/nested expressions")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01-02"), toDate("2020-01-03")),
            "(@2020-01-01 | @2020-01-02) | (@2020-01-02 | @2020-01-03)",
            "Grouped union with deduplication")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01-02")),
            "(@2020-01-01 | @2020-01-01) | (@2020-01-02 | @2020-01-02)",
            "Grouped with duplicates within groups")
        .group("Date union - precision handling in groups")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01"), toDate("2020-01-02")),
            "(@2020-01-01 | @2020-01) | (@2020-01-02 | @2020-01-02)",
            "Mixed precision in union (incomparable, keep both 2020-01-01 and 2020-01)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testDateTimeUnion() {
    return builder()
        .withSubject(
            sb ->
                sb.dateTime("dt1", "2020-01-01T10:00:00")
                    .dateTime("dt2", "2020-01-01T11:00:00")
                    .dateTime("dt3", "2020-01-01T10:00")
                    .dateTime("dt4", "2020-01-01T10:00:00+00:00")
                    .dateTime("dt5", "2020-01-01T10:00:00.000")
                    .dateTimeArray("samePrecision1", "2020-01-01T10:00:00", "2020-01-01T11:00:00")
                    .dateTimeArray("samePrecision2", "2020-01-01T10:00:00", "2020-01-01T10:00:00")
                    .dateTimeArray("mixedPrecision", "2020-01-01T10:00:00", "2020-01-01T10:00")
                    .dateTimeEmpty("emptyDateTime"))
        .group("DateTime union - empty collections")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"),
            "@2020-01-01T10:00:00 | {}",
            "DateTime literal union empty")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"),
            "{} | @2020-01-01T10:00:00",
            "Empty union dateTime literal")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"),
            "@2020-01-01T10:00:00 | emptyDateTime",
            "DateTime literal union empty DateTime")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"),
            "emptyDateTime | @2020-01-01T10:00:00",
            "Empty DateTime union dateTime literal")
        .testEmpty("emptyDateTime | emptyDateTime", "Empty DateTime union empty DateTime")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"),
            "{} | samePrecision2",
            "Empty union [2020-01-01T10:00:00, 2020-01-01T10:00:00] deduplicates to"
                + " [2020-01-01T10:00:00]")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"),
            "samePrecision2 | {}",
            "[2020-01-01T10:00:00, 2020-01-01T10:00:00] union empty deduplicates to"
                + " [2020-01-01T10:00:00]")
        .group("DateTime union - single values, same precision")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"),
            "@2020-01-01T10:00:00 | @2020-01-01T10:00:00",
            "Same dateTime union itself (deduplication)")
        .testEquals(
            List.of(toDateTime("2020-01-01T10:00:00"), toDateTime("2020-01-01T11:00:00")),
            "@2020-01-01T10:00:00 | @2020-01-01T11:00:00",
            "Different dateTimes union")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"), "dt1 | dt1", "Variable union itself (deduplication)")
        .testEquals(
            List.of(toDateTime("2020-01-01T10:00:00"), toDateTime("2020-01-01T11:00:00")),
            "dt1 | dt2",
            "Different dateTime variables union")
        .group("DateTime union - single values, different precision")
        .testEquals(
            List.of(toDateTime("2020-01-01T10:00:00"), toDateTime("2020-01-01T10:00")),
            "dt1 | dt3",
            "Different precision (2020-01-01T10:00:00 vs 2020-01-01T10:00), keep both")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"),
            "dt1 | dt5",
            "Both with fractional seconds precision (2020-01-01T10:00:00 vs"
                + " 2020-01-01T10:00:00.000), deduplicate")
        .group("DateTime union - timezone handling")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"),
            "dt1 | dt4",
            "With timezone vs without timezone (2020-01-01T10:00:00 vs 2020-01-01T10:00:00+00:00),"
                + " deduplicate")
        .testEquals(
            toDateTime("2020-01-01T10:00:00+00:00"),
            "dt4 | dt4",
            "Same dateTime with timezone union itself (deduplication)")
        .group("DateTime union - arrays, same precision")
        .testEquals(
            List.of(toDateTime("2020-01-01T10:00:00"), toDateTime("2020-01-01T11:00:00")),
            "samePrecision1 | samePrecision1",
            "Array union itself (deduplication)")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"),
            "samePrecision2 | samePrecision2",
            "Array [2020-01-01T10:00:00, 2020-01-01T10:00:00] union itself (deduplication)")
        .group("DateTime union - arrays, mixed precision")
        .testEquals(
            List.of(toDateTime("2020-01-01T10:00:00"), toDateTime("2020-01-01T10:00")),
            "mixedPrecision | mixedPrecision",
            "Array [2020-01-01T10:00:00, 2020-01-01T10:00] union itself (incomparable, keep both)")
        .group("DateTime union - mixed singleton and array")
        .testEquals(
            List.of(toDateTime("2020-01-01T10:00:00"), toDateTime("2020-01-01T11:00:00")),
            "@2020-01-01T10:00:00 | samePrecision1",
            "Singleton union array containing it (deduplication)")
        .testEquals(
            List.of(toDateTime("2020-01-01T10:00:00"), toDateTime("2020-01-01T11:00:00")),
            "samePrecision1 | @2020-01-01T10:00:00",
            "Array union singleton it contains (deduplication)")
        .testEquals(
            List.of(
                toDateTime("2020-01-01T10:00:00"),
                toDateTime("2020-01-01T11:00:00"),
                toDateTime("2020-01-01T12:00:00")),
            "samePrecision1 | @2020-01-01T12:00:00",
            "Array union singleton not in array")
        .group("DateTime union - grouped/nested expressions")
        .testEquals(
            List.of(
                toDateTime("2020-01-01T10:00:00"),
                toDateTime("2020-01-01T11:00:00"),
                toDateTime("2020-01-01T12:00:00")),
            "(@2020-01-01T10:00:00 | @2020-01-01T11:00:00) | (@2020-01-01T11:00:00 |"
                + " @2020-01-01T12:00:00)",
            "Grouped union with deduplication")
        .testEquals(
            List.of(toDateTime("2020-01-01T10:00:00"), toDateTime("2020-01-01T11:00:00")),
            "(@2020-01-01T10:00:00 | @2020-01-01T10:00:00) | (@2020-01-01T11:00:00 |"
                + " @2020-01-01T11:00:00)",
            "Grouped with duplicates within groups")
        .group("DateTime union - precision handling in groups")
        .testEquals(
            List.of(
                toDateTime("2020-01-01T10:00:00"),
                toDateTime("2020-01-01T10:00"),
                toDateTime("2020-01-01T11:00:00")),
            "(@2020-01-01T10:00:00 | @2020-01-01T10:00) | (@2020-01-01T11:00:00 |"
                + " @2020-01-01T11:00:00)",
            "Mixed precision in union (incomparable, keep both 2020-01-01T10:00:00 and"
                + " 2020-01-01T10:00)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testDateDateTimeUnion() {
    return builder()
        .withSubject(
            sb ->
                sb.date("d1", "2020-01-01")
                    .date("d2", "2020-01-02")
                    .dateTime("dt1", "2020-01-01T00:00:00")
                    .dateTime("dt2", "2020-01-01T10:00:00")
                    .dateArray("dateArray", "2020-01-01", "2020-01-02")
                    .dateTimeArray("dateTimeArray", "2020-01-01T00:00:00", "2020-01-01T10:00:00"))
        .group("Date and DateTime union - type promotion with precision differences")
        .testEquals(
            List.of(toDate("2020-01-01"), toDateTime("2020-01-01T00:00:00")),
            "@2020-01-01 | @2020-01-01T00:00:00",
            "Date union DateTime (type promotion, but different precision - incomparable, keep"
                + " both)")
        .testEquals(
            List.of(toDate("2020-01-01"), toDateTime("2020-01-01T10:00:00")),
            "@2020-01-01 | @2020-01-01T10:00:00",
            "Date union DateTime with different instant (type promotion, incomparable)")
        .testEquals(
            List.of(toDate("2020-01-01"), toDateTime("2020-01-01T00:00:00")),
            "d1 | dt1",
            "Date variable union DateTime variable (type promotion, but different precision -"
                + " incomparable, keep both)")
        .testEquals(
            List.of(toDate("2020-01-01"), toDateTime("2020-01-01T10:00:00")),
            "d1 | dt2",
            "Date variable union DateTime variable with different instant (type promotion,"
                + " incomparable)")
        .testEquals(
            List.of(
                toDate("2020-01-01"),
                toDate("2020-01-02"),
                toDateTime("2020-01-01T00:00:00"),
                toDateTime("2020-01-01T10:00:00")),
            "dateArray | dateTimeArray",
            "Date array union DateTime array (type promotion, but different precision -"
                + " incomparable, keep all)")
        .testEquals(
            List.of(
                toDate("2020-01-02"),
                toDate("2020-01-01"),
                toDateTime("2020-01-01T00:00:00"),
                toDateTime("2020-01-01T10:00:00")),
            "d2 | (d1 | dateTimeArray)",
            "Mixed Date and DateTime in grouped union (type promotion, incomparable)")
        .build();
  }
}
