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
        .withSubject(sb -> sb
            .boolArray("trueFalseArray", true, false)
            .boolArray("falseTrueArray", false, true)
            .boolArray("trueTrueArray", true, true)
            .boolEmpty("emptyBool")
        )
        .group("Boolean union - empty collections")
        .testEquals(List.of(true), "true | {}", "True union empty literal")
        .testEquals(List.of(true), "{} | true", "Empty literal union true")
        .testEquals(List.of(false), "false | {}", "False union empty literal")
        .testEquals(List.of(false), "{} | false", "Empty literal union false")
        .testEquals(List.of(true), "true | emptyBool", "True union empty Boolean")
        .testEquals(List.of(true), "emptyBool | true", "Empty Boolean union true")
        .testEmpty("emptyBool | emptyBool", "Empty Boolean union empty Boolean")
        .testEquals(List.of(true), "{} | trueTrueArray", "Empty union [true, true] deduplicates to [true]")
        .testEquals(List.of(true), "trueTrueArray | {}", "[true, true] union empty deduplicates to [true]")
        .group("Boolean union - single values")
        .testEquals(List.of(true, false), "true | false", "True union false")
        .testEquals(List.of(false, true), "false | true", "False union true")
        .testEquals(List.of(true), "true | true", "True union true (deduplication)")
        .testEquals(List.of(false), "false | false", "False union false (deduplication)")
        .group("Boolean union - arrays")
        .testEquals(List.of(true, false), "trueFalseArray | falseTrueArray",
            "Array [true, false] union [false, true] (deduplication)")
        .testEquals(List.of(true, false), "trueFalseArray | trueFalseArray",
            "Array union itself (deduplication)")
        .group("Boolean union - mixed singleton and array")
        .testEquals(List.of(true, false), "true | trueFalseArray",
            "True singleton union [true, false] array")
        .testEquals(List.of(true, false), "trueFalseArray | true",
            "Array [true, false] union true singleton")
        .testEquals(List.of(false, true), "false | trueFalseArray",
            "False singleton union [true, false] array")
        .testEquals(List.of(true, false), "trueFalseArray | false",
            "Array [true, false] union false singleton")
        .group("Boolean union - grouped/nested expressions")
        .testEquals(List.of(true, false), "(true | false) | (false | true)",
            "Grouped: (true | false) | (false | true)")
        .testEquals(List.of(true, false), "(true | true) | (false | false)",
            "Grouped with duplicates: (true | true) | (false | false)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testIntegerUnion() {
    return builder()
        .withSubject(sb -> sb
            .integerArray("oneTwoThree", 1, 2, 3)
            .integerArray("twoThreeFour", 2, 3, 4)
            .integerArray("oneOneTwoThree", 1, 1, 2, 3)
            .integerArray("oneOne", 1, 1)
            .integerEmpty("emptyInt")
        )
        .group("Integer union - empty collections")
        .testEquals(List.of(1), "1 | {}", "1 union empty literal")
        .testEquals(List.of(1), "{} | 1", "Empty literal union 1")
        .testEquals(List.of(1), "1 | emptyInt", "1 union empty Integer")
        .testEquals(List.of(1), "emptyInt | 1", "Empty Integer union 1")
        .testEmpty("emptyInt | emptyInt", "Empty Integer union empty Integer")
        .testEquals(List.of(1), "{} | oneOne", "Empty union [1, 1] deduplicates to [1]")
        .testEquals(List.of(1), "oneOne | {}", "[1, 1] union empty deduplicates to [1]")
        .group("Integer union - single values")
        .testEquals(List.of(1, 2), "1 | 2", "1 union 2")
        .testEquals(List.of(2, 1), "2 | 1", "2 union 1")
        .testEquals(List.of(1), "1 | 1", "1 union 1 (deduplication)")
        .group("Integer union - arrays")
        .testEquals(List.of(1, 2, 3, 4), "oneTwoThree | twoThreeFour",
            "Array [1, 2, 3] union [2, 3, 4] (deduplication)")
        .testEquals(List.of(1, 2, 3), "oneTwoThree | oneTwoThree",
            "Array union itself (deduplication)")
        .testEquals(List.of(1, 2, 3), "oneOneTwoThree | oneTwoThree",
            "Array [1, 1, 2, 3] union [1, 2, 3] (deduplication)")
        .group("Integer union - mixed singleton and array")
        .testEquals(List.of(1, 2, 3), "1 | oneTwoThree",
            "1 singleton union [1, 2, 3] array")
        .testEquals(List.of(1, 2, 3), "oneTwoThree | 1",
            "Array [1, 2, 3] union 1 singleton")
        .testEquals(List.of(1, 2, 3, 4), "oneTwoThree | 4",
            "Array [1, 2, 3] union 4 literal")
        .group("Integer union - grouped/nested expressions")
        .testEquals(List.of(1, 2, 3), "(1 | 2) | (2 | 3)",
            "Grouped: (1 | 2) | (2 | 3)")
        .testEquals(List.of(1, 2), "(1 | 1) | (2 | 2)",
            "Grouped with duplicates: (1 | 1) | (2 | 2)")
        .testEquals(List.of(1, 2, 3, 4), "(1 | 2) | (3 | 4)",
            "Grouped with literal: (1 | 2) | (3 | 4)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testStringUnion() {
    return builder()
        .withSubject(sb -> sb
            .stringArray("abc", "a", "b", "c")
            .stringArray("bcd", "b", "c", "d")
            .stringArray("aabbc", "a", "a", "b", "b", "c")
            .stringArray("aaa", "a", "a", "a")
            .stringEmpty("emptyString")
        )
        .group("String union - empty collections")
        .testEquals(List.of("a"), "'a' | {}", "'a' union empty literal")
        .testEquals(List.of("a"), "{} | 'a'", "Empty literal union 'a'")
        .testEquals(List.of("a"), "'a' | emptyString", "'a' union empty String")
        .testEquals(List.of("a"), "emptyString | 'a'", "Empty String union 'a'")
        .testEmpty("emptyString | emptyString", "Empty String union empty String")
        .testEquals(List.of("a"), "{} | aaa", "Empty union ['a', 'a', 'a'] deduplicates to ['a']")
        .testEquals(List.of("a"), "aaa | {}", "['a', 'a', 'a'] union empty deduplicates to ['a']")
        .group("String union - single values")
        .testEquals(List.of("a", "b"), "'a' | 'b'", "'a' union 'b'")
        .testEquals(List.of("b", "a"), "'b' | 'a'", "'b' union 'a'")
        .testEquals(List.of("a"), "'a' | 'a'", "'a' union 'a' (deduplication)")
        .group("String union - arrays")
        .testEquals(List.of("a", "b", "c", "d"), "abc | bcd",
            "Array ['a', 'b', 'c'] union ['b', 'c', 'd'] (deduplication)")
        .testEquals(List.of("a", "b", "c"), "abc | abc",
            "Array union itself (deduplication)")
        .testEquals(List.of("a", "b", "c"), "aabbc | abc",
            "Array ['a', 'a', 'b', 'b', 'c'] union ['a', 'b', 'c'] (deduplication)")
        .group("String union - mixed singleton and array")
        .testEquals(List.of("a", "b", "c"), "'a' | abc",
            "'a' singleton union ['a', 'b', 'c'] array")
        .testEquals(List.of("a", "b", "c"), "abc | 'a'",
            "Array ['a', 'b', 'c'] union 'a' singleton")
        .testEquals(List.of("a", "b", "c", "d"), "abc | 'd'",
            "Array ['a', 'b', 'c'] union 'd' literal")
        .group("String union - grouped/nested expressions")
        .testEquals(List.of("a", "b", "c"), "('a' | 'b') | ('b' | 'c')",
            "Grouped: ('a' | 'b') | ('b' | 'c')")
        .testEquals(List.of("a", "b"), "('a' | 'a') | ('b' | 'b')",
            "Grouped with duplicates: ('a' | 'a') | ('b' | 'b')")
        .testEquals(List.of("a", "b", "c", "d"), "('a' | 'b') | ('c' | 'd')",
            "Grouped with literal: ('a' | 'b') | ('c' | 'd')")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testDecimalUnion() {
    return builder()
        .withSubject(sb -> sb
            .decimalArray("dec123", 1.1, 2.2, 3.3)
            .decimalArray("dec234", 2.2, 3.3, 4.4)
            .decimalArray("dec111", 1.1, 1.1)
            .decimalEmpty("emptyDec")
        )
        .group("Decimal union - empty collections")
        .testEquals(List.of(2.5), "2.5 | {}", "2.5 union empty literal")
        .testEquals(List.of(2.5), "{} | 2.5", "Empty literal union 2.5")
        .testEquals(List.of(2.5), "2.5 | emptyDec", "2.5 union empty Decimal")
        .testEquals(List.of(2.5), "emptyDec | 2.5", "Empty Decimal union 2.5")
        .testEmpty("emptyDec | emptyDec", "Empty Decimal union empty Decimal")
        .testEquals(List.of(1.1), "{} | dec111", "Empty union [1.1, 1.1] deduplicates to [1.1]")
        .testEquals(List.of(1.1), "dec111 | {}", "[1.1, 1.1] union empty deduplicates to [1.1]")
        .group("Decimal union - single values")
        .testEquals(List.of(2.5, 5.5), "2.5 | 5.5", "2.5 union 5.5")
        .testEquals(List.of(5.5, 2.5), "5.5 | 2.5", "5.5 union 2.5")
        .testEquals(List.of(2.5), "2.5 | 2.5", "2.5 union 2.5 (deduplication)")
        .group("Decimal union - precision variations")
        .testEquals(List.of(2.5), "2.5 | 2.50", "2.5 union 2.50 (different precision, same value)")
        .testEquals(List.of(1.1), "1.1 | 1.10", "1.1 union 1.10 (trailing zeros)")
        .testEquals(List.of(1.0), "1.0 | 1.00", "1.0 union 1.00 (trailing zeros after decimal)")
        .group("Decimal union - arrays")
        .testEquals(List.of(1.1, 2.2, 3.3, 4.4), "dec123 | dec234",
            "Array [1.1, 2.2, 3.3] union [2.2, 3.3, 4.4] (deduplication)")
        .testEquals(List.of(1.1, 2.2, 3.3), "dec123 | dec123",
            "Array union itself (deduplication)")
        .group("Decimal union - mixed with Integer")
        .testEquals(List.of(2.5, 5.0), "2.5 | 5", "Decimal union integer (integer promoted to decimal)")
        .testEquals(List.of(5.0, 2.5), "5 | 2.5", "Integer union decimal (integer promoted to decimal)")
        .testEquals(List.of(1.0, 2.0), "1.0 | 2", "1.0 union 2")
        .testEquals(List.of(1.0), "1.0 | 1", "1.0 union 1 (should deduplicate)")
        .group("Decimal union - grouped/nested expressions")
        .testEquals(List.of(1.1, 2.2, 3.3), "(1.1 | 2.2) | (2.2 | 3.3)",
            "Grouped: (1.1 | 2.2) | (2.2 | 3.3)")
        .testEquals(List.of(1.1, 2.2), "(1.1 | 1.1) | (2.2 | 2.2)",
            "Grouped with duplicates: (1.1 | 1.1) | (2.2 | 2.2)")
        .build();
  }
}
