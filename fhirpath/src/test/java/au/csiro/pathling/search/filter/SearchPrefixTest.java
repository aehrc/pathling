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

package au.csiro.pathling.search.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link SearchPrefix}. */
class SearchPrefixTest {

  static Stream<Arguments> prefixParsingCases() {
    return Stream.of(
        // Explicit prefixes with date values
        Arguments.of("eq2023-01-15", SearchPrefix.EQ, "2023-01-15"),
        Arguments.of("ne2023-01-15", SearchPrefix.NE, "2023-01-15"),
        Arguments.of("gt2023-01-15", SearchPrefix.GT, "2023-01-15"),
        Arguments.of("ge2023-01-15", SearchPrefix.GE, "2023-01-15"),
        Arguments.of("lt2023-01-15", SearchPrefix.LT, "2023-01-15"),
        Arguments.of("le2023-01-15", SearchPrefix.LE, "2023-01-15"),

        // Date values without prefix default to EQ
        Arguments.of("2023-01-15", SearchPrefix.EQ, "2023-01-15"),
        Arguments.of("2023-01", SearchPrefix.EQ, "2023-01"),
        Arguments.of("2023", SearchPrefix.EQ, "2023"),

        // Different date precisions with prefixes
        Arguments.of("ge2023-01", SearchPrefix.GE, "2023-01"),
        Arguments.of("lt2023", SearchPrefix.LT, "2023"),
        Arguments.of("le2023-01-15T10:00", SearchPrefix.LE, "2023-01-15T10:00"),
        Arguments.of("gt2023-01-15T10:00:30", SearchPrefix.GT, "2023-01-15T10:00:30"),

        // Explicit prefixes with number values
        Arguments.of("eq0.5", SearchPrefix.EQ, "0.5"),
        Arguments.of("ne100", SearchPrefix.NE, "100"),
        Arguments.of("gt0.8", SearchPrefix.GT, "0.8"),
        Arguments.of("ge-5", SearchPrefix.GE, "-5"),
        Arguments.of("lt1e2", SearchPrefix.LT, "1e2"),
        Arguments.of("le0.001", SearchPrefix.LE, "0.001"),

        // Number values without prefix default to EQ
        Arguments.of("0.5", SearchPrefix.EQ, "0.5"),
        Arguments.of("100", SearchPrefix.EQ, "100"),
        Arguments.of("-5.5", SearchPrefix.EQ, "-5.5"),

        // Decimal numbers with prefixes
        Arguments.of("ge0.75", SearchPrefix.GE, "0.75"),
        Arguments.of("lt0.25", SearchPrefix.LT, "0.25"),

        // Negative numbers with prefixes
        Arguments.of("ge-10", SearchPrefix.GE, "-10"),
        Arguments.of("lt-0.5", SearchPrefix.LT, "-0.5"),

        // Case insensitive prefixes
        Arguments.of("EQ0.5", SearchPrefix.EQ, "0.5"),
        Arguments.of("GE100", SearchPrefix.GE, "100"),
        Arguments.of("LT2023-01-15", SearchPrefix.LT, "2023-01-15"),

        // Scientific notation
        Arguments.of("1e2", SearchPrefix.EQ, "1e2"),
        Arguments.of("gt1e-3", SearchPrefix.GT, "1e-3"),
        Arguments.of("le5e10", SearchPrefix.LE, "5e10"));
  }

  @ParameterizedTest(name = "fromValue(\"{0}\") = {1}")
  @MethodSource("prefixParsingCases")
  void testFromValue(
      final String value, final SearchPrefix expectedPrefix, final String ignoredExpectedValue) {
    assertEquals(expectedPrefix, SearchPrefix.fromValue(value));
  }

  @ParameterizedTest(name = "stripPrefix(\"{0}\") = \"{2}\"")
  @MethodSource("prefixParsingCases")
  void testStripPrefix(
      final String value, final SearchPrefix ignoredPrefix, final String expectedValue) {
    assertEquals(expectedValue, SearchPrefix.stripPrefix(value));
  }
}
