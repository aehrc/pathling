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

/**
 * Tests for {@link DatePrefix}.
 */
class DatePrefixTest {

  static Stream<Arguments> prefixParsingCases() {
    return Stream.of(
        // Explicit prefixes
        Arguments.of("eq2023-01-15", DatePrefix.EQ, "2023-01-15"),
        Arguments.of("ne2023-01-15", DatePrefix.NE, "2023-01-15"),
        Arguments.of("gt2023-01-15", DatePrefix.GT, "2023-01-15"),
        Arguments.of("ge2023-01-15", DatePrefix.GE, "2023-01-15"),
        Arguments.of("lt2023-01-15", DatePrefix.LT, "2023-01-15"),
        Arguments.of("le2023-01-15", DatePrefix.LE, "2023-01-15"),

        // No prefix defaults to EQ
        Arguments.of("2023-01-15", DatePrefix.EQ, "2023-01-15"),
        Arguments.of("2023-01", DatePrefix.EQ, "2023-01"),
        Arguments.of("2023", DatePrefix.EQ, "2023"),

        // Different precisions with prefixes
        Arguments.of("ge2023-01", DatePrefix.GE, "2023-01"),
        Arguments.of("lt2023", DatePrefix.LT, "2023"),
        Arguments.of("le2023-01-15T10:00", DatePrefix.LE, "2023-01-15T10:00"),
        Arguments.of("gt2023-01-15T10:00:30", DatePrefix.GT, "2023-01-15T10:00:30"),

        // Year-only values
        Arguments.of("1990", DatePrefix.EQ, "1990"),
        Arguments.of("ge1990", DatePrefix.GE, "1990"),
        Arguments.of("lt2000", DatePrefix.LT, "2000")
    );
  }

  @ParameterizedTest(name = "fromValue(\"{0}\") = {1}")
  @MethodSource("prefixParsingCases")
  void testFromValue(final String value, final DatePrefix expectedPrefix,
      final String ignoredExpectedDate) {
    assertEquals(expectedPrefix, DatePrefix.fromValue(value));
  }

  @ParameterizedTest(name = "stripPrefix(\"{0}\") = \"{2}\"")
  @MethodSource("prefixParsingCases")
  void testStripPrefix(final String value, final DatePrefix ignoredPrefix, final String expectedDate) {
    assertEquals(expectedDate, DatePrefix.stripPrefix(value));
  }
}
