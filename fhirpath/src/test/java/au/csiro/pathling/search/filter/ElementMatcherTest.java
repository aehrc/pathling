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

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.test.SpringBootUnitTest;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Unit tests for {@link ElementMatcher} implementations.
 * <p>
 * Tests matching logic in isolation using simple DataFrames without full FHIR resources.
 */
@SpringBootUnitTest
class ElementMatcherTest {

  @Autowired
  SparkSession spark;

  // ========== TokenMatcher tests ==========

  static Stream<Arguments> tokenMatcherCases() {
    return Stream.of(
        // Exact match
        Arguments.of("male", "male", true),
        Arguments.of("female", "female", true),
        // Case-sensitive - no match
        Arguments.of("MALE", "male", false),
        Arguments.of("Male", "male", false),
        // Different values - no match
        Arguments.of("female", "male", false),
        Arguments.of("other", "male", false),
        // Empty string
        Arguments.of("", "", true),
        Arguments.of("male", "", false)
    );
  }

  @ParameterizedTest(name = "TokenMatcher: \"{0}\" matches \"{1}\" = {2}")
  @MethodSource("tokenMatcherCases")
  void testTokenMatcher(final String element, final String searchValue, final boolean expected) {
    final Dataset<Row> df = spark.createDataset(List.of(element), Encoders.STRING())
        .toDF("value");

    final TokenMatcher matcher = new TokenMatcher();
    final Column result = matcher.match(col("value"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }

  // ========== StringMatcher tests ==========

  static Stream<Arguments> stringMatcherCases() {
    return Stream.of(
        // Exact match (case-insensitive)
        Arguments.of("Smith", "Smith", true),
        Arguments.of("Smith", "smith", true),
        Arguments.of("smith", "Smith", true),
        Arguments.of("SMITH", "smith", true),
        // Starts with (case-insensitive)
        Arguments.of("Smith", "Smi", true),
        Arguments.of("Smith", "smi", true),
        Arguments.of("SMITH", "smi", true),
        Arguments.of("Smithson", "Smith", true),
        // Full match as starts-with
        Arguments.of("Jones", "Jones", true),
        // No match - different strings
        Arguments.of("Jones", "Smith", false),
        Arguments.of("Brown", "Bro", true),
        Arguments.of("Brown", "Brown", true),
        Arguments.of("Brown", "Browning", false),  // search longer than element
        // No match - not a prefix
        Arguments.of("Smith", "mith", false),
        Arguments.of("McSmith", "Smith", false),
        // Empty string
        Arguments.of("Smith", "", true),  // empty string matches everything as prefix
        Arguments.of("", "", true)
    );
  }

  @ParameterizedTest(name = "StringMatcher: \"{0}\" matches \"{1}\" = {2}")
  @MethodSource("stringMatcherCases")
  void testStringMatcher(final String element, final String searchValue, final boolean expected) {
    final Dataset<Row> df = spark.createDataset(List.of(element), Encoders.STRING())
        .toDF("value");

    final StringMatcher matcher = new StringMatcher();
    final Column result = matcher.match(col("value"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }

  // ========== ExactStringMatcher tests ==========

  static Stream<Arguments> exactStringMatcherCases() {
    return Stream.of(
        // Exact match - case-sensitive
        Arguments.of("Smith", "Smith", true),
        Arguments.of("Jones", "Jones", true),
        // Case matters - no match
        Arguments.of("Smith", "smith", false),
        Arguments.of("smith", "Smith", false),
        Arguments.of("SMITH", "Smith", false),
        Arguments.of("SMITH", "smith", false),
        // Must match exactly - not prefix
        Arguments.of("Smithson", "Smith", false),
        Arguments.of("Smith", "Smi", false),
        // Different values - no match
        Arguments.of("Jones", "Smith", false),
        Arguments.of("Brown", "Browning", false),
        // Empty string
        Arguments.of("", "", true),
        Arguments.of("Smith", "", false),
        Arguments.of("", "Smith", false)
    );
  }

  @ParameterizedTest(name = "ExactStringMatcher: \"{0}\" matches \"{1}\" = {2}")
  @MethodSource("exactStringMatcherCases")
  void testExactStringMatcher(final String element, final String searchValue,
      final boolean expected) {
    final Dataset<Row> df = spark.createDataset(List.of(element), Encoders.STRING())
        .toDF("value");

    final ExactStringMatcher matcher = new ExactStringMatcher();
    final Column result = matcher.match(col("value"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }

  // ========== DateMatcher tests ==========

  static Stream<Arguments> dateMatcherCases() {
    return Stream.of(
        // ========== eq prefix (default) - ranges overlap ==========
        // Same precision (day) - exact match
        Arguments.of("2013-01-14", "2013-01-14", true),
        Arguments.of("2013-01-14", "eq2013-01-14", true),
        Arguments.of("1990-05-20", "1990-05-20", true),

        // Same precision (day) - no overlap
        Arguments.of("2013-01-14", "2013-01-15", false),
        Arguments.of("2013-01-14", "2013-02-14", false),
        Arguments.of("2013-01-14", "2014-01-14", false),

        // Coarser search precision (year-month) - overlaps with day in that month
        Arguments.of("2013-01-14", "2013-01", true),
        Arguments.of("2013-01-14", "eq2013-01", true),
        Arguments.of("2013-01-01", "2013-01", true),
        Arguments.of("2013-01-31", "2013-01", true),
        Arguments.of("2013-01-14", "2013-02", false),
        Arguments.of("2013-02-01", "2013-01", false),

        // Coarser search precision (year) - overlaps with day in that year
        Arguments.of("2013-01-14", "2013", true),
        Arguments.of("2013-12-31", "2013", true),
        Arguments.of("2013-01-14", "2014", false),
        Arguments.of("2014-01-01", "2013", false),

        // Finer search precision (datetime) - time within the day
        Arguments.of("2013-01-14", "2013-01-14T00:00", true),
        Arguments.of("2013-01-14", "2013-01-14T10:00", true),
        Arguments.of("2013-01-14", "2013-01-14T23:59", true),
        Arguments.of("2013-01-14", "2013-01-14T23:59:59", true),

        // Finer search precision (datetime) - time in different day should not match
        Arguments.of("2013-01-14", "2013-01-15T00:00", false),
        Arguments.of("2013-01-14", "2013-01-13T23:59:59", false),

        // ========== ne prefix - no overlap ==========
        Arguments.of("2023-01-15", "ne2023-01-15", false),   // same day = overlap exists
        Arguments.of("2023-01-15", "ne2023-01-14", true),    // different day = no overlap
        Arguments.of("2023-01-15", "ne2023-01-16", true),    // different day = no overlap
        Arguments.of("2023-01-15", "ne2023-02", true),       // different month = no overlap
        Arguments.of("2023-01-15", "ne2023-01", false),      // same month = overlap exists
        Arguments.of("2023-01-15", "ne2024", true),          // different year = no overlap

        // ========== gt prefix - resource ends after parameter ==========
        Arguments.of("2023-01-15", "gt2023-01-14", true),    // 15 ends after 14
        Arguments.of("2023-01-15", "gt2023-01-15", false),   // 15 does not end after 15
        Arguments.of("2023-01-15", "gt2023-01-16", false),   // 15 does not end after 16
        Arguments.of("2023-01-15", "gt2023-01", false),      // day doesn't end after month containing it
        Arguments.of("2023-02-01", "gt2023-01", true),       // Feb 1 ends after Jan

        // ========== ge prefix - resource starts at or after parameter start ==========
        Arguments.of("2023-01-15", "ge2023-01-14", true),    // 15 >= 14
        Arguments.of("2023-01-15", "ge2023-01-15", true),    // 15 >= 15
        Arguments.of("2023-01-15", "ge2023-01-16", false),   // 15 not >= 16
        Arguments.of("2023-01-15", "ge2023-01", true),       // 15 starts after month start
        Arguments.of("2023-01-01", "ge2023-01", true),       // 1st starts at month start
        Arguments.of("2022-12-31", "ge2023-01", false),      // Dec 31 before Jan start

        // ========== lt prefix - resource starts before parameter ==========
        Arguments.of("2023-01-15", "lt2023-01-16", true),    // 15 < 16
        Arguments.of("2023-01-15", "lt2023-01-15", false),   // 15 not < 15
        Arguments.of("2023-01-15", "lt2023-01-14", false),   // 15 not < 14
        Arguments.of("2023-01-15", "lt2023-02", true),       // 15 starts before Feb
        Arguments.of("2023-01-15", "lt2023-01", false),      // 15 not before Jan start

        // ========== le prefix - resource ends at or before parameter end ==========
        Arguments.of("2023-01-15", "le2023-01-16", true),    // 15 ends <= 16 end
        Arguments.of("2023-01-15", "le2023-01-15", true),    // 15 ends <= 15 end
        Arguments.of("2023-01-15", "le2023-01-14", false),   // 15 ends not <= 14 end
        Arguments.of("2023-01-15", "le2023-01", true),       // 15 ends within Jan
        Arguments.of("2023-01-31", "le2023-01", true),       // last day ends at month end
        Arguments.of("2023-02-01", "le2023-01", false)       // Feb 1 ends after Jan
    );
  }

  @ParameterizedTest(name = "DateMatcher: \"{0}\" matches \"{1}\" = {2}")
  @MethodSource("dateMatcherCases")
  void testDateMatcher(final String element, final String searchValue, final boolean expected) {
    final Dataset<Row> df = spark.createDataset(List.of(element), Encoders.STRING())
        .toDF("value");

    final DateMatcher matcher = new DateMatcher();
    final Column result = matcher.match(col("value"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }
}
