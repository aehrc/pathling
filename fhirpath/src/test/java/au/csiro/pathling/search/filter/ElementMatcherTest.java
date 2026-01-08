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
}
