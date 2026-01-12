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
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.test.SpringBootUnitTest;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
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

  @ParameterizedTest(name = "TokenMatcher(code): \"{0}\" matches \"{1}\" = {2}")
  @MethodSource("tokenMatcherCases")
  void testTokenMatcher(final String element, final String searchValue, final boolean expected) {
    final Dataset<Row> df = spark.createDataset(List.of(element), Encoders.STRING())
        .toDF("value");

    final TokenMatcher matcher = new TokenMatcher(FHIRDefinedType.CODE);
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

  // ========== NumberMatcher tests ==========
  // Note: eq/ne use range-based semantics based on significant figures
  // Comparison prefixes (gt, ge, lt, le) use exact value semantics

  static Stream<Arguments> numberMatcherCases() {
    return Stream.of(
        // ========== eq prefix (default) - range-based matching ==========
        // "100" (3 sig figs) matches range [99.5, 100.5)
        Arguments.of(new BigDecimal("100"), "100", true),       // exact match - within range
        Arguments.of(new BigDecimal("99.7"), "100", true),      // within [99.5, 100.5)
        Arguments.of(new BigDecimal("100.4"), "100", true),     // within [99.5, 100.5)
        Arguments.of(new BigDecimal("99.4"), "100", false),     // below 99.5
        Arguments.of(new BigDecimal("100.5"), "100", false),    // at upper boundary (exclusive)
        Arguments.of(new BigDecimal("100.6"), "100", false),    // above 100.5

        // "100.00" (5 sig figs) matches range [99.995, 100.005)
        Arguments.of(new BigDecimal("100"), "100.00", true),      // within range
        Arguments.of(new BigDecimal("99.999"), "100.00", true),   // within range
        Arguments.of(new BigDecimal("100.004"), "100.00", true),  // within range
        Arguments.of(new BigDecimal("99.99"), "100.00", false),   // outside range
        Arguments.of(new BigDecimal("100.01"), "100.00", false),  // outside range

        // "0.75" (2 sig figs) matches range [0.745, 0.755)
        Arguments.of(new BigDecimal("0.75"), "0.75", true),
        Arguments.of(new BigDecimal("0.75"), "eq0.75", true),
        Arguments.of(new BigDecimal("0.749"), "0.75", true),      // within [0.745, 0.755)
        Arguments.of(new BigDecimal("0.744"), "0.75", false),     // outside range
        Arguments.of(new BigDecimal("0.755"), "0.75", false),     // at upper boundary (exclusive)

        // Negative values: "-5.5" (2 sig figs) matches range [-5.55, -5.45)
        Arguments.of(new BigDecimal("-5.5"), "-5.5", true),
        Arguments.of(new BigDecimal("-5.52"), "-5.5", true),      // within range
        Arguments.of(new BigDecimal("-5.48"), "-5.5", true),      // within range
        Arguments.of(new BigDecimal("-5.56"), "-5.5", false),     // outside range
        Arguments.of(new BigDecimal("-5.44"), "-5.5", false),     // outside range

        // ========== ne prefix - outside range ==========
        Arguments.of(new BigDecimal("100"), "ne100", false),      // within [99.5, 100.5) - no match
        Arguments.of(new BigDecimal("99.4"), "ne100", true),      // outside range - matches
        Arguments.of(new BigDecimal("100.5"), "ne100", true),     // at upper boundary - matches
        Arguments.of(new BigDecimal("0.75"), "ne0.75", false),    // within range
        Arguments.of(new BigDecimal("0.76"), "ne0.75", true),     // outside range

        // ========== gt prefix - greater than (exact semantics) ==========
        Arguments.of(new BigDecimal("0.75"), "gt0.5", true),
        Arguments.of(new BigDecimal("0.75"), "gt0.74", true),
        Arguments.of(new BigDecimal("0.75"), "gt0.75", false),
        Arguments.of(new BigDecimal("0.75"), "gt0.76", false),
        Arguments.of(new BigDecimal("0.75"), "gt0.8", false),
        Arguments.of(new BigDecimal("-5"), "gt-10", true),
        Arguments.of(new BigDecimal("-5"), "gt0", false),

        // ========== ge prefix - greater or equal (exact semantics) ==========
        Arguments.of(new BigDecimal("0.75"), "ge0.5", true),
        Arguments.of(new BigDecimal("0.75"), "ge0.75", true),
        Arguments.of(new BigDecimal("0.75"), "ge0.76", false),
        Arguments.of(new BigDecimal("0.75"), "ge0.8", false),
        Arguments.of(new BigDecimal("100"), "ge100", true),
        Arguments.of(new BigDecimal("100"), "ge99", true),
        Arguments.of(new BigDecimal("100"), "ge101", false),

        // ========== lt prefix - less than (exact semantics) ==========
        Arguments.of(new BigDecimal("0.75"), "lt0.8", true),
        Arguments.of(new BigDecimal("0.75"), "lt0.76", true),
        Arguments.of(new BigDecimal("0.75"), "lt0.75", false),
        Arguments.of(new BigDecimal("0.75"), "lt0.5", false),
        Arguments.of(new BigDecimal("-5"), "lt0", true),
        Arguments.of(new BigDecimal("-5"), "lt-10", false),

        // ========== le prefix - less or equal (exact semantics) ==========
        Arguments.of(new BigDecimal("0.75"), "le0.8", true),
        Arguments.of(new BigDecimal("0.75"), "le0.75", true),
        Arguments.of(new BigDecimal("0.75"), "le0.74", false),
        Arguments.of(new BigDecimal("0.75"), "le0.5", false),
        Arguments.of(new BigDecimal("100"), "le100", true),
        Arguments.of(new BigDecimal("100"), "le101", true),
        Arguments.of(new BigDecimal("100"), "le99", false),

        // ========== Scientific notation with range semantics ==========
        // "1e2" (1 sig fig) matches range [50, 150)
        Arguments.of(new BigDecimal("100"), "1e2", true),         // exact value
        Arguments.of(new BigDecimal("50"), "1e2", true),          // at lower boundary (inclusive)
        Arguments.of(new BigDecimal("149"), "1e2", true),         // within range
        Arguments.of(new BigDecimal("49"), "1e2", false),         // below lower boundary
        Arguments.of(new BigDecimal("150"), "1e2", false),        // at upper boundary (exclusive)

        // "1e-3" (1 sig fig) matches range [0.0005, 0.0015)
        Arguments.of(new BigDecimal("0.001"), "1e-3", true),      // exact value
        Arguments.of(new BigDecimal("0.0005"), "1e-3", true),     // at lower boundary
        Arguments.of(new BigDecimal("0.0014"), "1e-3", true),     // within range

        // Scientific notation with exact semantics
        Arguments.of(new BigDecimal("1000"), "gt1e2", true),      // 1000 > 100
        Arguments.of(new BigDecimal("50"), "lt1e2", true)         // 50 < 100
    );
  }

  @ParameterizedTest(name = "NumberMatcher(DECIMAL): {0} matches \"{1}\" = {2}")
  @MethodSource("numberMatcherCases")
  void testNumberMatcher(final BigDecimal element, final String searchValue,
      final boolean expected) {
    final Dataset<Row> df = spark.createDataset(List.of(element), Encoders.DECIMAL())
        .toDF("value");

    // Use DECIMAL type for the existing tests (range-based semantics for eq/ne)
    final NumberMatcher matcher = new NumberMatcher(FHIRDefinedType.DECIMAL);
    final Column result = matcher.match(col("value"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }

  // ========== NumberMatcher tests for INTEGER types ==========
  // Integer types use exact match semantics for eq/ne

  static Stream<Arguments> integerNumberMatcherCases() {
    return Stream.of(
        // ========== eq prefix - exact match for integers ==========
        Arguments.of(100, "100", true),          // exact match
        Arguments.of(100, "eq100", true),        // explicit eq prefix
        Arguments.of(99, "100", false),          // not equal
        Arguments.of(101, "100", false),         // not equal
        Arguments.of(100, "100.0", true),        // 100.0 has no fractional part -> exact match
        Arguments.of(99, "100.0", false),        // not equal

        // Search with fractional value -> no integer can match
        Arguments.of(100, "100.5", false),       // fractional search -> no matches
        Arguments.of(100, "99.5", false),        // fractional search -> no matches
        Arguments.of(1, "0.5", false),           // fractional search -> no matches

        // ========== ne prefix - not equal for integers ==========
        Arguments.of(100, "ne100", false),       // equal -> not a match
        Arguments.of(99, "ne100", true),         // not equal -> matches
        Arguments.of(101, "ne100", true),        // not equal -> matches

        // Search with fractional value -> all integers match ne
        Arguments.of(100, "ne100.5", true),      // all integers ne fractional
        Arguments.of(1, "ne0.5", true),          // all integers ne fractional

        // ========== Comparison prefixes - same as decimal (exact semantics) ==========
        Arguments.of(100, "gt99", true),
        Arguments.of(100, "gt100", false),
        Arguments.of(100, "ge100", true),
        Arguments.of(100, "ge101", false),
        Arguments.of(100, "lt101", true),
        Arguments.of(100, "lt100", false),
        Arguments.of(100, "le100", true),
        Arguments.of(100, "le99", false)
    );
  }

  @ParameterizedTest(name = "NumberMatcher(INTEGER): {0} matches \"{1}\" = {2}")
  @MethodSource("integerNumberMatcherCases")
  void testIntegerNumberMatcher(final Integer element, final String searchValue,
      final boolean expected) {
    final Dataset<Row> df = spark.createDataset(List.of(element), Encoders.INT())
        .toDF("value");

    // Use INTEGER type for integer tests (exact match semantics for eq/ne)
    final NumberMatcher matcher = new NumberMatcher(FHIRDefinedType.INTEGER);
    final Column result = matcher.match(col("value"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }

  // ========== DateMatcher Period tests ==========

  static Stream<Arguments> periodMatcherCases() {
    return Stream.of(
        // ========== eq prefix (default) - ranges overlap ==========
        // Period fully within search range
        Arguments.of("2023-01-15", "2023-06-30", "2023-03-15", true),
        Arguments.of("2023-01-15", "2023-06-30", "2023-01", true),
        Arguments.of("2023-01-15", "2023-06-30", "2023", true),
        // Period partially overlaps search range
        Arguments.of("2023-01-15", "2023-06-30", "2023-06-15", true),  // overlaps end
        Arguments.of("2023-01-15", "2023-06-30", "2023-01-15", true),  // overlaps start (same day as Period start)
        // Period does not overlap search range
        Arguments.of("2023-01-15", "2023-06-30", "2022-12-01", false),
        Arguments.of("2023-01-15", "2023-06-30", "2023-07-01", false),

        // ========== ne prefix - no overlap ==========
        Arguments.of("2023-01-15", "2023-06-30", "ne2023-03-15", false),  // overlap exists
        Arguments.of("2023-01-15", "2023-06-30", "ne2022-12-01", true),   // no overlap
        Arguments.of("2023-01-15", "2023-06-30", "ne2023-07-01", true),   // no overlap

        // ========== ge prefix - resource starts at or after parameter start ==========
        Arguments.of("2023-06-01", "2023-12-31", "ge2023-06-01", true),   // starts at same time
        Arguments.of("2023-06-01", "2023-12-31", "ge2023-05-01", true),   // starts after
        Arguments.of("2023-06-01", "2023-12-31", "ge2023-07-01", false),  // starts before

        // ========== le prefix - resource ends at or before parameter end ==========
        Arguments.of("2023-01-01", "2023-06-30", "le2023-06-30", true),   // ends at same time
        Arguments.of("2023-01-01", "2023-06-30", "le2023-07-31", true),   // ends before
        Arguments.of("2023-01-01", "2023-06-30", "le2023-05-31", false),  // ends after

        // ========== gt prefix - resource ends after parameter ==========
        Arguments.of("2023-01-01", "2023-12-31", "gt2023-06-30", true),   // ends after Jun 30
        Arguments.of("2023-01-01", "2023-06-30", "gt2023-06-30", false),  // ends at Jun 30
        Arguments.of("2023-01-01", "2023-06-30", "gt2023-12-31", false),  // ends before Dec 31

        // ========== lt prefix - resource starts before parameter ==========
        Arguments.of("2023-01-01", "2023-06-30", "lt2023-06-01", true),   // starts before Jun 1
        Arguments.of("2023-06-01", "2023-12-31", "lt2023-06-01", false),  // starts at Jun 1
        Arguments.of("2023-06-01", "2023-12-31", "lt2023-01-01", false)   // starts after Jan 1
    );
  }

  @ParameterizedTest(name = "DateMatcher(Period): [{0}, {1}] matches \"{2}\" = {3}")
  @MethodSource("periodMatcherCases")
  void testPeriodMatcher(final String periodStart, final String periodEnd,
      final String searchValue, final boolean expected) {
    // Create a DataFrame with a Period-like struct (start, end as strings)
    final Dataset<Row> df = spark.createDataset(List.of(1), Encoders.INT())
        .select(struct(lit(periodStart).as("start"), lit(periodEnd).as("end")).as("period"));

    final DateMatcher matcher = new DateMatcher(true);  // isPeriodType = true
    final Column result = matcher.match(col("period"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }

  // ========== DateMatcher Period with null boundaries tests ==========

  static Stream<Arguments> periodNullBoundaryMatcherCases() {
    return Stream.of(
        // Open-ended future (null end = positive infinity)
        Arguments.of("2024-01-01", null, "gt2023-12-31", true),   // ends after Dec 31 (infinity > anything)
        Arguments.of("2024-01-01", null, "gt2025-12-31", true),   // still ends after (infinity)
        Arguments.of("2024-01-01", null, "le2023-12-31", false),  // doesn't end <= Dec 31

        // Open-ended past (null start = negative infinity)
        Arguments.of(null, "2022-12-31", "lt2023-01-01", true),   // starts before Jan 1 (-infinity < anything)
        Arguments.of(null, "2022-12-31", "lt2020-01-01", true),   // still starts before (-infinity)
        Arguments.of(null, "2022-12-31", "ge2023-01-01", false),  // doesn't start >= Jan 1

        // Both null (infinite range - overlaps with any eq, but ge/le have specific semantics)
        Arguments.of(null, null, "2023-06-15", true),   // eq: infinite range overlaps with anything
        Arguments.of(null, null, "ge2023-01-01", false), // ge: -infinity is NOT >= 2023-01-01
        Arguments.of(null, null, "le2023-12-31", false)  // le: +infinity is NOT <= 2023-12-31
    );
  }

  @ParameterizedTest(name = "DateMatcher(Period null): [{0}, {1}] matches \"{2}\" = {3}")
  @MethodSource("periodNullBoundaryMatcherCases")
  void testPeriodMatcherWithNullBoundaries(final String periodStart, final String periodEnd,
      final String searchValue, final boolean expected) {
    // Create a DataFrame with a Period-like struct with potentially null boundaries
    final Column startCol = periodStart != null ? lit(periodStart) : lit(null).cast("string");
    final Column endCol = periodEnd != null ? lit(periodEnd) : lit(null).cast("string");

    final Dataset<Row> df = spark.createDataset(List.of(1), Encoders.INT())
        .select(struct(startCol.as("start"), endCol.as("end")).as("period"));

    final DateMatcher matcher = new DateMatcher(true);  // isPeriodType = true
    final Column result = matcher.match(col("period"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }

  // ========== TokenMatcher Coding tests ==========

  static Stream<Arguments> tokenCodingMatcherCases() {
    return Stream.of(
        // Code only (any system)
        Arguments.of("http://example.org", "male", "male", true),
        Arguments.of("http://example.org", "female", "male", false),
        Arguments.of(null, "male", "male", true),  // null system still matches

        // System and code
        Arguments.of("http://example.org", "male", "http://example.org|male", true),
        Arguments.of("http://other.org", "male", "http://example.org|male", false),  // wrong system
        Arguments.of("http://example.org", "female", "http://example.org|male", false),  // wrong code

        // Explicit no system (|code)
        Arguments.of(null, "male", "|male", true),
        Arguments.of("http://example.org", "male", "|male", false),  // has system, should not match

        // System only (system|)
        Arguments.of("http://example.org", "male", "http://example.org|", true),
        Arguments.of("http://example.org", "female", "http://example.org|", true),  // any code
        Arguments.of("http://other.org", "male", "http://example.org|", false)  // wrong system
    );
  }

  @ParameterizedTest(name = "TokenMatcher(Coding): system=\"{0}\", code=\"{1}\" matches \"{2}\" = {3}")
  @MethodSource("tokenCodingMatcherCases")
  void testTokenCodingMatcher(final String system, final String code,
      final String searchValue, final boolean expected) {
    // Create a DataFrame with a Coding-like struct
    final Column systemCol = system != null ? lit(system) : lit(null).cast("string");
    final Dataset<Row> df = spark.createDataset(List.of(1), Encoders.INT())
        .select(struct(systemCol.as("system"), lit(code).as("code")).as("coding"));

    final TokenMatcher matcher = new TokenMatcher(FHIRDefinedType.CODING);
    final Column result = matcher.match(col("coding"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }

  // ========== TokenMatcher Identifier tests ==========

  static Stream<Arguments> tokenIdentifierMatcherCases() {
    return Stream.of(
        // Value only (any system)
        Arguments.of("http://hospital.org/mrn", "12345", "12345", true),
        Arguments.of("http://hospital.org/mrn", "67890", "12345", false),
        Arguments.of(null, "12345", "12345", true),  // null system still matches

        // System and value
        Arguments.of("http://hospital.org/mrn", "12345", "http://hospital.org/mrn|12345", true),
        Arguments.of("http://other.org", "12345", "http://hospital.org/mrn|12345", false),

        // Explicit no system (|value)
        Arguments.of(null, "12345", "|12345", true),
        Arguments.of("http://hospital.org/mrn", "12345", "|12345", false)
    );
  }

  @ParameterizedTest(name = "TokenMatcher(Identifier): system=\"{0}\", value=\"{1}\" matches \"{2}\" = {3}")
  @MethodSource("tokenIdentifierMatcherCases")
  void testTokenIdentifierMatcher(final String system, final String value,
      final String searchValue, final boolean expected) {
    // Create a DataFrame with an Identifier-like struct
    final Column systemCol = system != null ? lit(system) : lit(null).cast("string");
    final Dataset<Row> df = spark.createDataset(List.of(1), Encoders.INT())
        .select(struct(systemCol.as("system"), lit(value).as("value")).as("identifier"));

    final TokenMatcher matcher = new TokenMatcher(FHIRDefinedType.IDENTIFIER);
    final Column result = matcher.match(col("identifier"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }

  // ========== TokenMatcher ContactPoint tests ==========

  static Stream<Arguments> tokenContactPointMatcherCases() {
    return Stream.of(
        // Value only matching
        Arguments.of("555-1234", "555-1234", true),
        Arguments.of("555-1234", "555-5678", false),
        Arguments.of("test@example.com", "test@example.com", true),
        Arguments.of("test@example.com", "other@example.com", false)
    );
  }

  @ParameterizedTest(name = "TokenMatcher(ContactPoint): value=\"{0}\" matches \"{1}\" = {2}")
  @MethodSource("tokenContactPointMatcherCases")
  void testTokenContactPointMatcher(final String value, final String searchValue,
      final boolean expected) {
    // Create a DataFrame with a ContactPoint-like struct
    final Dataset<Row> df = spark.createDataset(List.of(1), Encoders.INT())
        .select(struct(lit(value).as("value")).as("telecom"));

    final TokenMatcher matcher = new TokenMatcher(FHIRDefinedType.CONTACTPOINT);
    final Column result = matcher.match(col("telecom"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }

  // ========== TokenMatcher Boolean tests ==========

  static Stream<Arguments> tokenBooleanMatcherCases() {
    return Stream.of(
        Arguments.of(true, "true", true),
        Arguments.of(true, "false", false),
        Arguments.of(false, "false", true),
        Arguments.of(false, "true", false),
        Arguments.of(true, "TRUE", true),  // case-insensitive
        Arguments.of(false, "FALSE", true)  // case-insensitive
    );
  }

  @ParameterizedTest(name = "TokenMatcher(Boolean): {0} matches \"{1}\" = {2}")
  @MethodSource("tokenBooleanMatcherCases")
  void testTokenBooleanMatcher(final boolean element, final String searchValue,
      final boolean expected) {
    final Dataset<Row> df = spark.createDataset(List.of(element), Encoders.BOOLEAN())
        .toDF("active");

    final TokenMatcher matcher = new TokenMatcher(FHIRDefinedType.BOOLEAN);
    final Column result = matcher.match(col("active"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }

  // ========== TokenMatcher CodeableConcept tests ==========

  static Stream<Arguments> tokenCodeableConceptMatcherCases() {
    return Stream.of(
        // Code matches first coding
        Arguments.of("http://loinc.org", "12345-6", null, null, "12345-6", true),
        // Code matches second coding
        Arguments.of("http://loinc.org", "12345-6", "http://snomed.info/sct", "123456", "123456", true),
        // System|code matches first coding
        Arguments.of("http://loinc.org", "12345-6", null, null, "http://loinc.org|12345-6", true),
        // System|code matches second coding
        Arguments.of("http://loinc.org", "12345-6", "http://snomed.info/sct", "123456",
            "http://snomed.info/sct|123456", true),
        // No match
        Arguments.of("http://loinc.org", "12345-6", null, null, "99999-9", false),
        // Wrong system
        Arguments.of("http://loinc.org", "12345-6", null, null, "http://other.org|12345-6", false)
    );
  }

  @ParameterizedTest(name = "TokenMatcher(CodeableConcept): coding1=[{0},{1}], coding2=[{2},{3}] matches \"{4}\" = {5}")
  @MethodSource("tokenCodeableConceptMatcherCases")
  void testTokenCodeableConceptMatcher(final String system1, final String code1,
      final String system2, final String code2,
      final String searchValue, final boolean expected) {
    // Create a CodeableConcept with one or two codings

    final Column coding1 = struct(
        (system1 != null ? lit(system1) : lit(null).cast("string")).as("system"),
        lit(code1).as("code"));

    final Column codingArray;
    if (system2 != null || code2 != null) {
      final Column coding2 = struct(
          (system2 != null ? lit(system2) : lit(null).cast("string")).as("system"),
          lit(code2).as("code"));
      codingArray = org.apache.spark.sql.functions.array(coding1, coding2);
    } else {
      codingArray = org.apache.spark.sql.functions.array(coding1);
    }

    final Dataset<Row> df = spark.createDataset(List.of(1), Encoders.INT())
        .select(struct(codingArray.as("coding")).as("codeableConcept"));

    final TokenMatcher matcher = new TokenMatcher(FHIRDefinedType.CODEABLECONCEPT);
    final Column result = matcher.match(col("codeableConcept"), searchValue);

    final boolean actual = df.select(result).first().getBoolean(0);
    assertEquals(expected, actual);
  }
}
