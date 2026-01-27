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

package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link FhirPathNumber}. */
class FhirPathNumberTest {

  @ParameterizedTest
  @MethodSource("significantFiguresTestCases")
  void testCountSignificantFigures(final String input, final int expectedSigFigs) {
    assertEquals(
        expectedSigFigs,
        FhirPathNumber.countSignificantFigures(input),
        "Significant figures for '" + input + "'");
  }

  static Stream<Arguments> significantFiguresTestCases() {
    return Stream.of(
        // Basic integers
        Arguments.of("1", 1),
        Arguments.of("12", 2),
        Arguments.of("123", 3),
        Arguments.of("100", 3),
        Arguments.of("1000", 4),

        // With sign
        Arguments.of("-1", 1),
        Arguments.of("+100", 3),
        Arguments.of("-123", 3),

        // With decimal point - trailing zeros are significant
        Arguments.of("1.0", 2),
        Arguments.of("1.00", 3),
        Arguments.of("100.0", 4),
        Arguments.of("100.00", 5),
        Arguments.of("1.23", 3),

        // Decimal values less than 1
        Arguments.of("0.1", 1),
        Arguments.of("0.01", 1),
        Arguments.of("0.001", 1),
        Arguments.of("0.5", 1),
        Arguments.of("0.50", 2),
        Arguments.of("0.500", 3),
        Arguments.of("0.123", 3),
        Arguments.of("0.0123", 3),

        // Zero - decimal places determine precision for zero
        // Returns 0 for "0", 1 for "0.0", 2 for "0.00" etc.
        Arguments.of("0", 0), // No decimal precision
        Arguments.of("0.0", 1), // 1 decimal place
        Arguments.of("0.00", 2), // 2 decimal places

        // Exponential notation - count mantissa digits
        Arguments.of("1e2", 1),
        Arguments.of("1.0e2", 2),
        Arguments.of("1.00e2", 3),
        Arguments.of("1.5e2", 2),
        Arguments.of("1.23e4", 3),
        Arguments.of("1e-2", 1),
        Arguments.of("1.0e-2", 2));
  }

  @ParameterizedTest
  @MethodSource("boundaryTestCases")
  void testBoundaries(final String input, final String expectedLower, final String expectedUpper) {
    final FhirPathNumber number = FhirPathNumber.parse(input);

    // Use compareTo() for BigDecimal comparison since equals() considers scale
    // (e.g., 5E+1 is numerically equal to 50, but not equal via equals())
    assertEquals(
        0,
        new BigDecimal(expectedLower).compareTo(number.getLowerBoundary()),
        "Lower boundary for '"
            + input
            + "': expected "
            + expectedLower
            + " but was "
            + number.getLowerBoundary());
    assertEquals(
        0,
        new BigDecimal(expectedUpper).compareTo(number.getUpperBoundary()),
        "Upper boundary for '"
            + input
            + "': expected "
            + expectedUpper
            + " but was "
            + number.getUpperBoundary());
  }

  static Stream<Arguments> boundaryTestCases() {
    return Stream.of(
        // Integer notation - FHIR spec examples
        // "100" (3 sig figs) -> [99.5, 100.5)
        Arguments.of("100", "99.5", "100.5"),
        Arguments.of("1", "0.5", "1.5"),
        Arguments.of("5", "4.5", "5.5"),
        Arguments.of("10", "9.5", "10.5"),

        // Decimal notation with trailing zeros - more precision
        // "100.00" (5 sig figs) -> [99.995, 100.005)
        Arguments.of("100.00", "99.995", "100.005"),
        Arguments.of("1.0", "0.95", "1.05"),
        Arguments.of("1.00", "0.995", "1.005"),
        Arguments.of("10.0", "9.95", "10.05"),

        // Decimal values less than 1
        Arguments.of("0.5", "0.45", "0.55"),
        Arguments.of("0.50", "0.495", "0.505"),
        Arguments.of("0.001", "0.0005", "0.0015"),
        Arguments.of("0.123", "0.1225", "0.1235"),

        // Exponential notation
        // "1e2" (1 sig fig) -> [50, 150)
        Arguments.of("1e2", "50", "150"),
        // "1.0e2" (2 sig figs) -> [95, 105)
        Arguments.of("1.0e2", "95", "105"),
        // "1.00e2" (3 sig figs) -> [99.5, 100.5)
        Arguments.of("1.00e2", "99.5", "100.5"),

        // Small exponential values
        Arguments.of("1e-2", "0.005", "0.015"),
        Arguments.of("1.0e-2", "0.0095", "0.0105"),

        // Negative values
        Arguments.of("-100", "-100.5", "-99.5"),
        Arguments.of("-1", "-1.5", "-0.5"),
        Arguments.of("-0.5", "-0.55", "-0.45"),
        Arguments.of("-1.0", "-1.05", "-0.95"),

        // Zero
        Arguments.of("0", "-0.5", "0.5"),
        Arguments.of("0.0", "-0.05", "0.05"),
        Arguments.of("0.00", "-0.005", "0.005"));
  }

  @Test
  void testParseValue() {
    final FhirPathNumber number = FhirPathNumber.parse("123.45");

    assertEquals(new BigDecimal("123.45"), number.getValue());
    assertEquals(5, number.getSignificantFigures());
  }

  @Test
  void testParseWithWhitespace() {
    final FhirPathNumber number = FhirPathNumber.parse("  100  ");

    assertEquals(new BigDecimal("100"), number.getValue());
    assertEquals(3, number.getSignificantFigures());
  }

  @ParameterizedTest
  @ValueSource(strings = {"abc", "", "  ", "1.2.3", "1e", "e5"})
  void testParseInvalidInput(final String input) {
    assertThrows(NumberFormatException.class, () -> FhirPathNumber.parse(input));
  }

  @Test
  void testToString() {
    final FhirPathNumber number = FhirPathNumber.parse("100");
    final String str = number.toString();

    assertEquals("FhirPathNumber{value=100, sigFigs=3, range=[99.5, 100.5)}", str);
  }

  @Test
  void testLargeNumber() {
    final FhirPathNumber number = FhirPathNumber.parse("1000000");

    assertEquals(new BigDecimal("1000000"), number.getValue());
    assertEquals(7, number.getSignificantFigures());
    assertEquals(new BigDecimal("999999.5"), number.getLowerBoundary());
    assertEquals(new BigDecimal("1000000.5"), number.getUpperBoundary());
  }

  @Test
  void testVerySmallNumber() {
    final FhirPathNumber number = FhirPathNumber.parse("0.0001");

    assertEquals(new BigDecimal("0.0001"), number.getValue());
    assertEquals(1, number.getSignificantFigures());
    assertEquals(new BigDecimal("0.00005"), number.getLowerBoundary());
    assertEquals(new BigDecimal("0.00015"), number.getUpperBoundary());
  }

  @Test
  void testHighPrecision() {
    final FhirPathNumber number = FhirPathNumber.parse("1.23456789");

    assertEquals(new BigDecimal("1.23456789"), number.getValue());
    assertEquals(9, number.getSignificantFigures());
    // Half unit = 0.5 * 10^(0 - 9 + 1) = 0.5 * 10^-8 = 0.000000005
    assertEquals(new BigDecimal("1.234567885"), number.getLowerBoundary());
    assertEquals(new BigDecimal("1.234567895"), number.getUpperBoundary());
  }

  // ========== hasFractionalPart tests ==========

  @ParameterizedTest
  @MethodSource("fractionalPartTestCases")
  void testHasFractionalPart(final String input, final boolean expectedHasFractional) {
    final FhirPathNumber number = FhirPathNumber.parse(input);
    assertEquals(
        expectedHasFractional, number.hasFractionalPart(), "hasFractionalPart for '" + input + "'");
  }

  static Stream<Arguments> fractionalPartTestCases() {
    return Stream.of(
        // Values with fractional parts
        Arguments.of("1.5", true),
        Arguments.of("0.5", true),
        Arguments.of("100.5", true),
        Arguments.of("-1.5", true),
        Arguments.of("0.001", true),
        Arguments.of("99.99", true),

        // Values without fractional parts
        Arguments.of("1", false),
        Arguments.of("100", false),
        Arguments.of("0", false),
        Arguments.of("-5", false),
        Arguments.of("1.0", false), // 1.0 = 1, no fractional part
        Arguments.of("100.0", false), // 100.0 = 100, no fractional part
        Arguments.of("100.00", false), // 100.00 = 100, no fractional part

        // Exponential notation
        Arguments.of("1e2", false), // 100, no fractional part
        Arguments.of("1.5e2", false), // 150, no fractional part
        Arguments.of("1.5e-1", true), // 0.15, has fractional part
        Arguments.of("5e-1", true) // 0.5, has fractional part
        );
  }
}
