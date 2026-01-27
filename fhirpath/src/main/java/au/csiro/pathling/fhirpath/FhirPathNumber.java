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

import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import lombok.Getter;

/**
 * Represents a parsed FHIR search number value with precision-aware boundaries.
 *
 * <p>FHIR numeric search uses implicit range semantics based on significant figures. For example,
 * searching for "100" (3 significant figures) matches any value in the range [99.5, 100.5). The
 * precision is determined by the number of significant figures in the search value.
 *
 * <p>This class parses numeric search values and computes the lower and upper boundaries for range
 * matching.
 *
 * @see <a href="https://hl7.org/fhir/search.html#number">FHIR Search - Number</a>
 */
@Getter
public class FhirPathNumber {

  @Nonnull private final BigDecimal value;

  private final int significantFigures;

  @Nonnull private final BigDecimal lowerBoundary;

  @Nonnull private final BigDecimal upperBoundary;

  private FhirPathNumber(
      @Nonnull final BigDecimal value,
      final int significantFigures,
      @Nonnull final BigDecimal lowerBoundary,
      @Nonnull final BigDecimal upperBoundary) {
    this.value = value;
    this.significantFigures = significantFigures;
    this.lowerBoundary = lowerBoundary;
    this.upperBoundary = upperBoundary;
  }

  /**
   * Parses a numeric search value and computes precision-aware boundaries.
   *
   * @param input the string representation of the number
   * @return a FhirPathNumber with computed boundaries
   * @throws NumberFormatException if the input cannot be parsed as a number
   */
  @Nonnull
  public static FhirPathNumber parse(@Nonnull final String input) {
    final String normalizedInput = input.strip();

    // Parse the value as BigDecimal
    final BigDecimal value = new BigDecimal(normalizedInput);

    // Count significant figures from the string representation
    final int significantFigures = countSignificantFigures(normalizedInput);

    // Calculate the half-unit based on precision
    final BigDecimal halfUnit = calculateHalfUnit(value, significantFigures);

    // Calculate boundaries
    final BigDecimal lowerBoundary = value.subtract(halfUnit);
    final BigDecimal upperBoundary = value.add(halfUnit);

    return new FhirPathNumber(value, significantFigures, lowerBoundary, upperBoundary);
  }

  /**
   * Counts the number of significant figures in a numeric string.
   *
   * <p>Rules:
   *
   * <ul>
   *   <li>All non-zero digits are significant
   *   <li>Zeros between non-zero digits are significant
   *   <li>Leading zeros are NOT significant
   *   <li>Trailing zeros after decimal point ARE significant
   *   <li>For exponential notation, count digits in mantissa only
   *   <li>For zero with decimal places (e.g., "0.0", "0.00"), count decimal places as precision
   * </ul>
   *
   * @param input the string representation of the number
   * @return the number of significant figures
   */
  static int countSignificantFigures(@Nonnull final String input) {
    final String normalized = input.strip().toLowerCase();

    // Handle exponential notation - extract mantissa
    final String mantissa;
    if (normalized.contains("e")) {
      mantissa = normalized.substring(0, normalized.indexOf('e'));
    } else {
      mantissa = normalized;
    }

    // Remove sign if present
    String digits = mantissa;
    if (digits.startsWith("-") || digits.startsWith("+")) {
      digits = digits.substring(1);
    }

    // Check for decimal point before removing
    final boolean hasDecimalPoint = digits.contains(".");
    final int decimalIndex = digits.indexOf('.');

    // For zero values, the number of decimal places determines precision
    // "0" -> 0 decimal precision, "0.0" -> 1 decimal place, "0.00" -> 2 decimal places
    // We return the decimal places count, which calculateHalfUnit uses to compute exponent
    final String digitsWithoutDecimal = digits.replace(".", "");
    if (isAllZeros(digitsWithoutDecimal)) {
      if (!hasDecimalPoint) {
        return 0; // "0" has no decimal precision -> half unit = 0.5
      }
      // For "0.0", "0.00", etc., return the number of decimal places
      // This determines the half-unit: "0.0" -> half unit of 0.05, "0.00" -> 0.005
      return digits.length() - decimalIndex - 1;
    }

    // Remove decimal point for counting non-zero cases
    digits = digitsWithoutDecimal;

    // Remove leading zeros
    int startIndex = 0;
    while (startIndex < digits.length() && digits.charAt(startIndex) == '0') {
      startIndex++;
    }

    if (startIndex >= digits.length()) {
      return 1; // All zeros - at least 1 significant figure
    }

    // Count from first non-zero to last digit
    return digits.length() - startIndex;
  }

  /** Checks if a string consists entirely of zeros. */
  private static boolean isAllZeros(@Nonnull final String digits) {
    if (digits.isEmpty()) {
      return true;
    }
    for (int i = 0; i < digits.length(); i++) {
      if (digits.charAt(i) != '0') {
        return false;
      }
    }
    return true;
  }

  /**
   * Calculates the half-unit value used for boundary computation.
   *
   * <p>The half-unit is 0.5 × 10^(magnitude - significantFigures + 1), where magnitude is the power
   * of 10 of the most significant digit.
   *
   * @param value the numeric value
   * @param significantFigures the number of significant figures
   * @return the half-unit for boundary calculation
   */
  @Nonnull
  private static BigDecimal calculateHalfUnit(
      @Nonnull final BigDecimal value, final int significantFigures) {
    if (value.compareTo(BigDecimal.ZERO) == 0) {
      // For zero, use the precision from significant figures
      // "0" (1 sig fig) -> half unit of 0.5
      // "0.0" (1 sig fig at tenths) -> half unit of 0.05
      // "0.00" (2 sig figs at hundredths) -> half unit of 0.005
      // The exponent is -(significantFigures) since we're measuring decimal places
      final int exponent = -significantFigures;
      return new BigDecimal("0.5").scaleByPowerOfTen(exponent);
    }

    // Get the precision position (exponent of the least significant digit)
    // For 100 (3 sig figs), this is 0 (ones place) -> half unit = 0.5
    // For 100.00 (5 sig figs), this is -2 (hundredths) -> half unit = 0.005
    // For 1e2 (1 sig fig), this is 2 (hundreds place) -> half unit = 50

    final BigDecimal absValue = value.abs();
    final int magnitude = getMagnitude(absValue);
    final int leastSigDigitExponent = magnitude - significantFigures + 1;

    // Half-unit = 0.5 × 10^(leastSigDigitExponent)
    return new BigDecimal("0.5").scaleByPowerOfTen(leastSigDigitExponent);
  }

  /**
   * Gets the magnitude (power of 10 of the most significant digit).
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>100 -> magnitude 2
   *   <li>50 -> magnitude 1
   *   <li>5 -> magnitude 0
   *   <li>0.5 -> magnitude -1
   *   <li>0.05 -> magnitude -2
   * </ul>
   *
   * @param value the absolute value (must be positive)
   * @return the magnitude
   */
  private static int getMagnitude(@Nonnull final BigDecimal value) {
    if (value.compareTo(BigDecimal.ZERO) == 0) {
      return 0;
    }

    // Use precision and scale to compute magnitude
    // For BigDecimal: precision is total significant digits, scale is digits after decimal
    // magnitude = precision - scale - 1

    // However, for values like "100" that are stored without trailing zeros,
    // we need a different approach using logarithm

    // Calculate floor(log10(abs(value)))
    final double log10 = Math.log10(value.doubleValue());
    return (int) Math.floor(log10);
  }

  /**
   * Checks if this number has a non-zero fractional part.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>"1.5" has a fractional part (returns true)
   *   <li>"1.0" does not have a fractional part (returns false)
   *   <li>"100" does not have a fractional part (returns false)
   *   <li>"0.5" has a fractional part (returns true)
   * </ul>
   *
   * @return true if the value has a non-zero fractional part
   */
  public boolean hasFractionalPart() {
    // Check if the value equals its integer part
    // BigDecimal.remainder(ONE) gives the fractional part
    return value.remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0;
  }

  @Override
  public String toString() {
    return String.format(
        "FhirPathNumber{value=%s, sigFigs=%d, range=[%s, %s)}",
        value.toPlainString(),
        significantFigures,
        lowerBoundary.toPlainString(),
        upperBoundary.toPlainString());
  }
}
