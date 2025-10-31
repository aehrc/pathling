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

package au.csiro.pathling.fhirpath.unit;

import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.RoundingMode;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Represents a conversion factor between two units of measure, expressed as a fraction.
 * <p>
 * A conversion factor is stored as a fraction (numerator/denominator) rather than a decimal
 * to preserve precision during conversions. This is particularly important for conversions
 * involving irrational or repeating decimals.
 * <p>
 * Conversion factors can be applied to numeric values to convert them from one unit to another,
 * or composed with other conversion factors to create multi-step conversions.
 * <p>
 * Example usage:
 * <pre>
 *   // Convert 1000 mg to kg (factor = 1/1000)
 *   ConversionFactor factor = ConversionFactor.ofFraction(BigDecimal.ONE, new BigDecimal(1000));
 *   BigDecimal result = factor.apply(new BigDecimal(1000)); // = 1
 * </pre>
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConversionFactor  {

  /**
   * The numerator of the conversion factor fraction.
   */
  @Nonnull
  BigDecimal numerator;

  /**
   * The denominator of the conversion factor fraction.
   */
  @Nonnull
  BigDecimal denominator;


  /**
   * Applies this conversion factor to a numeric value.
   * <p>
   * The conversion is performed as: {@code value * numerator / denominator}
   * <p>
   * The result is calculated with {@value FhirPathUnit#CONVERSION_PRECISION} decimal places of
   * precision using {@link RoundingMode#HALF_UP}, and trailing zeros are stripped.
   *
   * @param value the value to convert
   * @return the converted value
   */
  @Nonnull
  public BigDecimal apply(@Nonnull final BigDecimal value) {
    return value.multiply(numerator)
        .divide(denominator, FhirPathUnit.CONVERSION_PRECISION, RoundingMode.HALF_UP)
        .stripTrailingZeros();
  }

  /**
   * Composes this conversion factor with another conversion factor.
   * <p>
   * This creates a new conversion factor representing the combined conversion. This is useful
   * for multi-step conversions (e.g., calendar unit → second → UCUM unit).
   * <p>
   * The composition is performed as:
   * {@code (this.numerator * value.numerator) / (this.denominator * value.denominator)}
   *
   * @param value the conversion factor to compose with this one
   * @return a new conversion factor representing the combined conversion
   */
  @Nonnull
  public ConversionFactor apply(@Nonnull final ConversionFactor value) {
    return ofFraction(
        this.numerator.multiply(value.numerator),
        this.denominator.multiply(value.denominator)
    );
  }

  /**
   * Creates a conversion factor from a single numeric value.
   * <p>
   * This is a convenience method for creating a conversion factor with denominator 1.
   *
   * @param factor the conversion factor value
   * @return a ConversionFactor representing the given value
   */
  @Nonnull
  public static ConversionFactor of(@Nonnull final BigDecimal factor) {
    return ConversionFactor.ofFraction(factor, BigDecimal.ONE);
  }

  /**
   * Creates a conversion factor from a numerator and denominator.
   *
   * @param numerator the numerator of the fraction
   * @param denominator the denominator of the fraction
   * @return a ConversionFactor representing the fraction
   * @throws IllegalArgumentException if denominator is zero
   */
  @Nonnull
  static ConversionFactor ofFraction(@Nonnull final BigDecimal numerator,
      @Nonnull final BigDecimal denominator) {
    if (BigDecimal.ZERO.equals(denominator)) {
      throw new IllegalArgumentException("denominator cannot be zero");
    }
    return new ConversionFactor(numerator, denominator);
  }

  /**
   * Creates a conversion factor representing the inverse (1/value) of the given value.
   * <p>
   * This is useful for creating reciprocal conversions (e.g., if kg→mg is 1000, then mg→kg is 1/1000).
   *
   * @param value the value to invert
   * @return a ConversionFactor representing 1/value
   */
  @Nonnull
  static ConversionFactor inverseOf(@Nonnull final BigDecimal value) {
    return ofFraction(BigDecimal.ONE, value);
  }
}
