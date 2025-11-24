/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
 *
 */

package au.csiro.pathling.encoders.terminology.ucum;

import au.csiro.pathling.annotations.UsedByReflection;
import io.github.fhnaumann.funcs.CanonicalizerService;
import io.github.fhnaumann.funcs.ConverterService;
import io.github.fhnaumann.funcs.ConverterService.ConversionResult;
import io.github.fhnaumann.funcs.UCUMService;
import io.github.fhnaumann.model.UCUMExpression.CanonicalTerm;
import io.github.fhnaumann.util.PreciseDecimal;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import lombok.extern.slf4j.Slf4j;

/**
 * Makes UCUM services available to the rest of the application.
 *
 * @author John Grimes
 */
@Slf4j
public class Ucum {

  public static final String NO_UNIT_CODE = "1";

  /**
   * A record to hold a canonical value and unit pair.
   */
  public record ValueWithUnit(
      @Nonnull BigDecimal value,
      @Nonnull String unit
  ) {

  }

  private static final UCUMService service;

  static {
    // ucumate handles UCUM essence loading internally, using UCUM version 2.2 by default.
    service = new UCUMService();
  }

  private Ucum() {
  }

  @Nonnull
  public static UCUMService service() {
    return service;
  }

  /**
   * Gets both the canonical value and code for a given value and UCUM code in a single operation.
   * This method performs a single canonicalization call and returns both results together, ensuring
   * consistency and better performance compared to calling getCanonicalValue and getCanonicalCode
   * separately.
   *
   * @param value the value to canonicalize
   * @param code the UCUM code of the value
   * @return a ValueWithUnit containing both canonical value and code, or null if canonicalization
   * fails
   */
  @Nullable
  public static ValueWithUnit getCanonical(@Nullable final BigDecimal value,
      @Nullable final String code) {
    if (value == null || code == null) {
      return null;
    }

    try {
      // We need to delegate the canonicalization to the service including both value and code.
      // This is because some UCUM conversions use multiplicative factors and some use additive
      // offsets (e.g., temperature conversions).
      final CanonicalizerService.CanonicalizationResult result = service.canonicalize(
          new PreciseDecimal(value.toPlainString()),
          code
      );

      // Check if the result is a Success instance.
      if (!(result instanceof CanonicalizerService.Success(
          PreciseDecimal magnitude,
          CanonicalTerm canonicalTerm
      ))) {
        log.warn("Failed to canonicalise UCUM code '{}': {}", code, result);
        return null;
      }

      // Get the magnitude of the value in canonical units.
      if (magnitude == null) {
        log.warn("No magnitude available for UCUM code '{}'", code);
        return null;
      }

      // Get the canonical unit code by printing the canonical term.
      @Nullable final String canonicalCode = service.print(canonicalTerm);
      if (canonicalCode == null) {
        log.warn("No canonical code available for UCUM code '{}'", code);
        return null;
      }

      // Handle empty canonical code by converting to NO_UNIT_CODE
      final String adjustedCode = canonicalCode.isEmpty()
                                  ? NO_UNIT_CODE
                                  : canonicalCode;

      return new ValueWithUnit(magnitude.getValue(), adjustedCode);
    } catch (final Exception e) {
      log.warn("Error canonicalising UCUM code '{}': {}", code, e.getMessage());
      return null;
    }
  }

  /**
   * Gets the canonical value for a given value and UCUM code.
   *
   * @param value the value to canonicalize
   * @param code the UCUM code of the value
   * @return the canonical value, or null if canonicalization fails
   */
  @UsedByReflection
  @Nullable
  public static BigDecimal getCanonicalValue(@Nullable final BigDecimal value,
      @Nullable final String code) {
    @Nullable final ValueWithUnit canonical = getCanonical(value, code);
    return canonical != null
           ? canonical.value()
           : null;
  }

  /**
   * Gets the canonical UCUM code for a given value and UCUM code.
   *
   * @param value the value to canonicalize
   * @param code the UCUM code of the value
   * @return the canonical UCUM code, or null if canonicalization fails
   */
  @UsedByReflection
  @Nullable
  public static String getCanonicalCode(@Nullable final BigDecimal value,
      @Nullable final String code) {
    @Nullable final ValueWithUnit canonical = getCanonical(value, code);
    return canonical != null
           ? canonical.unit()
           : null;
  }

  /**
   * Converts a value from one UCUM unit to another. Supports both multiplicative conversions (e.g.,
   * mg to kg) and additive conversions (e.g., Celsius to Kelvin).
   *
   * @param value the value to convert
   * @param fromCode the source UCUM code
   * @param toCode the target UCUM code
   * @return the converted value, or null if conversion is not possible
   */
  @Nullable
  public static BigDecimal convertValue(@Nullable final BigDecimal value,
      @Nullable final String fromCode, @Nullable final String toCode) {
    if (value == null || fromCode == null || toCode == null) {
      return null;
    }

    try {
      // Use the ucumate library's convert method that handles both multiplicative and additive
      // conversions directly by taking the value as the first argument
      final ConversionResult conversionResult = service.convert(
          new PreciseDecimal(value.toPlainString()),
          fromCode,
          toCode
      );

      if (!(conversionResult instanceof ConverterService.Success(var convertedValue))
          || convertedValue == null) {
        log.warn("Failed to convert value {} from '{}' to '{}': {}",
            value, fromCode, toCode, conversionResult);
        return null;
      }

      return convertedValue.getValue();
    } catch (final Exception e) {
      log.warn("Error converting value {} from '{}' to '{}': {}",
          value, fromCode, toCode, e.getMessage());
      return null;
    }
  }

}
