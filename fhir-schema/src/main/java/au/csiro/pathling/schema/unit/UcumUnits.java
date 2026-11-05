/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.schema.unit;

import au.csiro.pathling.schema.annotation.kinds.CanonicalQuantity;
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
 * The UCUM computations of quantities: canonicalisation, which the canonical annotation is computed
 * by, and conversion between units. Both use the one service, so that a JVM loads the UCUM model
 * once, and canonicalisation and conversion agree.
 *
 * @author Piotr Szul
 */
@Slf4j
public final class UcumUnits {

  /** The canonical code of a quantity without units. */
  @Nonnull public static final String NO_UNIT_CODE = "1";

  /** The service is created once, and held here rather than captured by a function. */
  @Nonnull private static final UCUMService SERVICE = new UCUMService();

  private UcumUnits() {}

  /**
   * Canonicalises a value in a UCUM unit to the base units of UCUM.
   *
   * @param value the value
   * @param code the UCUM code of the unit
   * @return the canonical form, or null where the code cannot be canonicalised
   */
  @Nullable
  public static CanonicalQuantity canonicalize(
      @Nonnull final BigDecimal value, @Nonnull final String code) {
    try {
      // The value takes part, because some conversions, such as those between temperature scales,
      // add an offset rather than multiply.
      if (SERVICE.canonicalize(new PreciseDecimal(value.toPlainString()), code)
          instanceof CanonicalizerService.Success(PreciseDecimal magnitude, CanonicalTerm term)) {
        final String canonicalCode = SERVICE.print(term);
        return magnitude == null || canonicalCode == null
            ? null
            : new CanonicalQuantity(
                magnitude.getValue(), canonicalCode.isEmpty() ? NO_UNIT_CODE : canonicalCode);
      }
      return null;
    } catch (final RuntimeException e) {
      // A code UCUM cannot parse has no canonical form, and nothing is reported.
      return null;
    }
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
  public static BigDecimal convert(
      @Nullable final BigDecimal value,
      @Nullable final String fromCode,
      @Nullable final String toCode) {
    if (value == null || fromCode == null || toCode == null) {
      return null;
    }
    try {
      final ConversionResult conversionResult =
          SERVICE.convert(new PreciseDecimal(value.toPlainString()), fromCode, toCode);
      if (!(conversionResult instanceof ConverterService.Success(var convertedValue))
          || convertedValue == null) {
        log.warn(
            "Failed to convert value {} from '{}' to '{}': {}",
            value,
            fromCode,
            toCode,
            conversionResult);
        return null;
      }
      return convertedValue.getValue();
    } catch (final Exception e) {
      log.warn(
          "Error converting value {} from '{}' to '{}': {}",
          value,
          fromCode,
          toCode,
          e.getMessage());
      return null;
    }
  }
}
