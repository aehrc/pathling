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
import io.github.fhnaumann.funcs.UCUMService;
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

  @UsedByReflection
  @Nullable
  public static BigDecimal getCanonicalValue(@Nullable final BigDecimal value,
      @Nullable final String code) {
    if (value == null || code == null) {
      return null;
    }

    try {
      final CanonicalizerService.CanonicalizationResult result = service.canonicalize(code);

      // Check if the result is a Success instance.
      if (!(result instanceof CanonicalizerService.Success success)) {
        log.warn("Failed to canonicalise UCUM code '{}': {}", code, result);
        return null;
      }

      // Get the magnitude (conversion factor) from the success result.
      @Nullable final BigDecimal conversionFactor = success.magnitude().getValue();
      if (conversionFactor == null) {
        log.warn("No conversion factor available for UCUM code '{}'", code);
        return null;
      }

      // Apply the conversion factor to the value to get the canonical value.
      return value.multiply(conversionFactor);
    } catch (final Exception e) {
      log.warn("Error canonicalising UCUM code '{}': {}", code, e.getMessage());
      return null;
    }
  }

  @UsedByReflection
  @Nullable
  public static String getCanonicalCode(@Nullable final BigDecimal value,
      @Nullable final String code) {
    if (value == null || code == null) {
      return null;
    }

    try {
      final CanonicalizerService.CanonicalizationResult result = service.canonicalize(code);

      // Check if the result is a Success instance.
      if (!(result instanceof CanonicalizerService.Success success)) {
        log.warn("Failed to canonicalise UCUM code '{}': {}", code, result);
        return null;
      }

      // Get the canonical unit code by printing the canonical term.
      @Nullable final String canonicalCode = service.print(success.canonicalTerm());

      // Apply the NO_UNIT_CODE adjustment for empty codes.
      return adjustNoUnitCode(canonicalCode);
    } catch (final Exception e) {
      log.warn("Error canonicalising UCUM code '{}': {}", code, e.getMessage());
      return null;
    }
  }

  @Nullable
  private static String adjustNoUnitCode(@Nullable final String code) {
    if (code == null) {
      return null;
    }
    return code.isEmpty() ? NO_UNIT_CODE : code;
  }

}
