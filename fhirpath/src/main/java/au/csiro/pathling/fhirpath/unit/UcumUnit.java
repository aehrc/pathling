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

import io.github.fhnaumann.funcs.ConverterService;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * Represents a UCUM (Unified Code for Units of Measure) unit.
 * <p>
 * UCUM is an international standard for representing units of measure in a machine-readable form.
 * This record encapsulates a UCUM unit code and provides methods for converting between compatible
 * UCUM units using the UCUM conversion service.
 * <p>
 * Examples of UCUM codes include:
 * <ul>
 *   <li>"mg" - milligram</li>
 *   <li>"kg" - kilogram</li>
 *   <li>"mL" - milliliter</li>
 *   <li>"s" - second</li>
 *   <li>"ms" - millisecond</li>
 * </ul>
 *
 * @param code the UCUM unit code (e.g., "mg", "kg", "mL")
 */
public record UcumUnit(@Nonnull String code) implements FhirPathUnit {

  /**
   * The UCUM unit representing dimensionless unity ("1"). This is used as the default unit when no
   * unit is specified in FHIRPath quantity literals.
   */
  public static final UcumUnit ONE = new UcumUnit("1");
  /**
   * The system URI for UCUM units.
   */
  public static final String UCUM_SYSTEM_URI = "http://unitsofmeasure.org";

  /**
   * Returns the UCUM system URI.
   *
   * @return the UCUM system URI ({@value UcumUnit#UCUM_SYSTEM_URI})
   */
  @Override
  @Nonnull
  public String system() {
    return UCUM_SYSTEM_URI;
  }

  /**
   * Computes the conversion factor to convert values from this UCUM unit to the target UCUM unit.
   * <p>
   * This method uses the UCUM conversion service to determine if the two units are compatible and
   * calculate the appropriate conversion factor. Units are compatible if they measure the same
   * dimension (e.g., both measure mass, length, or time).
   * <p>
   * Examples of valid conversions:
   * <ul>
   *   <li>mg → kg (mass)</li>
   *   <li>mL → L (volume)</li>
   *   <li>s → ms (time)</li>
   * </ul>
   * <p>
   * Examples of invalid conversions that return empty:
   * <ul>
   *   <li>mg → mL (mass vs volume)</li>
   *   <li>s → kg (time vs mass)</li>
   * </ul>
   *
   * @param targetUnit the target UCUM unit to convert to
   * @return an Optional containing the conversion factor if conversion is possible, or empty if the
   * units are incompatible
   */
  @Nonnull
  public Optional<ConversionFactor> conversionFactorTo(
      @Nonnull UcumUnit targetUnit) {
    var conversionResult = au.csiro.pathling.encoders.terminology.ucum.Ucum.service()
        .convert(code(), targetUnit.code());
    if (conversionResult instanceof ConverterService.Success(var conversionFactor)) {
      return Optional.of(ConversionFactor.of(conversionFactor.getValue()));
    }
    return Optional.empty(); // Return empty if conversion fails.
  }

  /**
   * Checks if the given unit name is valid for this UCUM unit. For UCUM units, the name must
   * exactly match the code.
   *
   * @param unitName the unit name to validate
   * @return true if unitName equals this unit's code, false otherwise
   */
  @Override
  public boolean isValidName(@Nonnull final String unitName) {
    return this.code.equals(unitName);
  }
}
