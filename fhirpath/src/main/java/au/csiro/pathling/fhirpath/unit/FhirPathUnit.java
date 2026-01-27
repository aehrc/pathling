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
import java.util.Optional;

/**
 * Represents a FHIRPath unit of measure, which can be either a UCUM unit, a calendar duration unit,
 * or a custom unit from another system.
 *
 * <p>This sealed interface has three permitted implementations:
 *
 * <ul>
 *   <li>{@link UcumUnit} - UCUM (Unified Code for Units of Measure) units
 *   <li>{@link CalendarDurationUnit} - FHIRPath calendar duration units (year, month, week, etc.)
 * </ul>
 *
 * <p>Units can be converted between compatible types using the {@link #convertValue} method, which
 * converts values for both same-type conversions (UCUM-to-UCUM, calendar-to-calendar) and
 * cross-type conversions (calendar-to-UCUM, UCUM-to-calendar) where applicable.
 */
public sealed interface FhirPathUnit permits UcumUnit, CalendarDurationUnit {

  /** The precision (number of decimal places) to use when computing unit conversions. */
  int DEFAULT_PRECISION = 15;

  /**
   * Gets the system URI for this unit (e.g., UCUM system URI, calendar duration system URI).
   *
   * @return the system URI
   */
  @Nonnull
  String system();

  /**
   * Gets the canonical code for this unit (e.g., "mg", "second").
   *
   * @return the unit code
   */
  @Nonnull
  String code();

  /**
   * Validates if the given unit name is a valid representation for this unit. The validation rules
   * depend on the unit type:
   *
   * <ul>
   *   <li>For {@link UcumUnit}: the name must exactly match the code
   *   <li>For {@link CalendarDurationUnit}: the name must match either the singular or plural form
   *       (e.g., "year" or "years" for YEAR)
   * </ul>
   *
   * @param unitName the unit name to validate
   * @return true if unitName is valid for this unit, false otherwise
   */
  boolean isValidName(@Nonnull final String unitName);

  /**
   * Converts a value from one unit to another, supporting same-type conversions (UCUM-to-UCUM,
   * calendar-to-calendar) and cross-type conversions (calendar-to-UCUM, UCUM-to-calendar) where
   * applicable. Handles both multiplicative conversions (e.g., mg → kg) and additive conversions
   * (e.g., Celsius → Kelvin).
   *
   * <p>Cross-type conversions work by finding intermediate representations:
   *
   * <ul>
   *   <li>Calendar → UCUM: Converts this calendar unit to seconds, then seconds to UCUM equivalent,
   *       then performs UCUM-to-UCUM conversion to target
   *   <li>UCUM → Calendar: Converts source UCUM to seconds equivalent, then performs
   *       calendar-to-calendar conversion from seconds to target
   * </ul>
   *
   * <p>Examples of valid conversions:
   *
   * <ul>
   *   <li>UCUM → UCUM: 1000 mg → 1 kg
   *   <li>UCUM → UCUM (additive): 0 Cel → 273.15 K
   *   <li>Calendar → Calendar: 1 day → 24 hour
   *   <li>Calendar → UCUM: 1 millisecond → 1 ms
   *   <li>UCUM → Calendar: 60 s → 1 minute
   * </ul>
   *
   * <p>Conversions return empty when:
   *
   * <ul>
   *   <li>Units measure incompatible dimensions (e.g., mass vs volume)
   *   <li>Non-definite calendar units are involved in certain conversions
   * </ul>
   *
   * @param value the value to convert
   * @param sourceUnit the source unit to convert from
   * @param targetUnit the target unit to convert to
   * @return an Optional containing the converted value if conversion is possible, or empty if the
   *     units are incompatible
   */
  @Nonnull
  static Optional<BigDecimal> convertValue(
      @Nonnull final BigDecimal value,
      @Nonnull final FhirPathUnit sourceUnit,
      @Nonnull final FhirPathUnit targetUnit) {
    // This handles cross-unit conversions.
    return switch (sourceUnit) {
      case final CalendarDurationUnit cdUnitSource ->
          switch (targetUnit) {
            case final CalendarDurationUnit cdUnitTarget ->
                cdUnitSource.convertValue(value, cdUnitTarget);
            case final UcumUnit ucumUnitTarget -> cdUnitSource.convertValue(value, ucumUnitTarget);
          };
      case final UcumUnit ucumUnitSource ->
          switch (targetUnit) {
            case final UcumUnit ucumUnitTarget ->
                ucumUnitSource.convertValue(value, ucumUnitTarget);
            case final CalendarDurationUnit cdUnitTarget ->
                cdUnitTarget.convertValueFrom(value, ucumUnitSource);
          };
    };
  }

  /**
   * Parses a unit string and creates the appropriate FhirPathUnit instance.
   *
   * <p>The parsing strategy is:
   *
   * <ol>
   *   <li>First attempts to parse as a calendar duration unit (year, month, week, day, hour,
   *       minute, second, millisecond - both singular and plural forms)
   *   <li>If not recognized as a calendar duration, assumes it's a UCUM unit code
   * </ol>
   *
   * <p>using their constructor with both system and code parameters.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>"year" → {@link CalendarDurationUnit#YEAR}
   *   <li>"seconds" → {@link CalendarDurationUnit#SECOND}
   *   <li>"mg" → {@link UcumUnit} with code "mg"
   *   <li>"unknown-unit" → {@link UcumUnit} with code "unknown-unit" (may fail on conversion)
   * </ul>
   *
   * @param unit the unit string (e.g., "year", "seconds", "mg", "kg")
   * @return a FhirPathUnit instance (CalendarDurationUnit if recognized, otherwise UcumUnit)
   */
  @Nonnull
  static FhirPathUnit fromString(@Nonnull final String unit) {
    return CalendarDurationUnit.fromString(unit)
        .map(FhirPathUnit.class::cast)
        .orElseGet(() -> new UcumUnit(unit));
  }
}
