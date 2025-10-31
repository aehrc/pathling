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
import java.util.Optional;

/**
 * Represents a FHIRPath unit of measure, which can be either a UCUM unit, a calendar duration unit,
 * or a custom unit from another system.
 * <p>
 * This sealed interface has three permitted implementations:
 * <ul>
 *   <li>{@link UcumUnit} - UCUM (Unified Code for Units of Measure) units</li>
 *   <li>{@link CalendarDurationUnit} - FHIRPath calendar duration units (year, month, week, etc.)</li>
 *   <li>{@link CustomUnit} - Custom units from other systems</li>
 * </ul>
 * <p>
 * Units can be converted between compatible types using the {@link #conversionFactorTo} method,
 * which computes conversion factors for both same-type conversions (UCUM-to-UCUM, calendar-to-calendar)
 * and cross-type conversions (calendar-to-UCUM, UCUM-to-calendar) where applicable.
 */
public sealed interface FhirPathUnit permits UcumUnit, CalendarDurationUnit,
    CustomUnit {

  /**
   * The precision (number of decimal places) to use when computing unit conversions.
   */
  int CONVERSION_PRECISION = 20;

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
   * Computes the conversion factor between two units, supporting same-type conversions
   * (UCUM-to-UCUM, calendar-to-calendar) and cross-type conversions (calendar-to-UCUM,
   * UCUM-to-calendar) where applicable.
   * <p>
   * Cross-type conversions work by finding intermediate representations:
   * <ul>
   *   <li>Calendar → UCUM: Converts this calendar unit to seconds, then seconds to UCUM equivalent,
   *       then performs UCUM-to-UCUM conversion to target</li>
   *   <li>UCUM → Calendar: Converts source UCUM to seconds equivalent, then performs
   *       calendar-to-calendar conversion from seconds to target</li>
   * </ul>
   * <p>
   * Examples of valid conversions:
   * <ul>
   *   <li>UCUM → UCUM: mg → kg</li>
   *   <li>Calendar → Calendar: day → hour</li>
   *   <li>Calendar → UCUM: millisecond → ms</li>
   *   <li>UCUM → Calendar: s → second</li>
   * </ul>
   * <p>
   * Conversions return empty when:
   * <ul>
   *   <li>Either unit is a {@link CustomUnit}</li>
   *   <li>Units measure incompatible dimensions (e.g., mass vs volume)</li>
   *   <li>Non-definite calendar units are involved in certain conversions</li>
   * </ul>
   *
   * @param sourceUnit the source unit to convert from
   * @param targetUnit the target unit to convert to
   * @return an Optional containing the conversion factor if conversion is possible, or empty if the
   * units are incompatible or custom units are involved
   */
  @Nonnull
  static Optional<ConversionFactor> conversionFactorTo(@Nonnull FhirPathUnit sourceUnit,
      @Nonnull FhirPathUnit targetUnit) {
    // This handles cross-unit conversions.
    return switch (sourceUnit) {
      case CalendarDurationUnit cdUnitSource -> switch (targetUnit) {
        case CalendarDurationUnit cdUnitTarget -> cdUnitSource.conversionFactorTo(cdUnitTarget);
        case UcumUnit ucumUnitTarget -> cdUnitSource.conversionFactorTo(ucumUnitTarget);
        default -> Optional.empty();
      };
      case UcumUnit ucumUnitSource -> switch (targetUnit) {
        case UcumUnit ucumUnitTarget -> ucumUnitSource.conversionFactorTo(ucumUnitTarget);
        case CalendarDurationUnit cdUnitTarget -> cdUnitTarget.conversionFactorFrom(ucumUnitSource);
        default -> Optional.empty();
      };
      default -> Optional.empty();
    };
  }

  /**
   * Parses a unit string and creates the appropriate FhirPathUnit instance.
   * <p>
   * The parsing strategy is:
   * <ol>
   *   <li>First attempts to parse as a calendar duration unit (year, month, week, day, hour,
   *       minute, second, millisecond - both singular and plural forms)</li>
   *   <li>If not recognized as a calendar duration, assumes it's a UCUM unit code</li>
   * </ol>
   * <p>
   * This method never returns a {@link CustomUnit} - custom units must be created explicitly
   * using their constructor with both system and code parameters.
   * <p>
   * Examples:
   * <ul>
   *   <li>"year" → {@link CalendarDurationUnit#YEAR}</li>
   *   <li>"seconds" → {@link CalendarDurationUnit#SECOND}</li>
   *   <li>"mg" → {@link UcumUnit} with code "mg"</li>
   *   <li>"unknown-unit" → {@link UcumUnit} with code "unknown-unit" (may fail on conversion)</li>
   * </ul>
   *
   * @param unit the unit string (e.g., "year", "seconds", "mg", "kg")
   * @return a FhirPathUnit instance (CalendarDurationUnit if recognized, otherwise UcumUnit)
   */
  @Nonnull
  static FhirPathUnit fromString(@Nonnull String unit) {
    return CalendarDurationUnit.fromString(unit)
        .map(FhirPathUnit.class::cast)
        .orElseGet(() -> new UcumUnit(unit));
  }
}
