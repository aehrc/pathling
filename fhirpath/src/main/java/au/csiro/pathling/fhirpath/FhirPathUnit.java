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

import au.csiro.pathling.fhirpath.FhirPathUnit.CalendarDuration;
import io.github.fhnaumann.funcs.ConverterService;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;

/**
 * Represents a FHIRPath unit of measure, which can be either a UCUM unit, a calendar duration
 * unit, or a custom unit from another system.
 * <p>
 * This sealed interface has three permitted implementations:
 * <ul>
 *   <li>{@link Ucum} - UCUM (Unified Code for Units of Measure) units</li>
 *   <li>{@link CalendarDuration} - FHIRPath calendar duration units (year, month, week, etc.)</li>
 *   <li>{@link Custom} - Custom units from other systems</li>
 * </ul>
 * <p>
 * Units can be converted between compatible types using the {@link #conversionFactorTo} method,
 * which computes conversion factors for both same-type conversions (UCUM-to-UCUM, calendar-to-calendar)
 * and cross-type conversions (calendar-to-UCUM, UCUM-to-calendar) where applicable.
 */
public sealed interface FhirPathUnit permits FhirPathUnit.Ucum, CalendarDuration,
    FhirPathUnit.Custom {

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
   * Gets the unit string representation (may differ from code, e.g., "seconds" vs "second").
   *
   * @return the unit string
   */
  @Nonnull
  String unit();

  /**
   * Represents a UCUM (Unified Code for Units of Measure) unit.
   *
   * @param code the UCUM unit code (e.g., "mg", "kg", "mL")
   */
  record Ucum(@Nonnull String code) implements FhirPathUnit {

    @Override
    @Nonnull
    public String system() {
      return FhirPathQuantity.UCUM_SYSTEM_URI;
    }

    @Override
    @Nonnull
    public String unit() {
      return code;
    }

    /**
     * Computes the conversion factor to convert values from this UCUM unit to the target UCUM
     * unit.
     *
     * @param targetUnit the target UCUM unit to convert to
     * @return an Optional containing the conversion factor if conversion is possible, or empty if
     *     the units are incompatible
     */
    @Nonnull
    public Optional<BigDecimal> conversionFactorTo(@Nonnull Ucum targetUnit) {
      var conversionResult = au.csiro.pathling.encoders.terminology.ucum.Ucum.service()
          .convert(code(), targetUnit.code());
      if (conversionResult instanceof ConverterService.Success(var conversionFactor)) {
        return Optional.of(conversionFactor.getValue());
      }
      return Optional.empty(); // Return empty if conversion fails.
    }

    /**
     * Attempts to convert this UCUM unit to its calendar duration equivalent, if one exists.
     *
     * @return an Optional containing the equivalent CalendarDuration, or empty if this UCUM unit
     *     has no calendar duration equivalent
     */
    @Nonnull
    public Optional<CalendarDuration> asCalendarDuration() {
      return CalendarDurationUnit.fromUcumUnit(code)
          .map(CalendarDuration::new);
    }
  }

  /**
   * Represents a FHIRPath calendar duration unit (year, month, week, day, hour, minute, second,
   * millisecond).
   *
   * @param unitCode the CalendarDurationUnit enum value
   * @param unit     the string representation of the unit (e.g., "second", "seconds")
   */
  record CalendarDuration(
      @Nonnull CalendarDurationUnit unitCode,
      @Nonnull String unit
  ) implements FhirPathUnit {

    /**
     * Creates a CalendarDuration with the canonical unit name.
     *
     * @param unitCode the CalendarDurationUnit enum value
     */
    public CalendarDuration(@Nonnull CalendarDurationUnit unitCode) {
      this(unitCode, unitCode.getUnit());
    }

    @Override
    @Nonnull
    public String code() {
      return unitCode.getUnit();
    }

    @Override
    @Nonnull
    public String system() {
      return FhirPathQuantity.FHIRPATH_CALENDAR_DURATION_SYSTEM_URI;
    }

    /**
     * Computes the conversion factor to convert values from this calendar duration unit to the
     * target calendar duration unit. Only supports conversions to definite duration units (second,
     * millisecond) as per the FHIRPath specification.
     *
     * @param targetUnit the target calendar duration unit to convert to
     * @return an Optional containing the conversion factor if conversion is possible, or empty if
     *     the target unit is not a definite duration
     */
    @Nonnull
    public Optional<BigDecimal> conversionFactorTo(@Nonnull CalendarDuration targetUnit) {
      return Optional.of(targetUnit.unitCode())
          .filter(CalendarDurationUnit::isDefinite)
          .map(cdcTarget -> convertToMilliseconds(unitCode())
              .divide(convertToMilliseconds(cdcTarget), 10, RoundingMode.HALF_UP));
    }

    /**
     * Attempts to convert this calendar duration to its UCUM equivalent, if one exists. Only
     * definite duration units (second, millisecond) have UCUM equivalents.
     *
     * @return an Optional containing the equivalent Ucum unit, or empty if this calendar duration
     *     has no UCUM equivalent
     */
    public Optional<Ucum> asUcum() {
      return Optional.of(unitCode)
          .filter(CalendarDurationUnit::isDefinite)
          .map(cdu -> new Ucum(cdu.getUcumEquivalent()));
    }


    private static final BigDecimal MILLISECONDS_IN_MS = BigDecimal.ONE;
    private static final BigDecimal SECONDS_IN_MS = MILLISECONDS_IN_MS.multiply(
        new BigDecimal(1000));
    private static final BigDecimal MINUTES_IN_MS = SECONDS_IN_MS.multiply(new BigDecimal(60));
    private static final BigDecimal HOURS_IN_MS = MINUTES_IN_MS.multiply(new BigDecimal(60));
    private static final BigDecimal DAY_IN_MS = HOURS_IN_MS.multiply(new BigDecimal(24));
    private static final BigDecimal WEEK_IN_MS = DAY_IN_MS.multiply(new BigDecimal(7));
    private static final BigDecimal MONTH_IN_MS = DAY_IN_MS.multiply(new BigDecimal(30));
    private static final BigDecimal YEAR_IN_MS = DAY_IN_MS.multiply(new BigDecimal(365));

    /**
     * Converts a calendar duration unitCode to its equivalent in milliseconds.
     * <ul>
     *   <li>1 second = 1000 milliseconds</li>
     *   <li>1 minute = 60 seconds = 60,000 milliseconds</li>
     *   <li>1 hour = 60 minutes = 3,600,000 milliseconds</li>
     *   <li>1 day = 24 hours = 86,400,000 milliseconds</li>
     *   <li>1 week = 7 days = 604,800,000 milliseconds</li>
     * </ul>
     *
     * @param unit the calendar duration unitCode
     * @return the value in milliseconds
     */
    @Nonnull
    private static BigDecimal convertToMilliseconds(@Nonnull final CalendarDurationUnit unit) {
      return switch (unit) {
        case MILLISECOND -> MILLISECONDS_IN_MS;
        case SECOND -> SECONDS_IN_MS;
        case MINUTE -> MINUTES_IN_MS;
        case HOUR -> HOURS_IN_MS;
        case DAY -> DAY_IN_MS;
        case WEEK -> WEEK_IN_MS;
        case MONTH -> MONTH_IN_MS;
        case YEAR -> YEAR_IN_MS;
      };
    }
  }

  /**
   * Represents a custom unit from a system other than UCUM or calendar durations.
   *
   * @param system the system URI for this custom unit
   * @param code   the code within that system
   * @param unit   the string representation of the unit
   */
  record Custom(
      @Nonnull String system,
      @Nonnull String code,
      @Nonnull String unit
  ) implements FhirPathUnit {

  }

  /**
   * Computes the conversion factor between two units, supporting same-type conversions
   * (UCUM-to-UCUM, calendar-to-calendar) and cross-type conversions (calendar-to-UCUM,
   * UCUM-to-calendar) where applicable.
   * <p>
   * Cross-type conversions work by finding intermediate representations:
   * <ul>
   *   <li>Calendar → UCUM: Converts UCUM target to calendar duration equivalent (if it exists),
   *       then performs calendar-to-calendar conversion</li>
   *   <li>UCUM → Calendar: Converts calendar target to UCUM equivalent (if it exists),
   *       then performs UCUM-to-UCUM conversion</li>
   * </ul>
   *
   * @param sourceUnit the source unit to convert from
   * @param targetUnit the target unit to convert to
   * @return an Optional containing the conversion factor if conversion is possible, or empty if
   *     the units are incompatible or custom units are involved
   */
  @Nonnull
  static Optional<BigDecimal> conversionFactorTo(@Nonnull FhirPathUnit sourceUnit,
      @Nonnull FhirPathUnit targetUnit) {
    // This handles cross-unit conversions.
    return switch (sourceUnit) {
      case CalendarDuration cdUnitSource -> switch (targetUnit) {
        case CalendarDuration cdUnitTarget -> cdUnitSource.conversionFactorTo(cdUnitTarget);
        case Ucum ucumUnitTarget ->
            ucumUnitTarget.asCalendarDuration().flatMap(cdUnitSource::conversionFactorTo);
        default -> Optional.empty();
      };
      case Ucum ucumUnitSource -> switch (targetUnit) {
        case Ucum ucumUnitTarget -> ucumUnitSource.conversionFactorTo(ucumUnitTarget);
        case CalendarDuration cdUnitTarget ->
            cdUnitTarget.asUcum().flatMap(ucumUnitSource::conversionFactorTo);
        default -> Optional.empty();
      };
      default -> Optional.empty();
    };
  }

  /**
   * Parses a unit string and creates the appropriate FhirPathUnit instance. First attempts to
   * parse as a calendar duration unit; if that fails, assumes it's a UCUM unit.
   *
   * @param unit the unit string (e.g., "year", "seconds", "mg", "kg")
   * @return a FhirPathUnit instance (CalendarDuration if recognized, otherwise Ucum)
   */
  @Nonnull
  static FhirPathUnit fromString(@Nonnull String unit) {
    return CalendarDurationUnit.fromString(unit)
        .map(cdCode -> new CalendarDuration(cdCode, unit))
        .map(FhirPathUnit.class::cast)
        .orElseGet(() -> new Ucum(unit));
  }
}
