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
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Enumeration of valid FHIRPath calendar duration units from year to millisecond.
 * <p>
 * Calendar duration units represent time periods as defined in the FHIRPath specification. These
 * units are divided into two categories:
 * <ul>
 *   <li><b>Definite duration units</b>: second, millisecond - have fixed lengths and can be
 *       converted to UCUM equivalents</li>
 *   <li><b>Non-definite duration units</b>: year, month, week, day, hour, minute - have
 *       variable lengths (e.g., leap years, different month lengths) and cannot be reliably
 *       converted to other units in all contexts</li>
 * </ul>
 * <p>
 * Each calendar duration unit has:
 * <ul>
 *   <li>A canonical code (e.g., "year", "second")</li>
 *   <li>A UCUM equivalent code (e.g., "a" for year, "s" for second)</li>
 *   <li>A definite/non-definite classification</li>
 *   <li>A millisecond conversion factor for unit conversions</li>
 * </ul>
 * <p>
 * Calendar duration units support both singular and plural forms (e.g., "year" and "years").
 * <p>
 * <b>IMPORTANT:</b> For non-definite duration units (year, month), the millisecond values are
 * <b>approximations</b> as specified by the FHIRPath specification:
 * <ul>
 *   <li>1 year = 365 days (does not account for leap years)</li>
 *   <li>1 month = 30 days (does not account for varying month lengths)</li>
 * </ul>
 * These approximations are used for conversion purposes only, not for calendar-aware arithmetic.
 */
public enum CalendarDurationUnit implements FhirPathUnit {
  /**
   * APPROXIMATION: Calendar years are assumed to be 365 days for conversion purposes.
   */
  YEAR("year", false, "a", new BigDecimal("31536000000")),      // 365 * 24 * 60 * 60 * 1000
  /**
   * APPROXIMATION: Calendar months are assumed to be 30 days for conversion purposes.
   */
  MONTH("month", false, "mo", new BigDecimal("2592000000")),    // 30 * 24 * 60 * 60 * 1000
  WEEK("week", false, "wk", new BigDecimal("604800000")),       // 7 * 24 * 60 * 60 * 1000
  DAY("day", false, "d", new BigDecimal("86400000")),           // 24 * 60 * 60 * 1000
  HOUR("hour", false, "h", new BigDecimal("3600000")),          // 60 * 60 * 1000
  MINUTE("minute", false, "min", new BigDecimal("60000")),      // 60 * 1000
  SECOND("second", true, "s", new BigDecimal("1000")),          // 1000 ms
  MILLISECOND("millisecond", true, "ms", BigDecimal.ONE);       // 1 ms

  /**
   * The system URI for Fhipath calendar duration units (e.g. year, month, day).
   */
  public static final String FHIRPATH_CALENDAR_DURATION_SYSTEM_URI = "https://hl7.org/fhirpath/N1/calendar-duration";
  @Nonnull
  private final String unit;

  /**
   * Indicates whether the unit is a definite duration (i.e., has a fixed length in time). For
   * example seconds, and milliseconds are definite units, while years and months are not because
   * their lengths can vary (e.g., due to leap years or different month lengths).
   */
  @Getter
  private final boolean definite;

  /**
   * The UCUM equivalent of the calendar duration unit.
   */
  @Getter
  @Nonnull
  private final String ucumEquivalent;

  /**
   * The number of milliseconds equivalent to one unit of this calendar duration. For non-definite
   * units (year, month), this is an approximation as specified by the FHIRPath specification (1
   * year = 365 days, 1 month = 30 days).
   */
  @Getter
  @Nonnull
  private final BigDecimal millisecondsEquivalent;

  private static final Map<String, CalendarDurationUnit> NAME_MAP = new HashMap<>();

  static {
    for (CalendarDurationUnit unit : values()) {
      NAME_MAP.put(unit.unit, unit);
      NAME_MAP.put(unit.unit + "s", unit); // plural
    }
  }


  CalendarDurationUnit(@Nonnull String code, boolean definite, @Nonnull String ucumEquivalent,
      @Nonnull BigDecimal millisecondsEquivalent) {
    this.unit = code;
    this.definite = definite;
    this.ucumEquivalent = ucumEquivalent;
    this.millisecondsEquivalent = millisecondsEquivalent;
  }

  /**
   * Returns the FHIRPath calendar duration system URI.
   *
   * @return the calendar duration system URI
   * ({@value CalendarDurationUnit#FHIRPATH_CALENDAR_DURATION_SYSTEM_URI})
   */
  @Override
  @Nonnull
  public String system() {
    return FHIRPATH_CALENDAR_DURATION_SYSTEM_URI;
  }

  /**
   * Returns the canonical code for this calendar duration unit.
   *
   * @return the unit code (e.g., "year", "month", "second")
   */
  @Override
  @Nonnull
  public String code() {
    return unit;
  }

  /**
   * Checks if the given unit name is a valid representation of this calendar duration unit.
   * Validates that the unit name resolves to this specific enum value (case-sensitive, supports
   * both singular and plural forms).
   *
   * @param unitName the unit name to validate (e.g., "year", "years")
   * @return true if unitName is valid for this calendar duration unit, false otherwise
   */
  @Override
  public boolean isValidName(@Nonnull final String unitName) {
    return fromString(unitName)
        .map(this::equals)
        .orElse(false);
  }

  /**
   * Gets the CalendarDurationUnit from its string representation (case-insensitive, singular or
   * plural).
   *
   * @param name the name of the unit (e.g. "year", "years")
   * @return the corresponding CalendarDurationUnit
   * @throws IllegalArgumentException if the name is not valid
   */
  @Nonnull
  public static CalendarDurationUnit parseString(@Nonnull String name) {
    return fromString(name).orElseThrow(
        () -> new IllegalArgumentException("Unknown calendar duration unit: " + name));
  }

  /**
   * Gets the CalendarDurationUnit from its string representation (case-insensitive, singular or
   * plural).
   *
   * @param name the name of the unit (e.g. "year", "years")
   * @return an Optional containing the corresponding CalendarDurationUnit, or empty if not found
   */
  @Nonnull
  public static Optional<CalendarDurationUnit> fromString(@Nonnull String name) {
    return Optional.ofNullable(NAME_MAP.get(name));
  }

  /**
   * Computes the conversion factor to convert values from this calendar duration unit to the target
   * calendar duration unit. Only supports conversions to definite duration units (second,
   * millisecond) as per the FHIRPath specification.
   *
   * @param targetUnit the target calendar duration unit to convert to
   * @return an Optional containing the conversion factor if conversion is possible, or empty if the
   * units are incompatible (e.g., week to month)
   */
  @Nonnull
  public Optional<ConversionFactor> conversionFactorTo(
      @Nonnull CalendarDurationUnit targetUnit) {

    // Check for explicitly incompatible conversions first
    if (INCOMPATIBLE_CONVERSIONS.contains(Pair.of(this, targetUnit))) {
      return Optional.empty();
    }

    // Check for special case conversions (e.g., year to month)
    return Optional.ofNullable(SPECIAL_CASES.get(Pair.of(this, targetUnit)))
        .or(() ->
            // Fall back to milliseconds-based conversion for compatible units
            Optional.of(ConversionFactor.ofFraction(
                getMillisecondsEquivalent(),
                targetUnit.getMillisecondsEquivalent()
            ))
        );
  }

  /**
   * Computes the conversion factor to convert values from this calendar duration unit to a target
   * UCUM unit.
   * <p>
   * This is a cross-type conversion that works by:
   * <ol>
   *   <li>Converting this calendar unit to seconds (if possible)</li>
   *   <li>Converting seconds to its UCUM equivalent</li>
   *   <li>Converting the UCUM equivalent to the target UCUM unit</li>
   * </ol>
   *
   * @param ucumUnitTarget the target UCUM unit to convert to
   * @return an Optional containing the conversion factor if conversion is possible, or empty
   * otherwise
   */
  @Nonnull
  public Optional<ConversionFactor> conversionFactorTo(@Nonnull final UcumUnit ucumUnitTarget) {
    return conversionFactorTo(SECOND)
        .flatMap(cdFactor ->
            SECOND.asUcum().conversionFactorTo(ucumUnitTarget)
                .map(cdFactor::apply)
        );
  }

  /**
   * Computes the conversion factor to convert values from a source UCUM unit to this calendar
   * duration unit.
   * <p>
   * This is a cross-type conversion that works by:
   * <ol>
   *   <li>Converting the UCUM source to the UCUM equivalent of seconds ('s')</li>
   *   <li>Converting 'second' (calendar) to this calendar unit</li>
   * </ol>
   *
   * @param ucumUnitSource the source UCUM unit to convert from
   * @return an Optional containing the conversion factor if conversion is possible, or empty
   * otherwise
   */
  @Nonnull
  Optional<ConversionFactor> conversionFactorFrom(@Nonnull UcumUnit ucumUnitSource) {
    return ucumUnitSource.conversionFactorTo(SECOND.asUcum())
        .flatMap(ucumFactor ->
            SECOND.conversionFactorTo(this)
                .map(ucumFactor::apply)
        );
  }

  /**
   * Attempts to convert this calendar duration to its UCUM equivalent, if one exists.
   * <p>
   * Only definite duration units (second, millisecond) have UCUM equivalents because they have
   * fixed lengths. Non-definite units (year, month, week, day, hour, minute) have variable lengths
   * and cannot be represented as UCUM units.
   *
   * @return the equivalent UCUM unit
   * @throws IllegalArgumentException if this calendar duration unit has no UCUM equivalent (i.e.,
   * it is not a definite duration)
   */
  public UcumUnit asUcum() {
    return Optional.of(this)
        .filter(CalendarDurationUnit::isDefinite)
        .map(cdu -> new UcumUnit(cdu.getUcumEquivalent()))
        .orElseThrow(() -> new IllegalArgumentException("Cannot convert to Ucum: " + unit));
  }


  private static final BigDecimal MONTHS_IN_YEAR = new BigDecimal(12);

  /**
   * Conversions that are incompatible and have no defined conversion factor. These conversions
   * cannot use the milliseconds-based calculation because they involve non-definite duration units
   * with no meaningful relationship (e.g., weeks to months - a month isn't a whole number of
   * weeks).
   */
  private static final Set<Pair<CalendarDurationUnit, CalendarDurationUnit>> INCOMPATIBLE_CONVERSIONS =
      Set.of(
          Pair.of(WEEK, YEAR), Pair.of(YEAR, WEEK),
          Pair.of(WEEK, MONTH), Pair.of(MONTH, WEEK)
      );

  /**
   * Special case conversions that have defined relationships different from the standard
   * milliseconds-based calculation (e.g., year to month uses 12, not the millisecond
   * approximation).
   */
  private static final Map<Pair<CalendarDurationUnit, CalendarDurationUnit>, ConversionFactor> SPECIAL_CASES =
      Map.ofEntries(
          Map.entry(Pair.of(YEAR, MONTH), ConversionFactor.of(MONTHS_IN_YEAR)),
          Map.entry(Pair.of(MONTH, YEAR), ConversionFactor.inverseOf(MONTHS_IN_YEAR))
      );

}

