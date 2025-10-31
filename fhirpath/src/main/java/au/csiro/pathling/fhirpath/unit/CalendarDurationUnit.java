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

import au.csiro.pathling.fhirpath.FhirPathQuantity;
import jakarta.annotation.Nonnull;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Enumeration of valid FHIRPath calendar duration units from year to millisecond.
 * <p>
 * Calendar duration units represent time periods as defined in the FHIRPath specification.
 * These units are divided into two categories:
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
 * </ul>
 * <p>
 * Calendar duration units support both singular and plural forms (e.g., "year" and "years").
 */
public enum CalendarDurationUnit implements FhirPathUnit {
  YEAR("year", false, "a"),
  MONTH("month", false, "mo"),
  WEEK("week", false, "wk"),
  DAY("day", false, "d"),
  HOUR("hour", false, "h"),
  MINUTE("minute", false, "min"),
  SECOND("second", true, "s"),
  MILLISECOND("millisecond", true, "ms");

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

  private static final Map<String, CalendarDurationUnit> NAME_MAP = new HashMap<>();

  static {
    for (CalendarDurationUnit unit : values()) {
      NAME_MAP.put(unit.unit, unit);
      NAME_MAP.put(unit.unit + "s", unit); // plural
    }
  }


  CalendarDurationUnit(@Nonnull String code, boolean definite, @Nonnull String ucumEquivalent) {
    this.unit = code;
    this.definite = definite;
    this.ucumEquivalent = ucumEquivalent;
  }

  /**
   * Returns the FHIRPath calendar duration system URI.
   *
   * @return the calendar duration system URI
   * ({@value FhirPathQuantity#FHIRPATH_CALENDAR_DURATION_SYSTEM_URI})
   */
  @Override
  @Nonnull
  public String system() {
    return FhirPathQuantity.FHIRPATH_CALENDAR_DURATION_SYSTEM_URI;
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
    return Optional.ofNullable(NAME_MAP.get(name.toLowerCase(Locale.ROOT)));
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
                convertToMilliseconds(this),
                convertToMilliseconds(targetUnit)
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
   *   <li>Converting the UCUM source to the UCUM equivalent of seconds</li>
   *   <li>Converting seconds to this calendar unit</li>
   * </ol>
   *
   * @param ucumUnitSource the source UCUM unit to convert from
   * @return an Optional containing the conversion factor if conversion is possible, or empty
   * otherwise
   */
  @Nonnull
  Optional<ConversionFactor> conversionFactorFrom(@Nonnull UcumUnit ucumUnitSource) {
    return SECOND.asUcum().conversionFactorTo(ucumUnitSource)
        .flatMap(ucumFactor ->
            SECOND.conversionFactorTo(this)
                .map(ucumFactor::apply)
        );
  }

  /**
   * Attempts to convert this calendar duration to its UCUM equivalent, if one exists.
   * <p>
   * Only definite duration units (second, millisecond) have UCUM equivalents because they have
   * fixed lengths. Non-definite units (year, month, week, day, hour, minute) have variable
   * lengths and cannot be represented as UCUM units.
   *
   * @return the equivalent UCUM unit
   * @throws IllegalArgumentException if this calendar duration unit has no UCUM equivalent
   * (i.e., it is not a definite duration)
   */
  public UcumUnit asUcum() {
    return Optional.of(this)
        .filter(CalendarDurationUnit::isDefinite)
        .map(cdu -> new UcumUnit(cdu.getUcumEquivalent()))
        .orElseThrow(() -> new IllegalArgumentException("Cannot convert to Ucum: " + unit));
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
   * Converts a calendar duration unit to its equivalent in milliseconds.
   * <ul>
   *   <li>1 second = 1000 milliseconds</li>
   *   <li>1 minute = 60 seconds = 60,000 milliseconds</li>
   *   <li>1 hour = 60 minutes = 3,600,000 milliseconds</li>
   *   <li>1 day = 24 hours = 86,400,000 milliseconds</li>
   *   <li>1 week = 7 days = 604,800,000 milliseconds</li>
   * </ul>
   *
   * @param unit the calendar duration unit
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

  private static final BigDecimal MONTHS_IN_YEAR = new BigDecimal(12);

  /**
   * Conversions that are incompatible and have no defined conversion factor. These conversions
   * cannot use the milliseconds-based calculation because they involve non-definite duration units
   * with no meaningful relationship (e.g., weeks to months - a month isn't a whole number of weeks).
   */
  private static final Set<Pair<CalendarDurationUnit, CalendarDurationUnit>> INCOMPATIBLE_CONVERSIONS =
      Set.of(
          Pair.of(WEEK, YEAR), Pair.of(YEAR, WEEK),
          Pair.of(WEEK, MONTH), Pair.of(MONTH, WEEK)
      );

  /**
   * Special case conversions that have defined relationships different from the standard
   * milliseconds-based calculation (e.g., year to month uses 12, not the millisecond approximation).
   */
  private static final Map<Pair<CalendarDurationUnit, CalendarDurationUnit>, ConversionFactor> SPECIAL_CASES =
      Map.ofEntries(
          Map.entry(Pair.of(YEAR, MONTH), ConversionFactor.of(MONTHS_IN_YEAR)),
          Map.entry(Pair.of(MONTH, YEAR), ConversionFactor.inverseOf(MONTHS_IN_YEAR))
      );

}

