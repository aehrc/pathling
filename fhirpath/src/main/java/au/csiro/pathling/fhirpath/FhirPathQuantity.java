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
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a FHIRPath Quantity value.
 */
@Slf4j
@Value
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class FhirPathQuantity {

  /**
   * The system URI for Fhipath calendar duration units (e.g. year, month, day).
   */
  public static final String FHIRPATH_CALENDAR_DURATION_SYSTEM_URI = "https://hl7.org/fhirpath/N1/calendar-duration";

  /**
   * The system URI for UCUM units.
   */
  public static final String UCUM_SYSTEM_URI = "http://unitsofmeasure.org";

  /**
   * Regex pattern for parsing FHIRPath quantity literals. Unit is optional per FHIRPath spec -
   * defaults to '1' when omitted.
   */
  private static final Pattern QUANTITY_REGEX = Pattern.compile(
      "(?<value>[+-]?\\d+(?:\\.\\d+)?)\\s*(?:'(?<unit>[^']+)'|(?<time>[a-zA-Z]+))?"
  );

  @Nonnull
  BigDecimal value;
  @Nonnull
  String unit;
  @Nonnull
  String system;
  @Nonnull
  String code;

  /**
   * Check if the quantity is a calendar duration.
   *
   * @return true if the quantity is a calendar duration, false otherwise
   */
  public boolean isCalendarDuration() {
    return FHIRPATH_CALENDAR_DURATION_SYSTEM_URI.equals(system);
  }

  /**
   * Check if the quantity is a UCUM quantity.
   *
   * @return true if the quantity is a UCUM quantity, false otherwise
   */

  public boolean isUCUM() {
    return UCUM_SYSTEM_URI.equals(system);
  }

  /**
   * Factory method for UCUM quantities.
   *
   * @param value the numeric value
   * @param unit the UCUM unit string (e.g. 'mg', 'kg', 'mL')
   * @return UCUM quantity
   */
  @Nonnull
  public static FhirPathQuantity ofUCUM(@Nonnull final BigDecimal value,
      @Nonnull final String unit) {
    return new FhirPathQuantity(value, unit, UCUM_SYSTEM_URI, unit);
  }

  /**
   * Factory method for calendar duration quantities.
   *
   * @param value the numeric value
   * @param unit the CalendarDurationUnit enum value
   * @param unitName the name of the calendar duration unit (e.g. 'year', 'month', 'day')
   * @return calendar duration quantity
   */
  @Nonnull
  public static FhirPathQuantity ofCalendar(@Nonnull final BigDecimal value,
      @Nonnull final CalendarDurationUnit unit, @Nonnull final String unitName) {
    if (!CalendarDurationUnit.parseString(unitName).equals(unit)) {
      throw new IllegalArgumentException(
          "Unit name " + unitName + " does not match CalendarDurationUnit " + unit);
    }
    return new FhirPathQuantity(value, unitName,
        FHIRPATH_CALENDAR_DURATION_SYSTEM_URI,
        unit.getUnit());
  }

  /**
   * Factory method for calendar duration quantities with canonical unit name.
   *
   * @param value the numeric value
   * @param unit the CalendarDurationUnit enum value
   * @return calendar duration quantity
   */
  @Nonnull
  public static FhirPathQuantity ofCalendar(@Nonnull final BigDecimal value,
      @Nonnull final CalendarDurationUnit unit) {
    return ofCalendar(value, unit, unit.getUnit());
  }

  /**
   * Parses a FHIRPath quantity literal (e.g. 5.4 'mg', 1 year, 42). Only supports UCUM and calendar
   * duration units. When no unitCode is specified, defaults to '1' in UCUM system.
   *
   * @param literal the FHIRPath quantity literal
   * @return the parsed FhirPathQuantity
   * @throws IllegalArgumentException if the literal is not a valid FHIRPath quantity literal
   */
  @Nonnull
  public static FhirPathQuantity parse(@Nonnull final String literal) {
    // Regex with named groups for value, unitCode (quoted), and time (bareword calendar/ucum)
    final Matcher matcher = QUANTITY_REGEX.matcher(literal.trim());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid FHIRPath quantity literal: " + literal);
    }
    final BigDecimal value = new BigDecimal(matcher.group("value"));
    if (matcher.group("unit") != null) {
      // Quoted unitCode, always UCUM
      return ofUCUM(value, matcher.group("unit"));
    } else if (matcher.group("time") != null) {
      return ofCalendar(value,
          CalendarDurationUnit.parseString(matcher.group("time")),
          matcher.group("time")
      );
    } else {
      // No unitCode specified, default to '1' in UCUM system per FHIRPath spec
      return ofUCUM(value, "1");
    }
  }

  /**
   * Factory method for creating a quantity from a FhirPathUnit.
   *
   * @param value the numeric value
   * @param unit the FhirPathUnit (UCUM, calendar duration, or custom)
   * @return quantity with the specified value and unit
   */
  @Nonnull
  public static FhirPathQuantity ofUnit(@Nonnull final BigDecimal value,
      @Nonnull final FhirPathUnit unit) {
    return new FhirPathQuantity(value, unit.unit(), unit.system(), unit.code());
  }

  /**
   * Gets the FhirPathUnit representation of this quantity's unit.
   *
   * @return the FhirPathUnit (Ucum, CalendarDuration, or Custom)
   */
  @Nonnull
  public FhirPathUnit getFhirpathUnit() {
    return switch (system) {
      case FHIRPATH_CALENDAR_DURATION_SYSTEM_URI -> new CalendarDuration(
          CalendarDurationUnit.parseString(code), unit);
      case UCUM_SYSTEM_URI -> new FhirPathUnit.Ucum(code);
      default -> new FhirPathUnit.Custom(system, code, unit);
    };
  }

  /**
   * Converts this quantity to the specified target unit if a conversion is possible.
   * <p>
   * Supports:
   * <ul>
   *   <li>UCUM to UCUM conversions (e.g., 'mg' to 'kg')</li>
   *   <li>Calendar duration to calendar duration conversions (only to definite units: second,
   *       millisecond)</li>
   *   <li>Cross-type conversions: calendar duration to UCUM 's' or 'ms', and vice versa</li>
   * </ul>
   *
   * @param unit the target unit string (e.g., "mg", "second", "s")
   * @return an Optional containing the converted quantity, or empty if conversion is not possible
   */
  @Nonnull
  public Optional<FhirPathQuantity> convertToUnit(@Nonnull final String unit) {
    final FhirPathUnit sourceUnit = getFhirpathUnit();
    final FhirPathUnit targetUnit = FhirPathUnit.fromString(unit);
    if (targetUnit.equals(sourceUnit)) {
      return Optional.of(this);
    } else {
      return FhirPathUnit.conversionFactorTo(getFhirpathUnit(), targetUnit)
          .map(cf -> FhirPathQuantity.ofUnit(getValue().multiply(cf), targetUnit));
    }
  }
}
