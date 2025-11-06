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

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.fhirpath.unit.CalendarDurationUnit;
import au.csiro.pathling.fhirpath.unit.FhirPathUnit;
import au.csiro.pathling.fhirpath.unit.UcumUnit;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Value;

/**
 * Represents a FHIRPath Quantity value.
 */
@Value
public class FhirPathQuantity {

  /**
   * Regex pattern for parsing FHIRPath quantity literals. Unit is optional per FHIRPath spec -
   * defaults to '1' when omitted.
   */
  private static final Pattern QUANTITY_REGEX = Pattern.compile(
      "(?<value>[+-]?\\d+(?:\\.\\d+)?)\\s*(?:'(?<unit>[^']+)'|(?<time>[a-zA-Z]+))?"
  );

  private FhirPathQuantity(@Nonnull final BigDecimal value, @Nonnull final FhirPathUnit unit,
      @Nonnull final String unitName) {
    this.value = value;
    this.unit = unit;
    this.unitName = unitName;
    // validate the consistency between unit and unitName
    if (!unit.isValidName(unitName)) {
      throw new IllegalArgumentException(
          "Unit name " + unitName + " is not valid for unit " + unit);
    }
  }

  /**
   * The numeric value of the quantity.
   */
  @Nonnull
  BigDecimal value;

  /**
   * The FhirPathUnit representing the unit of measure (UCUM or calendar duration).
   */
  @Nonnull
  FhirPathUnit unit;

  /**
   * The string name of the unit as it appears in the original representation (e.g., "mg", "year",
   * "years").
   */
  @Nonnull
  String unitName;

  /**
   * Gets the system URI for this quantity's unit.
   *
   * @return the system URI (e.g., {@value UcumUnit#UCUM_SYSTEM_URI} or
   * {@value CalendarDurationUnit#FHIRPATH_CALENDAR_DURATION_SYSTEM_URI})
   */
  @Nonnull
  public String getSystem() {
    return unit.system();
  }

  /**
   * Gets the canonical code for this quantity's unit.
   *
   * @return the unit code (e.g., "mg", "second")
   */
  @Nonnull
  public String getCode() {
    return unit.code();
  }

  /**
   * Check if the quantity is a calendar duration.
   *
   * @return true if the quantity is a calendar duration, false otherwise
   */
  public boolean isCalendarDuration() {
    return unit instanceof CalendarDurationUnit;
  }

  /**
   * Check if the quantity is a UCUM quantity.
   *
   * @return true if the quantity is a UCUM quantity, false otherwise
   */

  public boolean isUcum() {
    return unit instanceof UcumUnit;
  }

  /**
   * Factory method for UCUM quantities.
   *
   * @param value the numeric value
   * @param unit the UCUM unit string (e.g. 'mg', 'kg', 'mL')
   * @return UCUM quantity
   */
  @Nonnull
  public static FhirPathQuantity ofUcum(@Nonnull final BigDecimal value,
      @Nonnull final String unit) {
    return new FhirPathQuantity(value, new UcumUnit(unit), unit);
  }

  /**
   * Factory method for calendar duration quantities.
   *
   * @param value the numeric value
   * @param calendarDurationUnit the FHIRPath calendar duration unit string (e.g., "year", "month",
   * "days") singular or plural
   * @return calendar duration quantity
   */
  @Nonnull
  public static FhirPathQuantity ofDuration(@Nonnull final BigDecimal value,
      @Nonnull final String calendarDurationUnit) {
    return ofUnit(value, CalendarDurationUnit.parseString(calendarDurationUnit),
        calendarDurationUnit);
  }

  /**
   * Parses a FHIRPath quantity literal (e.g. 5.4 'mg', 1 year, 42). Only supports UCUM and calendar
   * duration units. When no unit is specified, defaults to '1' in UCUM system.
   *
   * @param literal the FHIRPath quantity literal
   * @return the parsed FhirPathQuantity
   * @throws IllegalArgumentException if the literal is not a valid FHIRPath quantity literal
   */
  @Nonnull
  public static FhirPathQuantity parse(@Nonnull final String literal) {
    // Regex with named groups for value, unit (quoted), and time (bareword calendar/ucum)
    final Matcher matcher = QUANTITY_REGEX.matcher(literal.trim());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid FHIRPath quantity literal: " + literal);
    }
    final BigDecimal value = new BigDecimal(matcher.group("value"));
    if (matcher.group("unit") != null) {
      // Quoted unit, always UCUM
      return ofUcum(value, matcher.group("unit"));
    } else if (matcher.group("time") != null) {
      return ofDuration(value, matcher.group("time"));
    } else {
      // No unit specified, default to '1' in UCUM system per FHIRPath spec
      return ofUnit(value, UcumUnit.ONE);
    }
  }

  /**
   * Factory method for creating a quantity from a FhirPathUnit.
   *
   * @param value the numeric value
   * @param unit the FhirPathUnit (UCUM, calendar duration, or custom)
   * @param unitName the name of the unit
   * @return quantity with the specified value and unit
   */
  @Nonnull
  public static FhirPathQuantity ofUnit(@Nonnull final BigDecimal value,
      @Nonnull final FhirPathUnit unit, @Nonnull final String unitName) {
    return new FhirPathQuantity(value, unit, unitName);
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
    return ofUnit(value, unit, unit.code());
  }

  /**
   * Factory method for creating a quantity from system, code, and optional unit name. This method
   * is typically used when reconstructing quantities from FHIR Quantity resources.
   *
   * @param value the numeric value (nullable)
   * @param system the system URI (nullable)
   * @param code the unit code (nullable)
   * @param unit the optional unit name (nullable, defaults to code if not provided)
   * @return quantity with the specified value and unit, or null if any required parameter is null
   */
  @Nullable
  public static FhirPathQuantity of(@Nullable final BigDecimal value, @Nullable final String system,
      @Nullable final String code, @Nullable final String unit) {

    if (isNull(value) || isNull(system) || isNull(code)) {
      return null;
    }
    return switch (requireNonNull(system)) {
      case UcumUnit.UCUM_SYSTEM_URI -> ofUcum(requireNonNull(value), requireNonNull(code));
      case CalendarDurationUnit.FHIRPATH_CALENDAR_DURATION_SYSTEM_URI ->
          new FhirPathQuantity(value, CalendarDurationUnit.parseString(code),
              unit != null
              ? unit
              : code);
      default -> null;
    };
  }

  /**
   * Converts this quantity to the specified target unit if a conversion is possible, using the
   * default precision.
   * <p>
   * Supports:
   * <ul>
   *   <li>UCUM to UCUM conversions (e.g., 'mg' to 'kg')</li>
   *   <li>Calendar duration to calendar duration conversions (only to definite units: second,
   *       millisecond)</li>
   *   <li>Cross-type conversions: calendar duration to UCUM 's' or 'ms', and vice versa</li>
   * </ul>
   *
   * @param unitName the target unit string (e.g., "mg", "second", "s")
   * @return an Optional containing the converted quantity, or empty if conversion is not possible
   */
  @Nonnull
  public Optional<FhirPathQuantity> convertToUnit(@Nonnull final String unitName) {
    return convertToUnit(unitName, FhirPathUnit.CONVERSION_PRECISION);
  }

  /**
   * Converts this quantity to the specified target unit if a conversion is possible, using the
   * specified precision.
   * <p>
   * Supports:
   * <ul>
   *   <li>UCUM to UCUM conversions (e.g., 'mg' to 'kg')</li>
   *   <li>Calendar duration to calendar duration conversions (only to definite units: second,
   *       millisecond)</li>
   *   <li>Cross-type conversions: calendar duration to UCUM 's' or 'ms', and vice versa</li>
   * </ul>
   *
   * @param unitName the target unit string (e.g., "mg", "second", "s")
   * @param precision the number of decimal places to use in the conversion (must be between 1 and
   * 100)
   * @return an Optional containing the converted quantity, or empty if conversion is not possible
   * @throws IllegalArgumentException if precision is not between 1 and 100
   */
  @Nonnull
  public Optional<FhirPathQuantity> convertToUnit(@Nonnull final String unitName,
      final int precision) {
    final FhirPathUnit sourceUnit = getUnit();
    final FhirPathUnit targetUnit = FhirPathUnit.fromString(unitName);
    if (targetUnit.equals(sourceUnit)) {
      return Optional.of(FhirPathQuantity.ofUnit(getValue(), targetUnit, unitName));
    } else {
      return FhirPathUnit.conversionFactorTo(getUnit(), targetUnit)
          .map(cf -> FhirPathQuantity.ofUnit(cf.apply(getValue(), precision), targetUnit,
              unitName));
    }
  }

  /**
   * Helper method to create a canonical UCUM quantity.
   *
   * @param value the numeric value
   * @param code the UCUM unit code
   * @return an Optional containing the canonical UCUM quantity, or empty if conversion is not
   * possible
   */
  private static Optional<FhirPathQuantity> ofUcumCanonical(
      @Nonnull final BigDecimal value,
      @Nonnull final String code) {
    return Optional.ofNullable(Ucum.getCanonicalCode(value, code))
        .flatMap(canonicalCode ->
            Optional.ofNullable(Ucum.getCanonicalValue(value, code))
                .map(canonicalValue -> ofUcum(canonicalValue, canonicalCode))
        );
  }

  /**
   * Converts this quantity to its canonical UCUM representation if possible.
   * <p>
   * Uses the UCUM conversion service to determine the canonical form of the quantity. UCUM
   * quantities are converted to their canonical UCUM units, while definite calendar durations are
   * converted to their UCUM equivalents (e.g., 's' for seconds).
   *
   * @return an Optional containing the canonical UCUM quantity, or empty if conversion is not
   * possible
   */
  public Optional<FhirPathQuantity> asCanonical() {
    return switch (unit) {
      case UcumUnit(var code) -> ofUcumCanonical(value, code);
      case final CalendarDurationUnit cdUnit when (cdUnit.isDefinite()) ->
          ofUcumCanonical(value, cdUnit.getUcumEquivalent());
      default -> Optional.empty();
    };
  }

  @Override
  @Nonnull
  public String toString() {
    final String formattedValue = getValue().stripTrailingZeros().toPlainString();
    return switch (unit) {
      case final UcumUnit ignore -> formattedValue + " '" + unitName + "'";
      case final CalendarDurationUnit ignore -> formattedValue + " " + unitName;
    };
  }
}
