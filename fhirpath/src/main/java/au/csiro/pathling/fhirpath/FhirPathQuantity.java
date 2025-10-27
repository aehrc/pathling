package au.csiro.pathling.fhirpath;

import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Represents a FHIRPath Quantity value.
 */
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
   * Regex pattern for parsing FHIRPath quantity literals.
   */
  private static final Pattern QUANTITY_REGEX = Pattern.compile(
      "(?<value>[+-]?\\d+(?:\\.\\d+)?)\\s*(?:'(?<unit>[^']+)'|(?<time>[a-zA-Z]+))"
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
   * @param unit the UCUM unit string (e.g. 'mg', 'kg', 'mL')
   * @param value the numeric value
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
   * @param unitName the name of the calendar duration unit (e.g. 'year', 'month', 'day')
   * @param unit the CalendarDurationUnit enum value
   * @param value the numeric value
   * @return calendar duration quantity
   */
  @Nonnull
  public static FhirPathQuantity ofCalendar(@Nonnull final BigDecimal value,
      @Nonnull final CalendarDurationUnit unit, @Nonnull final String unitName) {
    if (!CalendarDurationUnit.fromString(unitName).equals(unit)) {
      throw new IllegalArgumentException(
          "Unit name " + unitName + " does not match CalendarDurationUnit " + unit);
    }
    return new FhirPathQuantity(value, unitName,
        FHIRPATH_CALENDAR_DURATION_SYSTEM_URI,
        unit.getUnit());
  }

  /**
   * Factory method for calendar duration quantities.
   *
   * @param unit the CalendarDurationUnit enum value
   * @param value the numeric value
   * @return calendar duration quantity
   */
  @Nonnull
  public static FhirPathQuantity ofCalendar(@Nonnull final BigDecimal value,
      @Nonnull final CalendarDurationUnit unit) {
    return ofCalendar(value, unit, unit.getUnit());
  }

  /**
   * Parses a FHIRPath quantity literal (e.g. 5.4 'mg', 1 year). Only supports UCUM and calendar
   * duration units.
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
      return ofUCUM(value, matcher.group("unit"));
    } else if (matcher.group("time") != null) {
      return ofCalendar(value,
          CalendarDurationUnit.fromString(matcher.group("time")),
          matcher.group("time")
      );
    } else {
      // one of "unit" or "time" groups should have matched
      throw new IllegalStateException("Unexpected parsing state for literal: " + literal);
    }
  }
}
