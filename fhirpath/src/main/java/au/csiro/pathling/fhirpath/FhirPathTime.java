package au.csiro.pathling.fhirpath;

import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Utility class for handling FHIRPath time values with partial precision.
 * <p>
 * This class can parse FHIRPath time strings, determine their precision, and compute lower and
 * upper boundary Instants based on that precision. The time is always based on the Java epoch date
 * (1970-01-01).
 */
@Value
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class FhirPathTime {

  /**
   * Regular expression for a FHIR time string.
   */
  public static final String TIME_FORMAT =
      "(?<hours>\\d\\d)(:(?<minutes>\\d\\d)(:(?<seconds>\\d\\d)(\\.(?<frac>\\d+))?)?)?";

  private static final Pattern TIME_REGEX =
      Pattern.compile("^" + TIME_FORMAT + "$");

  @Nonnull
  Instant value;
  @Nonnull
  TemporalPrecision precision;

  /**
   * Parse a FHIR time string into a FhirpathTime object.
   *
   * @param timeString the FHIR time string to parse (e.g., 12:00)
   * @return a new FhirpathTime object
   * @throws DateTimeParseException if the string cannot be parsed
   */
  @Nonnull
  public static FhirPathTime parse(@Nonnull final String timeString) {
    final Matcher matcher = TIME_REGEX.matcher(timeString);
    if (!matcher.matches()) {
      throw new DateTimeParseException("Invalid FHIR time format", timeString, 0);
    }

    final String hoursGroup = matcher.group("hours");
    final String minutesGroup = matcher.group("minutes");
    final String secondsGroup = matcher.group("seconds");
    final String fracGroup = matcher.group("frac");

    if (hoursGroup != null && (Integer.parseInt(hoursGroup) > 23)) {
      throw new DateTimeParseException("Hours must be in range [0, 23]", timeString, 0);
    }

    // Determine precision
    final TemporalPrecision precision;
    if (fracGroup != null) {
      precision = TemporalPrecision.FRACS;
    } else if (secondsGroup != null) {
      precision = TemporalPrecision.SECOND;
    } else if (minutesGroup != null) {
      precision = TemporalPrecision.MINUTE;
    } else if (hoursGroup != null) {
      precision = TemporalPrecision.HOUR;
    } else {
      throw new DateTimeParseException("Invalid time precision", timeString, 0);
    }
    // Build a parseable time string with defaults for missing components
    final String parseableTime =
        (hoursGroup != null
         ? hoursGroup
         : "00") +
            (minutesGroup != null
             ? ":" + minutesGroup
             : ":00") +
            (secondsGroup != null
             ? ":" + secondsGroup
             : ":00") +
            (fracGroup != null
             ? "." + fracGroup
             : "");
    return fromLocalTime(LocalTime.parse(parseableTime), precision);
  }

  /**
   * Gets the lower boundary for this time value based on its precision.
   *
   * @return the lower boundary as an Instant
   */
  @Nonnull
  public Instant getLowerBoundary() {
    return value;
  }

  /**
   * Gets the upper boundary for this time value based on its precision.
   * <p>
   * For example:
   * </p>
   * <ul>
   *   <li>{@code 12} (HOUR precision) -> 12:59:59.999999999</li>
   *   <li>{@code 12:30} (MINUTE precision) -> 12:30:59.999999999</li>
   *   <li>{@code 12:30:45} (SECOND precision) -> 12:30:45.000000000</li>
   *   <li>{@code 12:30:45.123} (FRACS precision) -> 12:30:45.123</li>
   * </ul>
   *
   * @return the upper boundary as an Instant
   */
  @Nonnull
  public Instant getUpperBoundary() {
    return switch (precision) {
      case FRACS, SECOND -> value;
      default ->
        // Add one unit, subtract one nanosecond
          value.plus(1, precision.getChronoUnit()).minusNanos(1);
    };
  }

  /**
   * Utility method to create a FhirpathTime from a LocalTime and precision.
   *
   * @param localTime the LocalTime to convert
   * @param precision the desired precision (must be time-based)
   * @return a new FhirpathTime
   */
  @Nonnull
  public static FhirPathTime fromLocalTime(@Nonnull final LocalTime localTime,
      @Nonnull final TemporalPrecision precision) {
    if (!precision.isTimeBased()) {
      throw new IllegalArgumentException("Precision must be time-based for FhirpathTime");
    }
    final Instant instant = localTime.atDate(LocalDate.ofEpochDay(0))
        .toInstant(ZoneOffset.UTC);
    return new FhirPathTime(instant, precision);
  }


  /**
   * Checks if given string represents a valid Fhirpath time value.
   *
   * @param timeValue the time value
   * @return true is the value is a valid time.
   */
  public static boolean isTimeValue(@Nonnull final String timeValue) {
    try {
      parse(timeValue);
      return true;
    } catch (final DateTimeParseException ignore) {
      return false;
    }
  }
}

