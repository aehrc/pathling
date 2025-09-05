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

import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAdjusters;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Utility class for handling FHIR date/dateTime values with partial precision.
 * <p>
 * This class can parse FHIR date/dateTime strings, determine their precision, and compute lower and
 * upper boundary Instants based on that precision.
 */
@Value
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class FhirpathDateTime {

  /**
   * Enumeration of supported temporal precision levels from year to millisecond.
   */
  public enum TemporalPrecision {
    /**
     * Year precision (e.g., 2023)
     */
    YEAR,

    /**
     * Month precision (e.g., 2023-06)
     */
    MONTH,

    /**
     * Day precision (e.g., 2023-06-15)
     */
    DAY,

    /**
     * Hour precision (e.g., 2023-06-15T14)
     */
    HOUR,

    /**
     * Minute precision (e.g., 2023-06-15T14:30)
     */
    MINUTE,

    /**
     * Second precision (e.g., 2023-06-15T14:30:45)
     */
    SECOND,

    /**
     * Up to nanoseconds precision (e.g., 2023-06-15T14:30:45.123456789)
     */
    FRACS
  }

  // Define regex patterns for date/time components
  private static final String TIME_FORMAT =
      "(?<time>(?<hours>[0-9][0-9])(:(?<minutes>[0-9][0-9])(:(?<seconds>[0-9][0-9])(\\.(?<frac>[0-9]+))?)?)?)";
  private static final String OFFSET_FORMAT =
      "(?<offset>Z|([+\\-])([0-9][0-9]):([0-9][0-9]))";
  private static final String DATETIME_FORMAT =
      "(?<year>[0-9]{4})(-(?<month>[0-9][0-9])(-(?<day>[0-9][0-9])(T" + TIME_FORMAT + ")?)?)?" +
          OFFSET_FORMAT + "?";
  private static final Pattern DATETIME_REGEX =
      Pattern.compile("^" + DATETIME_FORMAT + "$");

  @Nonnull
  OffsetDateTime value;
  @Nonnull
  TemporalPrecision precision;

  /**
   * Parse a FHIR date/dateTime string into a PartialDateTime object.
   *
   * @param dateTimeString the FHIR date/dateTime string to parse
   * @return a new PartialDateTime object
   * @throws DateTimeParseException if the string cannot be parsed
   */
  @Nonnull
  public static FhirpathDateTime parse(@Nonnull final String dateTimeString) {
    final Matcher matcher = DATETIME_REGEX.matcher(dateTimeString);
    if (!matcher.matches()) {
      throw new DateTimeParseException("Invalid FHIR date/dateTime format", dateTimeString, 0);
    }

    final String yearGroup = matcher.group("year");
    final String monthGroup = matcher.group("month");
    final String dayGroup = matcher.group("day");
    final String hoursGroup = matcher.group("hours");
    final String minutesGroup = matcher.group("minutes");
    final String secondsGroup = matcher.group("seconds");
    final String fracGroup = matcher.group("frac");
    final String offsetGroup = matcher.group("offset");

    // special case for the value of hours being 24 which is valid in java 
    // (represents midnight of the following day) but not in FHIR where hours must be in range [0, 23]
    if (hoursGroup != null && (Integer.parseInt(hoursGroup) > 23)) {
      throw new DateTimeParseException("Hours must be in range [0, 23]", dateTimeString, 0);
    }

    // Determine precision based on which groups are present
    final TemporalPrecision precision;
    if (fracGroup != null) {
      precision = TemporalPrecision.FRACS;
    } else if (secondsGroup != null) {
      precision = TemporalPrecision.SECOND;
    } else if (minutesGroup != null) {
      precision = TemporalPrecision.MINUTE;
    } else if (hoursGroup != null) {
      precision = TemporalPrecision.HOUR;
    } else if (dayGroup != null) {
      precision = TemporalPrecision.DAY;
    } else if (monthGroup != null) {
      precision = TemporalPrecision.MONTH;
    } else {
      precision = TemporalPrecision.YEAR;
    }

    // Build a parseable datetime string with defaults for missing components
    final String parseableDateTime = yearGroup +
        (monthGroup != null
         ? "-" + monthGroup
         : "-01") +
        (dayGroup != null
         ? "-" + dayGroup
         : "-01") +
        (hoursGroup != null
         ? "T" + hoursGroup
         : "T00") +
        (minutesGroup != null
         ? ":" + minutesGroup
         : ":00") +
        (secondsGroup != null
         ? ":" + secondsGroup
         : ":00") +
        (fracGroup != null
         ? "." + fracGroup
         : "") +
        (offsetGroup != null
         ? offsetGroup
         : "Z");
    final OffsetDateTime dateTime = OffsetDateTime.parse(parseableDateTime);
    return new FhirpathDateTime(dateTime, precision);
  }

  /**
   * Gets the lower boundary for this date/time value based on its precision.
   * <p>
   * For example:
   * <ul>
   *   <li>2023 (YEAR precision) -> 2023-01-01T00:00:00Z</li>
   *   <li>2023-06 (MONTH precision) -> 2023-06-01T00:00:00Z</li>
   *   <li>2023-06-15 (DAY precision) -> 2023-06-15T00:00:00Z</li>
   * </ul>
   * </p>
   *
   * @return the lower boundary as an Instant
   */
  @Nonnull
  public Instant getLowerBoundary() {
    // we store the value at the lower boundary already
    return value.toInstant();
  }

  /**
   * Gets the upper boundary for this date/time value based on its precision.
   * <p>
   * For example:
   * <ul>
   *   <li>2023 (YEAR precision) -> 2023-12-31T23:59:59.999999999Z</li>
   *   <li>2023-06 (MONTH precision) -> 2023-06-30T23:59:59.999999999Z</li>
   *   <li>2023-06-15 (DAY precision) -> 2023-06-15T23:59:59.999999999Z</li>
   * </ul>
   * </p>
   *
   * @return the upper boundary as an Instant
   */
  @Nonnull
  public Instant getUpperBoundary() {

    switch (precision) {
      case YEAR:
        return value.with(ChronoField.MONTH_OF_YEAR, 12)
            .with(TemporalAdjusters.lastDayOfMonth())
            .with(ChronoField.HOUR_OF_DAY, 23)
            .with(ChronoField.MINUTE_OF_HOUR, 59)
            .with(ChronoField.SECOND_OF_MINUTE, 59)
            .with(ChronoField.NANO_OF_SECOND, 999_999_999)
            .toInstant();
      case MONTH:
        return value.with(TemporalAdjusters.lastDayOfMonth())
            .with(ChronoField.HOUR_OF_DAY, 23)
            .with(ChronoField.MINUTE_OF_HOUR, 59)
            .with(ChronoField.SECOND_OF_MINUTE, 59)
            .with(ChronoField.NANO_OF_SECOND, 999_999_999)
            .toInstant();
      case DAY:
        return value.with(ChronoField.HOUR_OF_DAY, 23)
            .with(ChronoField.MINUTE_OF_HOUR, 59)
            .with(ChronoField.SECOND_OF_MINUTE, 59)
            .with(ChronoField.NANO_OF_SECOND, 999_999_999)
            .toInstant();
      case HOUR:
        return value.with(ChronoField.MINUTE_OF_HOUR, 59)
            .with(ChronoField.SECOND_OF_MINUTE, 59)
            .with(ChronoField.NANO_OF_SECOND, 999_999_999)
            .toInstant();
      case MINUTE:
        return value.with(ChronoField.SECOND_OF_MINUTE, 59)
            .with(ChronoField.NANO_OF_SECOND, 999_999_999)
            .toInstant();
      case SECOND:
        return value.with(ChronoField.NANO_OF_SECOND, 999_999_999)
            .toInstant();
      case FRACS:
      default:
        return value.toInstant();
    }
  }

  /**
   * Utility method to create a PartialDateTime from a Java Instant.
   *
   * @param dateTime the OffsetDateTime to convert
   * @param precision the desired precision
   * @return a new PartialDateTime with the specified precision
   */
  @Nonnull
  static FhirpathDateTime fromDateTime(@Nonnull final OffsetDateTime dateTime,
      @Nonnull final TemporalPrecision precision) {
    return new FhirpathDateTime(dateTime, precision);
  }
}
