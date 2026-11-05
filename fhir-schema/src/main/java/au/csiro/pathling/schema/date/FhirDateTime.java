/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.schema.date;

import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A FHIR {@code date}, {@code dateTime} or {@code instant} parsed with FHIR's date grammar, at the
 * precision it states, and the range of instants that precision implies.
 *
 * <p>This is the one grammar of decision 133's principle 7, shared by the engine and by ingest
 * (decision 135). A time without a timezone is accepted (decision 133's addendum of 2026-10-21),
 * and is taken as UTC.
 */
public final class FhirDateTime {

  /** The grammar of a FHIR time, without a timezone. */
  @Nonnull
  public static final String TIME_FORMAT =
      "(?<hours>\\d\\d)(:(?<minutes>\\d\\d)(:(?<seconds>\\d\\d)(\\.(?<frac>\\d+))?)?)?";

  @Nonnull private static final String OFFSET_FORMAT = "(?<offset>Z|([+\\-])(\\d\\d):(\\d\\d))";

  @Nonnull
  private static final String DATETIME_FORMAT =
      "(?<year>\\d{4})(-(?<month>\\d\\d)(-(?<day>\\d\\d)(T(?<time>"
          + TIME_FORMAT
          + "))?)?)?"
          + OFFSET_FORMAT
          + "?";

  @Nonnull
  private static final Pattern DATETIME_REGEX = Pattern.compile("^" + DATETIME_FORMAT + "$");

  /** The seconds of a leap second, which FHIR allows. */
  @Nonnull private static final String LEAP_SECOND = "60";

  /** The digits of a fraction of a second that Java holds, to the nanosecond. */
  private static final int MAX_FRACTION_DIGITS = 9;

  @Nonnull private final OffsetDateTime value;

  @Nonnull private final ChronoUnit precision;

  private FhirDateTime(@Nonnull final OffsetDateTime value, @Nonnull final ChronoUnit precision) {
    this.value = value;
    this.precision = precision;
  }

  /**
   * Parses a FHIR date, dateTime or instant.
   *
   * @param text the text of the value
   * @return the value, at the precision it states
   * @throws DateTimeParseException if the text does not follow the grammar, or names a date or time
   *     that does not exist
   */
  @Nonnull
  public static FhirDateTime parse(@Nonnull final String text) {
    final Matcher matcher = DATETIME_REGEX.matcher(text);
    if (!matcher.matches()) {
      throw new DateTimeParseException("Invalid FHIR date/dateTime format", text, 0);
    }
    final String yearGroup = matcher.group("year");
    final String monthGroup = matcher.group("month");
    final String dayGroup = matcher.group("day");
    final String hoursGroup = matcher.group("hours");
    final String minutesGroup = matcher.group("minutes");
    final String secondsGroup = matcher.group("seconds");
    final String fracGroup = matcher.group("frac");
    final String offsetGroup = matcher.group("offset");

    // Java takes an hour of 24 as midnight of the following day, which FHIR does not allow.
    if (hoursGroup != null && (Integer.parseInt(hoursGroup) > 23)) {
      throw new DateTimeParseException("Hours must be in range [0, 23]", text, 0);
    }

    final ChronoUnit precision;
    if (fracGroup != null) {
      precision = ChronoUnit.NANOS;
    } else if (secondsGroup != null) {
      precision = ChronoUnit.SECONDS;
    } else if (minutesGroup != null) {
      precision = ChronoUnit.MINUTES;
    } else if (hoursGroup != null) {
      precision = ChronoUnit.HOURS;
    } else if (dayGroup != null) {
      precision = ChronoUnit.DAYS;
    } else if (monthGroup != null) {
      precision = ChronoUnit.MONTHS;
    } else {
      precision = ChronoUnit.YEARS;
    }

    // The components the value does not state take their lowest value, and a missing timezone is
    // taken as UTC. FHIR allows a leap second, which Java does not, so it is taken as the second
    // before it, as the FHIRPath reference implementation takes it. A fraction is taken to the
    // nanosecond, the finest Java holds.
    final String parseable =
        yearGroup
            + (monthGroup != null ? "-" + monthGroup : "-01")
            + (dayGroup != null ? "-" + dayGroup : "-01")
            + (hoursGroup != null ? "T" + hoursGroup : "T00")
            + (minutesGroup != null ? ":" + minutesGroup : ":00")
            + (secondsGroup != null
                ? ":" + (LEAP_SECOND.equals(secondsGroup) ? "59" : secondsGroup)
                : ":00")
            + (fracGroup != null
                ? "." + fracGroup.substring(0, Math.min(fracGroup.length(), MAX_FRACTION_DIGITS))
                : "")
            + (offsetGroup != null ? offsetGroup : "Z");
    return new FhirDateTime(OffsetDateTime.parse(parseable), precision);
  }

  /**
   * Returns the value, with the components it does not state at their lowest value.
   *
   * @return the value
   */
  @Nonnull
  public OffsetDateTime getValue() {
    return value;
  }

  /**
   * Returns the precision the value states: years, months or days for a date, hours, minutes or
   * seconds for a time, and nanoseconds for a time with a fraction of a second.
   *
   * @return the unit of the precision
   */
  @Nonnull
  public ChronoUnit getPrecision() {
    return precision;
  }

  /**
   * Returns the earliest instant the value's precision allows: {@code 2023-06} gives {@code
   * 2023-06-01T00:00:00Z}.
   *
   * @return the lower boundary
   */
  @Nonnull
  public Instant getLowerBoundary() {
    return value.toInstant();
  }

  /**
   * Returns the latest instant the value's precision allows, to the nanosecond: {@code 2023-06}
   * gives {@code 2023-06-30T23:59:59.999999999Z}.
   *
   * <p>A value stated to the second or beyond is taken as exact, so its upper boundary is its lower
   * one.
   *
   * @return the upper boundary
   */
  @Nonnull
  public Instant getUpperBoundary() {
    return upperBoundary(value, precision);
  }

  /**
   * Returns the latest instant a value at a precision allows, to the nanosecond, as {@link
   * #getUpperBoundary()} does.
   *
   * @param value the value, with the components the precision does not state at their lowest
   * @param precision the unit of the precision
   * @return the upper boundary
   */
  @Nonnull
  public static Instant upperBoundary(
      @Nonnull final OffsetDateTime value, @Nonnull final ChronoUnit precision) {
    if (precision == ChronoUnit.SECONDS || precision == ChronoUnit.NANOS) {
      return value.toInstant();
    }
    // The start of the next period at this precision, less one nanosecond. An offset date and time
    // has no daylight saving, so adding a period has no anomalies.
    return value.plus(1, precision).minusNanos(1).toInstant();
  }
}
