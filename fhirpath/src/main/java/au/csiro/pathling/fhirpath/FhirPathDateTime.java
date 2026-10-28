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

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.schema.date.FhirDateTime;
import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Utility class for handling FHIRPath date/dateTime values with partial precision.
 *
 * <p>This class can parse FHIRPath date/dateTime strings, determine their precision, and compute
 * lower and upper boundary Instants based on that precision. The grammar and the boundaries are
 * {@link FhirDateTime}'s, which ingest shares (decision 135).
 */
@Value
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class FhirPathDateTime {

  @Nonnull OffsetDateTime value;
  @Nonnull TemporalPrecision precision;

  /**
   * Parse a FHIR date/dateTime string into a PartialDateTime object.
   *
   * @param dateTimeString the FHIR date/dateTime string to parse
   * @return a new PartialDateTime object
   * @throws DateTimeParseException if the string cannot be parsed
   */
  @Nonnull
  public static FhirPathDateTime parse(@Nonnull final String dateTimeString) {
    final FhirDateTime parsed = FhirDateTime.parse(dateTimeString);
    final TemporalPrecision precision =
        Stream.of(TemporalPrecision.values())
            .filter(candidate -> candidate.getChronoUnit() == parsed.getPrecision())
            .findFirst()
            .orElseThrow();
    return new FhirPathDateTime(parsed.getValue(), precision);
  }

  /**
   * Gets the lower boundary for this date/time value based on its precision.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>2023 (YEAR precision) -> 2023-01-01T00:00:00Z
   *   <li>2023-06 (MONTH precision) -> 2023-06-01T00:00:00Z
   *   <li>2023-06-15 (DAY precision) -> 2023-06-15T00:00:00Z
   * </ul>
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
   *
   * <p>For example:
   *
   * <ul>
   *   <li>2023 (YEAR precision) -> 2023-12-31T23:59:59.999999999Z
   *   <li>2023-06 (MONTH precision) -> 2023-06-30T23:59:59.999999999Z
   *   <li>2023-06-15 (DAY precision) -> 2023-06-15T23:59:59.999999999Z
   * </ul>
   *
   * <p>The maximum precision for upper bound is SECONDS, i.e.: SECOND for second and FRACS
   * precision upper bound is the same as the lower bound.
   *
   * @return the upper boundary as an Instant
   */
  @Nonnull
  public Instant getUpperBoundary() {
    return FhirDateTime.upperBoundary(value, precision.getChronoUnit());
  }

  /**
   * Utility method to create a PartialDateTime from a Java Instant.
   *
   * @param dateTime the OffsetDateTime to convert
   * @param precision the desired precision
   * @return a new PartialDateTime with the specified precision
   */
  @Nonnull
  static FhirPathDateTime fromDateTime(
      @Nonnull final OffsetDateTime dateTime, @Nonnull final TemporalPrecision precision) {
    return new FhirPathDateTime(dateTime, precision);
  }

  /**
   * Checks if given string represents a valid FhirPath dateTime value.
   *
   * @param dateTimeValue the dateTime value
   * @return true is the value is valid dateTime
   */
  public static boolean isDateTimeValue(@Nonnull final String dateTimeValue) {
    try {
      parse(dateTimeValue);
      return true;
    } catch (final DateTimeParseException ignore) {
      return false;
    }
  }

  /**
   * Checks if given string represents a valid FhirPath date value.
   *
   * @param dateValue the date value
   * @return true is the value is as valid date
   */
  public static boolean isDateValue(@Nonnull final String dateValue) {
    try {
      return parse(dateValue).getPrecision().ordinal() <= TemporalPrecision.DAY.ordinal();
    } catch (final DateTimeParseException ignore) {
      return false;
    }
  }
}
