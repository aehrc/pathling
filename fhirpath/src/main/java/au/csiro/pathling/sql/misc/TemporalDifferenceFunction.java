/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql.misc;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.sql.udf.SqlFunction3;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.LocalDate;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Map;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Calculates the difference between two temporal values, returning an integer value using the
 * requested unit. Used for the <code>until</code> function.
 *
 * @author John Grimes
 */
public class TemporalDifferenceFunction implements SqlFunction3<String, String, String, Long> {

  private static final long serialVersionUID = -7306741471632636471L;
  public static final String FUNCTION_NAME = "date_diff";

  static final Map<String, TemporalUnit> CALENDAR_DURATION_TO_TEMPORAL = new ImmutableMap.Builder<String, TemporalUnit>()
      .put("year", ChronoUnit.YEARS)
      .put("years", ChronoUnit.YEARS)
      .put("month", ChronoUnit.MONTHS)
      .put("months", ChronoUnit.MONTHS)
      .put("day", ChronoUnit.DAYS)
      .put("days", ChronoUnit.DAYS)
      .put("hour", ChronoUnit.HOURS)
      .put("hours", ChronoUnit.HOURS)
      .put("minute", ChronoUnit.MINUTES)
      .put("minutes", ChronoUnit.MINUTES)
      .put("second", ChronoUnit.SECONDS)
      .put("seconds", ChronoUnit.SECONDS)
      .put("millisecond", ChronoUnit.MILLIS)
      .put("milliseconds", ChronoUnit.MILLIS)
      .build();

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.LongType;
  }

  @Nullable
  @Override
  public Long call(@Nullable final String encodedFrom, @Nullable final String encodedTo,
      @Nullable final String calendarDuration) throws Exception {
    if (encodedFrom == null || encodedTo == null) {
      return null;
    } else if (calendarDuration == null) {
      throw new InvalidUserInputError("Calendar duration must be provided");
    }

    final TemporalUnit temporalUnit = CALENDAR_DURATION_TO_TEMPORAL.get(calendarDuration);

    if (temporalUnit == null) {
      throw new InvalidUserInputError("Invalid calendar duration: " + calendarDuration);
    }

    final ZonedDateTime from = parse(encodedFrom);
    final ZonedDateTime to = parse(encodedTo);

    if (from == null || to == null) {
      // If either of the arguments is null (invalid input), then the result is null.
      return null;
    }

    return from.until(to, temporalUnit);
  }

  @Nullable
  public static ZonedDateTime parse(final @Nonnull String encodedFrom) {
    try {
      return ZonedDateTime.parse(encodedFrom);
    } catch (final DateTimeParseException e) {
      try {
        return LocalDate.parse(encodedFrom).atStartOfDay(ZoneId.of("UTC"));
      } catch (final DateTimeParseException ex) {
        try {
          return Year.parse(encodedFrom).atDay(1).atStartOfDay(ZoneId.of("UTC"));
        } catch (final DateTimeParseException exc) {
          // If we can't parse the value as a date or datetime, return null.
          return null;
        }
      }
    }
  }

  public static boolean isValidCalendarDuration(final String literalValue) {
    return CALENDAR_DURATION_TO_TEMPORAL.containsKey(literalValue);
  }

}
