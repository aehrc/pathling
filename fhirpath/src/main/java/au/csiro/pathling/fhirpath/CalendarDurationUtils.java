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

package au.csiro.pathling.fhirpath;

import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;
import org.hl7.fhir.r4.model.Quantity;

/**
 * Utility methods for working with calendar durations.
 *
 * @author Piotr Szul
 */
@UtilityClass
public final class CalendarDurationUtils {
  
  public static final String FHIRPATH_CALENDAR_DURATION_URI = "https://hl7.org/fhirpath/N1/calendar-duration";
  private static final Pattern CALENDAR_DURATION_PATTERN = Pattern.compile("([0-9.]+) (\\w+)");

  public static boolean isCalendarDuration(@Nonnull final Quantity maybeCalendarDuration) {
    return maybeCalendarDuration.getSystem()
        .equals(FHIRPATH_CALENDAR_DURATION_URI);
  }

  @Nonnull
  public static Quantity ensureCalendarDuration(@Nonnull final Quantity maybeCalendarDuration) {
    if (!isCalendarDuration(maybeCalendarDuration)) {
      throw new IllegalArgumentException("Calendar duration must have a system of "
          + FHIRPATH_CALENDAR_DURATION_URI);
    }
    return maybeCalendarDuration;
  }

  public static FhirPathDurationUnit getTemporalUnit(@Nonnull final Quantity calendarDuration) {
    return FhirPathDurationUnit.fromUnitName(ensureCalendarDuration(calendarDuration).getCode());
  }

  @Nonnull
  public static Quantity parseCalendarDuration(@Nonnull final String calendarDurationString) {
    final Matcher matcher = CALENDAR_DURATION_PATTERN.matcher(calendarDurationString);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "Calendar duration literal has invalid format: " + calendarDurationString);
    }
    final String value = matcher.group(1);
    final String keyword = matcher.group(2);

    final Quantity calendarDuration = new Quantity();
    calendarDuration.setValue(new BigDecimal(value));
    calendarDuration.setSystem(FHIRPATH_CALENDAR_DURATION_URI);
    calendarDuration.setCode(keyword);
    return calendarDuration;
  }

}
