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

package au.csiro.pathling.sql.dates.time;

import jakarta.annotation.Nonnull;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Common functionality relating to time operations.
 *
 * @author John Grimes
 */
abstract class TimeFunction {

  private static final Pattern HOURS_ONLY = Pattern.compile("^\\d{2}$");

  @Nullable
  static LocalTime parseEncodedTime(@Nonnull final String encoded) {
    try {
      // LocalDate will not successfully parse the HH format.
      return HOURS_ONLY.matcher(encoded).matches()
             ? LocalTime.parse(encoded + ":00")
             : LocalTime.parse(encoded);
    } catch (final DateTimeParseException e) {
      return null;
    }
  }

}
