/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.time;

import java.time.LocalTime;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

abstract class TimeFunction {

  private static final Pattern HOURS_ONLY = Pattern.compile("^\\d{2}$");

  @Nonnull
  static LocalTime parseEncodedTime(@Nonnull final String encoded) {
    // LocalDate will not successfully parse the HH format.
    return HOURS_ONLY.matcher(encoded).matches()
           ? LocalTime.parse(encoded + ":00")
           : LocalTime.parse(encoded);
  }

}
