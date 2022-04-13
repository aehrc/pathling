/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.time;

import au.csiro.pathling.sql.dates.TemporalComparisonFunction;
import java.time.LocalTime;
import java.util.function.Function;

public abstract class TimeComparisonFunction extends TemporalComparisonFunction<LocalTime> {

  private static final long serialVersionUID = 3661335567427062952L;

  @Override
  protected Function<String, LocalTime> parseEncodedValue() {
    return TimeFunction::parseEncodedTime;
  }

}
