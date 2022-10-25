/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.time;

import java.time.LocalTime;
import java.util.function.BiFunction;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Determines whether one time is after or at the same time as another.
 *
 * @author John Grimes
 */
@Component
@Profile("core | unit-test")
public class TimeGreaterThanOrEqualToFunction extends TimeComparisonFunction {

  private static final long serialVersionUID = -8098751595303018323L;

  public static final String FUNCTION_NAME = "time_gte";

  @Override
  protected BiFunction<LocalTime, LocalTime, Boolean> getOperationFunction() {
    return (a, b) -> a.isAfter(b) || a.equals(b);
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
