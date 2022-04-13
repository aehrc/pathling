/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.time;

import java.time.LocalTime;
import java.util.function.BiFunction;
import org.hl7.fhir.r4.model.Quantity;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("core")
public class TimeSubtractDurationFunction extends TimeArithmeticFunction {

  private static final long serialVersionUID = 1909732257090337898L;
 
  public static final String FUNCTION_NAME = "time_subtract_duration";

  @Override
  protected BiFunction<LocalTime, Quantity, LocalTime> getOperationFunction() {
    return this::performSubtraction;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
