/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.datetime;

import java.time.temporal.Temporal;
import java.util.function.BiFunction;
import org.hl7.fhir.r4.model.Quantity;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("core")
public class DateTimeAddDurationFunction extends DateTimeArithmeticFunction {

  private static final long serialVersionUID = 6922227603585641053L;

  public static final String FUNCTION_NAME = "datetime_add_duration";

  @Override
  protected BiFunction<Temporal, Quantity, Temporal> getOperationFunction() {
    return this::performAddition;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
