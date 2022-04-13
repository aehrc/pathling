/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.date;

import java.time.LocalDate;
import java.util.function.BiFunction;
import org.hl7.fhir.r4.model.Quantity;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("core")
public class DateSubtractDurationFunction extends DateArithmeticFunction {

  private static final long serialVersionUID = 5201879133976866457L;

  public static final String FUNCTION_NAME = "date_subtract_duration";

  @Override
  protected BiFunction<LocalDate, Quantity, LocalDate> getOperationFunction() {
    return this::performSubtraction;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
