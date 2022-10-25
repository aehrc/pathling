/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.date;

import java.util.function.BiFunction;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Quantity;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Adds a duration to a date.
 *
 * @author John Grimes
 */
@Component
@Profile("core | unit-test")
public class DateAddDurationFunction extends DateArithmeticFunction {

  private static final long serialVersionUID = -5029179160644275584L;

  public static final String FUNCTION_NAME = "date_add_duration";

  @Override
  protected BiFunction<DateType, Quantity, DateType> getOperationFunction() {
    return this::performAddition;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
