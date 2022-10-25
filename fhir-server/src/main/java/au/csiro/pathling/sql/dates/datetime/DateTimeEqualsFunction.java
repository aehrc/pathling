/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.datetime;

import java.util.function.BiFunction;
import org.hl7.fhir.r4.model.BaseDateTimeType;
import org.hl7.fhir.r4.model.DateTimeType;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Determines the equality of two datetimes.
 *
 * @author John Grimes
 */
@Component
@Profile("core | unit-test")
public class DateTimeEqualsFunction extends DateTimeComparisonFunction {

  private static final long serialVersionUID = -8717420985056046161L;

  public static final String FUNCTION_NAME = "datetime_eq";

  @Override
  protected BiFunction<DateTimeType, DateTimeType, Boolean> getOperationFunction() {
    return BaseDateTimeType::equalsUsingFhirPathRules;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
