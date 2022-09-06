/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.datetime;

import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.DateTimeType;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("core | unit-test")
public class DateTimeLessThanOrEqualToFunction extends DateTimeComparisonFunction {

  private static final long serialVersionUID = 787654631927909813L;

  public static final String FUNCTION_NAME = "datetime_lte";

  @Nonnull
  @Override
  protected BiFunction<DateTimeType, DateTimeType, Boolean> getOperationFunction() {
    return (left, right) -> left.before(right) || left.equalsUsingFhirPathRules(right);
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
