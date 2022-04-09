/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates;

import au.csiro.pathling.sql.udf.SqlFunction2;
import java.time.LocalTime;
import java.util.function.BiFunction;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Quantity;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("core")
public class AddDurationToTime extends TimeArithmeticFunction implements
    SqlFunction2<String, Row, String> {

  private static final long serialVersionUID = 5839806160423512490L;

  public static final String FUNCTION_NAME = "add_duration_to_time";

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  BiFunction<LocalTime, Quantity, LocalTime> getOperationFunction() {
    return this::performAddition;
  }

}
