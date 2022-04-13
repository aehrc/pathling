/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.date;

import au.csiro.pathling.sql.dates.TemporalArithmeticFunction;
import java.util.function.Function;
import org.hl7.fhir.r4.model.DateType;

public abstract class DateArithmeticFunction extends TemporalArithmeticFunction<DateType> {

  private static final long serialVersionUID = 6759548804191034570L;

  @Override
  protected Function<String, DateType> parseEncodedValue() {
    return DateType::new;
  }

  @Override
  protected Function<DateType, String> encodeResult() {
    return DateType::getValueAsString;
  }

}
