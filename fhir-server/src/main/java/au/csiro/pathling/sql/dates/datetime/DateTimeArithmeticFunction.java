/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.datetime;

import au.csiro.pathling.sql.dates.TemporalArithmeticFunction;
import java.util.function.Function;
import org.hl7.fhir.r4.model.DateTimeType;

/**
 * Base class for functions that perform arithmetic on datetimes.
 *
 * @author John Grimes
 */
public abstract class DateTimeArithmeticFunction extends
    TemporalArithmeticFunction<DateTimeType> {

  private static final long serialVersionUID = -6669722492626320119L;

  @Override
  protected Function<String, DateTimeType> parseEncodedValue() {
    return DateTimeType::new;
  }

  @Override
  protected Function<DateTimeType, String> encodeResult() {
    return DateTimeType::getValueAsString;
  }

}
