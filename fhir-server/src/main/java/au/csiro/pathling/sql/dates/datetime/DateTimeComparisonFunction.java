/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.datetime;

import au.csiro.pathling.sql.dates.TemporalComparisonFunction;
import java.util.function.Function;
import org.hl7.fhir.r4.model.DateTimeType;

/**
 * Base class for functions that compare datetimes.
 *
 * @author John Grimes
 */
public abstract class DateTimeComparisonFunction extends TemporalComparisonFunction<DateTimeType> {

  private static final long serialVersionUID = -2449192480093120211L;

  @Override
  protected Function<String, DateTimeType> parseEncodedValue() {
    return DateTimeType::new;
  }

}
