/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.time;

import au.csiro.pathling.sql.dates.TemporalArithmeticFunction;
import java.time.LocalTime;
import java.util.function.Function;
import org.hl7.fhir.r4.model.TimeType;

public abstract class TimeArithmeticFunction extends TemporalArithmeticFunction<LocalTime> {

  private static final long serialVersionUID = 7504107794184508523L;

  @Override
  protected Function<String, LocalTime> parseEncodedValue() {
    return TimeFunction::parseEncodedTime;
  }

  @Override
  protected Function<LocalTime, String> encodeResult() {
    return (resultTime) -> new TimeType(resultTime.toString())
        .getValueAsString();
  }

}
