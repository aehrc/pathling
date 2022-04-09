/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates;

import java.time.LocalTime;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.hl7.fhir.r4.model.TimeType;

public abstract class TimeArithmeticFunction extends TemporalArithmeticFunction<LocalTime> {

  private static final long serialVersionUID = 7504107794184508523L;

  private static final Pattern HOURS_ONLY = Pattern.compile("^\\d{2}$");

  @Override
  Function<String, LocalTime> parseEncodedValue() {
    // LocalDate will not successfully parse the HH format.
    return (temporal) -> HOURS_ONLY.matcher(temporal).matches()
                         ? LocalTime.parse(temporal + ":00")
                         : LocalTime.parse(temporal);
  }

  @Override
  Function<LocalTime, String> encodeResult() {
    return (resultTime) -> new TimeType(resultTime.toString())
        .getValueAsString();
  }

}
