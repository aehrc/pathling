/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.function.Function;
import org.hl7.fhir.r4.model.DateType;

public abstract class DateArithmeticFunction extends TemporalArithmeticFunction<LocalDate> {

  private static final long serialVersionUID = 6759548804191034570L;

  @Override
  Function<String, LocalDate> parseEncodedValue() {
    return LocalDate::parse;
  }

  @Override
  Function<LocalDate, String> encodeResult() {
    return (resultDate) -> new DateType(
        new Date(resultDate.toEpochSecond(LocalTime.MIN, ZoneOffset.UTC) * 1000))
        .getValueAsString();
  }

}
