/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates;

import au.csiro.pathling.fhirpath.element.DateTimePath;
import ca.uhn.fhir.model.api.TemporalPrecisionEnum;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.function.Function;
import org.hl7.fhir.r4.model.BaseDateTimeType;
import org.hl7.fhir.r4.model.DateTimeType;

public abstract class DateTimeArithmeticFunction extends TemporalArithmeticFunction<ZonedDateTime> {

  private static final long serialVersionUID = -6669722492626320119L;

  @Override
  Function<String, ZonedDateTime> parseEncodedValue() {
    return ZonedDateTime::parse;
  }

  @Override
  Function<ZonedDateTime, String> encodeResult() {
    return (resultDateTime) -> {
      final BaseDateTimeType dateTime = new DateTimeType(Date.from(resultDateTime.toInstant()))
          .setTimeZone(DateTimePath.getDefaultTimeZone());
      dateTime.setPrecision(TemporalPrecisionEnum.MILLI);
      return dateTime.getValueAsString();
    };
  }

}
