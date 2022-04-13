/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.datetime;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.sql.dates.TemporalArithmeticFunction;
import ca.uhn.fhir.model.api.TemporalPrecisionEnum;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;
import org.hl7.fhir.r4.model.BaseDateTimeType;
import org.hl7.fhir.r4.model.DateTimeType;

public abstract class DateTimeArithmeticFunction extends
    TemporalArithmeticFunction<Temporal> {

  private static final long serialVersionUID = -6669722492626320119L;

  static final Map<ChronoUnit, TemporalPrecisionEnum> JAVA_TO_HAPI_TIME_UNIT = new ImmutableMap.Builder<ChronoUnit, TemporalPrecisionEnum>()
      .put(ChronoUnit.MINUTES, TemporalPrecisionEnum.MINUTE)
      .put(ChronoUnit.SECONDS, TemporalPrecisionEnum.SECOND)
      .put(ChronoUnit.MILLIS, TemporalPrecisionEnum.MILLI)
      .build();

  @Override
  protected Function<String, Temporal> parseEncodedValue() {
    return (encoded) -> {
      final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
          "yyyy-MM-dd'T'HH[:mm[:ss[.SSS][XXX]]");
      return (Temporal) formatter.parseBest(encoded, ZonedDateTime::from, LocalDateTime::from);
    };
  }

  @Override
  protected Function<Temporal, String> encodeResult() {
    return (temporal) -> {
      final Instant instant = temporal instanceof ZonedDateTime
                              ? ((ZonedDateTime) temporal).toInstant()
                              : ((LocalDateTime) temporal).toInstant(ZoneOffset.UTC);
      final BaseDateTimeType dateTime = new DateTimeType(Date.from(instant));

      // Attempt to get the time zone from the parsed object.
      try {
        final TimeZone timeZone = TimeZone.getTimeZone(temporal.query(ZoneId::from));
        dateTime.setTimeZone(timeZone);
      } catch (final Exception ignored) {
      }

      // Find the appropriate precision for the result.
      for (final ChronoUnit unit : List.of(ChronoUnit.MILLIS, ChronoUnit.SECONDS,
          ChronoUnit.MINUTES)) {
        if (temporal.isSupported(unit)) {
          final TemporalPrecisionEnum precision = JAVA_TO_HAPI_TIME_UNIT.get(unit);
          checkNotNull(precision);
          dateTime.setPrecision(precision);
          break;
        }
      }

      return dateTime.getValueAsString();
    };
  }

}
