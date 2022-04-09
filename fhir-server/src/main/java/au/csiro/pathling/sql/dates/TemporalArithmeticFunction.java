/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates;

import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import au.csiro.pathling.sql.udf.SqlFunction2;
import com.google.common.collect.ImmutableMap;
import java.math.RoundingMode;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Quantity;

public abstract class TemporalArithmeticFunction<IntermediateType extends Temporal> implements
    SqlFunction2<String, Row, String> {

  private static final long serialVersionUID = -5016153440496309996L;

  static final Map<String, TemporalUnit> CALENDAR_DURATION_TO_UCUM = new ImmutableMap.Builder<String, TemporalUnit>()
      .put("year", ChronoUnit.YEARS)
      .put("years", ChronoUnit.YEARS)
      .put("month", ChronoUnit.MONTHS)
      .put("months", ChronoUnit.MONTHS)
      .put("week", ChronoUnit.WEEKS)
      .put("weeks", ChronoUnit.WEEKS)
      .put("day", ChronoUnit.DAYS)
      .put("days", ChronoUnit.DAYS)
      .put("hour", ChronoUnit.HOURS)
      .put("hours", ChronoUnit.HOURS)
      .put("minute", ChronoUnit.MINUTES)
      .put("minutes", ChronoUnit.MINUTES)
      .put("second", ChronoUnit.SECONDS)
      .put("seconds", ChronoUnit.SECONDS)
      .put("millisecond", ChronoUnit.MILLIS)
      .put("milliseconds", ChronoUnit.MILLIS)
      .build();

  @Nonnull
  protected IntermediateType performAddition(@Nonnull final IntermediateType temporal,
      @Nonnull final Quantity calendarDuration) {
    if (!calendarDuration.getSystem().equals(QuantityLiteralPath.FHIRPATH_CALENDAR_DURATION_URI)) {
      throw new IllegalArgumentException("Calendar duration must have a system of "
          + QuantityLiteralPath.FHIRPATH_CALENDAR_DURATION_URI);
    }
    final long amountToAdd = calendarDuration.getValue()
        .setScale(0, RoundingMode.HALF_UP)
        .longValue();
    final TemporalUnit temporalUnit = TemporalArithmeticFunction.CALENDAR_DURATION_TO_UCUM.get(
        calendarDuration.getCode());

    //noinspection unchecked
    return (IntermediateType) temporal.plus(amountToAdd, temporalUnit);
  }

  @Nonnull
  protected IntermediateType performSubtraction(@Nonnull final IntermediateType temporal,
      @Nonnull final Quantity calendarDuration) {
    if (!calendarDuration.getSystem().equals(QuantityLiteralPath.FHIRPATH_CALENDAR_DURATION_URI)) {
      throw new IllegalArgumentException("Calendar duration must have a system of "
          + QuantityLiteralPath.FHIRPATH_CALENDAR_DURATION_URI);
    }
    final long amountToAdd = calendarDuration.getValue()
        .setScale(0, RoundingMode.HALF_UP)
        .longValue();
    final TemporalUnit temporalUnit = TemporalArithmeticFunction.CALENDAR_DURATION_TO_UCUM.get(
        calendarDuration.getCode());

    //noinspection unchecked
    return (IntermediateType) temporal.minus(amountToAdd, temporalUnit);
  }

  abstract Function<String, IntermediateType> parseEncodedValue();

  abstract BiFunction<IntermediateType, Quantity, IntermediateType> getOperationFunction();

  abstract Function<IntermediateType, String> encodeResult();

  @Override
  public DataType getReturnType() {
    return DataTypes.StringType;
  }

  @Nullable
  @Override
  public String call(@Nullable final String temporalValue, @Nullable final Row calendarDurationRow)
      throws Exception {
    if (temporalValue == null || calendarDurationRow == null) {
      return null;
    }
    final IntermediateType temporal = parseEncodedValue().apply(temporalValue);
    final Quantity calendarDuration = QuantityEncoding.decode(calendarDurationRow);
    final IntermediateType result = getOperationFunction().apply(temporal, calendarDuration);
    return encodeResult().apply(result);
  }

}
