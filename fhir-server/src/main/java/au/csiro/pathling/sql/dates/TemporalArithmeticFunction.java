/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import au.csiro.pathling.sql.udf.SqlFunction2;
import com.google.common.collect.ImmutableMap;
import java.math.RoundingMode;
import java.util.Calendar;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.BaseDateTimeType;
import org.hl7.fhir.r4.model.Quantity;

public abstract class TemporalArithmeticFunction<IntermediateType extends BaseDateTimeType> implements
    SqlFunction2<String, Row, String> {

  private static final long serialVersionUID = -5016153440496309996L;

  static final Map<String, Integer> CALENDAR_DURATION_TO_UCUM = new ImmutableMap.Builder<String, Integer>()
      .put("year", Calendar.YEAR)
      .put("years", Calendar.YEAR)
      .put("month", Calendar.MONTH)
      .put("months", Calendar.MONTH)
      .put("day", Calendar.DATE)
      .put("days", Calendar.DATE)
      .put("hour", Calendar.HOUR)
      .put("hours", Calendar.HOUR)
      .put("minute", Calendar.MINUTE)
      .put("minutes", Calendar.MINUTE)
      .put("second", Calendar.SECOND)
      .put("seconds", Calendar.SECOND)
      .put("millisecond", Calendar.MILLISECOND)
      .put("milliseconds", Calendar.MILLISECOND)
      .build();

  @Nonnull
  protected IntermediateType performAddition(@Nonnull final IntermediateType temporal,
      @Nonnull final Quantity calendarDuration) {
    return performArithmetic(temporal, calendarDuration, false);
  }

  @Nonnull
  protected IntermediateType performSubtraction(@Nonnull final IntermediateType temporal,
      @Nonnull final Quantity calendarDuration) {
    return performArithmetic(temporal, calendarDuration, true);
  }

  @Nonnull
  private IntermediateType performArithmetic(final @Nonnull IntermediateType temporal,
      final @Nonnull Quantity calendarDuration, final boolean subtract) {
    if (!calendarDuration.getSystem().equals(QuantityLiteralPath.FHIRPATH_CALENDAR_DURATION_URI)) {
      throw new IllegalArgumentException("Calendar duration must have a system of "
          + QuantityLiteralPath.FHIRPATH_CALENDAR_DURATION_URI);
    }
    final int amountToAdd = calendarDuration.getValue()
        .setScale(0, RoundingMode.HALF_UP)
        .intValue();
    final Integer temporalUnit = TemporalArithmeticFunction.CALENDAR_DURATION_TO_UCUM.get(
        calendarDuration.getCode());
    checkUserInput(temporalUnit != null,
        "Unsupported calendar duration unit: " + calendarDuration.getCode());

    @SuppressWarnings("unchecked")
    final IntermediateType result = (IntermediateType) temporal.copy();
    result.add(temporalUnit, subtract
                             ? amountToAdd * -1
                             : amountToAdd);
    return result;
  }

  protected abstract Function<String, IntermediateType> parseEncodedValue();

  protected abstract BiFunction<IntermediateType, Quantity, IntermediateType> getOperationFunction();

  protected abstract Function<IntermediateType, String> encodeResult();

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
