/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates;

import au.csiro.pathling.fhirpath.CalendarDurationUtils;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
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

public abstract class TemporalArithmeticFunction<T extends BaseDateTimeType> implements
    SqlFunction2<String, Row, String> {

  private static final long serialVersionUID = -5016153440496309996L;

  @Nonnull
  protected T performAddition(@Nonnull final T temporal, @Nonnull final Quantity calendarDuration) {
    return performArithmetic(temporal, calendarDuration, false);
  }

  @Nonnull
  protected T performSubtraction(@Nonnull final T temporal,
      @Nonnull final Quantity calendarDuration) {
    return performArithmetic(temporal, calendarDuration, true);
  }

  @Nonnull
  private T performArithmetic(final @Nonnull T temporal, final @Nonnull Quantity calendarDuration,
      final boolean subtract) {
    final int amountToAdd = calendarDuration.getValue().setScale(0, RoundingMode.HALF_UP)
        .intValue();
    final int temporalUnit = CalendarDurationUtils.getTemporalUnit(calendarDuration);

    @SuppressWarnings("unchecked") final T result = (T) temporal.copy();
    result.add(temporalUnit, subtract
                             ? -amountToAdd
                             : amountToAdd);
    return result;
  }

  protected abstract Function<String, T> parseEncodedValue();

  protected abstract BiFunction<T, Quantity, T> getOperationFunction();

  protected abstract Function<T, String> encodeResult();

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
    final T temporal = parseEncodedValue().apply(temporalValue);
    final Quantity calendarDuration = QuantityEncoding.decode(calendarDurationRow);
    final T result = getOperationFunction().apply(temporal, calendarDuration);
    return encodeResult().apply(result);
  }

}
