/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import static au.csiro.pathling.fhirpath.Temporal.buildDateArithmeticOperation;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.not;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.Temporal;
import au.csiro.pathling.fhirpath.literal.DateLiteralPath;
import au.csiro.pathling.fhirpath.literal.DateTimeLiteralPath;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import au.csiro.pathling.sql.dates.datetime.DateTimeAddDurationFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeEqualsFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeGreaterThanFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeGreaterThanOrEqualToFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeLessThanFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeLessThanOrEqualToFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeSubtractDurationFunction;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javolution.testing.AssertionException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.BaseDateTimeType;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.InstantType;

/**
 * Represents a FHIRPath expression which refers to a datetime typed element.
 *
 * @author John Grimes
 */
public class DateTimePath extends ElementPath implements Materializable<BaseDateTimeType>,
    Comparable, Temporal {

  private static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone("GMT");

  private static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES = ImmutableSet
      .of(DatePath.class, DateTimePath.class, DateLiteralPath.class, DateTimeLiteralPath.class,
          NullLiteralPath.class);

  protected DateTimePath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> currentResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, currentResource,
        thisColumn, fhirType);
  }

  @Nonnull
  @Override
  public Optional<BaseDateTimeType> getValueFromRow(@Nonnull final Row row,
      final int columnNumber) {
    return valueFromRow(row, columnNumber, getFhirType());
  }

  /**
   * Gets a value from a row for a DateTime or DateTime literal.
   *
   * @param row The {@link Row} from which to extract the value
   * @param columnNumber The column number to extract the value from
   * @param fhirType The FHIR type to assume when extracting the value
   * @return A {@link BaseDateTimeType}, or the absence of a value
   */
  @Nonnull
  public static Optional<BaseDateTimeType> valueFromRow(@Nonnull final Row row,
      final int columnNumber, final FHIRDefinedType fhirType) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }

    if (fhirType == FHIRDefinedType.INSTANT) {
      final InstantType value = new InstantType(row.getTimestamp(columnNumber));
      value.setTimeZone(DEFAULT_TIME_ZONE);
      return Optional.of(value);
    } else {
      final DateTimeType value = new DateTimeType(row.getString(columnNumber));
      value.setTimeZone(DEFAULT_TIME_ZONE);
      return Optional.of(value);
    }
  }

  /**
   * Builds a comparison function for date and date/time like paths.
   *
   * @param source the path to build the comparison function for
   * @param operation the {@link ComparisonOperation} that should be built
   * @return a new {@link Function}
   */
  @Nonnull
  public static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
      @Nonnull final ComparisonOperation operation) {
    return (target) -> {
      final String functionName;
      switch (operation) {
        case EQUALS:
        case NOT_EQUALS:
          functionName = DateTimeEqualsFunction.FUNCTION_NAME;
          final Column equals = callUDF(functionName,
              source.getValueColumn(), target.getValueColumn());
          return operation == ComparisonOperation.EQUALS
                 ? equals
                 : not(equals);
        case LESS_THAN:
          functionName = DateTimeLessThanFunction.FUNCTION_NAME;
          break;
        case LESS_THAN_OR_EQUAL_TO:
          functionName = DateTimeLessThanOrEqualToFunction.FUNCTION_NAME;
          break;
        case GREATER_THAN:
          functionName = DateTimeGreaterThanFunction.FUNCTION_NAME;
          break;
        case GREATER_THAN_OR_EQUAL_TO:
          functionName = DateTimeGreaterThanOrEqualToFunction.FUNCTION_NAME;
          break;
        default:
          throw new AssertionException("Unsupported operation: " + operation);
      }
      return callUDF(functionName, source.getValueColumn(), target.getValueColumn());
    };
  }

  public static TimeZone getDefaultTimeZone() {
    return DEFAULT_TIME_ZONE;
  }

  @Nonnull
  public static ImmutableSet<Class<? extends Comparable>> getComparableTypes() {
    return COMPARABLE_TYPES;
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return COMPARABLE_TYPES.contains(type);
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return super.canBeCombinedWith(target) || target instanceof DateTimeLiteralPath;
  }

  @Nonnull
  @Override
  public Function<QuantityLiteralPath, FhirPath> getDateArithmeticOperation(
      @Nonnull final MathOperation operation, @Nonnull final Dataset<Row> dataset,
      @Nonnull final String expression) {
    return buildDateArithmeticOperation(this, operation, dataset, expression,
        DateTimeAddDurationFunction.FUNCTION_NAME, DateTimeSubtractDurationFunction.FUNCTION_NAME);
  }

}
