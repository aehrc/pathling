package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.utilities.Preconditions.check;
import static org.apache.spark.sql.functions.callUDF;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DecimalRepresentation;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
import au.csiro.pathling.utilities.Preconditions;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public abstract class BoundaryFunctions {

  @FhirpathFunction
  @Nonnull
  public static Collection lowBoundary(@Nonnull final Collection input,
      @Nullable final IntegerCollection precision) {
    if (input instanceof DecimalCollection) {
      return decimalBoundary(input, precision, "lowBoundary", "low_boundary_for_decimal");
    } else if (input instanceof DateTimeCollection) {
      return boundary(input, precision, "lowBoundary", "low_boundary_for_date_time");
    } else if (input instanceof DateCollection) {
      return boundary(input, precision, "lowBoundary", "low_boundary_for_date");
    } else if (input instanceof TimeCollection) {
      return boundary(input, precision, "lowBoundary", "low_boundary_for_time");
    } else if (input instanceof IntegerCollection) {
      // The low boundary of an integer is always the integer itself.
      return input;
    } else {
      throw new InvalidUserInputError(
          "lowBoundary() can only be applied to a Decimal, Integer, Date, DateTime, or Time path");
    }
  }

  @FhirpathFunction
  @Nonnull
  public static Collection highBoundary(@Nonnull final Collection input,
      @Nullable final IntegerCollection precision) {
    if (input instanceof DecimalCollection) {
      return decimalBoundary(input, precision, "highBoundary", "high_boundary_for_decimal");
    } else if (input instanceof DateTimeCollection) {
      return boundary(input, precision, "highBoundary", "high_boundary_for_date_time");
    } else if (input instanceof DateCollection) {
      return boundary(input, precision, "highBoundary", "high_boundary_for_date");
    } else if (input instanceof TimeCollection) {
      return boundary(input, precision, "highBoundary", "high_boundary_for_time");
    } else if (input instanceof IntegerCollection) {
      // The high boundary of an integer is always the integer itself.
      return input;
    } else {
      throw new InvalidUserInputError(
          "highBoundary() can only be applied to a Decimal, Integer, Date, DateTime, or Time path");
    }
  }

  @Nonnull
  private static Collection decimalBoundary(@Nonnull final Collection input,
      @Nullable final IntegerCollection precision, @Nonnull final String functionName,
      @Nonnull final String udfName) {
    validateType(input, functionName);
    check(input.getColumn() instanceof DecimalRepresentation);
    final DecimalCollection decimalCollection = (DecimalCollection) input;
    final DecimalRepresentation column = (DecimalRepresentation) decimalCollection.getColumn();
    final Column precisionColumn = getPrecisionColumn(precision);
    final ColumnRepresentation result = column.call(
        c -> callUDF(udfName, c, precisionColumn));
    return input.copyWith(result);
  }

  @Nonnull
  private static Collection boundary(@Nonnull final Collection input,
      @Nullable final IntegerCollection precision,
      @Nonnull final String functionName, @Nonnull final String udfName) {
    validateType(input, functionName);
    final Column precisionColumn = getPrecisionColumn(precision);
    return input.copyWith(input.getColumn().call(c -> callUDF(udfName, c, precisionColumn)));
  }

  private static void validateType(@Nonnull final Collection input,
      @Nonnull final String functionName) {
    Preconditions.checkUserInput(
        input instanceof IntegerCollection || input instanceof DecimalCollection
            || input instanceof DateCollection || input instanceof DateTimeCollection
            || input instanceof TimeCollection,
        functionName + "() can only be applied to a Decimal, Date, DateTime, or Time path");
  }

  private static @Nullable Column getPrecisionColumn(final @Nullable IntegerCollection precision) {
    return Optional.ofNullable(precision)
        .map(Collection::getColumn)
        .map(ColumnRepresentation::getValue)
        .orElse(functions.lit(null));
  }

}
