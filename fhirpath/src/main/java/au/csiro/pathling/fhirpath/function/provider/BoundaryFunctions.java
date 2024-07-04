package au.csiro.pathling.fhirpath.function.provider;

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
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.utilities.Preconditions;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Contains functions for calculating the low and high boundaries of a value.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/HL7/FHIRPath/#lowboundaryprecision-integer-decimal--date--datetime--time">lowBoundary</a>
 * @see <a
 * href="https://build.fhir.org/ig/HL7/FHIRPath/#highboundaryprecision-integer-decimal--date--datetime--time">highBoundary</a>
 */
public abstract class BoundaryFunctions {

  /**
   * The least possible value of the input to the specified precision.
   *
   * @param input The input collection
   * @param precision The precision to which the boundary should be calculated
   * @return The low boundary of the input
   * @see <a
   * href="https://build.fhir.org/ig/HL7/FHIRPath/#lowboundaryprecision-integer-decimal--date--datetime--time">lowBoundary</a>
   */
  @FhirPathFunction
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

  /**
   * The greatest possible value of the input to the specified precision.
   *
   * @param input The input collection
   * @param precision The precision to which the boundary should be calculated
   * @return The high boundary of the input
   * @see <a
   * href="https://build.fhir.org/ig/HL7/FHIRPath/#highboundaryprecision-integer-decimal--date--datetime--time">highBoundary</a>
   */
  @FhirPathFunction
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
