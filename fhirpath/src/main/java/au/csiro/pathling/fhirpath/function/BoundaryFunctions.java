package au.csiro.pathling.fhirpath.function;

import static org.apache.spark.sql.functions.callUDF;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DecimalRepresentation;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
import au.csiro.pathling.utilities.Preconditions;
import javax.annotation.Nonnull;
import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.sql.Column;

public abstract class BoundaryFunctions {

  @FhirpathFunction
  @Nonnull
  public static Collection lowBoundary(@Nonnull final Collection input) {
    validateType(input, "lowBoundary");
    if (input instanceof DecimalCollection && input.getColumn() instanceof DecimalRepresentation) {
      final DecimalCollection decimalCollection = (DecimalCollection) input;
      final DecimalRepresentation column = (DecimalRepresentation) decimalCollection.getColumn();
      final Column scaleValue = column.getScaleValue().orElseThrow(
          () -> new IllegalArgumentException(
              "Decimal must have a scale value to be used with lowBoundary"));
      final ColumnRepresentation result = column.call(
          c -> callUDF("low_boundary_for_decimal", c, scaleValue));
      return input.copyWith(result);
    } else {
      throw new NotImplementedException();
    }
  }

  private static void validateType(@Nonnull final Collection input,
      @Nonnull final String functionName) {
    Preconditions.checkUserInput(
        input instanceof DecimalCollection || input instanceof DateCollection
            || input instanceof DateTimeCollection || input instanceof TimeCollection,
        functionName + "() can only be applied to a Decimal, Date, DateTime, or Time path");
  }

}
