package au.csiro.pathling.fhirpath.function;

import static org.apache.spark.sql.functions.callUDF;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
import au.csiro.pathling.utilities.Preconditions;
import javax.annotation.Nonnull;
import org.apache.commons.lang.NotImplementedException;

public abstract class BoundaryFunctions {

  @FhirpathFunction
  @Nonnull
  public static Collection lowBoundary(@Nonnull final Collection input) {
    validateType(input, "lowBoundary");
    if (input instanceof DecimalCollection) {
      final DecimalCollection decimalCollection = (DecimalCollection) input;
      return decimalCollection.map(ctx -> ctx.call(c ->
          callUDF("low_boundary_for_decimal", c))); //, ctx.getNumericContextColumn())
      // .cast(DecimalCollection.getDecimalType())
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
