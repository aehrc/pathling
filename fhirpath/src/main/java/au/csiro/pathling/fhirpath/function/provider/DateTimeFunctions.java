package au.csiro.pathling.fhirpath.function.provider;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.sql.misc.TemporalDifferenceFunction;
import jakarta.annotation.Nonnull;

/**
 * Contains date time functions.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public class DateTimeFunctions {


  /**
   * This function computes the time interval (duration) between two paths representing dates or
   * dates with time.The result is returned as an integer, with the unit of time specified by the
   * unit argument.
   *
   * @param from the start date or date with time
   * @param to the end date or date with time
   * @param calendarDurationArgument the unit of time to use for the result
   * @return The time interval between the two dates in the specified unit
   * @author John Grimes
   * @author Piotr Szul
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#until">until</a>
   */
  @Nonnull
  @FhirPathFunction
  public static IntegerCollection until(@Nonnull final Collection from,
      @Nonnull final Collection to,
      @Nonnull final StringCollection calendarDurationArgument) {

    // TODO: consider adding a type for Data | DateTime that can be used here for 
    //  static argument type validation
    checkUserInput(from instanceof DateTimeCollection || from instanceof DateCollection,
        "until function must be invoked on a DateTime or Date");
    checkUserInput(to instanceof DateTimeCollection || to instanceof DateCollection,
        "until function must have a DateTime or Date as the first argument");

    // TODO: consider doing literal mapping in the dispatcher and passing String here instead 
    //  of StringCollection
    final String calendarDuration = calendarDurationArgument.asLiteralValue()
        .orElseThrow(() -> new InvalidUserInputError(
            "until function must have a String literal as the second argument"));

    // TODO: maybe it does not need to be a literal  - the validation can happen 
    //  in the UDF itself. Of maybe we can validation literals eagerly but allow 
    //  non-literals to be passed to the UDF
    checkUserInput(TemporalDifferenceFunction.isValidCalendarDuration(calendarDuration),
        "Invalid calendar duration: " + calendarDuration);

    return IntegerCollection.build(
        from.getColumn().singular().callUdf(
            TemporalDifferenceFunction.FUNCTION_NAME,
            to.getColumn().singular(),
            DefaultRepresentation.literal(calendarDuration))
    );
  }
}
