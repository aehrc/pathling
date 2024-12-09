package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.function.CollectionTransform;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.utilities.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import org.apache.spark.sql.functions;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

/**
 * Functions for converting between different types.
 *
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
 * Conversion</a>
 */
@SuppressWarnings("unused")
public class ConversionFunctions {

  /**
   * If the input collection contains a single item, this function will return a single String if:
   * <ul>
   * <li>the item in the input collection is a String</li>
   * <li>the item in the input collection is an Integer,
   * Decimal, Date, Time, DateTime, or Quantity the output will contain its String representation</li>
   * <li>the item is a Boolean, where true results in 'true' and false in 'false'.</li>
   * </ul>
   * If the item is not one of the above types, the result is false.
   *
   * @param input The input collection
   * @return A collection containing the single item as a String, or an empty collection if the
   * input collection is empty or contains more than one item.
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#tostring--string">FHIRPath Specification
   * - toString</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection toString(@Nonnull final Collection input) {
    Preconditions.checkUserInput(input instanceof StringCoercible,
        "toString() can only be applied to a StringCoercible path");
    return ((StringCoercible) input).asStringPath();
  }


  /**
   * The iif function in FHIRPath is an immediate if, also known as a conditional operator ( such as
   * C's ? : operator).
   * <p>
   * The criterion expression is expected to evaluate to a Boolean. If criterion is true, the
   * function returns the value of the true-result argument. If criterion is false or an empty
   * collection, the function returns otherwise-result, unless the optional otherwise-result is not
   * given, in which case the function returns an empty collection.
   * <p>
   * Note that short-circuit behavior is expected in this function. In other words, true-result
   * should only be evaluated if the criterion evaluates to true, and otherwise-result should only
   * be evaluated otherwise. For implementations, this means delaying evaluation of the arguments.
   *
   * @param input The input collection
   * @param criterion The criteria to apply to the input collection
   * @param trueResult The result if the criterion is true
   * @param falseResult The result if the criterion is false
   * @return A collection containing the result
   * @see <a
   * href="https://build.fhir.org/ig/HL7/FHIRPath/#iifcriterion-expression-true-result-collection--otherwise-result-collection--collection">
   * FHIRPath Specification - iif</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection iif(@Nonnull final Collection input,
      @Nonnull final CollectionTransform criterion,
      @Nonnull final Collection trueResult,
      @Nullable final Collection falseResult) {

    Preconditions.checkUserInput(isNull(falseResult) || trueResult.getType()
            .equals(requireNonNull(falseResult.getType())),
        "iff() true and false results must have the same type");

    // TODO: check pre-conditions
    // TODO: if possible implement short-circuiting as per FHIRPath spec
    // TODO: we need to reconcile the sigularity of both expressions.
    // the only lazy way seem to be to convert both to arrays and then 
    // singularize if possible
    final Collection conditionResult = criterion.requireBoolean().apply(input);
    // TODO: Add the singularization of the result (requires a custom expression as may produce different types of results).
    // MAYBE not even possible to do it or maybe needs to be done differently (as if with dual ifArray (this is possibly a better option))
    // That is something along the the lines of ifArray(trueResult, ifArray(falseResult, trueResult, falseResult), ifArray(falseResult, trueResult, falseResult))
    return Optional.ofNullable(falseResult)
        .map(fr -> trueResult.map(ColumnRepresentation::asArray)
            .mapColumn(c -> functions.when(conditionResult.getColumnValue(), c)
                    .otherwise(fr.getColumn().asArray().getValue()))
        )
        .orElse(trueResult.mapColumn(c -> functions.when(conditionResult.getColumnValue(), c)));
  }

}
