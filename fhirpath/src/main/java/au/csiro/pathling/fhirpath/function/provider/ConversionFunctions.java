package au.csiro.pathling.fhirpath.function.provider;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.CollectionTransform;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.utilities.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.functions;

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
    return input.asSingular().asStringPath();
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

    return isNull(falseResult)
           ? iif(input, criterion, trueResult)
           : iifElse(input, criterion, trueResult, requireNonNull(falseResult));
  }

  @Nonnull
  private static Collection iif(@Nonnull final Collection input,
      @Nonnull final CollectionTransform criterion,
      @Nonnull final Collection trueResult) {
    final Collection conditionResult = criterion.requireBoolean().apply(input);
    return trueResult.mapColumn(c -> functions.when(conditionResult.getColumnValue(), c));
  }

  @Nonnull
  private static Collection iifElse(@Nonnull final Collection input,
      @Nonnull final CollectionTransform criterion,
      @Nonnull final Collection trueResult,
      @Nonnull final Collection falseResult) {
    Preconditions.checkUserInput(
        trueResult.convertibleTo(falseResult) || falseResult.convertibleTo(
            trueResult),
        "iff() trueResult and falseResult must have the compatible types");

    // TODO: if possible implement short-circuiting as per FHIRPath spec
    // TODO: implement this type adjutemnt in collection as it may be useful elsewhere
    final Collection returnType;
    final Collection compatibleTrue;
    final Collection compatibleFalse;
    if (trueResult.convertibleTo(falseResult) && falseResult.convertibleTo(trueResult)) {
      // same type
      returnType = trueResult;
      compatibleTrue = trueResult;
      compatibleFalse = falseResult;
    } else if (trueResult.convertibleTo(falseResult)) {
      // trueResult is convertible to falseResult
      returnType = falseResult;
      compatibleTrue = falseResult.mapColumn(c -> trueResult.getColumnValue());
      compatibleFalse = falseResult;
    } else if (falseResult.convertibleTo(trueResult)) {
      // falseResult is convertible to trueResult
      returnType = trueResult;
      compatibleTrue = trueResult;
      compatibleFalse = trueResult.mapColumn(c -> falseResult.getColumnValue());
    } else {
      throw new IllegalStateException(
          "iff() trueResult and falseResult must have the compatible types");
    }
    final Collection conditionResult = criterion.requireBoolean().apply(input);
    return returnType.copyWith(compatibleTrue.map(
        tr -> tr.vectorize2(compatibleFalse.getColumn(),
            (t, f) -> functions.when(conditionResult.getColumnValue(), t).otherwise(f),
            (t, f) -> functions.when(conditionResult.getColumnValue(), t).otherwise(f))
    ).getColumn());
  }

}
