package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.annotations.SofCompatibility;
import au.csiro.pathling.fhirpath.annotations.SofCompatibility.Profile;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.CollectionTransform;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;

/**
 * Contains functions for filtering and projecting elements in a collection.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#filtering-and-projection">FHIRPath
 * Specification - Filtering and projection</a>
 */
public class FilteringAndProjectionFunctions {

  private FilteringAndProjectionFunctions() {
  }

  /**
   * Returns a collection containing only those elements in the input collection for which the
   * stated criteria expression evaluates to {@code true}. Elements for which the expression
   * evaluates to {@code false} or empty are not included in the result.
   * <p>
   * If the input collection is empty, the result is empty.
   * <p>
   * If the result of evaluating the condition is other than a single boolean value, the evaluation
   * will end and signal an error to the calling environment, consistent with singleton evaluation
   * of collections behavior.
   *
   * @param input The input collection
   * @param expression The criteria expression
   * @return A collection containing only those elements for which the criteria expression evaluates
   * to {@code true}
   * @see <a
   * href="https://build.fhir.org/ig/HL7/FHIRPath/#wherecriteria--expression--collection">where</a>
   */
  @FhirPathFunction
  @SofCompatibility(Profile.SHARABLE)
  @Nonnull
  public static Collection where(@Nonnull final Collection input,
      @Nonnull final CollectionTransform expression) {
    return input.filter(expression.requireBooleanSingleton().toColumnTransformation(input));
  }

  /**
   * Returns a collection that contains all items in the input collection that are of the given type
   * or a subclass thereof. If the input collection is empty, the result is empty. The type argument
   * is an identifier that must resolve to the name of a type in a model. For implementations with
   * compile-time typing, this requires special-case handling when processing the argument to treat
   * it as type specifier rather than an identifier expression.
   *
   * @param input The input collection
   * @param typeSpecifier The type specifier
   * @return A collection containing only those elements that are of the specified type
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#oftype">ofType</a>
   */
  @FhirPathFunction
  @SofCompatibility(Profile.SHARABLE)
  @Nonnull
  public static Collection ofType(@Nonnull final Collection input,
      @Nonnull final TypeSpecifier typeSpecifier) {
    return input.filterByType(typeSpecifier);
  }

}
