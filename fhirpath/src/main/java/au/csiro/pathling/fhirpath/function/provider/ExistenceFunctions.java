package au.csiro.pathling.fhirpath.function.provider;

import static java.util.Objects.nonNull;

import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.CollectionTransform;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Contains functions for evaluating the existence of elements in a collection.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#existence">FHIRPath Specification -
 * Existence</a>
 */
@SuppressWarnings("unused")
public class ExistenceFunctions {

  private ExistenceFunctions() {
  }

  /**
   * Returns {@code true} if the input collection has any elements (optionally filtered by the
   * criteria), and {@code false} otherwise. This is the opposite of {@code empty()}, and as such is
   * a shorthand for {@code empty().not()}. If the input collection is empty, the result is
   * {@code false}.
   * <p>
   * Using the optional criteria can be considered a shorthand for
   * {@code where(criteria).exists()}.
   *
   * @param input The input collection
   * @param criteria The criteria to apply to the input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a
   * href="https://build.fhir.org/ig/HL7/FHIRPath/#existscriteria--expression--boolean">FHIRPath
   * Specification - exists</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static BooleanCollection exists(@Nonnull final Collection input,
      @Nullable final CollectionTransform criteria) {
    return BooleanLogicFunctions.not(empty(nonNull(criteria)
                                           ? FilteringAndProjectionFunctions.where(input, criteria)
                                           : input));

  }

  /**
   * Returns {@code true} if the input collection is empty and {@code false} otherwise.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#empty--boolean">FHIRPath Specification -
   * empty</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static BooleanCollection empty(@Nonnull final Collection input) {
    return BooleanCollection.build(input.getColumn().isEmpty());
  }
}
