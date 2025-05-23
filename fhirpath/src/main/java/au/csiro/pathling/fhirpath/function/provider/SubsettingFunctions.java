package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.annotations.SofCompatibility;
import au.csiro.pathling.fhirpath.annotations.SofCompatibility.Profile;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;

/**
 * Contains functions for subsetting collections.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#subsetting">FHIRPath Specification -
 * Subsetting</a>
 */
public class SubsettingFunctions {

  /**
   * Returns a collection containing only the first item in the input collection. This function is
   * equivalent to {@code item[0]}, so it will return an empty collection if the input collection
   * has no items.
   *
   * @param input The input collection
   * @return A collection containing only the first item in the input collection
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#first--collection">FHIRPath Specification
   * - first</a>
   */
  @FhirPathFunction
  @SofCompatibility(Profile.SHARABLE)
  @Nonnull
  public static Collection first(@Nonnull final Collection input) {
    return input.copyWith(input.getColumn().first());
  }

}
