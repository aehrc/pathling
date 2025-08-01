package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;

/**
 * Contains functions for boolean logic.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#boolean-logic">FHIRPath Specification -
 * Boolean logic</a>
 */
public class BooleanLogicFunctions {

  private BooleanLogicFunctions() {
  }

  /**
   * Returns {@code true} if the input collection evaluates to {@code false}, and {@code false} if
   * it evaluates to {@code true}.
   * <p>
   * Otherwise, the result is empty.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the negated values
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#not--boolean">not</a>
   */
  @Nonnull
  @SqlOnFhirConformance(Profile.SHARABLE)
  @FhirPathFunction
  public static BooleanCollection not(@Nonnull final Collection input) {
    return BooleanCollection.build(input.asBooleanSingleton().getColumn().not());
  }

}
