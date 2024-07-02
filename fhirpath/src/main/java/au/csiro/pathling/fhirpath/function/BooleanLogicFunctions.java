package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import javax.annotation.Nonnull;

/**
 * Contains functions for boolean logic.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#boolean-logic">FHIRPath Specification -
 * Boolean logic</a>
 */
public class BooleanLogicFunctions {

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
  @FhirPathFunction
  public static BooleanCollection not(@Nonnull final BooleanCollection input) {
    return BooleanCollection.build(input.getColumn().not());
  }

}
