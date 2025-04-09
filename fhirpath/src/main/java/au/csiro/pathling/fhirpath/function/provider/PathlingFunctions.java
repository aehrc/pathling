package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;

/**
 * Contains functions supported by Pathling that are not part of the FHIRPath specification.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public class PathlingFunctions {

  /**
   * Returns the sum of the numbers input collection. Returns 0 when the input collection is empty.
   *
   * @param input The input collection
   * @return An {@link Collection} containing the result sum
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions#sum">Pathling Functions -
   * sum()</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection sum(@Nonnull final Collection input) {
    return input.map(ColumnRepresentation::sum);
  }
}
