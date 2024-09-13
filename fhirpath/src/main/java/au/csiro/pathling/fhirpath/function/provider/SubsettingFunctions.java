package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import org.apache.spark.sql.functions;
import org.jetbrains.annotations.NotNull;

/**
 * Contains functions for subsetting collections.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/#subsetting">FHIRPath Specification -
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
   * @see <a href="https://hl7.org/fhirpath/#first--collection">FHIRPath Specification
   * - first</a>
   */
  @FhirPathFunction
  public static @NotNull Collection first(final @NotNull Collection input) {
    return input.map(c -> functions.get(c, functions.lit(0)));
  }

}
