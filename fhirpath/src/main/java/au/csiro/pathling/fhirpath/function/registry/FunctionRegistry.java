package au.csiro.pathling.fhirpath.function.registry;

import au.csiro.pathling.fhirpath.function.NamedFunction;
import jakarta.annotation.Nonnull;

/**
 * A registry of FHIRPath functions.
 *
 * @author John Grimes
 */
public interface FunctionRegistry {

  /**
   * Retrieves an instance of the function with the specified name in the specified context.
   *
   * @param name The name of the function
   * @return An instance of a T
   * @throws NoSuchFunctionException If the function is not found
   */
  @Nonnull
  NamedFunction getInstance(@Nonnull final String name) throws NoSuchFunctionException;

}
