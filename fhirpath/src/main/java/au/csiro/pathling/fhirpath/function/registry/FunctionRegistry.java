package au.csiro.pathling.fhirpath.function.registry;

import javax.annotation.Nonnull;

/**
 * A registry of FHIRPath functions.
 *
 * @param <T> The type of function to be stored in the registry
 * @author John Grimes
 */
public interface FunctionRegistry<T> {

  /**
   * Retrieves an instance of the function with the specified name in the specified context.
   *
   * @param name The name of the function
   * @return An instance of a T
   * @throws NoSuchFunctionException If the function is not found
   */
  @Nonnull
  T getInstance(@Nonnull final String name) throws NoSuchFunctionException;

}
