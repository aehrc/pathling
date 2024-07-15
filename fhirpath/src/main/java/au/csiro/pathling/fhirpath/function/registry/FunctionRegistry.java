package au.csiro.pathling.fhirpath.function.registry;

import java.util.Optional;

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
   * @return The function, if present
   */
  Optional<T> getInstance(final String name);

}
