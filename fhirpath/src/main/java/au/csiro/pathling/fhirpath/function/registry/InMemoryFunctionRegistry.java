package au.csiro.pathling.fhirpath.function.registry;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * An implementation of {@link FunctionRegistry} that stores function instances in an in-memory map
 * structure.
 *
 * @param <T> The type of function to be stored
 * @author John Grimes
 */
public class InMemoryFunctionRegistry<T> implements FunctionRegistry<T> {

  @Nonnull
  private final Map<String, T> functions;

  /**
   * @param functions The map of functions to store
   */
  public InMemoryFunctionRegistry(@Nonnull final Map<String, T> functions) {
    this.functions = functions;
  }

  @Nonnull
  @Override
  public T getInstance(@Nonnull final String name) throws NoSuchFunctionException {
    final T function = functions.get(name);
    if (function == null) {
      throw new NoSuchFunctionException("Unsupported function: " + name);
    }
    return function;
  }

}
