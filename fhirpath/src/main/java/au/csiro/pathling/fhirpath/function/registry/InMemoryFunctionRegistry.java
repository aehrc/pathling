package au.csiro.pathling.fhirpath.function.registry;

import au.csiro.pathling.fhirpath.function.NamedFunction;
import jakarta.annotation.Nonnull;
import java.util.Map;

/**
 * An implementation of {@link FunctionRegistry} that stores function instances in an in-memory map
 * structure.
 *
 * @author John Grimes
 */
public class InMemoryFunctionRegistry implements FunctionRegistry {

  @Nonnull
  private final Map<String, NamedFunction> functions;

  /**
   * @param functions The map of functions to store
   */
  public InMemoryFunctionRegistry(@Nonnull final Map<String, NamedFunction> functions) {
    this.functions = functions;
  }

  @Nonnull
  @Override
  public NamedFunction getInstance(@Nonnull final String name)
      throws NoSuchFunctionException {
    final NamedFunction function = functions.get(name);
    if (function == null) {
      throw new NoSuchFunctionException("Unsupported function: " + name);
    }
    return function;
  }

}
