package au.csiro.pathling.fhirpath.function.resolver;

import au.csiro.pathling.fhirpath.collection.Collection;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.Value;

/**
 * Represents a function method with its bound arguments, ready for invocation.
 */
@Value(staticConstructor = "of")
public class FunctionInvocation {

  /**
   * The method to be invoked
   */
  Method method;

  /**
   * The resolved arguments to pass to the method
   */
  Object[] arguments;

  /**
   * Invokes the function with the bound arguments.
   *
   * @return The result of the function invocation as a Collection
   * @throws RuntimeException if the invocation fails
   */
  public Collection invoke() {
    try {
      return (Collection) method.invoke(null, arguments);
    } catch (final IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Error invoking function: " + method.getName(), e);
    }
  }
}
