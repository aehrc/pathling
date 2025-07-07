package au.csiro.pathling.fhirpath.function.resolver;

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a function method with its bound arguments, ready for invocation.
 *
 * @param method The method to be invoked
 * @param arguments The resolved arguments to pass to the method
 */
public record FunctionInvocation(Method method, Object[] arguments) {

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
      throw new AssertionError("Error invoking function: " + method.getName(), e);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final FunctionInvocation that = (FunctionInvocation) o;
    return Objects.equals(method, that.method) && Arrays.equals(arguments, that.arguments);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(method);
    result = 31 * result + Arrays.hashCode(arguments);
    return result;
  }

  @Override
  @Nonnull
  public String toString() {
    return "FunctionInvocation{" +
        "method=" + method +
        ", arguments=" + Arrays.toString(arguments) +
        '}';
  }

}
