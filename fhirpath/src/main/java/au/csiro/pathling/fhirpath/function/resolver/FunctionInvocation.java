/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.function.resolver;

import au.csiro.pathling.errors.InvalidUserInputError;
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
    } catch (final IllegalAccessException e) {
      throw new AssertionError("Error accessing function: " + method.getName(), e);
    } catch (final InvocationTargetException e) {
      if (e.getCause() instanceof final InvalidUserInputError userInputError) {
        // If the cause is an InvalidUserInputError, rethrow it directly.
        throw userInputError;
      } else {
        // Otherwise, wrap the exception in a RuntimeException.
        throw new FunctionInvocationError(
            "Error occurred while invoking function: " + method.getName(), e);
      }
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
    return "FunctionInvocation{"
        + "method="
        + method
        + ", arguments="
        + Arrays.toString(arguments)
        + '}';
  }
}
