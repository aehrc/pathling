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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.Method;

/**
 * Provides context for parameter binding operations, including error reporting.
 *
 * <p>This class helps generate consistent, informative error messages that include the function
 * name and context (input or specific argument).
 *
 * @param method The method being bound
 * @param contextDescription Description of the current binding context (e.g., "input" or "argument
 *     0")
 */
public record BindingContext(@Nonnull Method method, @Nullable String contextDescription) {

  /**
   * Creates a binding context for a method.
   *
   * @param method The method being bound
   * @return A new binding context
   */
  @Nonnull
  public static BindingContext forMethod(@Nonnull final Method method) {
    return new BindingContext(method, null);
  }

  /**
   * Creates a binding context for the input of a method.
   *
   * @return A new binding context for the input
   */
  @Nonnull
  public BindingContext forInput() {
    return new BindingContext(method, "input");
  }

  /**
   * Creates a binding context for an argument of a method.
   *
   * @param index The index of the argument
   * @param parameterType The type of the parameter
   * @return A new binding context for the argument
   */
  @Nonnull
  public BindingContext forArgument(final int index, @Nonnull final Class<?> parameterType) {
    return new BindingContext(
        method, "argument " + index + " (" + parameterType.getSimpleName() + ")");
  }

  /**
   * Reports an error in this binding context.
   *
   * @param <T> the return type (never actually returned as method always throws)
   * @param issue The issue to report
   * @return Never returns, always throws an exception
   * @throws InvalidUserInputError Always thrown with a formatted message
   */
  public <T> T reportError(@Nonnull final String issue) {
    final StringBuilder message =
        new StringBuilder("Function '").append(method.getName()).append("'");

    if (contextDescription != null) {
      message.append(", ").append(contextDescription);
    }

    message.append(": ").append(issue);
    throw new InvalidUserInputError(message.toString());
  }

  /**
   * Checks a condition and reports an error if it's false.
   *
   * @param condition The condition to check
   * @param issue The issue to report if the condition is false
   * @throws InvalidUserInputError Thrown with a formatted message if condition is false
   */
  public void check(final boolean condition, @Nonnull final String issue) {
    if (!condition) {
      reportError(issue);
    }
  }
}
