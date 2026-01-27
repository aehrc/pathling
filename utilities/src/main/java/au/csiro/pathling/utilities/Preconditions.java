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

package au.csiro.pathling.utilities;

import au.csiro.pathling.errors.InvalidUserInputError;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import org.hl7.fhir.exceptions.FHIRException;
import org.jetbrains.annotations.Contract;

/**
 * Static convenience methods for checking the validity of inputs.
 *
 * @author John Grimes
 */
public abstract class Preconditions {

  private Preconditions() {}

  /**
   * Ensures the truth of an expression, throwing an {@link AssertionError} with the supplied
   * message if it does not evaluate as true.
   *
   * @param expression the expression that should be true
   */
  public static void check(final boolean expression) {
    if (!expression) {
      throw new AssertionError();
    }
  }

  /**
   * Ensures the truth of an expression, throwing an {@link AssertionError} with the supplied
   * message if it does not evaluate as true.
   *
   * @param expression the expression that should be true
   * @param messageTemplate the message template in the {@link String#format } format
   * @param params the parameters to the message template
   */
  public static void check(
      final boolean expression,
      final @Nonnull String messageTemplate,
      @Nonnull final Object... params) {
    if (!expression) {
      throw new AssertionError(String.format(messageTemplate, params));
    }
  }

  /**
   * Ensures the truth of an expression, throwing an {@link IllegalArgumentException} with the
   * supplied message if it does not evaluate as true.
   *
   * @param expression the expression that should be true
   * @param errorMessage the message to use if an error is thrown
   */
  public static void checkArgument(final boolean expression, @Nonnull final String errorMessage) {
    if (!expression) {
      throw new IllegalArgumentException(errorMessage);
    }
  }

  /**
   * Ensures that an object is not null, throwing an {@link IllegalArgumentException} if it is null.
   *
   * @param object the object to check
   * @param <T> the type of the object
   * @return the object if it is not null
   */
  @Contract("null -> fail; !null -> param1")
  @Nonnull
  public static <T> T checkArgumentNotNull(@Nullable final T object) {
    if (object == null) {
      throw new IllegalArgumentException("Argument must not be null");
    }
    return object;
  }

  /**
   * Ensures that an {@link Optional} value is present, throwing a {@link AssertionError} if it is
   * not.
   *
   * @param object the object to check
   * @param <T> the type of the object within the Optional
   * @return the unwrapped object
   */
  @Nonnull
  public static <T> T checkPresent(@Nonnull final Optional<T> object) {
    return checkPresent(object, null);
  }

  /**
   * Ensures that an {@link Optional} value is present, throwing a {@link AssertionError} if it is
   * not.
   *
   * @param object the object to check
   * @param message an error message
   * @param <T> the type of the object within the Optional
   * @return the unwrapped object
   */
  @Nonnull
  public static <T> T checkPresent(
      @Nonnull final Optional<T> object, @Nullable final String message) {
    try {
      return object.orElseThrow();
    } catch (final NoSuchElementException e) {
      throw new AssertionError(message == null ? e.getMessage() : message);
    }
  }

  /**
   * Ensures the truth of an expression, throwing an {@link InvalidUserInputError} with the supplied
   * message if it does not evaluate as true.
   *
   * @param expression the expression that should be true
   * @param errorMessage the message to use if an error is thrown
   */
  public static void checkUserInput(final boolean expression, @Nonnull final String errorMessage) {
    if (!expression) {
      throw new InvalidUserInputError(errorMessage);
    }
  }

  /**
   * Ensures the truth of an expression, throwing an {@link IllegalStateException} with the supplied
   * message if it does not evaluate as true.
   *
   * @param expression the expression that should be true
   * @param errorMessage the message to use if an error is thrown
   */
  public static void checkState(final boolean expression, @Nonnull final String errorMessage) {
    if (!expression) {
      throw new IllegalStateException(errorMessage);
    }
  }

  /**
   * Converts a function which throws a {@link FHIRException} to a function that throws an {@link
   * InvalidUserInputError} in the same situation.
   *
   * @param func the function throwing {@link FHIRException}
   * @param <T> the type of the function argument.
   * @param <R> the type of the function result.
   * @return the wrapped function.
   */
  public static <T, R> Function<T, R> wrapInUserInputError(@Nonnull final Function<T, R> func) {

    return s -> {
      try {
        return func.apply(s);
      } catch (final FHIRException ex) {
        throw new InvalidUserInputError(ex.getMessage(), ex);
      }
    };
  }
}
