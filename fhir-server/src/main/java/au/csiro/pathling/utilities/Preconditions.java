/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.utilities;

import au.csiro.pathling.errors.InvalidUserInputError;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Static convenience methods for checking the validity of inputs.
 *
 * @author John Grimes
 */
public class Preconditions {

  /**
   * Ensures the truth of an expression, throwing an IllegalArgumentException with the supplied
   * message if it does not evaluate as true.
   *
   * @param expression The expression that should be true
   */
  public static void check(final boolean expression) {
    if (!expression) {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Ensures that an object is not null, throwing a {@link NullPointerException} if it is.
   *
   * @param object The object to check
   * @param <T> The type of the object
   * @return The non-null object
   */
  @Nonnull
  public static <T> T checkNotNull(@Nullable final T object) {
    return Objects.requireNonNull(object);
  }

  /**
   * Ensures that an {@link Optional} value is present, throwing a {@link
   * java.util.NoSuchElementException} if it is not.
   *
   * @param object The object to check
   * @param <T> The type of the object within the Optional
   * @return The unwrapped object
   */
  @Nonnull
  public static <T> T checkPresent(@Nonnull final Optional<T> object) {
    return object.orElseThrow();
  }

  /**
   * Ensures the truth of an expression, throwing an {@link InvalidUserInputError} with the supplied
   * message if it does not evaluate as true.
   *
   * @param expression The expression that should be true
   * @param errorMessage The message to use if an error is thrown
   */
  public static void checkUserInput(final boolean expression, @Nonnull final String errorMessage) {
    if (!expression) {
      throw new InvalidUserInputError(errorMessage);
    }
  }

}
