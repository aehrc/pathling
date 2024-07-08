/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.utilities;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.errors.UnexpectedResponseException;
import jakarta.annotation.Nonnull;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.hl7.fhir.exceptions.FHIRException;

/**
 * Static convenience methods for checking the validity of inputs.
 *
 * @author John Grimes
 */
public abstract class Preconditions {

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
  public static void check(final boolean expression, final @Nonnull String messageTemplate,
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
  public static <T> T checkPresent(@Nonnull final Optional<T> object,
      @Nullable final String message) {
    try {
      return object.orElseThrow();
    } catch (final NoSuchElementException e) {
      throw new AssertionError(message == null
                               ? e.getMessage()
                               : message);
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
   * Ensures that an {@link String} value is not blank, throwing a {@link IllegalArgumentException}
   * if it is.
   *
   * @param string the object to check
   * @param msg the message to use if an error is thrown
   * @return non blank string
   */
  @Nonnull
  public static String requireNonBlank(@Nullable final String string, @Nonnull final String msg) {
    if (string == null || string.isBlank()) {
      throw new IllegalArgumentException(msg);
    }
    return string;
  }

  /**
   * Ensures the truth of an expression, throwing an {@link UnexpectedResponseException} with the
   * supplied formatted message if it does not evaluate as true.
   *
   * @param expression the expression that should be true
   * @param messageTemplate the message template in the {@link String#format} format
   * @param params the parameters to the message template
   */
  public static void checkResponse(final boolean expression, @Nonnull final String messageTemplate,
      final @Nonnull Object... params) {
    if (!expression) {
      throw new UnexpectedResponseException(String.format(messageTemplate, params));
    }
  }


  /**
   * Converts a function which throws a {@link FHIRException} to a function that throws an
   * {@link InvalidUserInputError} in the same situation.
   *
   * @param func the function throwing {@link FHIRException}
   * @param <T> the type of the function argument.
   * @param <R> the type of the function result.
   * @return the wrapped function.
   */
  public static <T, R> Function<T, R> wrapInUserInputError(
      @Nonnull final Function<T, R> func) {

    return s -> {
      try {
        return func.apply(s);
      } catch (final FHIRException ex) {
        throw new InvalidUserInputError(ex.getMessage(), ex);
      }
    };
  }
}
