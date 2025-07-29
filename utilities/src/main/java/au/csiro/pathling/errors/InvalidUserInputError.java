/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.errors;

import java.io.Serial;

/**
 * Thrown when invalid user input is detected, and we want to send the details of the problem back
 * to the user.
 *
 * @author John Grimes
 */
public class InvalidUserInputError extends RuntimeException {

  @Serial
  private static final long serialVersionUID = -5378096951525707512L;

  /**
   * Creates a new InvalidUserInputError with the specified message.
   *
   * @param message the error message describing the invalid input
   */
  public InvalidUserInputError(final String message) {
    super(message);
  }

  /**
   * Creates a new InvalidUserInputError with the specified message and cause.
   *
   * @param message the error message describing the invalid input
   * @param cause the underlying cause of the error
   */
  public InvalidUserInputError(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new InvalidUserInputError with the specified cause.
   *
   * @param cause the underlying cause of the error
   */
  public InvalidUserInputError(final Throwable cause) {
    super(cause);
  }

}
