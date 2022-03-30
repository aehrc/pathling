/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.errors;

/**
 * Thrown when invalid user input is detected, and we want to send the details of the problem back
 * to the user.
 *
 * @author John Grimes
 */
public class ResourceNotFoundError extends RuntimeException {

  private static final long serialVersionUID = -4574080049289748708L;

  public ResourceNotFoundError(final String message) {
    super(message);
  }

  public ResourceNotFoundError(final String message, final Throwable cause) {
    super(message, cause);
  }

}
