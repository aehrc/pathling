/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.errors;

/**
 * Thrown on a security violation when we want to send the details of the problem back to the user.
 *
 * @author Piotr Szul
 */
public class SecurityError extends RuntimeException {

  private static final long serialVersionUID = -4574080049289748708L;

  public SecurityError(final String message) {
    super(message);
  }

  public SecurityError(final String message, final Throwable cause) {
    super(message, cause);
  }

}
