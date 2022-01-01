/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.errors;

/**
 * @author John Grimes
 */
public class UnexpectedServerError extends RuntimeException {

  private static final long serialVersionUID = 5953491164133388069L;

  public UnexpectedServerError(final String message, final Throwable cause) {
    super(message, cause);
  }

  public UnexpectedServerError(final Throwable cause) {
    super(cause);
  }
  
}
