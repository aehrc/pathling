/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.errors;

/**
 * Thrown when a response from the upstream server does not match expectations.
 *
 * @author Piotr Szul
 */
public class UnexpectedResponseException extends RuntimeException {

  private static final long serialVersionUID = 5953491164133388069L;

  public UnexpectedResponseException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public UnexpectedResponseException(final String message) {
    super(message);
  }

  public UnexpectedResponseException(final Throwable cause) {
    super(cause);
  }

}
