/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.errors;

import java.io.Serial;

/**
 * Thrown when a response from the upstream server does not match expectations.
 *
 * @author Piotr Szul
 */
public class UnexpectedResponseException extends RuntimeException {

  @Serial
  private static final long serialVersionUID = 5953491164133388069L;

  /**
   * @param message The detailed message for the exception
   */
  public UnexpectedResponseException(final String message) {
    super(message);
  }

}
