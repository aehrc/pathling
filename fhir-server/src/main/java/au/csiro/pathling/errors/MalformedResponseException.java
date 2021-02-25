/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.errors;

/**
 * @author Piotr Szul
 */
public class MalformedResponseException extends RuntimeException {

  private static final long serialVersionUID = 5953491164133388069L;

  public MalformedResponseException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public MalformedResponseException(final String message) {
    super(message);
  }

  public MalformedResponseException(final Throwable cause) {
    super(cause);
  }

}
