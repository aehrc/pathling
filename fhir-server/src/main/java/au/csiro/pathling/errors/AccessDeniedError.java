/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.errors;

import javax.annotation.Nonnull;

/**
 * Thrown when invalid user input is detected, and we want to send the details of the problem back
 * to the user.
 *
 * @author John Grimes
 */
public class AccessDeniedError extends RuntimeException {

  private static final long serialVersionUID = -4574080049289748708L;

  @Nonnull
  private final String missingAuthority;

  @Nonnull
  public String getMissingAuthority() {
    return missingAuthority;
  }

  public AccessDeniedError(@Nonnull final String message,
      @Nonnull final String missingAuthority) {
    super(message);
    this.missingAuthority = missingAuthority;
  }
}
