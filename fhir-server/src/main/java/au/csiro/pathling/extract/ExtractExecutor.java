/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.extract;

import javax.annotation.Nonnull;

/**
 * @author John Grimes
 */
public interface ExtractExecutor {

  /**
   * Executes a {@link ExtractRequest}, producing an {@link ExtractResponse}.
   *
   * @param query The {@link ExtractRequest} to execute.
   * @return an {@link ExtractResponse}
   */
  ExtractResponse execute(@Nonnull final ExtractRequest query);

}
