/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import javax.annotation.Nonnull;

/**
 * @author John Grimes
 */
public interface AggregateExecutor {

  /**
   * Executes a {@link AggregateRequest}, producing an {@link AggregateResponse}.
   *
   * @param query The {@link AggregateRequest} to execute.
   * @return an {@link AggregateResponse}
   */
  AggregateResponse execute(@Nonnull final AggregateRequest query);

}
