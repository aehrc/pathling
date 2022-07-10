/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.terminology.TerminologyService;
import java.io.Serializable;
import javax.annotation.Nonnull;
import org.slf4j.Logger;

/**
 * Represents something that creates a {@link TerminologyService}.
 * <p>
 * Used for code that runs on Spark workers.
 */
public interface TerminologyServiceFactory extends Serializable {

  /**
   * Builds a new instance.
   *
   * @param logger a {@link Logger} to use for logging
   * @return a shiny new TerminologyClient instance
   */
  @Nonnull
  TerminologyService buildService(@Nonnull final Logger logger);
}
