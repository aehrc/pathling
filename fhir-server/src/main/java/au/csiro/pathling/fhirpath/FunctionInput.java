/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.parser.ParserContext;
import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * Represents the inputs to a FHIRPath function.
 *
 * @author John Grimes
 */
@Getter
public abstract class FunctionInput {

  /**
   * Context and dependencies for use in evaluating the function.
   */
  @Nonnull
  private final ParserContext context;

  protected FunctionInput(@Nonnull final ParserContext context) {
    this.context = context;
  }
 
}
