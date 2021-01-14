/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * Represents the inputs to a binary operator in FHIRPath.
 *
 * @author John Grimes
 */
@Getter
public class OperatorInput extends FunctionInput {

  /**
   * An expression representing the left operand.
   */
  @Nonnull
  private final FhirPath left;

  /**
   * An expression representing the right operand.
   */
  @Nonnull
  private final FhirPath right;

  /**
   * @param context The {@link ParserContext} that the operator should be executed within
   * @param left The {@link FhirPath} representing the left operand
   * @param right The {@link FhirPath} representing the right operand
   */
  public OperatorInput(@Nonnull final ParserContext context, @Nonnull final FhirPath left,
      @Nonnull final FhirPath right) {
    super(context);
    this.left = left;
    this.right = right;
  }

}
