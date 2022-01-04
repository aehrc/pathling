/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * Represents the inputs to a named function in FHIRPath.
 *
 * @author John Grimes
 */
@Getter
public class NamedFunctionInput extends FunctionInput {

  /**
   * An expression representing the input to the function, i.e. the expression on the left hand side
   * of the dot preceding the function invocation.
   */
  @Nonnull
  private final NonLiteralPath input;

  /**
   * A list of expressions representing the arguments to the function, i.e. the expressions inside
   * the parentheses following the function invocation, separated by commas.
   */
  @Nonnull
  private final List<FhirPath> arguments;

  /**
   * @param context The {@link ParserContext} that the function should be executed within
   * @param input The {@link NonLiteralPath} representing the expression on the left hand side of
   * the function invocation
   * @param arguments A list of {@link FhirPath} objects representing the arguments passed to the
   * function within the parentheses
   */
  public NamedFunctionInput(@Nonnull final ParserContext context,
      @Nonnull final NonLiteralPath input, @Nonnull final List<FhirPath> arguments) {
    super(context);
    this.input = input;
    this.arguments = arguments;
  }

}
