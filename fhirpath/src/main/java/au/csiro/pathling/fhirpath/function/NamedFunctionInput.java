/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import lombok.Getter;

/**
 * Represents the inputs to a named function in FHIRPath.
 *
 * @author John Grimes
 */
@Getter
public class NamedFunctionInput extends FunctionInput {

  /**
   * An expression representing the input to the function, i.e. the expression on the left-hand side
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
   * Override expression to use instead of the default one.
   */
  @Nonnull
  private final Optional<String> overrideExpression;

  /**
   * @param context The {@link ParserContext} that the function should be executed within
   * @param input The {@link NonLiteralPath} representing the expression on the left-hand side of
   * the function invocation
   * @param arguments A list of {@link FhirPath} objects representing the arguments passed to the
   * function within the parentheses
   * @param overrideExpression Override expression to use instead of the default one.
   */
  public NamedFunctionInput(@Nonnull final ParserContext context,
      @Nonnull final NonLiteralPath input, @Nonnull final List<FhirPath> arguments,
      @Nonnull final String overrideExpression) {
    super(context);
    this.input = input;
    this.arguments = arguments;
    this.overrideExpression = Optional.of(overrideExpression);
  }

  /**
   * @param context The {@link ParserContext} that the function should be executed within
   * @param input The {@link NonLiteralPath} representing the expression on the left-hand side of
   * the function invocation
   * @param arguments A list of {@link FhirPath} objects representing the arguments passed to the
   * function within the parentheses
   */
  public NamedFunctionInput(@Nonnull final ParserContext context,
      @Nonnull final NonLiteralPath input, @Nonnull final List<FhirPath> arguments) {
    super(context);
    this.input = input;
    this.arguments = arguments;
    this.overrideExpression = Optional.empty();
  }

}
