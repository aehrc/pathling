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

package au.csiro.pathling.fhirpath.function.terminology;

import au.csiro.pathling.fhirpath.annotations.Name;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.function.NamedFunction;

/**
 * This function returns the display name for given Coding
 *
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#display">display</a>
 */
@Name("display")
@NotImplemented
public class DisplayFunction implements NamedFunction {

  // TODO: implement as columns

  // private static final String NAME = "display";
  //
  // @Nonnull
  // @Override
  // public Collection invoke(@Nonnull final NamedFunctionInput input) {
  //
  //   validateInput(input);
  //   final PrimitivePath inputPath = (PrimitivePath) input.getInput();
  //   final String expression = expressionFromInput(input, NAME, input.getInput());
  //
  //   final Arguments arguments = Arguments.of(input);
  //   final Optional<StringType> acceptLanguage = arguments.getOptionalValue(0, StringType.class);
  //
  //   final Dataset<Row> dataset = inputPath.getDataset();
  //   final Column resultColumn = display(inputPath.getValueColumn(),
  //       acceptLanguage.map(StringType::getValue).orElse(null));
  //   return PrimitivePath.build(expression, dataset, inputPath.getIdColumn(), resultColumn,
  //       inputPath.getOrderingColumn(), inputPath.isSingular(), inputPath.getCurrentResource(),
  //       inputPath.getThisColumn(), FHIRDefinedType.STRING);
  // }
  //
  // private void validateInput(@Nonnull final NamedFunctionInput input) {
  //   final ParserContext context = input.getContext();
  //
  //   checkUserInput(input.getArguments().size() <= 1,
  //       NAME + " function accepts one optional language argument");
  //   if (input.getArguments().size() == 1) {
  //     checkUserInput(input.getArguments().get(0) instanceof StringLiteralPath,
  //         String.format("Function `%s` expects `%s` as argument %s", NAME, "String literal", 1));
  //   }
  //
  //   checkUserInput(context.getTerminologyServiceFactory()
  //       .isPresent(), "Attempt to call terminology function " + NAME
  //       + " when terminology service has not been configured");
  //
  //   final Collection inputPath = input.getInput();
  //   checkUserInput(inputPath instanceof PrimitivePath
  //           && (((PrimitivePath) inputPath).getFhirType().equals(FHIRDefinedType.CODING)),
  //       "Input to display function must be Coding but is: " + inputPath.getExpression());
  //
  // }
}
