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
 * This function returns the designations of a Coding.
 *
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#designation">designation</a>
 */
@Name("designation")
@NotImplemented
public class DesignationFunction implements NamedFunction {

  // TODO: implement as columns

  // private static final String NAME = "designation";
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
  //   @Nullable final Coding use = arguments.getOptionalValue(0, Coding.class).orElse(null);
  //   @Nullable final String languageCode = arguments.getOptionalValue(1, StringType.class)
  //       .map(StringType::getValue)
  //       .orElse(null);
  //
  //   final Dataset<Row> dataset = inputPath.getDataset();
  //   final Column designations = designation(inputPath.getValueColumn(), use, languageCode);
  //
  //   // The result is an array of designations per each input element, which we now need to explode 
  //   // in the same way as for path traversal, creating unique element ids.
  //   final Column explodedDesignations = explode_outer(designations);
  //   return PrimitivePath.build(expression, dataset, inputPath.getIdColumn(), explodedDesignations,
  //       Optional.empty(), inputPath.isSingular(), inputPath.getCurrentResource(),
  //       inputPath.getThisColumn(), FHIRDefinedType.STRING);
  // }
  //
  // private void validateInput(@Nonnull final NamedFunctionInput input) {
  //   final ParserContext context = input.getContext();
  //   checkUserInput(context.getTerminologyServiceFactory()
  //       .isPresent(), "Attempt to call terminology function " + NAME
  //       + " when terminology service has not been configured");
  //
  //   final Collection inputPath = input.getInput();
  //   checkUserInput(inputPath instanceof PrimitivePath
  //           && (((PrimitivePath) inputPath).getFhirType().equals(FHIRDefinedType.CODING)),
  //       "Input to " + NAME + " function must be Coding but is: " + inputPath.getExpression());
  //
  //   final List<Collection> arguments = input.getArguments();
  //   checkUserInput(arguments.size() <= 2,
  //       NAME + " function accepts two optional arguments");
  //   checkUserInput(arguments.isEmpty() || arguments.get(0) instanceof CodingLiteralPath,
  //       String.format("Function `%s` expects `%s` as argument %s", NAME, "Coding literal", 1));
  //   checkUserInput(arguments.size() <= 1 || arguments.get(1) instanceof StringLiteralPath,
  //       String.format("Function `%s` expects `%s` as argument %s", NAME, "String literal", 2));
  // }
}
