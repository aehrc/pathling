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

import au.csiro.pathling.fhirpath.annotations.Name;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;

/**
 * A function that returns the extensions of the current element that match a given URL.
 *
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#extension">extension</a>
 */
@Name("extension")
@NotImplemented
public class ExtensionFunction implements NamedFunction {

  // TODO: implement as columns
  // @Nonnull
  // @Override
  // public String getName() {
  //   return NAME;
  // }
  //
  // @Nonnull
  // @Override
  // public Collection invoke(@Nonnull final FunctionInput input) {
  //   final String expression = NamedFunction.expressionFromInput(input, getName(),
  //       input.getInput());
  //   checkUserInput(input.getArguments().size() == 1,
  //       "extension function must have one argument: " + expression);
  //
  //   final Collection<?> inputPath = input.getInput();
  //   final Collection<?> urlArgument = input.getArguments().get(0).apply(inputPath);
  //   checkUserInput(urlArgument instanceof StringCollection,
  //       "extension function must have argument of type String: " + expression);
  //   final Collection<?> extensionPath = checkPresent(
  //       inputPath.traverse(ExtensionSupport.EXTENSION_ELEMENT_NAME()));
  //
  //   final Collection<?> extensionUrlPath = checkPresent(extensionPath.traverse("url"));
  //   final Collection extensionUrCondition = new ComparisonOperator(ComparisonOperation.EQUALS)
  //       .invoke(new FunctionInput(input.getContext(), extensionUrlPath, List.of(i -> urlArgument)));
  //
  //   // Override the expression in the function input.
  //   return new WhereFunction()
  //       .invoke(new NamedFunctionInput(input.getContext(), extensionPath,
  //           Collections.singletonList(extensionUrCondition)));
  // }

}
