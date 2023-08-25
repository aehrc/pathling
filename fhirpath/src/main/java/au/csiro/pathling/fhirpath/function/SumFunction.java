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
 * A function for computing the sum of a collection of numeric values.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#sum">sum</a>
 */
@Name("sum")
@NotImplemented
public class SumFunction implements NamedFunction {

  // TODO: implement as columns 
  
  // private static final String NAME = "sum";
  //
  // public SumFunction() {
  // }
  //
  // @Nonnull
  // @Override
  // public Collection invoke(@Nonnull final NamedFunctionInput input) {
  //   checkNoArguments("sum", input);
  //   checkUserInput(input.getInput() instanceof Numeric,
  //       "Input to sum function must be numeric: " + input.getInput().getExpression());
  //
  //   final NonLiteralPath inputPath = input.getInput();
  //   final Dataset<Row> dataset = inputPath.getDataset();
  //   final String expression = expressionFromInput(input, NAME, input.getInput());
  //   final Column aggregateColumn = sum(inputPath.getValueColumn());
  //
  //   return buildAggregateResult(dataset, input.getContext(), inputPath, aggregateColumn,
  //       expression);
  // }

}
