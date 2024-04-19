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

import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.not;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import org.apache.spark.sql.Column;

/**
 * A function which is able to test whether the input collection is empty. It can also optionally
 * accept an argument which can filter the input collection prior to applying the test.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#exists">exists</a>
 */
public class ExistsFunction implements NamedFunction {

  private static final String NAME = "exists";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    final int numberOfArguments = input.getArguments().size();
    checkUserInput(numberOfArguments <= 1,
        "exists function accepts at most one argument");
    final String expression = expressionFromInput(input, NAME);

    if (numberOfArguments == 0) {
      // If there are no arguments, invoke the `empty` function and use the inverse of the result.
      final BooleanPath emptyResult = (BooleanPath) new EmptyFunction().invoke(input);
      final Column valueColumn = not(emptyResult.getValueColumn());
      return emptyResult.copy(expression, emptyResult.getDataset(), emptyResult.getIdColumn(),
          emptyResult.getEidColumn(), valueColumn, emptyResult.isSingular(),
          emptyResult.getThisColumn());
    } else {
      final FhirPath argument = input.getArguments().get(0);
      checkUserInput(argument.isSingular() && argument instanceof BooleanPath,
          "Argument to exists function must be a singular Boolean: " + argument.getExpression());

      // If there is an argument, first invoke the `where` function.
      final NonLiteralPath whereResult = (NonLiteralPath) new WhereFunction().invoke(input);
      // Then invoke the `exists` function on the result, with no arguments.
      final NamedFunctionInput existsInput = new NamedFunctionInput(input.getContext(),
          whereResult, Collections.emptyList());
      final BooleanPath existsResult = (BooleanPath) invoke(existsInput);
      return existsResult.copy(expression, existsResult.getDataset(), existsResult.getIdColumn(),
          existsResult.getEidColumn(), existsResult.getValueColumn(), existsResult.isSingular(),
          existsResult.getThisColumn());
    }

  }

}
