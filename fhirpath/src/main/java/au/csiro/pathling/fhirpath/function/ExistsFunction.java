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

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.not;

import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.annotations.Name;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import java.util.Collections;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * A function which is able to test whether the input collection is empty. It can also optionally
 * accept an argument which can filter the input collection prior to applying the test.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#exists">exists</a>
 */
@Name("exists")
public class ExistsFunction implements NamedFunction {

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final FunctionInput input) {
    final int numberOfArguments = input.getArguments().size();
    checkUserInput(numberOfArguments <= 1,
        "exists function accepts at most one argument");

    final Column column;
    if (numberOfArguments == 0) {
      // If there are no arguments, invoke the `empty` function and use the inverse of the result.
      final Collection emptyResult = new EmptyFunction().invoke(input);
      column = not(emptyResult.getColumn());
    } else {
      // If there is an argument, first invoke the `where` function.
      final Collection whereResult = new WhereFunction().invoke(input);
      // Then invoke the `exists` function on the result, with no arguments.
      final FunctionInput existsInput = new FunctionInput(input.getContext(),
          whereResult, Collections.emptyList());
      final Collection existsResult = invoke(existsInput);
      column = existsResult.getColumn();
    }

    return BooleanCollection.build(column, Optional.empty());
  }

}
