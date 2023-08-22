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

import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;

import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * This function returns true if the input collection is empty.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#empty">empty</a>
 */
public class EmptyFunction implements NamedFunction {

  private static final String NAME = "empty";

  @Nonnull
  @Override
  public String getName() {
    return NAME;
  }

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final FunctionInput input) {
    checkNoArguments(getName(), input);

    // We use the count function to determine whether there are zero items in the input collection.
    final Collection countResult = new CountFunction().invoke(input);
    final Column valueColumn = countResult.getColumn().equalTo(0);
    final String expression = expressionFromInput(input, getName(), input.getInput());

    return BooleanCollection.build(valueColumn, expression, Optional.empty());
  }

}
