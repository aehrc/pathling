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
import static org.apache.spark.sql.functions.size;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * A function for aggregating data based on counting the number of rows within the result.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#count">count</a>
 */
public class CountFunction implements NamedFunction {

  @Nonnull
  @Override
  public String getName() {
    return "count";
  }

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final FunctionInput input) {
    checkNoArguments(getName(), input);
    final Collection inputPath = input.getInput();
    final Column valueColumn = size(inputPath.getColumn());
    final String expression = expressionFromInput(input, getName(), input.getInput());
    return IntegerCollection.build(valueColumn, expression, Optional.empty());
  }

}
