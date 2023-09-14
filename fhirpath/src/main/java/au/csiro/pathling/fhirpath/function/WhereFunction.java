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

import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.filter;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.annotations.Name;
import au.csiro.pathling.fhirpath.collection.Collection;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Describes a function which can scope down the previous invocation within a FHIRPath expression,
 * based upon an expression passed in as an argument. Supports the use of `$this` to reference the
 * element currently in scope.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#where">where</a>
 */
@Name("where")
public class WhereFunction implements NamedFunction<Collection> {

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final FunctionInput input) {
    checkUserInput(input.getArguments().size() == 1,
        getName() + " function accepts one argument");
    final Collection previous = input.getInput();
    final FhirPath<Collection, Collection> argument = input.getArguments().get(0);

    final Column column = filter(previous.getColumn(), element -> {
      final Collection result = argument.apply(
          Collection.build(element, previous.getType(), previous.getFhirType(),
              previous.getDefinition()), input.getContext());
      final FhirPathType type = checkPresent(result.getType());
      checkUserInput(type.equals(FhirPathType.BOOLEAN),
          "Argument to " + getName() + " function must be a singular Boolean");
      return result.getSingleton();
    });

    return Collection.build(column, previous.getType(), previous.getFhirType(),
        previous.getDefinition());
  }

}
