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
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Describes a function which can scope down the previous invocation within a FHIRPath expression,
 * based upon an expression passed in as an argument. Supports the use of `$this` to reference the
 * element currently in scope.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#where">where</a>
 */
public class WhereFunction implements NamedFunction {

  private static final String NAME = "where";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkUserInput(input.getArguments().size() == 1,
        "where function accepts one argument");
    final NonLiteralPath inputPath = input.getInput();
    checkUserInput(input.getArguments().get(0) instanceof NonLiteralPath,
        "Argument to where function cannot be a literal: " + input.getArguments().get(0)
            .getExpression());
    final NonLiteralPath argumentPath = (NonLiteralPath) input.getArguments().get(0);

    checkUserInput(argumentPath instanceof BooleanPath && argumentPath.isSingular(),
        "Argument to where function must be a singular Boolean: " + argumentPath.getExpression());

    checkUserInput(argumentPath.getThisColumn().isPresent(),
        "Argument to where function must be navigable from collection item (use $this): "
            + argumentPath.getExpression());
    final Column argumentValue = argumentPath.getValueColumn();

    // The result is the input value if it is equal to true, or null otherwise (signifying the
    // absence of a value).
    final Column idColumn = argumentPath.getIdColumn();
    final Column thisValue = checkPresent(argumentPath.getThisValueColumn());
    final Column thisEid = checkPresent(argumentPath.getThisOrderingColumn());

    final Column valueColumn = when(argumentValue.equalTo(true), thisValue).otherwise(lit(null));
    final String expression = expressionFromInput(input, NAME);

    return inputPath
        .copy(expression, argumentPath.getDataset(), idColumn,
            inputPath.getEidColumn().map(c -> thisEid), valueColumn, inputPath.isSingular(),
            inputPath.getThisColumn());
  }

}
