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

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * This function takes three arguments, Returns the second argument if the first argument evaluates
 * to {@code true}, or the third argument otherwise.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#iif">iif</a>
 */
public class IifFunction implements NamedFunction {

  private static final String NAME = "iif";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    final NonLiteralPath inputPath = input.getInput();
    checkUserInput(input.getArguments().size() == 3,
        "3 arguments must be supplied to iif function");
    final FhirPath condition = input.getArguments().get(0);
    checkUserInput(condition instanceof BooleanPath,
        "Condition argument to iif must be Boolean: " + condition.getExpression());
    checkUserInput(condition.isSingular(),
        "Condition argument to iif must be singular: " + condition.getExpression());
    final BooleanPath conditionBoolean = (BooleanPath) condition;
    checkUserInput(conditionBoolean.getThisColumn().isPresent(),
        "Condition argument to iif function must be navigable from collection item (use $this): "
            + conditionBoolean.getExpression());

    // Join the three datasets together and create a value column.
    final FhirPath ifTrue = input.getArguments().get(1);
    final FhirPath otherwise = input.getArguments().get(2);
    final Dataset<Row> dataset = join(input.getContext(),
        Arrays.asList(conditionBoolean, ifTrue, otherwise), JoinType.LEFT_OUTER);
    final Column valueColumn =
        when(conditionBoolean.getValueColumn().equalTo(true), ifTrue.getValueColumn())
            .otherwise(otherwise.getValueColumn());

    // Build a new ElementPath based on the type of the literal `ifTrue` and `otherwise` arguments,
    // and populate it with the dataset and calculated value column.
    final String expression = expressionFromInput(input, NAME);
    return ifTrue.combineWith(otherwise, dataset, expression, inputPath.getIdColumn(),
        inputPath.getEidColumn(), valueColumn, inputPath.isSingular(), inputPath.getThisColumn());
  }

}
