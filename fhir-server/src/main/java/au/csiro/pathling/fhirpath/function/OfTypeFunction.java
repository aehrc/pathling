/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A function filters items in the input collection to only those that are of the given type.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#oftype">ofType</a>
 */
public class OfTypeFunction implements NamedFunction {

  private static final String NAME = "ofType";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    final String expression = NamedFunction.expressionFromInput(input, NAME);
    checkUserInput(input.getInput() instanceof UntypedResourcePath,
        "Input to ofType function must be a polymorphic resource type: " + input.getInput()
            .getExpression());
    checkUserInput(input.getArguments().size() == 1,
        "ofType function must have one argument: " + expression);
    final UntypedResourcePath inputPath = (UntypedResourcePath) input.getInput();
    final FhirPath argumentPath = input.getArguments().get(0);

    // If the input is a polymorphic resource reference, check that the argument is a resource 
    // type.
    checkUserInput(argumentPath instanceof ResourcePath,
        "Argument to ofType function must be a resource type: " + argumentPath.getExpression());
    final ResourcePath resourcePath = (ResourcePath) argumentPath;

    // Do a left outer join to the resource dataset using the reference in the untyped dataset - the
    // result will be null in the rows that are not of the resource type nominated.
    final Column joinCondition = inputPath.getResourceEquality(resourcePath);
    final Dataset<Row> dataset = join(inputPath.getDataset(), resourcePath.getDataset(),
        joinCondition, JoinType.LEFT_OUTER);

    // Return a new resource path with the joined dataset, and the argument's value column.
    final Optional<Column> thisColumn = inputPath.getThisColumn();
    final ResourcePath result = resourcePath.copy(expression, dataset, inputPath.getIdColumn(),
        inputPath.getEidColumn(), resourcePath.getValueColumn(), inputPath.isSingular(),
        thisColumn);
    result.setCurrentResource(resourcePath);
    return result;
  }

}
