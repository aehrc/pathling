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
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ReferenceNestingKey;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.definition.BasicElementDefinition;
import au.csiro.pathling.fhirpath.collection.ReferencePath;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A function for accessing elements of resources which refer to the input resource. The path to the
 * referring element is supplied as an argument.
 *
 * @author John Grimes
 * @see <a
 * href="https://pathling.csiro.au/docs/fhirpath/functions.html#reverseresolve">reverseResolve</a>
 */
public class ReverseResolveFunction implements NamedFunction {

  private static final String NAME = "reverseResolve";

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final NamedFunctionInput input) {
    checkUserInput(input.getInput() instanceof ResourceCollection,
        "Input to " + NAME + " function must be a resource: " + input.getInput().getExpression());
    final ResourceCollection inputPath = (ResourceCollection) input.getInput();
    final String expression = NamedFunction.expressionFromInput(input, NAME, input.getInput());
    checkUserInput(input.getArguments().size() == 1,
        "reverseResolve function accepts a single argument: " + expression);
    final Collection argument = input.getArguments().get(0);
    checkUserInput(argument instanceof ReferencePath,
        "Argument to reverseResolve function must be a Reference: " + argument.getExpression());

    // Check the argument for information about the current resource that it originated from - if it
    // is not present, reverse reference resolution will not be possible.
    final NonLiteralPath nonLiteralArgument = (NonLiteralPath) argument;
    checkUserInput(nonLiteralArgument.getCurrentResource().isPresent(),
        "Argument to reverseResolve must be an element that is navigable from a "
            + "target resource type: " + expression);
    final ResourceCollection currentResource = nonLiteralArgument.getCurrentResource().get();

    final ReferencePath referencePath = (ReferencePath) argument;
    final BasicElementDefinition referenceDefinition = checkPresent(referencePath.getDefinition());
    final ReferenceNestingKey referenceNestingKey = new ReferenceNestingKey(referenceDefinition,
        currentResource.getDefinition());

    return input.getContext().getNesting()
        .updateOrRetrieve(referenceNestingKey, expression, inputPath.getDataset(), false,
            inputPath.getThisColumn(), key -> {
              // Check that the input type is one of the possible types specified by the argument.
              final Set<ResourceType> argumentTypes = ((ReferencePath) argument).getResourceTypes();
              final ResourceType inputType = inputPath.getResourceType();
              checkUserInput(argumentTypes.contains(inputType),
                  "Reference in argument to reverseResolve does not support input resource type: "
                      + expression);

              // Do a left outer join from the input to the argument dataset using the reference field in 
              // the argument.
              final Column joinCondition = referencePath.getResourceEquality(inputPath);
              final Dataset<Row> dataset = join(inputPath.getDataset(), referencePath.getDataset(),
                  joinCondition, JoinType.LEFT_OUTER);

              final ResourceCollection result = currentResource.copy(expression, dataset,
                  inputPath.getIdColumn(),
                  currentResource.getValueColumn(), currentResource.getOrderingColumn(), false,
                  inputPath.getThisColumn());
              result.setCurrentResource(currentResource);
              return result;
            });
  }
}
