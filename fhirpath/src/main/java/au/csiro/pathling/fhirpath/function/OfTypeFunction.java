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

import static au.csiro.pathling.QueryHelpers.createColumn;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import au.csiro.pathling.fhirpath.element.ChoiceElementPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

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
    final NonLiteralPath inputPath = input.getInput();
    final boolean untypedResource = inputPath instanceof UntypedResourcePath;
    final boolean choiceElement = inputPath instanceof ChoiceElementPath;
    checkUserInput(untypedResource || choiceElement,
        "Input to ofType function must be a polymorphic resource type or choice element path: "
            + inputPath.getExpression());
    checkUserInput(input.getArguments().size() == 1,
        "ofType function must have one argument: " + expression);

    return untypedResource
           ? resolveUntypedResource(input, expression)
           : resolveChoiceElement(input, expression);
  }

  @Nonnull
  private static FhirPath resolveUntypedResource(final @Nonnull NamedFunctionInput input,
      final String expression) {
    final UntypedResourcePath inputPath = (UntypedResourcePath) input.getInput();
    final FhirPath argumentPath = input.getArguments().get(0);

    // If the input is a polymorphic resource reference, check that the argument is a resource 
    // type.
    checkUserInput(argumentPath instanceof ResourcePath,
        "Argument to ofType function must be a resource type: " + argumentPath.getExpression());
    final ResourcePath resourcePath = (ResourcePath) argumentPath;

    return ResolveFunction.resolveMonomorphicReference(inputPath,
        input.getContext().getDataSource(), input.getContext().getFhirContext(),
        resourcePath.getResourceType(), expression, input.getContext());
  }

  @Nonnull
  private FhirPath resolveChoiceElement(@Nonnull final NamedFunctionInput input,
      @Nonnull final String expression) {
    final ChoiceElementPath inputPath = (ChoiceElementPath) input.getInput();
    final FhirPath argumentPath = input.getArguments().get(0);

    // If the input is a choice element, check that the argument is a type specifier.
    checkUserInput(argumentPath instanceof TypeSpecifier,
        "Argument to ofType function must be a type specifier: " + argumentPath.getExpression());
    final TypeSpecifier typeSpecifier = (TypeSpecifier) argumentPath;
    final String type = typeSpecifier.getExpression();
    final Optional<ElementDefinition> maybeDefinition = inputPath.resolveChoice(type);
    checkUserInput(maybeDefinition.isPresent(),
        "Choice element does not have a child element with name " + type + ": "
            + inputPath.getExpression());
    final ElementDefinition definition = maybeDefinition.get();
    final Column valueColumn = checkPresent(inputPath.resolveChoiceColumn(type));
    final DatasetWithColumn datasetWithColumn = createColumn(inputPath.getDataset(),
        valueColumn);

    return ElementPath.build(expression, datasetWithColumn.getDataset(), inputPath.getIdColumn(),
        datasetWithColumn.getColumn(), inputPath.getOrderingColumn(), inputPath.isSingular(),
        inputPath.getCurrentResource(), inputPath.getThisColumn(), definition);
  }

}
