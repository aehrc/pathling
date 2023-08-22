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
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.MixedCollection;
import au.csiro.pathling.fhirpath.collection.PrimitivePath;
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
  public String getName() {
    return NAME;
  }

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final FunctionInput input) {
    final Collection inputCollection = input.getInput();
    final boolean choiceElement = inputCollection instanceof MixedCollection;
    checkUserInput(choiceElement,
        "Input to ofType function must be a mixed collection");
    checkUserInput(input.getArguments().size() == 1,
        "ofType function must have one argument");

    final MixedCollection mixedCollection = (MixedCollection) inputCollection;
    final Collection argument = input.getArguments().get(0).apply(inputCollection);

    // If the input is a choice element, check that the argument is a type specifier.
    checkUserInput(argument.getType().isPresent() && FhirPathType.TYPE_SPECIFIER.equals(
            argument.getType().get()),
        "Argument to ofType function must be a type specifier");
    final Optional<ElementDefinition> maybeDefinition = mixedCollection.resolveChoiceDefinition(
        type);
    checkUserInput(maybeDefinition.isPresent(),
        "Choice element does not have a child element with name " + type + ": "
            + mixedCollection.getExpression());
    final ElementDefinition definition = maybeDefinition.get();
    final Column valueColumn = checkPresent(
        mixedCollection.resolveChoice(type, mixedCollection.getExpression()));
    final DatasetWithColumn datasetWithColumn = createColumn(mixedCollection.getDataset(),
        valueColumn);

    return PrimitivePath.build(expression, datasetWithColumn.getDataset(),
        mixedCollection.getIdColumn(),
        datasetWithColumn.getColumn(), mixedCollection.getOrderingColumn(),
        mixedCollection.isSingular(),
        mixedCollection.getCurrentResource(), mixedCollection.getThisColumn(), definition);
  }

}
