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

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.QueryHelpers.createColumn;
import static au.csiro.pathling.QueryHelpers.flattenValue;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.definition.ChoiceElementDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ChoiceElementPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Provides the ability to move from one element to its child element, using the path selection
 * notation ".".
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/2018Sep/index.html#path-selection">Path selection</a>
 */
public class PathTraversalOperator {

  /**
   * Invokes this operator with the specified inputs.
   *
   * @param input A {@link PathTraversalInput} object
   * @return A {@link FhirPath} object representing the resulting expression
   */
  @Nonnull
  public NonLiteralPath invoke(@Nonnull final PathTraversalInput input) {
    checkUserInput(input.getLeft() instanceof NonLiteralPath,
        "Path traversal operator cannot be invoked on a literal value: " + input.getLeft()
            .getExpression());
    final NonLiteralPath left = (NonLiteralPath) input.getLeft();
    final String right = input.getRight();
    final String expression = buildExpression(input, left, right);

    final Optional<ElementDefinition> optionalChild = left.getChildElement(right);
    checkUserInput(optionalChild.isPresent(), "No such child: " + expression);
    final ElementDefinition childDefinition = optionalChild.get();

    final boolean maxCardinalityOfOne = childDefinition.getMaxCardinality().isPresent()
        && childDefinition.getMaxCardinality().get() == 1;
    final boolean resultSingular = left.isSingular() && maxCardinalityOfOne;

    final Dataset<Row> leftDataset = left.getDataset();
    final Dataset<Row> result;
    final Column valueColumn;
    if (left instanceof ResourcePath) {
      result = leftDataset;
      valueColumn = ((ResourcePath) left).getElementColumn(right).orElse(left.getIdColumn());
    } else {
      final DatasetWithColumn datasetAndColumn = createColumn(leftDataset,
          buildTraversalColumn(left, right));
      result = datasetAndColumn.getDataset();
      valueColumn = datasetAndColumn.getColumn();
    }

    if (childDefinition instanceof ChoiceElementDefinition) {
      return ChoiceElementPath.build(expression, left, result, valueColumn, Optional.empty(),
          resultSingular, (ChoiceElementDefinition) childDefinition);
    } else {
      return ElementPath.build(expression, result, left.getIdColumn(), valueColumn,
          Optional.empty(), resultSingular, left.getCurrentResource(), left.getThisColumn(),
          childDefinition);
    }
  }

  @Nonnull
  private static String buildExpression(final @Nonnull PathTraversalInput input,
      @Nonnull final NonLiteralPath left, @Nonnull final String right) {
    // If the input expression is the same as the input context, the child will be the start of the
    // expression. This is to account for where we omit the expression that represents the input
    // expression, e.g. "gender" instead of "Patient.gender".
    final String inputContextExpression = input.getContext().getInputContext().getExpression();
    return left.getExpression().equals(inputContextExpression)
           ? right
           : left.getExpression() + "." + right;
  }

  @Nonnull
  public static Column buildTraversalColumn(@Nonnull final NonLiteralPath left,
      @Nonnull final String right) {
    final Column flattenedValue = flattenValue(left.getDataset(), left.getValueColumn());
    return flattenedValue.getField(right);
  }

}
