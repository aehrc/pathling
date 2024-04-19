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

import static au.csiro.pathling.QueryHelpers.createColumns;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.QueryHelpers.DatasetWithColumnMap;
import au.csiro.pathling.encoders.ExtensionSupport;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.commons.lang3.tuple.MutablePair;
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
  public ElementPath invoke(@Nonnull final PathTraversalInput input) {
    checkUserInput(input.getLeft() instanceof NonLiteralPath,
        "Path traversal operator cannot be invoked on a literal value: " + input.getLeft()
            .getExpression());
    final NonLiteralPath left = (NonLiteralPath) input.getLeft();
    final String right = input.getRight();

    // If the input expression is the same as the input context, the child will be the start of the
    // expression. This is to account for where we omit the expression that represents the input
    // expression, e.g. "gender" instead of "Patient.gender".
    final String inputContextExpression = input.getContext().getInputContext().getExpression();
    final String expression = left.getExpression().equals(inputContextExpression)
                              ? right
                              : left.getExpression() + "." + right;

    final Optional<ElementDefinition> optionalChild = left.getChildElement(right);
    checkUserInput(optionalChild.isPresent(), "No such child: " + expression);
    final ElementDefinition childDefinition = optionalChild.get();

    final Dataset<Row> leftDataset = left.getDataset();

    final Column field;
    if (ExtensionSupport.EXTENSION_ELEMENT_NAME().equals(right)) {
      // Lookup the extensions by _fid in the extension container.
      field = left.getExtensionContainerColumn()
          .apply(getValueField(left, ExtensionSupport.FID_FIELD_NAME()));
    } else {
      field = getValueField(left, right);
    }

    // If the element has a max cardinality of more than one, it will need to be "exploded" out into
    // multiple rows.
    final boolean maxCardinalityOfOne = childDefinition.getMaxCardinality() == 1;
    final boolean resultSingular = left.isSingular() && maxCardinalityOfOne;

    final Column valueColumn;
    final Optional<Column> eidColumnCandidate;
    final Dataset<Row> resultDataset;

    if (maxCardinalityOfOne) {
      valueColumn = field;
      eidColumnCandidate = left.getEidColumn();
      resultDataset = leftDataset;
    } else {
      final MutablePair<Column, Column> valueAndEidColumns = new MutablePair<>();
      final Dataset<Row> explodedDataset = left.explodeArray(leftDataset, field,
          valueAndEidColumns);
      final DatasetWithColumnMap datasetWithColumnMap = createColumns(explodedDataset,
          valueAndEidColumns.getLeft(), valueAndEidColumns.getRight());
      resultDataset = datasetWithColumnMap.getDataset();
      valueColumn = datasetWithColumnMap.getColumn(valueAndEidColumns.getLeft());
      eidColumnCandidate = Optional.of(
          datasetWithColumnMap.getColumn(valueAndEidColumns.getRight()));
    }

    final Optional<Column> eidColumn = resultSingular
                                       ? Optional.empty()
                                       : eidColumnCandidate;

    // If there is an element ID column, we need to add it to the parser context so that it can
    // be used within joins in certain situations, e.g. extract.
    eidColumn.ifPresent(c -> input.getContext().getNodeIdColumns().putIfAbsent(expression, c));

    return ElementPath
        .build(expression, resultDataset, left.getIdColumn(), eidColumn, valueColumn,
            resultSingular, left.getCurrentResource(), left.getThisColumn(), childDefinition);
  }

  @Nonnull
  private static Column getValueField(@Nonnull final NonLiteralPath path,
      @Nonnull final String fieldName) {
    // If the input path is a ResourcePath, we look for a bare column. Otherwise, we will need to
    // extract it from a struct.
    final Column field;
    if (path instanceof ResourcePath) {
      final ResourcePath resourcePath = (ResourcePath) path;
      // When the value column of the ResourcePath is null, the path traversal results in null. This
      // can happen when attempting to do a path traversal on the result of a function like when.
      field = when(resourcePath.getValueColumn().isNull(), lit(null))
          .otherwise(resourcePath.getElementColumn(fieldName));
    } else {
      field = path.getValueColumn().getField(fieldName);
    }
    return field;
  }

}
