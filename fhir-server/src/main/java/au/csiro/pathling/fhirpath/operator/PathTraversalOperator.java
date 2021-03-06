/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import java.util.Optional;
import javax.annotation.Nonnull;
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

    // If the input path is a ResourcePath, we look for a bare column. Otherwise, we will need to
    // extract it from a struct.
    final Column field;
    if (left instanceof ResourcePath) {
      final ResourcePath resourcePath = (ResourcePath) left;
      // When the value column of the ResourcePath is null, the path traversal results in null. This
      // can happen when attempting to do a path traversal on the result of a function like when.
      field = when(resourcePath.getValueColumn().isNull(), lit(null))
          .otherwise(resourcePath.getElementColumn(right));
    } else {
      field = left.getValueColumn().getField(right);
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
      resultDataset = left.explodeArray(leftDataset, field, valueAndEidColumns);
      valueColumn = valueAndEidColumns.getLeft();
      eidColumnCandidate = Optional.of(valueAndEidColumns.getRight());
    }

    final Optional<Column> eidColumn = resultSingular
                                       ? Optional.empty()
                                       : eidColumnCandidate;
    return ElementPath.build(expression, resultDataset, left.getIdColumn(), eidColumn, valueColumn,
        resultSingular, left.getForeignResource(), left.getThisColumn(), childDefinition);
  }

}
