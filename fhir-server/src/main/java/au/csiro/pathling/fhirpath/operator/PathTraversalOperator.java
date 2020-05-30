/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.QueryHelpers.hashColumn;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.explode_outer;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
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
  public ElementPath invoke(@Nonnull final PathTraversalInput input) {
    final FhirPath left = input.getLeft();
    final String right = input.getRight();

    // If the input expression is empty, the child will be the start of the expression. This is to 
    // account for where we omit the expression that represents the input expression, e.g. "gender" 
    // instead of "Patient.gender".
    final String expression = left.getExpression().isEmpty()
                              ? right
                              : left.getExpression() + "." + right;
    final Optional<ElementDefinition> optionalChild = left.getChildElement(right);
    checkUserInput(optionalChild.isPresent(), "No such child: " + expression);
    final ElementDefinition childDefinition = optionalChild.get();

    final Column leftValueColumn = left.getValueColumn();
    final Dataset<Row> leftDataset = left.getDataset();

    // If the element has a max cardinality of more than one, it will need to be "exploded" out into 
    // multiple rows.
    final Column field = leftValueColumn.getField(right);
    final boolean maxCardinalityOfOne = childDefinition.getMaxCardinality() == 1;
    Column valueColumn = maxCardinalityOfOne
                         ? field
                         : explode_outer(field);

    final Dataset<Row> dataset;
    if (maxCardinalityOfOne) {
      dataset = leftDataset;
    } else {
      // If we are exploding a field, we need to explicitly make a new column out of it. Row
      // generators can not be nested inside expressions. 
      // 
      // Also, the column needs to be hashed for the same reason we do this when we join - otherwise 
      // subsequent explosions will make it impossible to refer to in downstream aggregations.
      final DatasetWithColumn datasetWithColumn = hashColumn(leftDataset, valueColumn, "exploded");
      dataset = datasetWithColumn.getDataset();
      valueColumn = datasetWithColumn.getColumn();
    }

    final boolean singular = left.isSingular() && maxCardinalityOfOne;

    return ElementPath.build(left, expression, dataset, valueColumn, singular, childDefinition);
  }

}
