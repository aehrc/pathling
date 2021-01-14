/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.QueryHelpers.createColumn;
import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.fhirpath.NonLiteralPath.findEidColumn;
import static au.csiro.pathling.fhirpath.NonLiteralPath.findThisColumn;
import static au.csiro.pathling.fhirpath.operator.Operator.buildExpression;
import static au.csiro.pathling.fhirpath.operator.Operator.checkArgumentsAreComparable;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.Comparable.ComparisonOperation;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Provides the functionality of the family of comparison operators within FHIRPath, i.e. {@code =},
 * {@code !=}, {@code <=}, {@code <}, {@code >}, {@code >=}.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#equality">Equality</a>
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#comparison">Comparison</a>
 */
public class ComparisonOperator implements Operator {

  @Nonnull
  private final ComparisonOperation type;

  /**
   * @param type The type of operator
   */
  public ComparisonOperator(@Nonnull final ComparisonOperation type) {
    this.type = type;
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final OperatorInput input) {
    final FhirPath left = input.getLeft();
    final FhirPath right = input.getRight();
    checkUserInput(left.isSingular(), "Left operand must be singular: " + left.getExpression());
    checkUserInput(right.isSingular(),
        "Right operand must be singular: " + right.getExpression());
    checkArgumentsAreComparable(input, type.toString());

    final String expression = buildExpression(input, type.toString());
    final Dataset<Row> dataset = join(input.getContext(), left, right, JoinType.LEFT_OUTER);

    final Comparable leftComparable = (Comparable) left;
    final Comparable rightComparable = (Comparable) right;
    final Column valueColumn = leftComparable.getComparison(type).apply(rightComparable);
    final Column idColumn = left.getIdColumn();
    final Optional<Column> eidColumn = findEidColumn(left, right);
    final Optional<Column> thisColumn = findThisColumn(left, right);
    final DatasetWithColumn datasetWithColumn = createColumn(dataset, valueColumn);

    return ElementPath.build(expression, datasetWithColumn.getDataset(), idColumn, eidColumn,
        datasetWithColumn.getColumn(), true, Optional.empty(), thisColumn, FHIRDefinedType.BOOLEAN);
  }

}
