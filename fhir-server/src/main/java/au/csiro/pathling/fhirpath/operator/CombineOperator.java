/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.QueryHelpers.createColumn;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import java.util.Arrays;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Merges the left and right operands into a single collection.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#combine">combine</a>
 */
public class CombineOperator implements Operator {

  private static final String NAME = "combine";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final OperatorInput input) {
    final String expression = Operator.buildExpression(input, NAME);
    final FhirPath left = input.getLeft();
    final FhirPath right = input.getRight();
    final ParserContext context = input.getContext();

    final Dataset<Row> leftTrimmed = left.trimDataset(context);
    final Dataset<Row> rightTrimmed = right.trimDataset(context);
    final int valueColumnIndex = Arrays.asList(leftTrimmed.columns())
        .indexOf(left.getValueColumn().toString());

    final Dataset<Row> dataset = leftTrimmed.union(rightTrimmed);
    final String columnName = dataset.columns()[valueColumnIndex];
    final DatasetWithColumn datasetWithColumn = createColumn(dataset,
        dataset.col("`" + columnName + "`"));
    final Optional<Column> eidColumn = Optional.of(array(monotonically_increasing_id()));
    final Optional<Column> thisColumn = left instanceof NonLiteralPath
                                        ? ((NonLiteralPath) left).getThisColumn()
                                        : Optional.empty();
    return left
        .combineWith(right, datasetWithColumn.getDataset(), expression, left.getIdColumn(),
            eidColumn, datasetWithColumn.getColumn(), false, thisColumn);
  }

}
