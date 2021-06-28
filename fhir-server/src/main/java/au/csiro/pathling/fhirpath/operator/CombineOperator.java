/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    checkUserInput(left.canBeCombinedWith(right),
        "Input and argument to combine function are not compatible");

    final List<Dataset<Row>> trimmed = new ArrayList<>();
    for (final FhirPath operand : Arrays.asList(left, right)) {
      final List<Column> columns = new ArrayList<>(
          Arrays.asList(operand.getIdColumn(), operand.getValueColumn()));
      if (operand instanceof NonLiteralPath) {
        final NonLiteralPath nonLiteralLeft = (NonLiteralPath) operand;
        nonLiteralLeft.getThisColumn().ifPresent(columns::add);
      }
      input.getContext().getGroupingColumns().ifPresent(columns::addAll);
      trimmed.add(operand.getDataset().select(columns.toArray(new Column[]{})));
    }
    final Dataset<Row> leftTrimmed = trimmed.get(0);
    final Dataset<Row> rightTrimmed = trimmed.get(1);

    final Dataset<Row> dataset = leftTrimmed.union(rightTrimmed);
    final Optional<Column> thisColumn = left instanceof NonLiteralPath
                                        ? ((NonLiteralPath) left).getThisColumn()
                                        : Optional.empty();
    return left
        .mergeWith(right, dataset, expression, left.getIdColumn(),
            Optional.empty(), left.getValueColumn(), false, thisColumn);
  }

}
