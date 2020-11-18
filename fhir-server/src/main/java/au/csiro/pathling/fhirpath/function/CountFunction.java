/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.aliasColumn;
import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * A function for aggregating data based on counting the number of rows within the result.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#count">count</a>
 */
public class CountFunction extends AggregateFunction implements NamedFunction {

  private static final String NAME = "count";

  protected CountFunction() {
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkNoArguments("count", input);
    final NonLiteralPath inputPath = input.getInput();
    final String expression = expressionFromInput(input, NAME);
    final Column inputIdColumn = inputPath.getIdColumn();
    final Column inputValueColumn = inputPath.getValueColumn();
    final Optional<List<Column>> groupingColumns = input.getContext().getGroupingColumns();

    final Dataset<Row> dataset;

    // If the input is a ResourcePath, we need to make sure that we are counting distinct resources
    // from the dataset.
    if (inputPath instanceof ResourcePath) {
      final WindowSpec window;
      if (groupingColumns.isEmpty() || groupingColumns.get().isEmpty()) {
        window = Window.partitionBy(inputIdColumn).orderBy(inputIdColumn);
      } else {
        final List<Column> partitionBy = new ArrayList<>();
        partitionBy.add(inputIdColumn);
        partitionBy.addAll(groupingColumns.get());
        final Column[] partitionByArray = partitionBy.toArray(new Column[0]);
        window = Window.partitionBy(partitionByArray).orderBy(partitionByArray);
      }
      final DatasetWithColumn datasetWithColumn = aliasColumn(inputPath.getDataset(),
          row_number().over(window).equalTo(1));
      dataset = datasetWithColumn.getDataset().filter(datasetWithColumn.getColumn());
    } else {
      dataset = inputPath.getDataset();
    }

    // Create a column representing the count, taking into account that an empty collection should
    // produce a count of zero.
    final Optional<WindowSpec> window = getWindowSpec(input.getContext());
    final Column countColumn = columnOver(count(inputValueColumn), window);
    final Column valueColumn = when(countColumn.isNull(), 0L).otherwise(countColumn);

    return buildResult(dataset, window, inputPath, valueColumn, expression,
        FHIRDefinedType.UNSIGNEDINT);
  }

}
