/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.ID_COLUMN_SUFFIX;
import static au.csiro.pathling.QueryHelpers.joinOnColumns;
import static au.csiro.pathling.QueryHelpers.joinOnId;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static au.csiro.pathling.utilities.Strings.randomShortString;

import au.csiro.pathling.QueryHelpers.DatasetWithColumns;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Describes a function which can scope down the previous invocation within a FHIRPath expression,
 * based upon an expression passed in as an argument. Supports the use of `$this` to reference the
 * element currently in scope.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#where">where</a>
 */
public class WhereFunction implements NamedFunction {

  private static final String NAME = "where";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkUserInput(input.getArguments().size() == 1,
        "where function accepts one argument");
    final NonLiteralPath inputPath = input.getInput();
    checkUserInput(input.getArguments().get(0) instanceof NonLiteralPath,
        "Argument to where function cannot be a literal: " + input.getArguments().get(0)
            .getExpression());
    final NonLiteralPath argumentPath = (NonLiteralPath) input.getArguments().get(0);

    checkUserInput(argumentPath instanceof BooleanPath && argumentPath.isSingular(),
       "Argument to where function must be a singular Boolean: " + argumentPath.getExpression());

    checkUserInput(argumentPath.getThisColumn().isPresent(),
        "Argument to where function must be navigable from collection item (use $this): "
            + argumentPath.getExpression());

    // The result is the input value if it is equal to true, or null otherwise (signifying the
    // absence of a value).
    final Column argumentTrue = argumentPath.getValueColumn().equalTo(true);
    final Column thisColumn = argumentPath.getThisColumn().get();
    final Column thisValue = thisColumn.getField("value");
    final Column thisEid = thisColumn.getField("eid");

    final Dataset<Row> dataset;
    final Optional<Column> idColumn;
    final List<Column> groupingColumns = input.getContext().getGroupingColumns();
    final Dataset<Row> argumentDataset = argumentPath.getDataset();

    if (!groupingColumns.isEmpty()) {
      final Dataset<Row> distinctIds = argumentDataset
          .select(groupingColumns.toArray(new Column[]{})).distinct();
      final DatasetWithColumns datasetWithColumns = joinOnColumns(distinctIds, groupingColumns,
          argumentDataset, groupingColumns, Optional.of(argumentTrue), JoinType.LEFT_OUTER);
      dataset = datasetWithColumns.getDataset();
      input.getContext().setGroupingColumns(datasetWithColumns.getColumns());
      idColumn = Optional.empty();
    } else {
      final Column argumentId = checkPresent(argumentPath.getIdColumn());
      final String hash = randomShortString();
      final String idColumnName = hash + ID_COLUMN_SUFFIX;
      Dataset<Row> distinctIds = argumentDataset.withColumn(idColumnName, argumentId);
      final Column distinctIdCol = distinctIds.col(idColumnName);
      distinctIds = distinctIds.select(distinctIdCol).distinct();
      dataset = joinOnId(distinctIds, distinctIdCol, argumentPath, Optional.of(argumentTrue),
          JoinType.LEFT_OUTER);
      idColumn = Optional.of(distinctIdCol);
    }

    final String expression = expressionFromInput(input, NAME);
    return inputPath
        .copy(expression, dataset, idColumn, inputPath.getEidColumn().map(c -> thisEid), thisValue,
            inputPath.isSingular(),
            Optional.empty());
  }

}
