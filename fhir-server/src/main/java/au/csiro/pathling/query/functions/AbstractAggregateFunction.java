/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import au.csiro.pathling.query.parsing.ParsedExpression;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * This class contains functionality common to all functions which can be used as aggregate
 * functions.
 *
 * @author Piotr Szul
 */
public abstract class AbstractAggregateFunction implements Function {

  protected final String functionName;

  protected AbstractAggregateFunction(String functionName) {
    super();
    this.functionName = functionName;
  }

  /**
   * The non-aggregate invocation of an aggregate function is simply the result of running the
   * aggregation immediately, and updating the `dataset`, `idColumn` and `valueColumn` fields for
   * consumption by downstream invocations.
   */
  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression result = aggregate(input);

    Dataset<Row> aggDataset = result.getAggregationDataset();
    // Apply the aggregate Spark SQL function to the grouping.
    Column aggIdColumn = result.getAggregationIdColumn();
    Column aggColumn = result.getAggregationColumn();

    // First apply a grouping based upon the resource ID.
    Dataset<Row> dataset = aggDataset.groupBy(aggIdColumn).agg(aggColumn);
    Column idColumn = dataset.col(dataset.columns()[0]);
    Column valueColumn = dataset.col(dataset.columns()[1]);
    result.setDataset(dataset);
    result.setHashedValue(idColumn, valueColumn);
    return result;
  }

  @Nonnull
  protected abstract ParsedExpression aggregate(@Nonnull FunctionInput input);

  protected abstract void validateInput(FunctionInput input);

  /**
   * This method applies a Spark Column function to a function input, returning a ParsedExpression.
   * This can be used for implementing functions that correlate one-to-one with Spark functions, and
   * do not require complex logic.
   */
  protected ParsedExpression wrapSparkFunction(@Nonnull FunctionInput input,
      java.util.function.Function<Column, Column> aggFunction, boolean copyInputContext) {
    // Construct a new parse result.
    // Should preserve most of the metadata such as types, definitions, etc.
    ParsedExpression inputResult = input.getInput();
    Column prevValueColumn = inputResult.getValueColumn();
    Column prevIdColumn = inputResult.getIdColumn();

    ParsedExpression result =
        copyInputContext
        ? new ParsedExpression(inputResult)
        : new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setSingular(true);
    result.setDataset(null);
    result.setIdColumn(null);
    result.setValueColumn(null);
    result.setAggregationDataset(inputResult.getDataset());
    result.setAggregationIdColumn(prevIdColumn);
    result.setAggregationColumn(aggFunction.apply(prevValueColumn));
    return result;
  }

}
