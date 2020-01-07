package au.csiro.pathling.query.functions;

import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import au.csiro.pathling.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;

/**
 * Base class for aggreation/selection fucntion based on their SQL counterparts
 *
 * @author Piotr Szul
 */

public abstract class AbstractAggFunction implements Function {

  protected final String functionName;

  protected AbstractAggFunction(String functionName) {
    super();
    this.functionName = functionName;
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression result = invokeAgg(input);

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
  protected abstract ParsedExpression invokeAgg(@Nonnull FunctionInput input);

  protected abstract void validateInput(FunctionInput input);

  // helper functions

  protected ParsedExpression wrapSparkFunction(@Nonnull FunctionInput input,
      java.util.function.Function<Column, Column> aggFunction, boolean copyInputContext) {
    // Construct a new parse result.
    // Should preserve most of the metadata such as types, definitions, etc.
    ParsedExpression inputResult = input.getInput();
    Column prevValueColumn = inputResult.getValueColumn();
    Column prevIdColumn = inputResult.getIdColumn();

    ParsedExpression result =
        copyInputContext ? new ParsedExpression(inputResult) : new ParsedExpression();
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

  protected void validateNoArgumentInput(FunctionInput input) {
    if (!input.getArguments().isEmpty()) {
      throw new InvalidRequestException(
          "Arguments can not be passed to " + functionName + " function: " + input.getExpression());
    }
  }

}
