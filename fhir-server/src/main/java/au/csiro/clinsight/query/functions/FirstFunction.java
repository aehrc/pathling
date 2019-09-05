/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.utilities.Strings.md5Short;
import static org.apache.spark.sql.functions.first;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;

/**
 * This function allows the selection of only the first element of a collection.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#first-collection">http://hl7.org/fhirpath/2018Sep/index.html#first-collection</a>
 */
public class FirstFunction implements Function {

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    Dataset<Row> prevDataset = inputResult.getDataset();
    String prevColumn = inputResult.getDatasetColumn();
    String hash = md5Short(input.getExpression());

    // First apply a grouping based upon the resource ID.
    RelationalGroupedDataset grouped = prevDataset.groupBy(prevColumn);

    // Apply the aggregate Spark SQL function to the grouping.
    Dataset<Row> dataset = grouped.agg(first(prevDataset.col(prevColumn)));

    // Create new ID and value columns, based on the hash computed off the FHIRPath expression.
    Column idColumn = dataset.col(prevColumn + "_id").alias(hash + "_id");
    Column column = dataset.col(dataset.columns()[1]).alias(hash);
    dataset = dataset.select(idColumn, column);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(inputResult.getFhirPathType());
    result.setFhirType(inputResult.getFhirType());
    result.setPrimitive(inputResult.isPrimitive());
    result.setSingular(true);
    result.setDataset(dataset);
    result.setDatasetColumn(hash);

    return result;
  }

  private void validateInput(FunctionInput input) {
    if (!input.getArguments().isEmpty()) {
      throw new InvalidRequestException(
          "Arguments can not be passed to first function: " + input.getExpression());
    }
  }

}
