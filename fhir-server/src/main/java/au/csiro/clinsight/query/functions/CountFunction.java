/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.utilities.Strings.md5Short;
import static org.apache.spark.sql.functions.count;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A function for aggregating data based on counting the number of rows within the result.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#count-integer">http://hl7.org/fhirpath/2018Sep/index.html#count-integer</a>
 */
public class CountFunction implements Function {

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    Dataset<Row> prevDataset = inputResult.getDataset();
    String prevColumn = inputResult.getDatasetColumn();
    String hash = md5Short(input.getExpression());

    // Create new ID and value columns, based on the hash computed off the FHIRPath expression.
    Column idColumn = prevDataset.col(prevColumn + "_id").alias(hash + "_id");
    Column column = inputResult.isResource()
        ? prevDataset.col(prevColumn).alias(hash + "id")
        : prevDataset.col(prevColumn).alias(hash);
    Dataset<Row> dataset = prevDataset.select(idColumn, column);
    Column aggregation = count(column);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.INTEGER);
    result.setFhirType(FhirType.UNSIGNED_INT);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(dataset);
    result.setDatasetColumn(hash);
    result.setAggregation(aggregation);

    return result;
  }

  private void validateInput(FunctionInput input) {
    if (!input.getArguments().isEmpty()) {
      throw new InvalidRequestException(
          "Arguments can not be passed to count function: " + input.getExpression());
    }
    assert !input.getContext().getGroupings().isEmpty() : "Count function called with no groupings";
  }

}
