/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

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
    Dataset<Row> dataset = inputResult.getDataset();
    Column column = dataset.col(inputResult.getDatasetColumn());
    Column idColumn = dataset.col(inputResult.getDatasetColumn() + "_id");

    // Create new ID and value columns, based on the hash computed off the FHIRPath expression.
    Column aggregation = inputResult.isResource()
        ? functions.countDistinct(idColumn)
        : functions.countDistinct(idColumn, column);

    // If the count is to be based upon an element, filter out any nulls so that they aren't
    // counted.
    if (!inputResult.isResource()) {
      dataset = dataset.where(column.isNotNull());
    }

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.INTEGER);
    result.setFhirType(FhirType.UNSIGNED_INT);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(dataset);
    result.setDatasetColumn(inputResult.getDatasetColumn());
    result.setAggregation(aggregation);

    return result;
  }

  private void validateInput(FunctionInput input) {
    if (!input.getArguments().isEmpty()) {
      throw new InvalidRequestException(
          "Arguments can not be passed to count function: " + input.getExpression());
    }
  }

}
