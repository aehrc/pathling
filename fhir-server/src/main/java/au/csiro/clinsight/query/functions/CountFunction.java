/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * A function for aggregating data based on counting the number of rows within the result.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#count-integer">http://hl7.org/fhirpath/2018Sep/index.html#count-integer</a>
 */
public class CountFunction implements Function {
  // TODO: Make count function work outside the context of an aggregation, e.g. name.given.count() = 3.

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    Dataset<Row> dataset = inputResult.getDataset();
    Column idColumn = inputResult.getIdColumn();
    Column valueColumn = inputResult.getValueColumn();

    // Create new ID and value columns, based on the hash computed off the FHIRPath expression.
    Column aggregationColumn = inputResult.isResource()
        ? functions.countDistinct(valueColumn)
        : functions.countDistinct(idColumn, valueColumn);

    // If the count is to be based upon an element, filter out any nulls so that they aren't
    // counted.
    if (!inputResult.isResource()) {
      dataset = dataset.where(inputResult.getValueColumn().isNotNull());
    }

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.INTEGER);
    result.setFhirType(FHIRDefinedType.UNSIGNEDINT);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(dataset);
    result.setHashedValue(idColumn, valueColumn);
    result.setAggregationColumn(aggregationColumn);

    return result;
  }

  private void validateInput(FunctionInput input) {
    if (!input.getArguments().isEmpty()) {
      throw new InvalidRequestException(
          "Arguments can not be passed to count function: " + input.getExpression());
    }
  }

}
