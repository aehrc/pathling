/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.*;
import static au.csiro.clinsight.utilities.Strings.md5Short;
import static org.apache.spark.sql.functions.max;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;

/**
 * A function for aggregating data based on finding the maximum value within the input set.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#in-membership">http://hl7.org/fhirpath/2018Sep/index.html#in-membership</a>
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#contains-containership">http://hl7.org/fhirpath/2018Sep/index.html#contains-containership</a>
 */
public class MaxFunction implements Function {

  private static final Set<FhirPathType> supportedTypes = new HashSet<FhirPathType>() {{
    add(STRING);
    add(INTEGER);
    add(DECIMAL);
    add(DATE_TIME);
    add(TIME);
  }};

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    Dataset<Row> prevDataset = inputResult.getDataset();
    String prevColumn = inputResult.getDatasetColumn();
    String hash = md5Short(input.getExpression());

    // First apply the groupings from the expression parser context to the previous dataset.
    String firstGrouping = input.getContext().getGroupings().get(0).getDatasetColumn();
    String[] remainingGroupings = (String[]) input.getContext().getGroupings().stream()
        .skip(1)
        .map(ParsedExpression::getDatasetColumn)
        .toArray();
    RelationalGroupedDataset grouped = prevDataset.groupBy(firstGrouping, remainingGroupings);
    Dataset<Row> dataset = grouped.agg(max(prevDataset.col(prevColumn)));

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
          "Arguments can not be passed to max function: " + input.getExpression());
    }

    ParsedExpression inputResult = input.getInput();
    if (!supportedTypes.contains(inputResult.getFhirPathType())) {
      throw new InvalidRequestException(
          "Input to max function is of unsupported type: " + inputResult.getFhirPath());
    }

    assert !input.getContext().getGroupings().isEmpty() : "Max function called with no groupings";
  }

}
