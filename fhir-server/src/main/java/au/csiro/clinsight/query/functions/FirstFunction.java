/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static org.apache.spark.sql.functions.first;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * This function allows the selection of only the first element of a collection.
 *
 * @author John Grimes
 * @see <a href=
 *      "http://hl7.org/fhirpath/2018Sep/index.html#first-collection">http://hl7.org/fhirpath/2018Sep/index.html#first-collection</a>
 */
public class FirstFunction implements Function {

	@Nonnull
	@Override
	public ParsedExpression invoke(@Nonnull FunctionInput input) {
		validateInput(input);
		ParsedExpression inputResult = input.getInput();
		ParsedExpression result = new ParsedExpression(inputResult);
		result.setFhirPath(input.getExpression());
		if (!inputResult.isResource()) {
			// Does not have any effect on resources
			Dataset<Row> prevDataset = inputResult.getDataset();
			// Apply the aggregate Spark SQL function to the grouping.
			Column prevIdColumn = inputResult.getIdColumn();
			Column prevValueColumn = inputResult.getValueColumn();

			// First apply a grouping based upon the resource ID.
			Dataset<Row> dataset = prevDataset.groupBy(prevIdColumn).agg(first(prevValueColumn));
			Column valueColumn = dataset.col(dataset.columns()[1]);
			// Construct a new parse result.
			// Should preserve most of the metadata such as types, definitions, etc.
			result.setSingular(true);
			result.setDataset(dataset);
			result.setHashedValue(prevIdColumn, valueColumn);
		}
		return result;
	}

	private void validateInput(FunctionInput input) {
		if (!input.getArguments().isEmpty()) {
			throw new InvalidRequestException("Arguments can not be passed to first function: " + input.getExpression());
		}
	}

}
