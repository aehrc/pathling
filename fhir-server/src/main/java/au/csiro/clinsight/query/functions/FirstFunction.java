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
	public ParsedExpression invokeAgg(@Nonnull FunctionInput input) {
		validateInput(input);
		// Construct a new parse result.
		// Should preserve most of the metadata such as types, definitions, etc.		
		ParsedExpression inputResult = input.getInput();
		Column prevValueColumn = inputResult.getValueColumn();
		Column prevIdColumn = inputResult.getIdColumn();
		
		ParsedExpression result = new ParsedExpression(inputResult);
		result.setFhirPath(input.getExpression());
		result.setSingular(true);
		result.setDataset(null);
		result.setIdColumn(null);
		result.setValueColumn(null);
		result.setAggregationDataset(inputResult.getDataset());
		result.setAggregationIdColumn(prevIdColumn);
		result.setAggregationColumn(first(prevValueColumn));
		return result;
	}
	
	private void validateInput(FunctionInput input) {
		if (!input.getArguments().isEmpty()) {
			throw new InvalidRequestException("Arguments can not be passed to first function: " + input.getExpression());
		}
	}

}
