/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A function filters items in the input collection to only those that are of the given type.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#oftypetype-identifier-collection">http://hl7.org/fhirpath/2018Sep/index.html#oftypetype-identifier-collection</a>
 */
public class OfTypeFunction implements Function {

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ResourceType resourceType = input.getArguments().get(0).getResourceType();
    ParsedExpression inputResult = input.getInput();
    Dataset<Row> inputDataset = inputResult.getDataset();
    Column inputValueCol = inputResult.getValueColumn();
    Column resourceTypeColumn = inputResult.getResourceTypeColumn();

    // Get the dataset representing the set of all resources of the specified type.
    Dataset<Row> targetDataset = input.getContext().getResourceReader().read(resourceType);
    Column targetIdCol = targetDataset.col("id");

    // Join from the filtered input dataset to the target resource dataset.
    Column resourceTypeMatches = resourceTypeColumn.equalTo(resourceType.toCode());
    Dataset<Row> dataset = inputDataset
        .join(targetDataset, resourceTypeMatches.and(inputValueCol.equalTo(targetIdCol)),
            "left_outer");
    Column idColumn = inputResult.getIdColumn();
    Column valueColumn = targetDataset.col("resource");

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setSingular(inputResult.isSingular());
    result.setDataset(dataset);
    result.setResource(true);
    result.setResourceType(resourceType);
    result.setHashedValue(idColumn, valueColumn);

    return result;
  }

  private void validateInput(FunctionInput input) {
    if (!input.getInput().isPolymorphic()) {
      throw new InvalidRequestException(
          "Input to ofType function must be polymorphic resource reference");
    }
    if (input.getArguments().size() != 1) {
      throw new InvalidRequestException("ofType function must be provided with a single argument");
    }
    if (!input.getArguments().get(0).isResource()) {
      throw new InvalidRequestException("Argument to ofType function must be a resource type");
    }
  }
}
