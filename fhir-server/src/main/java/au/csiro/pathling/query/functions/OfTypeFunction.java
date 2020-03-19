/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import au.csiro.pathling.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A function filters items in the input collection to only those that are of the given type.
 *
 * @author John Grimes
 * @see <a href="https://pathling.app/docs/fhirpath/functions.html#oftype">ofType</a>
 */
public class OfTypeFunction implements Function {

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);

    ParsedExpression inputResult = input.getInput();
    Dataset<Row> inputDataset = inputResult.getDataset();
    Column inputValueCol = inputResult.getValueColumn();
    Column resourceTypeColumn = inputResult.getResourceTypeColumn();

    ParsedExpression argumentResult = input.getArguments().get(0);
    ResourceType resourceType = argumentResult.getResourceType();
    Dataset<Row> argumentDataset = argumentResult.getDataset();
    Column argumentIdCol = argumentResult.getIdColumn();

    // Join from the filtered input dataset to the argument dataset.
    Column resourceTypeMatches = resourceTypeColumn.equalTo(resourceType.toCode());
    Dataset<Row> dataset = inputDataset
        .join(argumentDataset, resourceTypeMatches.and(inputValueCol.equalTo(argumentIdCol)),
            "left_outer");
    Column idColumn = inputResult.getIdColumn();
    Column valueColumn = argumentResult.getValueColumn();

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setSingular(inputResult.isSingular());
    result.setDataset(dataset);
    result.setResource(true);
    result.setResourceType(resourceType);
    result.setFhirType(FHIRDefinedType.fromCode(resourceType.toCode()));
    result.setHashedValue(idColumn, valueColumn);

    return result;
  }

  private void validateInput(FunctionInput input) {
    if (!input.getInput().isPolymorphic()) {
      throw new InvalidRequestException(
          "Input to ofType function must be polymorphic resource expression");
    }
    if (input.getArguments().size() != 1) {
      throw new InvalidRequestException("ofType function must be provided with a single argument");
    }
    if (!input.getArguments().get(0).isResource()) {
      throw new InvalidRequestException("Argument to ofType function must be a resource type");
    }
  }

}
