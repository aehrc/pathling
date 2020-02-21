/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import au.csiro.pathling.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A function for accessing elements of resources which refer to the input resource. The path to the
 * referring element is supplied as an argument.
 *
 * @author John Grimes
 * @see <a href="https://pathling.app/docs/fhirpath/functions.html#reverseresolve">reverseResolve</a>
 */
public class ReverseResolveFunction implements Function {

  private static boolean referenceRefersToType(ParsedExpression reference,
      ResourceType resourceType) {
    Set<ResourceType> referenceTypes = reference.getReferenceResourceTypes();
    return referenceTypes.contains(ResourceType.RESOURCE) || referenceTypes.contains(resourceType);
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput(),
        argument = input.getArguments().get(0);
    Dataset<Row> argumentDataset = argument.getDataset(),
        inputDataset = inputResult.getDataset();
    Column argumentValueCol = argument.getValueColumn(),
        inputIdCol = inputResult.getIdColumn(),
        resourceCol = argument.getOrigin().getValueColumn();

    // Create a new dataset by joining from the argument to the input dataset.
    Dataset<Row> dataset = inputDataset
        .join(argumentDataset, inputIdCol.equalTo(argumentValueCol.getField("reference")),
            "left_outer");

    // Construct a new parse result.
    ResourceType originResourceType = argument.getOrigin().getResourceType();
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setResource(true);
    result.setResourceType(originResourceType);
    result.setFhirType(FHIRDefinedType.fromCode(originResourceType.toCode()));
    result.setDataset(dataset);
    result.setHashedValue(inputIdCol, resourceCol);

    return result;
  }

  private void validateInput(FunctionInput input) {
    ParsedExpression inputResult = input.getInput();
    if (!inputResult.isResource()) {
      throw new InvalidRequestException(
          "Input to reverseResolve function must be Resource: " + inputResult.getFhirPath());
    }
    if (input.getArguments().size() == 1) {
      ParsedExpression argument = input.getArguments().get(0);
      if (argument.getFhirType() != FHIRDefinedType.REFERENCE) {
        throw new InvalidRequestException(
            "Argument to reverseResolve function must be Reference: " + argument.getFhirPath());
      }
      if (!referenceRefersToType(argument, inputResult.getResourceType())) {
        throw new InvalidRequestException(
            "Reference in argument to reverseResolve does not support input resource type: " + input
                .getExpression());
      }
    } else {
      throw new InvalidRequestException(
          "reverseResolve function accepts one argument: " + input.getExpression());
    }
  }
}
