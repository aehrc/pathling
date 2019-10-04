/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.REFERENCE;
import static au.csiro.clinsight.utilities.Strings.md5Short;

import au.csiro.clinsight.fhir.definitions.PathTraversal;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A function for accessing elements of resources which refer to the input resource. The path to the
 * referring element is supplied as an argument.
 *
 * @author John Grimes
 */
public class ReverseResolveFunction implements Function {

  private static boolean referenceRefersToType(ParsedExpression reference,
      ResourceType resourceType) {
    PathTraversal pathTraversal = reference.getPathTraversal();
    Set<ResourceType> referenceTypes = pathTraversal.getElementDefinition().getReferenceTypes();
    return referenceTypes.contains(ResourceType.RESOURCE) || referenceTypes.contains(resourceType);
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    ParsedExpression argument = input.getArguments().get(0);
    String hash = md5Short(input.getExpression());

    // Create a new dataset by joining from the argument to the input dataset.
    Dataset<Row> argumentDataset = argument.getDataset();
    Dataset<Row> resourceDataset = argument.getOrigin().getDataset().alias("resource");
    Dataset<Row> inputDataset = inputResult.getDataset();
    Column argumentCol = argumentDataset.col(argument.getDatasetColumn());
    Column argumentIdCol = argumentDataset.col(argument.getDatasetColumn() + "_id");
    String inputIdColName = inputResult.getDatasetColumn() + "_id";
    Column inputIdCol = inputDataset.col(inputIdColName);
    Column resourceIdCol = resourceDataset.col(argument.getOrigin().getDatasetColumn() + "_id");
    Dataset<Row> dataset = inputDataset
        .join(argumentDataset, inputIdCol.equalTo(argumentCol), "left_outer")
        .join(resourceDataset, argumentIdCol.equalTo(resourceIdCol), "left_outer");
    dataset = dataset.select(inputIdColName, "resource.*");
    dataset = dataset.withColumnRenamed(inputIdColName, hash + "_id");

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setResource(true);
    result.setResourceType(argument.getOrigin().getResourceType());

    result.setDataset(dataset);
    result.setDatasetColumn(hash);
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
      if (argument.getPathTraversal().getType() != REFERENCE) {
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
