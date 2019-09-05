/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.REFERENCE;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.BASE_RESOURCE_URL_PREFIX;
import static au.csiro.clinsight.utilities.Strings.md5Short;

import au.csiro.clinsight.fhir.definitions.ElementDefinition;
import au.csiro.clinsight.fhir.definitions.PathTraversal;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A function for resolving a Reference element in order to access the elements of the target
 * resource. Supports polymorphic references through the use of an argument specifying the target
 * resource type.
 *
 * @author John Grimes
 */
public class ResolveFunction implements Function {

  private static boolean referenceIsPolymorphic(ParsedExpression reference) {
    PathTraversal pathTraversal = reference.getPathTraversal();
    List<String> referenceTypes = pathTraversal.getElementDefinition().getReferenceTypes();
    assert referenceTypes.size() != 0 : "Encountered reference with no types";
    return referenceTypes.size() > 1 || referenceTypes.get(0)
        .equals(BASE_RESOURCE_URL_PREFIX + "/Resource");
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    Dataset<Row> inputDataset = inputResult.getDataset().alias("input");
    Dataset<Row> argumentDataset = null;
    String argumentCol = null;
    String resourceDefinition = null;
    String hash = md5Short(input.getExpression());

    // Work out the target resource dataset that we will need to join to.
    if (referenceIsPolymorphic(inputResult)) {
      // If an argument has been provided, we can get the dataset straight out of the argument parse
      // result.
      ParsedExpression argument = input.getArguments().get(0);
      argumentDataset = argument.getDataset();
      argumentCol = argument.getDatasetColumn();
      resourceDefinition = argument.getResourceDefinition();
    } else {
      // If an argument has not been provided, we have to work out which table to get the dataset
      // from based upon the reference types within the element definition.
      PathTraversal pathTraversal = inputResult.getPathTraversal();
      ElementDefinition elementDefinition = pathTraversal.getElementDefinition();
      String referenceType = elementDefinition.getReferenceTypes().get(0);
      resourceDefinition = referenceType;
      if (referenceType.contains(BASE_RESOURCE_URL_PREFIX)) {
        String resourceName = referenceType.replaceFirst(BASE_RESOURCE_URL_PREFIX, "");
        argumentCol = md5Short(resourceName);
        argumentDataset = input.getContext().getResourceReader().read(resourceName);
        argumentDataset = argumentDataset.withColumnRenamed("id", argumentCol + "_id");
      } else {
        assert false : "Non-base resource reference encountered: " + referenceType;
      }
    }
    argumentDataset = argumentDataset.alias("argument");

    // Create a new dataset by joining to the target resource dataset.
    String inputIdColName = inputResult.getDatasetColumn() + "_id";
    Column referenceCol = inputDataset.col(inputResult.getDatasetColumn());
    Column argumentIdCol = argumentDataset.col(argumentCol + "_id");
    Dataset<Row> dataset = inputDataset
        .join(argumentDataset, referenceCol.equalTo(argumentIdCol), "left_outer");
    dataset = dataset.select(inputIdColName, "argument.*");
    dataset = dataset.withColumnRenamed(inputIdColName, hash + "_id");

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setResource(true);
    result.setResourceDefinition(resourceDefinition);

    result.setDataset(dataset);
    result.setDatasetColumn(hash);
    return result;
  }

  private void validateInput(FunctionInput input) {
    ParsedExpression inputResult = input.getInput();
    PathTraversal pathTraversal = inputResult.getPathTraversal();
    if (pathTraversal.getType() != REFERENCE) {
      throw new InvalidRequestException(
          "Input to resolve function must be a Reference: " + inputResult.getFhirPath());
    }
    // Input is polymorphic is there is more than one reference type, or if the only reference type
    // is a reference to Resource (which implies an "any" reference).
    if (referenceIsPolymorphic(inputResult)) {
      ParsedExpression argument = input.getArguments().get(0);
      if (input.getArguments().size() != 1 || !argument.isResource()) {
        // If the reference is polymorphic and an argument has not been provided, throw an error.
        throw new InvalidRequestException(
            "resolve function requires an argument of type Resource when input is polymorphic: "
                + argument.getFhirPath());
      }
    } else {
      if (input.getArguments().size() > 0) {
        // If the reference is monomorphic and an argument has been provided, throw an error.
        throw new InvalidRequestException(
            "Argument provided to resolve function when input is a monomorphic reference: "
                + inputResult.getFhirPath());
      }
    }
  }

}
