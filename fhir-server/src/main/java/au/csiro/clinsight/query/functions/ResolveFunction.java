/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.REFERENCE;
import static au.csiro.clinsight.utilities.Strings.md5Short;
import static org.apache.spark.sql.functions.lit;

import au.csiro.clinsight.fhir.definitions.ElementDefinition;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.PathTraversal;
import au.csiro.clinsight.fhir.definitions.ResourceDefinitions;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A function for resolving a Reference element in order to access the elements of the target
 * resource. Supports polymorphic references through the use of an argument specifying the target
 * resource type.
 *
 * @author John Grimes
 */
public class ResolveFunction implements Function {

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    SparkSession spark = input.getContext().getSparkSession();
    ParsedExpression inputResult = input.getInput();
    Dataset<Row> inputDataset = inputResult.getDataset();
    String hash = md5Short(input.getExpression());
    Column inputIdCol = inputDataset.col(inputResult.getDatasetColumn() + "_id");
    Column referenceCol = inputDataset.col(inputResult.getDatasetColumn());

    // Get the allowed types for the input reference. This gives us the set of possible resource
    // types that this reference could resolve to.
    PathTraversal pathTraversal = inputResult.getPathTraversal();
    ElementDefinition elementDefinition = pathTraversal.getElementDefinition();
    Set<ResourceType> referenceTypes = elementDefinition.getReferenceTypes();
    // If the type is Resource, all resource types need to be looked at.
    if (referenceTypes.contains(ResourceType.RESOURCE)) {
      referenceTypes = ResourceDefinitions.getSupportedResources();
    }
    assert referenceTypes.size() > 0 : "Encountered reference with no types";

    Dataset<Row> targetDataset;
    Column targetIdCol = null, targetCol = null;
    if (referenceTypes.size() == 1) {
      // If this is a monomorphic reference, we just need to retrieve the appropriate table and
      // create a dataset with the full resources.
      targetDataset = input.getContext().getResourceReader().read(
          (ResourceType) referenceTypes.toArray()[0]);
      targetIdCol = targetDataset.col("id");
      targetCol = org.apache.spark.sql.functions.struct(targetDataset.col("*"));
    } else {
      // If this is a polymorphic reference, create a dataset for each reference type, and union
      // them together to produce the target dataset. The dataset will not contain the resources
      // themselves, only a type and identifier for later resolution.
      List<Dataset<Row>> referenceTypeDatasets = new ArrayList<>();
      for (ResourceType referenceType : referenceTypes) {
        Dataset<Row> referenceTypeDataset = input.getContext().getResourceReader()
            .read(referenceType);
        targetIdCol = referenceTypeDataset.col("id");
        Column referenceTypeCol = lit(referenceType.toCode()).alias("type");
        referenceTypeDataset = referenceTypeDataset.select(targetIdCol, referenceTypeCol);
        referenceTypeDatasets.add(referenceTypeDataset);
      }
      targetDataset = referenceTypeDatasets.stream()
          .reduce(Dataset::union)
          .orElse(null);
      assert targetDataset != null;
      targetIdCol = targetDataset.col("id");
      targetCol = targetDataset.col("type");
    }

    // Create a new dataset by joining to the target resource dataset.
    // TODO: Implement support for resolving references based upon identifier.
    Column equality = referenceCol.getField("reference").equalTo(targetIdCol);
    Dataset<Row> dataset = inputDataset.join(targetDataset, equality, "left_outer");
    if (referenceTypes.size() == 1) {
      dataset = dataset.select(inputIdCol.alias(hash + "_id"), targetCol.alias(hash));
    } else {
      dataset = dataset.select(inputIdCol.alias(hash + "_id"), targetCol.alias(hash + "_type"),
          targetIdCol.alias(hash));
    }

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setSingular(inputResult.isSingular());
    result.setDataset(dataset);
    result.setDatasetColumn(hash);

    // Resolution of polymorphic references are flagged, and don't carry information about the
    // resource type and path traversal.
    if (referenceTypes.size() == 1) {
      result.setResource(true);
      result.setResourceType(
          ResourceType.fromCode(((ResourceType) referenceTypes.toArray()[0]).toCode()));
      result.setPathTraversal(PathResolver.resolvePath(result.getResourceType().toCode()));
    } else {
      result.setPolymorphic(true);
    }

    return result;
  }

  private void validateInput(FunctionInput input) {
    ParsedExpression inputResult = input.getInput();
    PathTraversal pathTraversal = inputResult.getPathTraversal();
    if (pathTraversal.getType() != REFERENCE) {
      throw new InvalidRequestException(
          "Input to resolve function must be a Reference: " + inputResult.getFhirPath());
    }
    if (!input.getArguments().isEmpty()) {
      throw new InvalidRequestException("resolve function does not accept arguments");
    }
  }

}
