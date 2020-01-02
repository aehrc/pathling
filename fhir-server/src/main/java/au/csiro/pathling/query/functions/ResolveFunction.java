/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.query.ResourceReader;
import au.csiro.pathling.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
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
    ParsedExpression inputResult = input.getInput();
    Dataset<Row> inputDataset = inputResult.getDataset();
    ResourceReader resourceReader = input.getContext().getResourceReader();
    Column inputIdCol = inputResult.getIdColumn();
    Column referenceCol = inputResult.getValueColumn();

    // Get the allowed types for the input reference. This gives us the set of possible resource
    // types that this reference could resolve to.
    Set<ResourceType> referenceTypes = inputResult.getReferenceResourceTypes();
    // If the type is Resource, all resource types need to be looked at.
    if (referenceTypes.contains(ResourceType.RESOURCE)) {
      referenceTypes = resourceReader.getAvailableResourceTypes();
    }
    assert referenceTypes.size() > 0 : "Encountered reference with no types";
    boolean isPolymorphic = referenceTypes.size() > 1;

    Dataset<Row> targetDataset;
    Column targetIdCol, targetValueCol, targetTypeCol;
    if (isPolymorphic) {
      // If this is a polymorphic reference, create a dataset for each reference type, and union
      // them together to produce the target dataset. The dataset will not contain the resources
      // themselves, only a type and identifier for later resolution.
      List<Dataset<Row>> referenceTypeDatasets = new ArrayList<>();
      for (ResourceType referenceType : referenceTypes) {
        if (resourceReader.getAvailableResourceTypes().contains(referenceType)) {
          Dataset<Row> referenceTypeDataset = resourceReader.read(referenceType);
          targetIdCol = referenceTypeDataset.col("id");
          targetTypeCol = lit(referenceType.toCode());
          referenceTypeDataset = referenceTypeDataset.select(targetIdCol, targetTypeCol);
          referenceTypeDatasets.add(referenceTypeDataset);
        }
      }
      targetDataset = referenceTypeDatasets.stream()
          .reduce(Dataset::union)
          .orElse(null);
      assert targetDataset != null;
      targetIdCol = targetDataset.col(targetDataset.columns()[0]);
      targetValueCol = targetDataset.col(targetDataset.columns()[1]);
    } else {
      // If this is a monomorphic reference, we just need to retrieve the appropriate table and
      // create a dataset with the full resources.
      targetDataset = resourceReader.read(
          (ResourceType) referenceTypes.toArray()[0]);
      targetIdCol = targetDataset.col("id");
      targetValueCol = org.apache.spark.sql.functions.struct(targetDataset.col("*"));
    }

    // Create a new dataset by joining to the target resource dataset.
    Column equality = referenceCol.getField("reference").equalTo(targetIdCol);
    Dataset<Row> dataset = inputDataset.join(targetDataset, equality, "left_outer");

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setSingular(inputResult.isSingular());
    result.setDataset(dataset);

    // Resolution of polymorphic references are flagged, and don't carry information about the
    // resource type and path traversal.
    if (isPolymorphic) {
      result.setHashedValue(inputIdCol, targetIdCol);
      result.setPolymorphic(true);
      result.setResourceTypeColumn(targetValueCol);
    } else {
      result.setHashedValue(inputIdCol, targetValueCol);
      result.setResource(true);
      String resourceCode = ((ResourceType) referenceTypes.toArray()[0]).toCode();
      result.setResourceType(ResourceType.fromCode(resourceCode));
      result.setFhirType(FHIRDefinedType.fromCode(resourceCode));
    }

    return result;
  }

  private void validateInput(FunctionInput input) {
    ParsedExpression inputResult = input.getInput();
    if (!inputResult.getFhirType().equals(FHIRDefinedType.REFERENCE)) {
      throw new InvalidRequestException(
          "Input to resolve function must be a Reference: " + inputResult.getFhirPath());
    }
    if (!input.getArguments().isEmpty()) {
      throw new InvalidRequestException("resolve function does not accept arguments");
    }
  }

}
