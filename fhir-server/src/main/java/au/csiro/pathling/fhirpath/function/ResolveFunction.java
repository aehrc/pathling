/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.convertRawResource;
import static au.csiro.pathling.QueryHelpers.joinOnReferenceAndId;
import static au.csiro.pathling.QueryHelpers.union;
import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.QueryHelpers.DatasetWithIdAndValue;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import au.csiro.pathling.fhirpath.element.ReferencePath;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A function for resolving a Reference element in order to access the elements of the target
 * resource. Supports polymorphic references through the use of an argument specifying the target
 * resource type.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#resolve">resolve</a>
 */
public class ResolveFunction implements NamedFunction {

  private static final String NAME = "resolve";

  protected ResolveFunction() {
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkUserInput(input.getInput() instanceof ReferencePath,
        "Input to " + NAME + " function must be a Reference: " + input.getInput().getExpression());
    checkNoArguments(NAME, input);
    final ReferencePath inputPath = (ReferencePath) input.getInput();
    final ResourceReader resourceReader = input.getContext().getResourceReader();

    // Get the allowed types for the input reference. This gives us the set of possible resource
    // types that this reference could resolve to.
    Set<ResourceType> referenceTypes = inputPath.getResourceTypes();
    // If the type is Resource, all resource types need to be looked at.
    if (referenceTypes.contains(ResourceType.RESOURCE)) {
      referenceTypes = resourceReader.getAvailableResourceTypes();
    }
    check(referenceTypes.size() > 0);
    final boolean isPolymorphic = referenceTypes.size() > 1;

    final String expression = expressionFromInput(input, NAME);

    if (isPolymorphic) {
      return resolvePolymorphicReference(inputPath, resourceReader, referenceTypes, expression);
    } else {
      final FhirContext fhirContext = input.getContext().getFhirContext();
      return resolveMonomorphicReference(inputPath, resourceReader, fhirContext, referenceTypes,
          expression);
    }
  }

  @Nonnull
  private FhirPath resolveMonomorphicReference(@Nonnull final FhirPath referencePath,
      @Nonnull final ResourceReader resourceReader, @Nonnull final FhirContext fhirContext,
      @Nonnull final Collection<ResourceType> referenceTypes, final String expression) {
    // If this is a monomorphic reference, we just need to retrieve the appropriate table and
    // create a dataset with the full resources.
    final ResourceType resourceType = (ResourceType) referenceTypes.toArray()[0];
    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(resourceType.toCode());
    final ResourceDefinition definition = new ResourceDefinition(resourceType, hapiDefinition);

    final DatasetWithIdAndValue targetDataset = convertRawResource(
        resourceReader.read(resourceType));
    final Dataset<Row> dataset = joinOnReferenceAndId(referencePath, targetDataset.getDataset(),
        targetDataset.getIdColumn(), JoinType.LEFT_OUTER);

    final Optional<Column> inputId = referencePath.getIdColumn();
    return new ResourcePath(expression, dataset, inputId, targetDataset.getValueColumn(),
        referencePath.isSingular(),
        definition);
  }

  @Nonnull
  private static FhirPath resolvePolymorphicReference(@Nonnull final FhirPath referencePath,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Set<ResourceType> referenceTypes, final String expression) {
    // If this is a polymorphic reference, create a dataset for each reference type, and union
    // them together to produce the target dataset. The dataset will not contain the resources
    // themselves, only a type and identifier for later resolution.
    final Collection<Dataset<Row>> typeDatasets = new ArrayList<>();
    for (final ResourceType referenceType : referenceTypes) {
      if (resourceReader.getAvailableResourceTypes().contains(referenceType)) {
        // Unfortunately we can't include the full content of the resource, as Spark won't tolerate 
        // the structure of two rows in the same dataset being different.
        final DatasetWithIdAndValue typeDatasetWithColumns = convertRawResource(
            resourceReader.read(referenceType));
        Dataset<Row> typeDataset = typeDatasetWithColumns.getDataset()
            .withColumn("type", lit(referenceType.toCode()));
        typeDataset = typeDataset
            .select(typeDatasetWithColumns.getIdColumn(), typeDataset.col("type"));

        typeDatasets.add(typeDataset);
      }
    }
    checkUserInput(!typeDatasets.isEmpty(),
        "No types within reference are available, cannot resolve: " + referencePath
            .getExpression());
    final Dataset<Row> targetDataset = union(typeDatasets);
    final Column targetId = targetDataset.col(targetDataset.columns()[0]);

    checkNotNull(targetId);
    final Column typeColumn = targetDataset.col("type");
    final Column valueColumn = referencePath.getValueColumn();
    final Dataset<Row> dataset = joinOnReferenceAndId(referencePath, targetDataset, targetId,
        JoinType.LEFT_OUTER);

    final Optional<Column> inputId = referencePath.getIdColumn();
    return UntypedResourcePath.build(expression, dataset, inputId, valueColumn,
        referencePath.isSingular(), typeColumn, referenceTypes);
  }

}
