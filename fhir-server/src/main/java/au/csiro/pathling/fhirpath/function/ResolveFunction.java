/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.QueryHelpers.union;
import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import au.csiro.pathling.fhirpath.element.ReferencePath;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
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
  private FhirPath resolveMonomorphicReference(@Nonnull final ReferencePath referencePath,
      @Nonnull final ResourceReader resourceReader, @Nonnull final FhirContext fhirContext,
      @Nonnull final Collection<ResourceType> referenceTypes, final String expression) {
    // If this is a monomorphic reference, we just need to retrieve the appropriate table and
    // create a dataset with the full resources.
    final ResourceType resourceType = (ResourceType) referenceTypes.toArray()[0];
    final ResourcePath resourcePath = ResourcePath
        .build(fhirContext, resourceReader, resourceType, expression, referencePath.isSingular());

    // Join the resource dataset to the reference dataset.
    final Column resourceIdColumn = resourcePath.getIdColumn();
    final Column referenceColumn = referencePath.getReferenceColumn();
    final Dataset<Row> dataset = join(referencePath.getDataset(), referenceColumn,
        resourcePath.getDataset(), resourceIdColumn, JoinType.LEFT_OUTER);

    final Column inputId = referencePath.getIdColumn();
    final Optional<Column> inputEid = referencePath.getEidColumn();

    return resourcePath.copy(expression, dataset, inputId, inputEid, resourcePath.getValueColumn(),
        referencePath.isSingular(), referencePath.getThisColumn());
  }

  @Nonnull
  private static FhirPath resolvePolymorphicReference(@Nonnull final ReferencePath referencePath,
      @Nonnull final ResourceReader resourceReader, @Nonnull final Set<ResourceType> referenceTypes,
      final String expression) {
    // If this is a polymorphic reference, create a dataset for each reference type, and union
    // them together to produce the target dataset. The dataset will not contain the resources
    // themselves, only a type and identifier for later resolution.
    final Collection<Dataset<Row>> typeDatasets = new ArrayList<>();
    for (final ResourceType referenceType : referenceTypes) {
      if (resourceReader.getAvailableResourceTypes().contains(referenceType)) {
        // Unfortunately we can't include the full content of the resource, as Spark won't tolerate
        // the structure of two rows in the same dataset being different.
        final Dataset<Row> typeDatasetWithColumns = resourceReader.read(referenceType);
        final Column idColumn = typeDatasetWithColumns.col("id");
        Dataset<Row> typeDataset = typeDatasetWithColumns
            .withColumn("type", lit(referenceType.toCode()));
        typeDataset = typeDataset.select(idColumn, typeDataset.col("type"));

        typeDatasets.add(typeDataset);
      }
    }
    checkUserInput(!typeDatasets.isEmpty(),
        "No types within reference are available, cannot resolve: " + referencePath
            .getExpression());
    final String idColumnName = randomAlias();
    final String targetColumnName = randomAlias();
    Dataset<Row> targetDataset = union(typeDatasets);
    Column targetId = targetDataset.col(targetDataset.columns()[0]);
    Column targetType = targetDataset.col(targetDataset.columns()[1]);
    targetDataset = targetDataset
        .withColumn(idColumnName, targetId)
        .withColumn(targetColumnName, targetType);
    targetId = targetDataset.col(idColumnName);
    targetType = targetDataset.col(targetColumnName);
    targetDataset = targetDataset.select(targetId, targetType);

    checkNotNull(targetId);
    final Column referenceColumn = referencePath.getReferenceColumn();
    final Dataset<Row> dataset = join(referencePath.getDataset(), referenceColumn,
        targetDataset, targetId, JoinType.LEFT_OUTER);

    final Column inputId = referencePath.getIdColumn();
    final Optional<Column> inputEid = referencePath.getEidColumn();
    return UntypedResourcePath
        .build(referencePath, expression, dataset, inputId, inputEid, targetType, referenceTypes);
  }

}
