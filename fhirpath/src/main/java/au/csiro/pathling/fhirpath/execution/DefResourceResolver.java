package au.csiro.pathling.fhirpath.execution;

import static java.util.Objects.isNull;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.definition.DefinitionContext;
import au.csiro.pathling.fhirpath.definition.ResourceTag;
import au.csiro.pathling.io.source.DataSource;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;


@Value(staticConstructor = "of")
public class DefResourceResolver implements ResourceResolver {

  @Nonnull
  ResourceTag subjectResource;

  @Nonnull
  DefinitionContext definitionContext;

  @Nonnull
  Dataset<Row> subjectDataset;

  @Override
  public @Nonnull ResourceCollection resolveResource(
      @Nonnull final ResourceType resourceType) {
    throw new UnsupportedOperationException("resolveForeignResource() is not supported");
  }

  @Override
  @Nonnull
  public ResourceCollection resolveSubjectResource() {
    return createResource(getSubjectResource());
  }

  @Nonnull
  ResourceCollection resolveForeignResource(@Nonnull final ResourceType resourceType) {
    throw new UnsupportedOperationException("resolveForeignResource() is not supported");
  }

  @Override
  @Nonnull
  public Collection resolveJoin(@Nonnull final ReferenceCollection referenceCollection) {
    throw new UnsupportedOperationException("resolveJoin() is not supported");
  }

  @Override
  @Nonnull
  public ResourceCollection resolveReverseJoin(
      @Nonnull final ResourceCollection parentResource, @Nonnull final String expression) {
    throw new UnsupportedOperationException("resolveReverseJoin() is not supported");
  }


  @Nonnull
  ResourceCollection createResource(@Nonnull final ResourceTag resourceType) {
    return ResourceCollection.build(
        new DefaultRepresentation(functions.col(resourceType.toCode())),
        definitionContext.findResourceDefinition(resourceType.toCode()));
  }

  @Nonnull
  public Dataset<Row> createView() {
    return subjectDataset.select(
        subjectDataset.col("id"),
        subjectDataset.col("id_versioned").alias("key"),
        functions.struct(
            Stream.of(subjectDataset.columns())
                .map(subjectDataset::col).toArray(Column[]::new)
        ).alias(subjectResource.toCode()));
  }
  
}
