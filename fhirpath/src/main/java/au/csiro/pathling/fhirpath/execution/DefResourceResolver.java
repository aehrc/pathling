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
    throw new UnsupportedOperationException("createView() is not supported");
  }

  @Nonnull
  static Dataset<Row> resourceDataset(@Nonnull final DataSource dataSource,
      @Nonnull final ResourceTag resourceType) {

    @Nullable final Dataset<Row> dataset = dataSource.read(resourceType.toCode());
    // Despite DataSource.read() annotation, this method can return null sometime
    // which leads to an obscure NullPointerException in SparkSQL
    if (isNull(dataset)) {
      throw new IllegalArgumentException("Resource type not found: " + resourceType);
    }
    return dataset.select(
        dataset.col("id"),
        dataset.col("id_versioned").alias("key"),
        functions.struct(
            Stream.of(dataset.columns())
                .map(dataset::col).toArray(Column[]::new)
        ).alias(resourceType.toCode()));
  }
}
