package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.isNull;


public abstract class BaseResourceResolver implements ResourceResolver {

  @Nonnull
  public abstract ResourceType getSubjectResource();

  @Nonnull
  public abstract FhirContext getFhirContext();

  @Override
  public @Nonnull Optional<ResourceCollection> resolveResource(
      @Nonnull final String resourceCode) {
    if (resourceCode.equals(getSubjectResource())) {
      return Optional.of(resolveSubjectResource());
    } else {
      return resolveForeignResource(resourceCode);
    }
  }

  @Override
  @Nonnull
  public ResourceCollection resolveSubjectResource() {
    return createResource(getSubjectResource());
  }

  @Nonnull
  Optional<ResourceCollection> resolveForeignResource(@Nonnull final String resourceCode) {
    return Optional.empty();
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
  protected ResourceCollection createResource(@Nonnull final ResourceType resourceType) {
    return ResourceCollection.build(
        new DefaultRepresentation(functions.col(resourceType.toCode())),
        getFhirContext(), resourceType);
  }

  @Nonnull
  public Dataset<Row> createView() {
    throw new UnsupportedOperationException("createView() is not supported");
  }

  @Nonnull
  protected static Dataset<Row> resourceDataset(@Nonnull final DataSource dataSource,
      @Nonnull final ResourceType resourceType) {

    @Nullable final Dataset<Row> dataset = dataSource.read(resourceType);
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
