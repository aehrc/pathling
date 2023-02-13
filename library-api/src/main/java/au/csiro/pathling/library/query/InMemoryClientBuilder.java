package au.csiro.pathling.library.query;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.DataSource;
import au.csiro.pathling.query.ImmutableInMemoryDataSource;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * The {@link PathlingClient} builder that binds the client to an {@link
 * ImmutableInMemoryDataSource}
 */
public class InMemoryClientBuilder extends AbstractClientBuilder<InMemoryClientBuilder> {

  @Nonnull
  private final ImmutableInMemoryDataSource.Builder inMemoryDataSourceBuilder;

  protected InMemoryClientBuilder(@Nonnull final PathlingContext pathlingContext) {
    super(pathlingContext);
    this.inMemoryDataSourceBuilder = ImmutableInMemoryDataSource.builder();
  }

  /**
   * Registers a dataset as the source of data the specified resource type.
   *
   * @param resourceType the type of the resource.
   * @param dataset the dataset to use as the source of data.
   * @return this builder.
   */
  @Nonnull
  public InMemoryClientBuilder withResource(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> dataset) {
    inMemoryDataSourceBuilder.withResource(resourceType, dataset);
    return this;
  }

  @Nonnull
  @Override
  protected DataSource buildDataSource() {
    return inMemoryDataSourceBuilder.build();
  }
}
