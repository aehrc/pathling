package au.csiro.pathling.library.query;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.extract.ExtractQueryExecutor;
import au.csiro.pathling.extract.ExtractRequest;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.DataSource;
import au.csiro.pathling.query.ImmutableInMemoryDataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;


/**
 * The class representing a client that can execute complex queries such as extract or aggregate on
 * a {@link DataSource}.
 */
@Value
public class PathlingClient {

  @Nonnull
  FhirContext fhirContext;

  @Nonnull
  SparkSession sparkSession;

  @Nonnull
  DataSource dataSource;

  @Nonnull
  QueryConfiguration configuration;

  @Nonnull
  Optional<TerminologyServiceFactory> terminologyClientFactory;

  public static class Builder {

    @Nonnull
    final FhirContext fhirContext;
    @Nonnull
    final SparkSession sparkSession;
    @Nonnull
    final TerminologyServiceFactory terminologyClientFactory;

    @Nonnull
    QueryConfiguration queryConfiguration = QueryConfiguration.builder().build();

    @Nullable
    ImmutableInMemoryDataSource.Builder inMemoryDataSourceBuilder;

    @Nullable
    StorageConfiguration storageConfiguration;

    private Builder(@Nonnull final FhirContext fhirContext,
        @Nonnull final SparkSession sparkSession,
        @Nonnull final TerminologyServiceFactory terminologyClientFactory) {
      this.fhirContext = fhirContext;
      this.sparkSession = sparkSession;
      this.terminologyClientFactory = terminologyClientFactory;
    }

    @Nonnull
    public Builder withQueryConfiguration(@Nonnull final QueryConfiguration queryConfiguration) {
      this.queryConfiguration = queryConfiguration;
      return this;
    }

    @Nonnull
    public Builder withResource(@Nonnull final ResourceType resourceType,
        @Nonnull final Dataset<Row> dataset) {
      getInMemoryDataSourceBuilder().withResource(resourceType, dataset);
      return this;
    }

    @Nonnull
    public PathlingClient build() {
      return new PathlingClient(fhirContext, sparkSession, buildDataSource(), queryConfiguration,
          Optional.of(terminologyClientFactory));
    }

    @Nonnull
    public ExtractQuery buildExtractQuery(@Nonnull final ResourceType resourceType) {
      return this.build().newExtractQuery(resourceType);
    }

    @Nonnull
    private DataSource buildDataSource() {
      if (inMemoryDataSourceBuilder != null) {
        return inMemoryDataSourceBuilder.build();
      } else {
        throw new IllegalStateException("DataSource has not been defined");
      }
    }

    @Nonnull
    private ImmutableInMemoryDataSource.Builder getInMemoryDataSourceBuilder() {
      if (inMemoryDataSourceBuilder == null) {
        inMemoryDataSourceBuilder = new ImmutableInMemoryDataSource.Builder();
      }
      return inMemoryDataSourceBuilder;
    }
  }
  
  /**
   * Creates a new extract query bound to this client with given subject resource type.
   *
   * @param subjectResourceType the type of subject resource for the query.
   * @return the new instance of extract query.
   */
  @Nonnull
  public ExtractQuery newExtractQuery(@Nonnull final ResourceType subjectResourceType) {
    return ExtractQuery.of(subjectResourceType).withClient(this);
  }
  
  /**
   * Creates a builder for pathling client.
   * @param pathlingContext the context to build the client for.
   * @return the new instance of the pathling client builder.
   */
  @Nonnull
  public static Builder builder(@Nonnull final PathlingContext pathlingContext) {
    return new Builder(pathlingContext.getFhirContext(), pathlingContext.getSpark(),
        pathlingContext.getTerminologyServiceFactory());
  }

  @Nonnull
  Dataset<Row> execute(@Nonnull final ExtractRequest extractRequest) {
    return new ExtractQueryExecutor(
        configuration,
        fhirContext,
        sparkSession,
        dataSource,
        terminologyClientFactory
    ).buildQuery(extractRequest);
  }
}
