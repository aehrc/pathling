package au.csiro.pathling.library.query;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;

/**
 * The {@link PathlingClient} builder that binds the client to a persistent data storage using
 * {@link Database} as it's data source.
 */
public class DatabaseClientBuilder extends AbstractClientBuilder<DatabaseClientBuilder> {

  @Nullable
  private StorageConfiguration storageConfiguration = null;

  protected DatabaseClientBuilder(@Nonnull final PathlingContext pathlingContext) {
    super(pathlingContext);
  }

  /**
   * Sets the storage configuration for the {@link Database} instance to use with this client.
   *
   * @param storageConfiguration the storage configuration.
   * @return this builder.
   */
  @Nonnull
  public DatabaseClientBuilder withStorageConfiguration(
      @Nonnull final StorageConfiguration storageConfiguration) {
    this.storageConfiguration = storageConfiguration;
    return this;
  }

  @Nonnull
  @Override
  protected DataSource buildDataSource() {
    if (isNull(storageConfiguration)) {
      throw new IllegalStateException("Storage configuration has not been defined");
    }
    return new Database(requireNonNull(storageConfiguration), pathlingContext.getSpark(),
        pathlingContext.getFhirEncoders());
  }

}
