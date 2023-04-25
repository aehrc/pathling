package au.csiro.pathling.io;

import static au.csiro.pathling.io.FileSystemPersistence.safelyJoinPaths;

import au.csiro.pathling.caching.Cacheable;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * The database implementation used by Pathling Server.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Component
@Profile("(core | import) & !ga4gh")
@Slf4j
public class ServerDatabase extends Database implements Cacheable {

  protected ServerPersistence persistence;

  @Nonnull
  protected final ThreadPoolTaskExecutor executor;

  /**
   * @param configuration a {@link StorageConfiguration} object which controls the behaviour of the
   * database
   * @param spark a {@link SparkSession} for interacting with Spark
   * @param fhirEncoders {@link FhirEncoders} object for creating empty datasets
   * @param executor a {@link ThreadPoolTaskExecutor} for executing asynchronous tasks
   */
  public ServerDatabase(@Nonnull final StorageConfiguration configuration,
      @Nonnull final SparkSession spark, @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final ThreadPoolTaskExecutor executor) {
    super(spark, fhirEncoders, new ServerPersistence(spark,
        safelyJoinPaths(configuration.getWarehouseUrl(), configuration.getDatabaseName()), executor,
        configuration.getCompactionThreshold()), configuration.getCacheDatasets());
    this.executor = executor;
  }

  @Override
  public Optional<String> getCacheKey() {
    return persistence.getCacheKey();
  }

  @Override
  public boolean cacheKeyMatches(@Nonnull final String otherKey) {
    return persistence.cacheKeyMatches(otherKey);
  }

}
