package au.csiro.pathling.io;

import static au.csiro.pathling.io.PersistenceScheme.getTableUrl;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.desc;

import au.csiro.pathling.caching.Cacheable;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import io.delta.tables.DeltaTable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

@Component
@Profile("(core | import) & !ga4gh")
@Slf4j
public class DatabaseComponent extends Database implements Cacheable {
  
  @Nonnull
  @Getter
  private Optional<String> cacheKey;

  @Nonnull
  protected final ThreadPoolTaskExecutor executor;

  /**
   * @param configuration a {@link StorageConfiguration} object which controls the behaviour of the
   * database
   * @param spark a {@link SparkSession} for interacting with Spark
   * @param fhirEncoders {@link FhirEncoders} object for creating empty datasets
   * @param executor a {@link ThreadPoolTaskExecutor} for executing asynchronous tasks
   */
  public DatabaseComponent(@Nonnull final StorageConfiguration configuration,
      @Nonnull final SparkSession spark, @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final ThreadPoolTaskExecutor executor) {
    super(configuration, spark, fhirEncoders);
    this.executor = executor;
    this.cacheKey = buildCacheKeyFromDatabase();
  }

  @Override
  public boolean cacheKeyMatches(@Nonnull final String otherKey) {
    return cacheKey.map(key -> key.equals(otherKey)).orElse(false);
  }

  @Override
  protected void onDataChange(@Nonnull final ResourceType resourceType,
      @Nonnull final Optional<String> maybeTableUrl) {
    final String tableUrl = maybeTableUrl.orElse(
        getTableUrl(warehouseUrl, databaseName, resourceType));
    invalidateCache(tableUrl);
    compact(resourceType, tableUrl);
  }

  private void invalidateCache(final String tableUrl) {
    executor.execute(() -> {
      cacheKey = buildCacheKeyFromTable(tableUrl);
      spark.sqlContext().clearCache();
    });
  }

  @Nonnull
  private Optional<String> buildCacheKeyFromTable(@Nonnull final String path) {
    return latestUpdateToTable(path).map(this::cacheKeyFromTimestamp);
  }
  
  private Optional<String> buildCacheKeyFromDatabase() {
    return latestUpdateToDatabase().map(this::cacheKeyFromTimestamp);
  }

  /**
   * Checks the warehouse location and gets the latest snapshot timestamp found within all the
   * tables.
   */
  private Optional<Long> latestUpdateToDatabase() {
    final String databasePath = warehouseUrl + "/" + databaseName;
    log.info("Querying latest snapshot from database: {}", databasePath);

    @Nullable final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark.sparkContext()
        .hadoopConfiguration();
    requireNonNull(hadoopConfiguration);
    @Nullable final FileSystem warehouse;
    try {
      warehouse = FileSystem.get(new URI(warehouseUrl), hadoopConfiguration);
    } catch (final IOException | URISyntaxException e) {
      log.debug("Unable to access warehouse location, returning empty snapshot time: {}",
          warehouseUrl);
      return Optional.empty();
    }
    requireNonNull(warehouse);
    
    // Check that the database path exists.
    try {
      warehouse.exists(new Path(databasePath));
    } catch (final IOException e) {
      log.debug("Unable to access database location, returning empty snapshot time: {}",
          databasePath);
      return Optional.empty();
    }

    // Find all the Parquet files within the warehouse and use them to create a set of resource
    // types.
    @Nullable final FileStatus[] fileStatuses;
    try {
      fileStatuses = warehouse.listStatus(new Path(databasePath));
    } catch (final IOException e) {
      log.debug("Unable to access database location, returning empty snapshot time: {}",
          databasePath);
      return Optional.empty();
    }
    requireNonNull(fileStatuses);

    final List<Long> timestamps = Arrays.stream(fileStatuses)
        // Get the filename of each item in the directory listing.
        .map(fileStatus -> {
          @Nullable final Path path = fileStatus.getPath();
          requireNonNull(path);
          return path.toString();
        })
        // Filter out any file names that don't match the pattern.
        .filter(path -> path.matches("^[^.]+\\.parquet$"))
        // Filter out anything that is not a Delta table.
        .filter(path -> DeltaTable.isDeltaTable(spark, path))
        // Get the latest history entry for each Delta table.
        .map(this::latestUpdateToTable)
        // Filter out any tables which don't have history rows.
        .filter(Optional::isPresent)
        // Get the timestamp from the history row.
        .map(Optional::get)
        .collect(Collectors.toList());

    return timestamps.isEmpty()
           ? Optional.empty()
           : Optional.ofNullable(Collections.max(timestamps));
  }

  @Nonnull
  private Optional<Long> latestUpdateToTable(@Nonnull final String path) {
    log.debug("Querying latest snapshot for table: {}", path);
    final DeltaTable deltaTable = DeltaTable.forPath(spark, path);
    @SuppressWarnings("RedundantCast") final Row[] head = (Row[]) deltaTable.history()
        .orderBy(desc("version"))
        .select("timestamp")
        .head(1);
    if (head.length != 1) {
      return Optional.empty();
    } else {
      return Optional.of(head[0].getTimestamp(0).getTime());
    }
  }

  @Nonnull
  private String cacheKeyFromTimestamp(@Nonnull final Long timestamp) {
    return Long.toString(timestamp, Character.MAX_RADIX);
  }


  /**
   * Compacts the table if it has a number of partitions that exceed the configured threshold.
   *
   * @param resourceType the resource type of the table to compact
   * @param tableUrl the URL under which the table is stored
   * @see <a href="https://docs.delta.io/latest/best-practices.html#compact-files">Delta Lake
   * Documentation - Compact files</a>
   */
  void compact(final @Nonnull ResourceType resourceType, @Nonnull final String tableUrl) {
    final DeltaTable table = getDeltaTable(resourceType, tableUrl);
    final int threshold = configuration.getCompactionThreshold();
    final int numPartitions = table.toDF().rdd().getNumPartitions();
    if (numPartitions > threshold) {

      log.debug("Scheduling table compaction (number of partitions: {}, threshold: {}): {}",
          numPartitions,
          threshold, tableUrl);
      executor.submit(() -> {
        log.debug("Commencing compaction: {}", tableUrl);
        read(resourceType)
            .repartition()
            .write()
            .option("dataChange", "false")
            .format("delta")
            .mode(SaveMode.Overwrite)
            .save(tableUrl);
        log.debug("Compaction complete: {}", tableUrl);
      });
    } else {
      log.debug("Compaction not needed (number of partitions: {}, threshold: {})", numPartitions,
          threshold);
    }
  }

}
