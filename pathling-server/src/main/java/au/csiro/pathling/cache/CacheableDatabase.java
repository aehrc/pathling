package au.csiro.pathling.cache;

import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.desc;

import au.csiro.pathling.library.io.FileSystemPersistence;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * A cache-aware implementation layer between the {@link au.csiro.pathling.library.io.sink.DataSink} implementations.
 * 
 * It works on the assumption that all changes happen through the official interactions so it's always logged.
 * 
 * @author Felix Naumann
 */
@Component
@Slf4j
public class CacheableDatabase implements Cacheable {

  @Nonnull
  private final ThreadPoolTaskExecutor executor;
  private final SparkSession spark;
  
  private final String databasePath;
  
  @Nonnull
  @Getter
  private Optional<String> cacheKey;

  @Autowired
  public CacheableDatabase(SparkSession spark, @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}") String databasePath, @Nonnull final ThreadPoolTaskExecutor executor) {
    this.spark = spark;
    this.databasePath = convertS3ToS3aUrl(databasePath);
    this.cacheKey = buildCacheKeyFromStorage();
    this.executor = executor;
  }

  /**
   * Generates a new cache key based upon the latest update time across all resource types within
   * the database.
   *
   * @return the cache key, or empty if no update time could be determined
   */
  @Nonnull
  private Optional<String> buildCacheKeyFromStorage() {
    return latestUpdate().map(this::cacheKeyFromTimestamp);
  }

  /**
   * Determines the latest snapshot time from all resource tables within the database.
   *
   * @return the latest snapshot time, or empty if no snapshot time could be determined
   */
  @Nonnull
  private Optional<Long> latestUpdate() {
    log.info("Querying latest snapshot from database: {}", databasePath);

    @Nullable final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark.sparkContext()
        .hadoopConfiguration();
    requireNonNull(hadoopConfiguration);
    @Nullable final FileSystem warehouse;
    try {
      warehouse = FileSystem.get(new URI(databasePath), hadoopConfiguration);
    } catch (final IOException | URISyntaxException e) {
      log.debug("Unable to access warehouse location, returning empty snapshot time: {}",
          databasePath);
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
        .map(path -> DeltaTable.forPath(spark, path))
        // Get the latest history entry for each Delta table.
        .map(CacheableDatabase::latestUpdateToTable)
        // Filter out any tables which don't have history rows.
        .filter(Optional::isPresent)
        // Get the timestamp from the history row.
        .map(Optional::get)
        .toList();

    return timestamps.isEmpty()
           ? Optional.empty()
           : Optional.ofNullable(Collections.max(timestamps));
  }

  /**
   * Queries a Delta table for the latest update time.
   *
   * @param deltaTable the Delta table to query
   * @return the latest update time, or empty if no update time could be determined
   */
  @Nonnull
  private static Optional<Long> latestUpdateToTable(@Nonnull final DeltaTable deltaTable) {
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

  /**
   * @param s3Url The S3 URL that should be converted
   * @return A S3A URL
   */
  @Nonnull
  public static String convertS3ToS3aUrl(@Nonnull final String s3Url) {
    return s3Url.replaceFirst("s3:", "s3a:");
  }
  
  /**
   * Converts a timestamp to a string cache key.
   *
   * @param timestamp the timestamp to convert
   * @return the cache key
   */
  @Nonnull
  private String cacheKeyFromTimestamp(@Nonnull final Long timestamp) {
    return Long.toString(timestamp, Character.MAX_RADIX);
  }
  
  @Override
  public boolean cacheKeyMatches(@Nonnull final String otherKey) {
    return cacheKey.map(key -> key.equals(otherKey)).orElse(false);
  }

  /**
   * Generates a new cache key based upon the latest update time of the specified table.
   *
   * @param table the table to generate the cache key for
   * @return the cache key, or empty if no update time could be determined
   */
  @Nonnull
  private Optional<String> buildCacheKeyFromTable(@Nonnull final DeltaTable table) {
    return latestUpdateToTable(table).map(this::cacheKeyFromTimestamp);
  }

  /**
   * @param path the URL of the warehouse location
   * @param resourceType the resource type to be read or written to
   * @return the URL of the resource within the warehouse
   */
  @Nonnull
  protected static String getTableUrl(@Nonnull final String path,
      @Nonnull final String resourceType) {
    return FileSystemPersistence.safelyJoinPaths(path, fileNameForResource(resourceType));
  }

  /**
   * @param resourceType A HAPI {@link ResourceType} describing the type of resource
   * @return The filename that should be used
   */
  @Nonnull
  private static String fileNameForResource(@Nonnull final String resourceType) {
    return resourceType + ".parquet";
  }
}
