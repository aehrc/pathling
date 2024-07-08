/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.io;

import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.desc;

import au.csiro.pathling.caching.Cacheable;
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
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * A file system-based persistence scheme that facilitates caching.
 *
 * @author John Grimes
 */
@Slf4j
public class CacheableFileSystemPersistence extends FileSystemPersistence implements Cacheable {

  @Nonnull
  private final ThreadPoolTaskExecutor executor;

  private final int compactionThreshold;

  @Nonnull
  @Getter
  private Optional<String> cacheKey;

  public CacheableFileSystemPersistence(@Nonnull final SparkSession spark,
      @Nonnull final String path, @Nonnull final ThreadPoolTaskExecutor executor,
      final int compactionThreshold) {
    super(spark, path);
    this.executor = executor;
    this.compactionThreshold = compactionThreshold;
    this.cacheKey = buildCacheKeyFromDatabase();
  }

  @Override
  public void invalidate(@Nonnull final ResourceType resourceType) {
    super.invalidate(resourceType);
    invalidateCache(resourceType);
    compact(resourceType);
  }

  @Override
  public boolean cacheKeyMatches(@Nonnull final String otherKey) {
    return cacheKey.map(key -> key.equals(otherKey)).orElse(false);
  }

  /**
   * Determines the latest snapshot time from all resource tables within the database.
   *
   * @return the latest snapshot time, or empty if no snapshot time could be determined
   */
  @Nonnull
  private Optional<Long> latestUpdate() {
    log.info("Querying latest snapshot from database: {}", path);

    @Nullable final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark.sparkContext()
        .hadoopConfiguration();
    requireNonNull(hadoopConfiguration);
    @Nullable final FileSystem warehouse;
    try {
      warehouse = FileSystem.get(new URI(path), hadoopConfiguration);
    } catch (final IOException | URISyntaxException e) {
      log.debug("Unable to access warehouse location, returning empty snapshot time: {}", path);
      return Optional.empty();
    }
    requireNonNull(warehouse);

    // Check that the database path exists.
    try {
      warehouse.exists(new Path(path));
    } catch (final IOException e) {
      log.debug("Unable to access database location, returning empty snapshot time: {}", path);
      return Optional.empty();
    }

    // Find all the Parquet files within the warehouse and use them to create a set of resource
    // types.
    @Nullable final FileStatus[] fileStatuses;
    try {
      fileStatuses = warehouse.listStatus(new Path(path));
    } catch (final IOException e) {
      log.debug("Unable to access database location, returning empty snapshot time: {}", path);
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
        .map(CacheableFileSystemPersistence::latestUpdateToTable)
        // Filter out any tables which don't have history rows.
        .filter(Optional::isPresent)
        // Get the timestamp from the history row.
        .map(Optional::get)
        .collect(Collectors.toList());

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
   * Updates the cache key based upon the latest update time of the specified resource type.
   *
   * @param resourceType the resource type to update the cache key for
   */
  private void invalidateCache(@Nonnull final ResourceType resourceType) {
    executor.execute(() -> {
      final DeltaTable table = read(resourceType);
      cacheKey = buildCacheKeyFromTable(table);
      this.spark.sqlContext().clearCache();
    });
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
   * Generates a new cache key based upon the latest update time across all resource types within
   * the database.
   *
   * @return the cache key, or empty if no update time could be determined
   */
  @Nonnull
  private Optional<String> buildCacheKeyFromDatabase() {
    return latestUpdate().map(this::cacheKeyFromTimestamp);
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

  /**
   * Compacts the table if it has a number of partitions that exceed the configured threshold.
   *
   * @param resourceType the resource type of the table to compact
   * @see <a href="https://docs.delta.io/latest/best-practices.html#compact-files">Delta Lake
   * Documentation - Compact files</a>
   */
  private void compact(final @Nonnull ResourceType resourceType) {
    final DeltaTable table = read(resourceType);
    final String tableUrl = getTableUrl(path, resourceType);
    final int numPartitions = table.toDF().rdd().getNumPartitions();
    if (numPartitions > compactionThreshold) {

      log.debug("Scheduling table compaction (number of partitions: {}, threshold: {}): {}",
          numPartitions, compactionThreshold, tableUrl);
      executor.submit(() -> {
        log.debug("Commencing compaction: {}", tableUrl);
        read(resourceType)
            .toDF()
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
          compactionThreshold);
    }
  }

}
