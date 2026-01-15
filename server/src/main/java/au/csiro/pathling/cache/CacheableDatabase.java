/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.cache;

import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.desc;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * A cache-aware implementation layer between the {@link au.csiro.pathling.library.io.sink.DataSink}
 * implementations.
 *
 * <p>It works on the assumption that all changes happen through the official interactions so it's
 * always logged.
 *
 * @author Felix Naumann
 */
@Component
@Slf4j
public class CacheableDatabase implements Cacheable {

  @Nonnull private final ThreadPoolTaskExecutor executor;

  private final SparkSession spark;

  private final String databasePath;

  @Nonnull @Getter private Optional<String> cacheKey;

  /**
   * Creates a new cacheable database instance.
   *
   * @param spark the Spark session
   * @param databasePath the path to the database
   * @param executor the executor for async cache invalidation
   */
  @Autowired
  public CacheableDatabase(
      final SparkSession spark,
      @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
          final String databasePath,
      @Nonnull final ThreadPoolTaskExecutor executor) {
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

    @Nullable
    final org.apache.hadoop.conf.Configuration hadoopConfiguration =
        spark.sparkContext().hadoopConfiguration();
    requireNonNull(hadoopConfiguration);
    @Nullable final FileSystem warehouse;
    try {
      warehouse = FileSystem.get(new URI(databasePath), hadoopConfiguration);
    } catch (final IOException | URISyntaxException e) {
      log.debug(
          "Unable to access warehouse location, returning empty snapshot time: {}", databasePath);
      return Optional.empty();
    }
    requireNonNull(warehouse);

    // Check that the database path exists.
    try {
      warehouse.exists(new Path(databasePath));
    } catch (final IOException e) {
      log.debug(
          "Unable to access database location, returning empty snapshot time: {}", databasePath);
      return Optional.empty();
    }

    // Find all the Parquet files within the warehouse and use them to create a set of resource
    // types.
    @Nullable final FileStatus[] fileStatuses;
    try {
      fileStatuses = warehouse.listStatus(new Path(databasePath));
    } catch (final IOException e) {
      log.debug(
          "Unable to access database location, returning empty snapshot time: {}", databasePath);
      return Optional.empty();
    }
    requireNonNull(fileStatuses);

    final List<Long> timestamps =
        Arrays.stream(fileStatuses)
            // Get the filename of each item in the directory listing.
            .map(
                fileStatus -> {
                  @Nullable final Path path = fileStatus.getPath();
                  requireNonNull(path);
                  return path.toString();
                })
            // Filter out any file names that don't match the pattern.
            .filter(path -> path.matches("^[^.]+\\.parquet$"))
            // Filter out anything that is not a Delta table.
            .filter(path -> DeltaTable.isDeltaTable(spark, path))
            // Safely attempt to open the table, handling race conditions where the table may
            // become inaccessible between the isDeltaTable check and forPath call.
            .map(
                path -> {
                  try {
                    return Optional.of(DeltaTable.forPath(spark, path));
                  } catch (final Exception e) {
                    log.debug("Table became inaccessible during scan: {}", path);
                    return Optional.<DeltaTable>empty();
                  }
                })
            .filter(Optional::isPresent)
            .map(Optional::get)
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
    @SuppressWarnings("RedundantCast")
    final Row[] head =
        (Row[]) deltaTable.history().orderBy(desc("version")).select("timestamp").head(1);
    if (head.length != 1) {
      return Optional.empty();
    } else {
      return Optional.of(head[0].getTimestamp(0).getTime());
    }
  }

  /**
   * Converts an S3 URL to an S3A URL.
   *
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
   * Invalidates the cache by asynchronously refreshing the cache key from the current database
   * state. This method scans all tables to determine the latest timestamp and should only be used
   * when multiple tables may have been modified (e.g., bulk import). For single-table
   * modifications, use {@link #invalidate(String)} instead.
   */
  public void invalidate() {
    executor.execute(
        () -> {
          cacheKey = buildCacheKeyFromStorage();
          spark.sqlContext().clearCache();
        });
  }

  /**
   * Invalidates the cache by querying only the specified table's timestamp. Since Delta writes
   * happen at the current time, the modified table's timestamp will always be greater than or equal
   * to any other table's timestamp, making this an O(1) operation instead of O(n).
   *
   * @param tablePath the path to the Delta table that was modified
   */
  public void invalidate(@Nonnull final String tablePath) {
    executor.execute(
        () -> {
          try {
            final DeltaTable table = DeltaTable.forPath(spark, tablePath);
            cacheKey = buildCacheKeyFromTable(table);
          } catch (final Exception e) {
            log.debug(
                "Unable to access table for cache invalidation, clearing cache: {}", tablePath, e);
          }
          spark.sqlContext().clearCache();
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
}
