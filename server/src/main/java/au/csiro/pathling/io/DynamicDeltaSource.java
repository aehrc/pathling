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

package au.csiro.pathling.io;

import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.FileSystemPersistence;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.LayoutDetector;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A {@link DelegatingSource} that dynamically discovers new resource types created after startup.
 * Delegates to the underlying data source for known types, and attempts on-demand discovery for
 * unknown types by checking if a Delta table exists at the expected path.
 *
 * <p>Discovery on read is matched by discovery on enumeration: {@link #getResourceTypes} also lists
 * the warehouse database directory, so a table that exists but has never been read is still
 * enumerable. Reads and enumeration therefore agree, and an unnarrowed export cannot silently omit
 * a type merely because nothing happened to read it first.
 *
 * <p>A type with no table is served as an empty dataset in the Parquet on FHIR layout. The
 * propagation of this source's read behaviour into derived sources is inherited from {@link
 * DelegatingSource}.
 *
 * @author John Grimes
 */
@Slf4j
public class DynamicDeltaSource extends DelegatingSource {

  /** The directory-name suffix under which each resource type's Delta table is stored. */
  private static final String TABLE_SUFFIX = ".parquet";

  @Nonnull private final SparkSession spark;

  @Nonnull private final String databasePath;

  private final boolean cacheDatasets;

  @Nonnull private final Set<String> dynamicallyDiscoveredTypes = ConcurrentHashMap.newKeySet();

  /**
   * Constructs a new DynamicDeltaSource.
   *
   * @param context the Pathling context, used to construct derived sources and empty datasets, and
   *     to check whether a listed table directory names a supported resource type
   * @param delegate the underlying QueryableDataSource to delegate to
   * @param spark the Spark session for Delta table operations
   * @param databasePath the path to the Delta database
   * @param storageConfiguration the storage configuration
   */
  public DynamicDeltaSource(
      @Nonnull final PathlingContext context,
      @Nonnull final QueryableDataSource delegate,
      @Nonnull final SparkSession spark,
      @Nonnull final String databasePath,
      @Nonnull final StorageConfiguration storageConfiguration) {
    super(context, delegate);
    this.spark = spark;
    this.databasePath = databasePath;
    this.cacheDatasets = storageConfiguration.getCacheDatasets();
  }

  @Override
  @Nonnull
  public Dataset<Row> read(@Nullable final String resourceCode) {
    if (resourceCode == null) {
      throw new IllegalArgumentException("Resource code must not be null");
    }

    // If delegate knows about this type, use it.
    if (delegate.getResourceTypes().contains(resourceCode)) {
      return cacheIfEnabled(delegate.read(resourceCode));
    }

    // If we've already discovered this type dynamically, read from Delta.
    if (dynamicallyDiscoveredTypes.contains(resourceCode)) {
      return cacheIfEnabled(readFromDelta(resourceCode));
    }

    // Try to discover the Delta table.
    final String tablePath = getTablePath(resourceCode);
    if (DeltaTable.isDeltaTable(spark, tablePath)) {
      log.debug("Dynamically discovered Delta table for resource type: {}", resourceCode);
      dynamicallyDiscoveredTypes.add(resourceCode);
      return cacheIfEnabled(readFromDelta(resourceCode));
    }

    // No data found - return an empty dataset.
    log.debug("No data found for resource type: {}, returning empty dataset", resourceCode);
    return ResourceEncoding.empty(context, resourceCode);
  }

  /**
   * Re-loads the Delta table for the given resource type and replaces the dataset served for it, so
   * that all consumers observe the table's current schema. Intended to be called after a
   * schema-evolving write. When dataset caching is enabled, the stale cached dataset is
   * unpersisted. If no Delta table exists for the type, the call is a no-op.
   *
   * @param resourceCode the resource type code to refresh
   */
  public void refresh(@Nonnull final String resourceCode) {
    final String tablePath = getTablePath(resourceCode);
    if (!DeltaTable.isDeltaTable(spark, tablePath)) {
      log.debug("No Delta table found for resource type {}, nothing to refresh", resourceCode);
      return;
    }

    // Unpersist the stale cached dataset before replacing it, so the cached plan for the old
    // snapshot does not linger in the Spark cache.
    if (cacheDatasets && delegate.getResourceTypes().contains(resourceCode)) {
      delegate.read(resourceCode).unpersist();
    }

    final Dataset<Row> refreshed =
        checkLayout(resourceCode, spark.read().format("delta").load(tablePath));
    if (delegate instanceof final DatasetSource datasetSource) {
      // Replace the pinned entry in the delegate's resource map, so every consumer that resolves
      // datasets through the delegate observes the evolved schema.
      datasetSource.dataset(resourceCode, refreshed);
      log.info("Refreshed dataset for resource type {}", resourceCode);
    } else {
      // The delegate cannot be mutated; serve the type through dynamic discovery, which re-loads
      // the Delta table on each read.
      dynamicallyDiscoveredTypes.add(resourceCode);
      log.info("Registered resource type {} for dynamic discovery following refresh", resourceCode);
    }
  }

  /**
   * Returns every resource type this source can serve: those the delegate knew at startup, those
   * discovered on demand since, and those whose table directory is present in the warehouse
   * database directory.
   *
   * <p>The directory listing is what makes a table created after startup - by this server or by
   * another process sharing the warehouse - enumerable before anything has read it. Without it, an
   * unnarrowed export would silently omit such a type until an unrelated request happened to read
   * it. Listed names are filtered to supported resource types, so an unrelated directory in the
   * warehouse is not mistaken for one; whether a listed table is actually readable is left to
   * {@link #read}, which falls back to an empty dataset.
   *
   * @return the resource types this source can serve
   */
  @Override
  @Nonnull
  public Set<String> getResourceTypes() {
    final Set<String> types = new HashSet<>(delegate.getResourceTypes());
    types.addAll(dynamicallyDiscoveredTypes);
    types.addAll(listTableDirectories());
    return types;
  }

  /**
   * Lists the resource types with a table directory in the database directory. A listing failure is
   * an enhancement lost rather than a request lost: it is logged and the caller falls back to the
   * types already known.
   */
  @Nonnull
  private Set<String> listTableDirectories() {
    final Set<String> listed = new HashSet<>();
    try {
      final Path databaseDir = new Path(databasePath);
      final FileSystem fileSystem =
          databaseDir.getFileSystem(spark.sparkContext().hadoopConfiguration());
      if (fileSystem.exists(databaseDir)) {
        for (final FileStatus status : fileSystem.listStatus(databaseDir)) {
          final String name = status.getPath().getName();
          if (status.isDirectory() && name.endsWith(TABLE_SUFFIX)) {
            final String resourceType = name.substring(0, name.length() - TABLE_SUFFIX.length());
            if (context.isResourceTypeSupported(resourceType)) {
              listed.add(resourceType);
            }
          }
        }
      }
    } catch (final IOException e) {
      log.warn("Failed to list the database directory while enumerating resource types", e);
    }
    return listed;
  }

  /**
   * Captures the current Delta version of every resource-type table and returns a source that
   * serves each of them at that version, so all reads made through it observe a single consistent
   * view of the data regardless of concurrent writes.
   *
   * <p>Versions for all tables are captured at one instant, which is what the {@code $sql-export}
   * single-snapshot guarantee requires. Pinning reads only each table's Delta log, so no data is
   * copied and reads stay lazy.
   *
   * @return a snapshot source pinned at the current instant
   */
  @Nonnull
  public SnapshotDeltaSource snapshot() {
    final Map<String, Dataset<Row>> pinnedDatasets = new HashMap<>();
    final Map<String, Long> pinnedVersions = new HashMap<>();

    for (final String resourceType : getResourceTypes()) {
      final String tablePath = getTablePath(resourceType);
      if (!DeltaTable.isDeltaTable(spark, tablePath)) {
        continue;
      }
      final long version = currentVersion(tablePath);
      pinnedVersions.put(resourceType, version);
      // Read with versionAsOf rather than through the delegate, so the pinned dataset is
      // independent of the mutable dataset cache, whose entries track the current table.
      pinnedDatasets.put(
          resourceType,
          checkLayout(
              resourceType,
              spark.read().format("delta").option("versionAsOf", version).load(tablePath)));
    }

    log.debug("Pinned {} resource-type tables for a snapshot read", pinnedVersions.size());
    return SnapshotDeltaSource.of(context, pinnedDatasets, pinnedVersions);
  }

  /** Reads the current version of a Delta table from its transaction log. */
  private long currentVersion(@Nonnull final String tablePath) {
    return DeltaTable.forPath(spark, tablePath).history(1).select("version").first().getLong(0);
  }

  @Nonnull
  private Dataset<Row> cacheIfEnabled(@Nonnull final Dataset<Row> dataset) {
    if (cacheDatasets) {
      return dataset.cache();
    }
    return dataset;
  }

  @Nonnull
  private Dataset<Row> readFromDelta(@Nonnull final String resourceCode) {
    final String tablePath = getTablePath(resourceCode);
    return checkLayout(resourceCode, spark.read().format("delta").load(tablePath));
  }

  /**
   * Refuses a table in a layout the library does not read, such as one restored from before the
   * startup migration or copied into the warehouse by hand, with the library's explanation. The
   * delegate checks the tables it found at startup; this covers the tables this source loads
   * itself. The schema is already resolved by the load, so the check reads nothing further.
   *
   * @throws au.csiro.pathling.library.io.source.UnsupportedLayoutException if the layout is not one
   *     the library reads
   */
  @Nonnull
  private static Dataset<Row> checkLayout(
      @Nonnull final String resourceCode, @Nonnull final Dataset<Row> table) {
    LayoutDetector.check(resourceCode, table.schema());
    return table;
  }

  @Nonnull
  private String getTablePath(@Nonnull final String resourceCode) {
    return FileSystemPersistence.safelyJoinPaths(databasePath, resourceCode + TABLE_SUFFIX);
  }
}
