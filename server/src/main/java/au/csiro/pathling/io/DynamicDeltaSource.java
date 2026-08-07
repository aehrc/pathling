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

import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.FileSystemPersistence;
import au.csiro.pathling.library.io.source.DatasetSource;
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
 * A {@link DriftGuardedSource} that dynamically discovers new resource types created after startup.
 * Delegates to the underlying data source for known types, and attempts on-demand discovery for
 * unknown types by checking if a Delta table exists at the expected path.
 *
 * <p>The drift guard behaviour, including its propagation into derived sources, is inherited from
 * {@link DriftGuardedSource}. The drifted types set is mutable so that a successful {@link
 * #refresh} clears the guard for the refreshed type.
 *
 * @author John Grimes
 */
@Slf4j
public class DynamicDeltaSource extends DriftGuardedSource {

  /** The directory-name suffix under which each resource type's Delta table is stored. */
  private static final String TABLE_SUFFIX = ".parquet";

  @Nonnull private final SparkSession spark;

  @Nonnull private final String databasePath;

  @Nonnull private final FhirEncoders fhirEncoders;

  private final boolean cacheDatasets;

  @Nonnull private final Set<String> dynamicallyDiscoveredTypes = ConcurrentHashMap.newKeySet();

  /**
   * Constructs a new DynamicDeltaSource with no drifted types.
   *
   * @param delegate the underlying QueryableDataSource to delegate to
   * @param spark the Spark session for Delta table operations
   * @param databasePath the path to the Delta database
   * @param fhirEncoders the FHIR encoders for creating empty datasets
   * @param storageConfiguration the storage configuration
   */
  public DynamicDeltaSource(
      @Nonnull final QueryableDataSource delegate,
      @Nonnull final SparkSession spark,
      @Nonnull final String databasePath,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final StorageConfiguration storageConfiguration) {
    this(delegate, spark, databasePath, fhirEncoders, storageConfiguration, Set.of());
  }

  /**
   * Constructs a new DynamicDeltaSource.
   *
   * @param delegate the underlying QueryableDataSource to delegate to
   * @param spark the Spark session for Delta table operations
   * @param databasePath the path to the Delta database
   * @param fhirEncoders the FHIR encoders for creating empty datasets
   * @param storageConfiguration the storage configuration
   * @param driftedTypes the resource types left drifted and unmigrated at startup
   */
  public DynamicDeltaSource(
      @Nonnull final QueryableDataSource delegate,
      @Nonnull final SparkSession spark,
      @Nonnull final String databasePath,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final StorageConfiguration storageConfiguration,
      @Nonnull final Set<String> driftedTypes) {
    super(delegate, concurrentCopyOf(driftedTypes));
    this.spark = spark;
    this.databasePath = databasePath;
    this.fhirEncoders = fhirEncoders;
    this.cacheDatasets = storageConfiguration.getCacheDatasets();
  }

  /**
   * Copies the given types into a mutable concurrent set, so that the drifted mark can be cleared
   * by {@link #refresh} and observed by derived sources.
   *
   * @param types the types to copy
   * @return a mutable concurrent set containing the given types
   */
  @Nonnull
  private static Set<String> concurrentCopyOf(@Nonnull final Set<String> types) {
    final Set<String> copy = ConcurrentHashMap.newKeySet();
    copy.addAll(types);
    return copy;
  }

  @Override
  @Nonnull
  public Dataset<Row> read(@Nullable final String resourceCode) {
    if (resourceCode == null) {
      throw new IllegalArgumentException("Resource code must not be null");
    }

    // A type whose table is drifted and unmigrated cannot be queried; fail with an actionable
    // error rather than an opaque analysis failure.
    checkNotDrifted(resourceCode);

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

    // No data found - return an empty dataset with the correct schema.
    log.debug("No data found for resource type: {}, returning empty dataset", resourceCode);
    return QueryHelpers.createEmptyDataset(spark, fhirEncoders, resourceCode);
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

    final Dataset<Row> refreshed = spark.read().format("delta").load(tablePath);
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

    // The freshly loaded table carries the current schema, so the type is no longer drifted.
    if (driftedTypes.remove(resourceCode)) {
      log.info("Cleared drifted mark for resource type {}", resourceCode);
    }
  }

  @Override
  @Nonnull
  public Set<String> getResourceTypes() {
    final Set<String> types = new HashSet<>(delegate.getResourceTypes());
    types.addAll(dynamicallyDiscoveredTypes);
    return types;
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
   * @param context the Pathling context used to hold the pinned datasets
   * @return a snapshot source pinned at the current instant
   */
  @Nonnull
  public SnapshotDeltaSource snapshot(@Nonnull final PathlingContext context) {
    final Map<String, Dataset<Row>> pinnedDatasets = new HashMap<>();
    final Map<String, Long> pinnedVersions = new HashMap<>();

    for (final String resourceType : snapshotCandidateTypes()) {
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
          spark.read().format("delta").option("versionAsOf", version).load(tablePath));
    }

    log.debug("Pinned {} resource-type tables for a snapshot read", pinnedVersions.size());
    return SnapshotDeltaSource.of(
        context, spark, fhirEncoders, pinnedDatasets, pinnedVersions, driftedTypes);
  }

  /**
   * Determines the resource types to consider for a snapshot: those the source already knows about,
   * plus any Delta table present in the database directory. The directory listing catches a table
   * created after startup that has never been read, which dynamic discovery would not yet know
   * about.
   */
  @Nonnull
  private Set<String> snapshotCandidateTypes() {
    final Set<String> candidates = new HashSet<>(getResourceTypes());
    try {
      final Path databaseDir = new Path(databasePath);
      final FileSystem fileSystem =
          databaseDir.getFileSystem(spark.sparkContext().hadoopConfiguration());
      if (fileSystem.exists(databaseDir)) {
        for (final FileStatus status : fileSystem.listStatus(databaseDir)) {
          final String name = status.getPath().getName();
          if (status.isDirectory() && name.endsWith(TABLE_SUFFIX)) {
            candidates.add(name.substring(0, name.length() - TABLE_SUFFIX.length()));
          }
        }
      }
    } catch (final IOException e) {
      // The listing is an enhancement over the known types; if it fails, fall back to those.
      log.warn("Failed to list the database directory while taking a snapshot", e);
    }
    return candidates;
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
    return spark.read().format("delta").load(tablePath);
  }

  @Nonnull
  private String getTablePath(@Nonnull final String resourceCode) {
    return FileSystemPersistence.safelyJoinPaths(databasePath, resourceCode + TABLE_SUFFIX);
  }
}
