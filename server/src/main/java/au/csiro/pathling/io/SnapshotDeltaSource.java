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
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A {@link QueryableDataSource} that serves every resource type at the Delta version it held at one
 * instant, so that all reads made through it observe a single consistent view of the data no matter
 * how long they take or how many concurrent writes land in the meantime.
 *
 * <p>This backs the {@code $sql-export} guarantee that every subject in a job is computed against a
 * single snapshot. Pinning costs nothing beyond reading each table's Delta log at creation: reads
 * stay lazy, no data is copied, and Delta's own history serves the pinned version. A type whose
 * table did not exist at the pinned instant is invisible to the snapshot, and reads of it yield an
 * empty dataset.
 *
 * <p>The snapshot deliberately bypasses the live source's mutable dataset cache, whose entries
 * track the current table rather than the pinned version.
 *
 * @author John Grimes
 */
@Slf4j
public class SnapshotDeltaSource extends DriftGuardedSource {

  @Nonnull private final SparkSession spark;

  @Nonnull private final FhirEncoders fhirEncoders;

  @Nonnull private final Map<String, Long> pinnedVersions;

  /**
   * Constructs a new SnapshotDeltaSource over datasets already pinned to their captured versions.
   *
   * @param context the Pathling context, used to construct derived sources
   * @param pinned a source holding one {@code versionAsOf} dataset per resource type
   * @param spark the Spark session, used to build empty datasets for unpinned types
   * @param fhirEncoders the FHIR encoders, used to build empty datasets for unpinned types
   * @param pinnedVersions the Delta version captured for each resource type
   * @param driftedTypes the resource types whose tables are drifted and unmigrated, held by
   *     reference so the guard tracks the live source's current state
   */
  SnapshotDeltaSource(
      @Nonnull final PathlingContext context,
      @Nonnull final QueryableDataSource pinned,
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final Map<String, Long> pinnedVersions,
      @Nonnull final Set<String> driftedTypes) {
    super(context, pinned, driftedTypes);
    this.spark = spark;
    this.fhirEncoders = fhirEncoders;
    this.pinnedVersions = Map.copyOf(pinnedVersions);
  }

  @Override
  @Nonnull
  public Dataset<Row> read(@Nullable final String resourceCode) {
    if (resourceCode == null) {
      throw new IllegalArgumentException("Resource code must not be null");
    }
    checkNotDrifted(resourceCode);
    if (!pinnedVersions.containsKey(resourceCode)) {
      // The type had no table at the pinned instant, so the job sees no data for it.
      return QueryHelpers.createEmptyDataset(spark, fhirEncoders, resourceCode);
    }
    return delegate.read(resourceCode);
  }

  /**
   * Returns the Delta version pinned for each resource type at snapshot creation.
   *
   * @return an immutable map of resource type to pinned Delta version
   */
  @Nonnull
  public Map<String, Long> getPinnedVersions() {
    return pinnedVersions;
  }

  /**
   * Builds a snapshot source over the given pinned datasets.
   *
   * @param context the Pathling context used to hold the pinned datasets
   * @param spark the Spark session
   * @param fhirEncoders the FHIR encoders
   * @param pinnedDatasets the pinned dataset for each resource type
   * @param pinnedVersions the Delta version captured for each resource type
   * @param driftedTypes the drifted resource types, held by reference
   * @return the snapshot source
   */
  @Nonnull
  static SnapshotDeltaSource of(
      @Nonnull final PathlingContext context,
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final Map<String, Dataset<Row>> pinnedDatasets,
      @Nonnull final Map<String, Long> pinnedVersions,
      @Nonnull final Set<String> driftedTypes) {
    final DatasetSource pinned = new DatasetSource(context);
    pinnedDatasets.forEach(pinned::dataset);
    return new SnapshotDeltaSource(
        context, pinned, spark, fhirEncoders, pinnedVersions, driftedTypes);
  }
}
