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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for {@link SnapshotDeltaSource}, which backs the {@code $sql-export} single-snapshot
 * guarantee: every subject in a job reads the data as at one instant, regardless of concurrent
 * writes.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class SnapshotDeltaSourceTest {

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  // The central guarantee: a write that advances the table after the snapshot was taken is
  // invisible to the snapshot, which continues to serve the pinned version.
  @Test
  void servesPinnedVersionAfterTableAdvances(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeTable(databasePath, "Patient", 2);
    final DynamicDeltaSource live = newLiveSource(databasePath);
    final SnapshotDeltaSource snapshot = live.snapshot();
    assertThat(snapshot.read("Patient").count()).isEqualTo(2);

    // A concurrent writer appends two more rows, advancing the table's Delta version.
    appendToTable(databasePath, "Patient", 2);

    assertThat(snapshot.read("Patient").count()).isEqualTo(2);
    // The live source, by contrast, observes the write.
    assertThat(live.read("Patient").count()).isEqualTo(4);
  }

  // Two reads of the same type within one snapshot see identical data, which is what lets outputs
  // from different subjects be joined on a shared key without skew.
  @Test
  void repeatedReadsWithinSnapshotAreConsistent(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeTable(databasePath, "Patient", 3);
    final SnapshotDeltaSource snapshot = newLiveSource(databasePath).snapshot();

    final long first = snapshot.read("Patient").count();
    appendToTable(databasePath, "Patient", 5);
    final long second = snapshot.read("Patient").count();

    assertThat(second).isEqualTo(first).isEqualTo(3);
  }

  // The guarantee has to survive the filtering the export applies, since every subject reads
  // through the filtered source rather than the snapshot directly.
  @Test
  void derivedSourcePreservesPinnedVersion(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeTable(databasePath, "Patient", 2);
    final SnapshotDeltaSource snapshot = newLiveSource(databasePath).snapshot();
    final QueryableDataSource filtered = snapshot.map((resourceType, dataset) -> dataset);

    appendToTable(databasePath, "Patient", 3);

    assertThat(filtered.read("Patient").count()).isEqualTo(2);
  }

  // Every table is pinned at the same instant, so a write to a second type after snapshot creation
  // is equally invisible.
  @Test
  void pinsEveryTypeAtSnapshotCreation(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeTable(databasePath, "Patient", 1);
    writeTable(databasePath, "Observation", 1);
    final SnapshotDeltaSource snapshot = newLiveSource(databasePath).snapshot();

    appendToTable(databasePath, "Patient", 4);
    appendToTable(databasePath, "Observation", 7);

    assertThat(snapshot.read("Patient").count()).isEqualTo(1);
    assertThat(snapshot.read("Observation").count()).isEqualTo(1);
    assertThat(snapshot.getResourceTypes()).contains("Patient", "Observation");
  }

  // A resource type whose table is created after the snapshot was taken did not exist at the
  // snapshot instant, so it is invisible to the job.
  @Test
  void typesCreatedAfterSnapshotAreInvisible(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeTable(databasePath, "Patient", 1);
    final SnapshotDeltaSource snapshot = newLiveSource(databasePath).snapshot();

    writeTable(databasePath, "Condition", 3);

    assertThat(snapshot.getResourceTypes()).doesNotContain("Condition");
    assertThat(snapshot.read("Condition").count()).isZero();
  }

  // The snapshot serves its own pinned reads rather than the live source's mutable dataset cache,
  // so a cached dataset from before the snapshot cannot leak into it.
  @Test
  void bypassesTheMutableDatasetCache(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeTable(databasePath, "Patient", 2);
    final DynamicDeltaSource live = newLiveSource(databasePath, true, Set.of());
    // Prime the live source's cache at the pre-write state.
    final Dataset<Row> cached = live.read("Patient");
    assertThat(cached.storageLevel()).isEqualTo(StorageLevel.MEMORY_AND_DISK());

    try {
      final SnapshotDeltaSource snapshot = live.snapshot();

      assertThat(snapshot.read("Patient").storageLevel()).isEqualTo(StorageLevel.NONE());
      assertThat(snapshot.read("Patient").count()).isEqualTo(2);
    } finally {
      cached.unpersist();
    }
  }

  // A type marked drifted and unmigrated cannot be queried through the snapshot either; the guard
  // is carried across so a job fails with the actionable error rather than an opaque one.
  @Test
  void preservesTheDriftGuard(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeTable(databasePath, "Patient", 1);
    final DynamicDeltaSource live = newLiveSource(databasePath, false, Set.of("Patient"));

    final SnapshotDeltaSource snapshot = live.snapshot();

    assertThatThrownBy(() -> snapshot.read("Patient"))
        .isInstanceOf(SchemaDriftError.class)
        .hasMessageContaining("Patient");
  }

  // ---- helpers ----

  @Nonnull
  private DynamicDeltaSource newLiveSource(@Nonnull final String databasePath) {
    return newLiveSource(databasePath, false, Set.of());
  }

  @Nonnull
  private DynamicDeltaSource newLiveSource(
      @Nonnull final String databasePath,
      final boolean cacheDatasets,
      @Nonnull final Set<String> driftedTypes) {
    final StorageConfiguration storageConfiguration = new StorageConfiguration();
    storageConfiguration.setCacheDatasets(cacheDatasets);
    final QueryableDataSource baseSource = pathlingContext.read().delta(databasePath);
    return new DynamicDeltaSource(
        pathlingContext,
        baseSource,
        sparkSession,
        databasePath,
        fhirEncoders,
        storageConfiguration,
        driftedTypes);
  }

  /** Writes a new single-column Delta table with the given number of rows. */
  private void writeTable(
      @Nonnull final String databasePath, @Nonnull final String resourceType, final int rowCount) {
    dataset(rowCount)
        .write()
        .format("delta")
        .mode(SaveMode.ErrorIfExists)
        .save(databasePath + "/" + resourceType + ".parquet");
  }

  /** Appends rows to an existing Delta table, advancing its version. */
  private void appendToTable(
      @Nonnull final String databasePath, @Nonnull final String resourceType, final int rowCount) {
    dataset(rowCount)
        .write()
        .format("delta")
        .mode(SaveMode.Append)
        .save(databasePath + "/" + resourceType + ".parquet");
  }

  @Nonnull
  private Dataset<Row> dataset(final int rowCount) {
    final StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, true, Metadata.empty())
            });
    final List<Row> rows =
        java.util.stream.IntStream.range(0, rowCount)
            .mapToObj(i -> RowFactory.create("id-" + i))
            .toList();
    return sparkSession.createDataFrame(rows, schema);
  }
}
