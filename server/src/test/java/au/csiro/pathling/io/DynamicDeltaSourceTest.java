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
import static org.assertj.core.api.Assertions.assertThatNoException;

import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.LogCapture;
import ch.qos.logback.classic.Level;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for {@link DynamicDeltaSource}.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class DynamicDeltaSourceTest {

  private static final String DATABASE_PATH =
      Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath().toString();

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  private DynamicDeltaSource dynamicDeltaSource;

  @BeforeEach
  void setUp() {
    dynamicDeltaSource = newDynamicDeltaSource(true);
  }

  // Verifies that reading a resource type that doesn't exist in the database returns an empty
  // dataset instead of throwing an exception.
  @Test
  void readReturnsEmptyDatasetForMissingResourceType() {
    // ImmunizationEvaluation is a valid FHIR resource type that doesn't have a table in our test
    // data.
    final Dataset<Row> result = dynamicDeltaSource.read("ImmunizationEvaluation");

    assertThat(result).isNotNull();
    assertThat(result.count()).isZero();
    // The type has no data, so the dataset carries only the id column the server relies on.
    assertThat(result.schema().fieldNames()).contains("id");
  }

  // Verifies that when StorageConfiguration.cacheDatasets is true, datasets returned by read()
  // are marked for caching. Guards against accidental changes to the caching contract introduced
  // by cacheIfEnabled().
  @Test
  void readCachesDatasetWhenCachingEnabled() {
    final DynamicDeltaSource source = newDynamicDeltaSource(true);

    final Dataset<Row> result = source.read("Patient");

    try {
      // Dataset.cache() applies MEMORY_AND_DISK; assert on the exact level rather than just
      // "anything but NONE" so accidental changes to the persistence level are caught.
      assertThat(result.storageLevel()).isEqualTo(StorageLevel.MEMORY_AND_DISK());
    } finally {
      result.unpersist();
    }
  }

  // Verifies that when StorageConfiguration.cacheDatasets is false, datasets returned by read()
  // are not cached. Pairs with readCachesDatasetWhenCachingEnabled() to lock in cacheIfEnabled()
  // behaviour for both configuration values.
  @Test
  void readDoesNotCacheDatasetWhenCachingDisabled() {
    final DynamicDeltaSource source = newDynamicDeltaSource(false);

    final Dataset<Row> result = source.read("Patient");

    assertThat(result.storageLevel()).isEqualTo(StorageLevel.NONE());
  }

  // Verifies that after the underlying Delta table's schema evolves, refresh() replaces the
  // pinned delegate entry so that read() returns a dataset with the new schema.
  @Test
  void refreshReplacesPinnedDatasetAfterSchemaEvolution(@TempDir final Path tempDir) {
    // Arrange: a Delta table for Patient with only an id column, pinned at construction.
    final String databasePath = tempDir.toAbsolutePath().toString();
    writePatientTable(databasePath, "id");
    final DynamicDeltaSource source = newTempDirSource(databasePath, false);
    assertThat(source.read("Patient").schema().fieldNames()).doesNotContain("newField");

    // Act: evolve the table schema by appending zero rows carrying an extra column, which leaves
    // the pinned dataset behind the table, then refresh.
    evolvePatientTable(databasePath, "id", "newField");
    assertThat(source.read("Patient").schema().fieldNames()).doesNotContain("newField");
    source.refresh("Patient");

    // Assert: the replacement dataset presents the evolved schema and still reads the data.
    final Dataset<Row> refreshed = source.read("Patient");
    assertThat(refreshed.schema().fieldNames()).contains("id", "newField");
    assertThat(refreshed.count()).isEqualTo(1);
  }

  // Verifies that with dataset caching enabled, refresh() unpersists the stale cached dataset and
  // subsequent reads serve the refreshed dataset with caching applied.
  @Test
  void refreshUnpersistsStaleCachedDataset(@TempDir final Path tempDir) {
    // Arrange: a cached source over a Patient table with only an id column.
    final String databasePath = tempDir.toAbsolutePath().toString();
    writePatientTable(databasePath, "id");
    final DynamicDeltaSource source = newTempDirSource(databasePath, true);
    final Dataset<Row> stale = source.read("Patient");
    assertThat(stale.storageLevel()).isEqualTo(StorageLevel.MEMORY_AND_DISK());

    // Act: evolve the table and refresh.
    evolvePatientTable(databasePath, "id", "newField");
    source.refresh("Patient");

    // Assert: the stale dataset is no longer persisted, and the refreshed dataset is served with
    // the evolved schema and caching applied.
    assertThat(stale.storageLevel()).isEqualTo(StorageLevel.NONE());
    final Dataset<Row> refreshed = source.read("Patient");
    try {
      assertThat(refreshed.schema().fieldNames()).contains("newField");
      assertThat(refreshed.storageLevel()).isEqualTo(StorageLevel.MEMORY_AND_DISK());
    } finally {
      refreshed.unpersist();
    }
  }

  // Verifies that refreshing a type with no Delta table is a harmless no-op.
  @Test
  void refreshOfMissingTableIsNoOp(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    final DynamicDeltaSource source = newTempDirSource(databasePath, false);

    source.refresh("ImmunizationEvaluation");

    assertThat(source.read("ImmunizationEvaluation").count()).isZero();
  }

  // Verifies that a table written into the warehouse after the source was built is enumerated even
  // though nothing has read it. Writing the table directly rather than through the source also
  // stands in for a table created by another process sharing the warehouse (FR-006).
  @Test
  void enumeratesNeverReadTableCreatedAfterConstruction(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    final DynamicDeltaSource source = newTempDirSource(databasePath, false);
    assertThat(source.getResourceTypes()).isEmpty();

    writePatientTable(databasePath, "id");

    assertThat(source.getResourceTypes()).containsExactly("Patient");
  }

  // Verifies that a directory whose name does not correspond to a supported resource type is not
  // reported as one, and that a stray file alongside the tables does not disturb enumeration.
  @Test
  void enumerationIgnoresUnsupportedNamesAndStrayFiles(@TempDir final Path tempDir)
      throws IOException {
    final String databasePath = tempDir.toAbsolutePath().toString();
    final DynamicDeltaSource source = newTempDirSource(databasePath, false);
    writePatientTable(databasePath, "id");
    Files.createDirectories(tempDir.resolve("NotAType.parquet"));
    Files.createDirectories(tempDir.resolve("jobs"));
    Files.writeString(tempDir.resolve("stray.txt"), "not a table");

    assertThat(source.getResourceTypes()).containsExactly("Patient");
  }

  // Verifies that a failure to list the database directory falls back to the types already known,
  // logs a warning and does not fail the caller (FR-008).
  @Test
  void enumerationFallsBackWhenListingFails() {
    // A scheme with no registered filesystem makes the listing throw an IOException.
    final DatasetSource delegate = new DatasetSource(pathlingContext);
    delegate.dataset("Patient", patientDataset(1, "id"));
    final StorageConfiguration storageConfiguration = new StorageConfiguration();
    storageConfiguration.setCacheDatasets(false);
    final DynamicDeltaSource source =
        new DynamicDeltaSource(
            pathlingContext,
            delegate,
            sparkSession,
            "nosuchscheme://host/database",
            storageConfiguration);

    try (final LogCapture logCapture = LogCapture.forClass(DynamicDeltaSource.class)) {
      assertThatNoException().isThrownBy(source::getResourceTypes);
      assertThat(source.getResourceTypes()).containsExactly("Patient");

      // The fallback must be accompanied by a warning naming the failed listing.
      assertThat(logCapture.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage())
                    .contains("Failed to list the database directory");
              });
    }
  }

  // Verifies that a snapshot pins and serves a table that exists in the warehouse but has never
  // been read, which is the behaviour the enumeration refactor has to preserve.
  @Test
  void snapshotPinsNeverReadTable(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    final DynamicDeltaSource source = newTempDirSource(databasePath, false);
    writePatientTable(databasePath, "id");

    final SnapshotDeltaSource snapshot = source.snapshot();

    assertThat(snapshot.getPinnedVersions()).containsKey("Patient");
    assertThat(snapshot.read("Patient").count()).isEqualTo(1);
  }

  // ---- helpers ----

  @Nonnull
  private DynamicDeltaSource newDynamicDeltaSource(final boolean cacheDatasets) {
    return newTempDirSource(DATABASE_PATH, cacheDatasets);
  }

  @Nonnull
  private DynamicDeltaSource newTempDirSource(
      @Nonnull final String databasePath, final boolean cacheDatasets) {
    final StorageConfiguration storageConfiguration = new StorageConfiguration();
    storageConfiguration.setCacheDatasets(cacheDatasets);
    final QueryableDataSource baseSource = pathlingContext.read().delta(databasePath);
    return new DynamicDeltaSource(
        pathlingContext, baseSource, sparkSession, databasePath, storageConfiguration);
  }

  /** Writes a single-row Patient Delta table whose schema has only the given string columns. */
  private void writePatientTable(
      @Nonnull final String databasePath, @Nonnull final String... columns) {
    patientDataset(1, columns)
        .write()
        .format("delta")
        .mode(SaveMode.ErrorIfExists)
        .save(databasePath + "/Patient.parquet");
  }

  /**
   * Evolves the Patient table's schema by appending zero rows carrying the given columns with the
   * {@code mergeSchema} option, as a write carrying previously unseen elements widens the table.
   */
  private void evolvePatientTable(
      @Nonnull final String databasePath, @Nonnull final String... columns) {
    patientDataset(0, columns)
        .write()
        .format("delta")
        .mode(SaveMode.Append)
        .option("mergeSchema", "true")
        .save(databasePath + "/Patient.parquet");
  }

  /** Creates a dataset with the given number of rows and string columns. */
  @Nonnull
  private Dataset<Row> patientDataset(final int rowCount, @Nonnull final String... columns) {
    final StructField[] fields = new StructField[columns.length];
    for (int i = 0; i < columns.length; i++) {
      fields[i] = new StructField(columns[i], DataTypes.StringType, true, Metadata.empty());
    }
    final List<Row> rows =
        rowCount == 0
            ? List.of()
            : List.of(RowFactory.create((Object[]) new String[columns.length]));
    return sparkSession.createDataFrame(rows, new StructType(fields));
  }
}
