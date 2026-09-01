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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.LogCapture;
import au.csiro.pathling.views.FhirView;
import ch.qos.logback.classic.Level;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
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

  @Autowired private FhirEncoders fhirEncoders;

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
    // Verify the schema has the expected structure for an ImmunizationEvaluation resource.
    assertThat(result.schema().fieldNames()).contains("id", "status");
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

  // Verifies that reading a type recorded as drifted and unmigrated throws SchemaDriftError
  // naming the type, and that other types are unaffected (FR-005, FR-006).
  @Test
  void readOfDriftedTypeThrowsSchemaDriftError(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writePatientTable(databasePath, "id");
    final DynamicDeltaSource source = newTempDirSource(databasePath, false, Set.of("Patient"));

    assertThatThrownBy(() -> source.read("Patient"))
        .isInstanceOf(SchemaDriftError.class)
        .hasMessageContaining("Patient")
        .hasMessageContaining("schemaAutoMerge");
    // A type that is not drifted continues to work.
    assertThat(source.read("ImmunizationEvaluation").count()).isZero();
  }

  // Verifies that a successful refresh clears the drifted mark, so a later schema-evolving update
  // recovers the type without a restart (update-driven recovery).
  @Test
  void refreshClearsDriftedMark(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writePatientTable(databasePath, "id");
    final DynamicDeltaSource source = newTempDirSource(databasePath, false, Set.of("Patient"));
    assertThatThrownBy(() -> source.read("Patient")).isInstanceOf(SchemaDriftError.class);

    source.refresh("Patient");

    assertThat(source.read("Patient").count()).isEqualTo(1);
  }

  // Verifies that constructing a view query over a drifted subject type throws SchemaDriftError.
  // The executed query resolves its subject dataset through the delegate's own dispatcher rather
  // than the guarded read(), so the guard must be applied when the query is constructed.
  @Test
  void viewOverDriftedTypeThrowsSchemaDriftError(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writePatientTable(databasePath, "id");
    final DynamicDeltaSource source = newTempDirSource(databasePath, false, Set.of("Patient"));
    final FhirView view =
        FhirView.ofResource("Patient")
            .select(FhirView.columns(FhirView.column("id", "id")))
            .build();

    assertThatThrownBy(() -> source.view(view))
        .isInstanceOf(SchemaDriftError.class)
        .hasMessageContaining("Patient");
    assertThatThrownBy(() -> source.view("Patient")).isInstanceOf(SchemaDriftError.class);
    // A view over a type that is not drifted can still be constructed.
    assertThatNoException().isThrownBy(() -> source.view("Observation"));
  }

  // Verifies that a source derived through map() keeps the drift guard, covering filtered view
  // runs and exports which resolve datasets through the derived source rather than this one.
  @Test
  void mappedSourcePreservesDriftGuard(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writePatientTable(databasePath, "id");
    final DynamicDeltaSource source = newTempDirSource(databasePath, false, Set.of("Patient"));

    final QueryableDataSource mapped = source.map((resourceType, dataset) -> dataset);

    assertThatThrownBy(() -> mapped.read("Patient")).isInstanceOf(SchemaDriftError.class);
    // The guard survives further derivation and applies to view construction on the derived
    // source.
    assertThatThrownBy(() -> mapped.map((resourceType, dataset) -> dataset).read("Patient"))
        .isInstanceOf(SchemaDriftError.class);
    assertThatThrownBy(() -> mapped.view("Patient")).isInstanceOf(SchemaDriftError.class);
  }

  // Verifies that a source derived through filterByResourceType() keeps the drift guard.
  @Test
  void filteredSourcePreservesDriftGuard(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writePatientTable(databasePath, "id");
    final DynamicDeltaSource source = newTempDirSource(databasePath, false, Set.of("Patient"));

    final QueryableDataSource filtered = source.filterByResourceType(resourceType -> true);

    assertThatThrownBy(() -> filtered.read("Patient")).isInstanceOf(SchemaDriftError.class);
  }

  // Verifies that once refresh() clears the drifted mark, newly derived sources read normally.
  @Test
  void derivedSourceAfterRefreshReadsSuccessfully(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writePatientTable(databasePath, "id");
    final DynamicDeltaSource source = newTempDirSource(databasePath, false, Set.of("Patient"));

    source.refresh("Patient");

    assertThat(source.map((resourceType, dataset) -> dataset).read("Patient").count()).isEqualTo(1);
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
            fhirEncoders,
            storageConfiguration,
            Set.of());

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
    return newTempDirSource(databasePath, cacheDatasets, Set.of());
  }

  @Nonnull
  private DynamicDeltaSource newTempDirSource(
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
   * {@code mergeSchema} option, mirroring the warmup write performed on update.
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
