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

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirEncoderFixtures;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.LogCapture;
import ch.qos.logback.classic.Level;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for {@link SchemaMigrator}, which detects Delta tables whose schemas are behind the
 * current FHIR encoders at startup and migrates them when {@code schemaAutoMerge} is enabled.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class SchemaMigratorTest {

  @Autowired private SparkSession sparkSession;

  @Autowired private FhirEncoders fhirEncoders;

  // Verifies that a drifted table is migrated when the flag is enabled: the schema gains the
  // missing fields, no rows are added, and existing rows present the new fields as null (FR-002,
  // FR-007).
  @Test
  void driftedTableIsMigratedWhenFlagEnabled(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    seedOldSchemaViewDefinitionTable(databasePath);

    final Set<String> drifted = newMigrator(databasePath, true).migrate();

    assertThat(drifted).isEmpty();
    final Dataset<Row> migrated = readTable(databasePath, "ViewDefinition");
    assertThat(migrated.schema().fieldNames()).contains("url", "version");
    assertThat(migrated.count()).isEqualTo(1);
    final Row row = migrated.select("id", "url", "version").first();
    assertThat(row.getString(0)).isEqualTo("test-view");
    assertThat(row.isNullAt(1)).isTrue();
    assertThat(row.isNullAt(2)).isTrue();
  }

  // Verifies that an undrifted table produces no Delta write: the table version is unchanged
  // after migration runs (FR-008).
  @Test
  void undriftedTableIsNotWritten(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    seedCurrentSchemaViewDefinitionTable(databasePath);
    final long versionBefore = latestTableVersion(databasePath, "ViewDefinition");

    final Set<String> drifted = newMigrator(databasePath, true).migrate();

    assertThat(drifted).isEmpty();
    assertThat(latestTableVersion(databasePath, "ViewDefinition")).isEqualTo(versionBefore);
  }

  // Verifies that a table whose only difference is extra on-disk fields unknown to the encoder is
  // not migrated (edge case: data written by a newer server version).
  @Test
  void extraFieldsOnlyTableIsNotMigrated(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    encoderDataset()
        .withColumn("fieldFromTheFuture", functions.lit((String) null))
        .write()
        .format("delta")
        .mode(SaveMode.ErrorIfExists)
        .save(databasePath + "/ViewDefinition.parquet");
    final long versionBefore = latestTableVersion(databasePath, "ViewDefinition");

    final Set<String> drifted = newMigrator(databasePath, true).migrate();

    assertThat(drifted).isEmpty();
    assertThat(latestTableVersion(databasePath, "ViewDefinition")).isEqualTo(versionBefore);
  }

  // Verifies that non-table files, directories that are not Delta tables, and directory names
  // that do not map to an encodable resource type are all skipped without error.
  @Test
  void nonTableEntriesAreSkipped(@TempDir final Path tempDir) throws IOException {
    final String databasePath = tempDir.toAbsolutePath().toString();
    Files.writeString(tempDir.resolve("stray-file.txt"), "not a table");
    Files.createDirectory(tempDir.resolve("Empty.parquet"));
    // A valid Delta table whose name does not map to an encodable resource type.
    sparkSession
        .createDataFrame(List.of(), encoderDataset().schema())
        .write()
        .format("delta")
        .save(databasePath + "/NotAResource.parquet");

    assertThatNoException().isThrownBy(() -> newMigrator(databasePath, true).migrate());
    assertThat(newMigrator(databasePath, true).migrate()).isEmpty();
  }

  // Verifies that an empty or missing database path does not break startup drift detection.
  @Test
  void emptyOrMissingDatabasePathIsHarmless(@TempDir final Path tempDir) {
    final String emptyPath = tempDir.toAbsolutePath().toString();
    final String missingPath = tempDir.resolve("does-not-exist").toAbsolutePath().toString();

    assertThat(newMigrator(emptyPath, true).migrate()).isEmpty();
    assertThat(newMigrator(missingPath, true).migrate()).isEmpty();
  }

  // Verifies that each migration is logged at INFO with the resource type and the added fields
  // (FR-010).
  @Test
  void migrationIsLoggedWithTypeAndFields(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    seedOldSchemaViewDefinitionTable(databasePath);

    try (final LogCapture logCapture = LogCapture.forClass(SchemaMigrator.class)) {
      newMigrator(databasePath, true).migrate();

      assertThat(logCapture.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.INFO);
                assertThat(event.getFormattedMessage())
                    .contains("ViewDefinition")
                    .contains("url")
                    .contains("version");
              });
    }
  }

  // Verifies that with the flag off, drifted tables are not written, a WARN names the type, the
  // missing fields, and the remedy, and the type is reported in the drifted set (FR-004).
  @Test
  void flagOffLogsWarningAndReportsDriftedType(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    seedOldSchemaViewDefinitionTable(databasePath);
    final long versionBefore = latestTableVersion(databasePath, "ViewDefinition");

    try (final LogCapture logCapture = LogCapture.forClass(SchemaMigrator.class)) {
      final Set<String> drifted = newMigrator(databasePath, false).migrate();

      assertThat(drifted).containsExactly("ViewDefinition");
      assertThat(latestTableVersion(databasePath, "ViewDefinition")).isEqualTo(versionBefore);
      assertThat(logCapture.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage())
                    .contains("ViewDefinition")
                    .contains("url")
                    .contains("version")
                    .contains("schemaAutoMerge");
              });
    }
  }

  // Verifies that a migration failure for one table is logged with its cause, does not propagate,
  // does not prevent other tables from migrating, and reports the failed type in the drifted set
  // (FR-006).
  @Test
  void migrationFailureIsIsolatedAndReported(@TempDir final Path tempDir) throws IOException {
    final String databasePath = tempDir.toAbsolutePath().toString();
    seedOldSchemaViewDefinitionTable(databasePath);
    seedOldSchemaPatientTable(databasePath);
    // Make the Patient table's Delta log read-only so its migration write fails.
    final Path patientLogDir = tempDir.resolve("Patient.parquet").resolve("_delta_log");
    setWritable(patientLogDir, false);

    try (final LogCapture logCapture = LogCapture.forClass(SchemaMigrator.class)) {
      final Set<String> drifted;
      try {
        drifted = newMigrator(databasePath, true).migrate();
      } finally {
        setWritable(patientLogDir, true);
      }

      // The failed type is reported; the other drifted table has still been migrated.
      assertThat(drifted).containsExactly("Patient");
      assertThat(readTable(databasePath, "ViewDefinition").schema().fieldNames())
          .contains("url", "version");
      assertThat(logCapture.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.ERROR);
                assertThat(event.getFormattedMessage()).contains("Patient");
                assertThat(event.getThrowableProxy()).isNotNull();
              });
    }
  }

  // ---- reporting the excess direction at startup (FR-005, SC-002) ----

  // Verifies that a table carrying fields the encoder does not emit is reported at startup, naming
  // the type and those paths, at a level reflecting that the condition is tolerated rather than
  // fatal. The type is not added to the drifted set and no migration is attempted, because the
  // columns cannot be reconstructed (US2 scenario 2).
  @Test
  void excessFieldsTableIsReportedAtStartup(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    seedWideOpenTypesPatientTable(databasePath);
    final long versionBefore = latestTableVersion(databasePath, "Patient");

    try (final LogCapture logCapture = LogCapture.forClass(SchemaMigrator.class)) {
      final Set<String> drifted = newMigrator(databasePath, true, narrowEncoders()).migrate();

      assertThat(drifted).isEmpty();
      assertThat(latestTableVersion(databasePath, "Patient")).isEqualTo(versionBefore);
      assertThat(logCapture.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.INFO);
                assertThat(event.getFormattedMessage())
                    .contains("Patient")
                    .contains("_extension.valuePeriod")
                    .contains("_extension.valueQuantity");
              });
    }
  }

  // Verifies that the excess direction is reported regardless of the schemaAutoMerge flag, since
  // the
  // flag governs the migratable direction only.
  @Test
  void excessFieldsTableIsReportedWithFlagDisabled(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    seedWideOpenTypesPatientTable(databasePath);

    try (final LogCapture logCapture = LogCapture.forClass(SchemaMigrator.class)) {
      assertThat(newMigrator(databasePath, false, narrowEncoders()).migrate()).isEmpty();

      assertThat(logCapture.events())
          .anySatisfy(
              event ->
                  assertThat(event.getFormattedMessage())
                      .contains("Patient")
                      .contains("_extension.valuePeriod"));
    }
  }

  // Verifies that a table differing in both directions at once produces both messages,
  // distinguishably: the missing direction is migrated and logged as such, and the excess direction
  // is reported separately (US2 edge case, FR-005).
  @Test
  void tableDifferingInBothDirectionsProducesBothMessages(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    // The ambient encoder's open types produce the excess direction against a narrowed migrator;
    // dropping gender produces the missing direction.
    FhirEncoderFixtures.encodeResources(sparkSession, fhirEncoders, "Patient", List.of(patient()))
        .drop("gender")
        .write()
        .format("delta")
        .mode(SaveMode.ErrorIfExists)
        .save(databasePath + "/Patient.parquet");

    try (final LogCapture logCapture = LogCapture.forClass(SchemaMigrator.class)) {
      newMigrator(databasePath, true, narrowEncoders()).migrate();

      // The missing direction is migrated, and says so, naming the field it added.
      assertThat(logCapture.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.INFO);
                assertThat(event.getFormattedMessage()).contains("Patient").contains("gender");
              });
      // The excess direction is reported separately, naming its own paths and not gender.
      assertThat(logCapture.events())
          .anySatisfy(
              event ->
                  assertThat(event.getFormattedMessage())
                      .contains("Patient")
                      .contains("_extension.valuePeriod")
                      .doesNotContain("gender"));
    }
  }

  // Verifies that a warehouse whose tables all reconcile produces neither message (US2 scenario 3).
  @Test
  void reconcilingTableProducesNoMessage(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    seedCurrentSchemaViewDefinitionTable(databasePath);

    try (final LogCapture logCapture = LogCapture.forClass(SchemaMigrator.class)) {
      assertThat(newMigrator(databasePath, true).migrate()).isEmpty();

      assertThat(logCapture.events())
          .noneSatisfy(
              event ->
                  assertThat(event.getFormattedMessage())
                      .containsAnyOf("missing fields", "does not emit", "Migrated schema"));
    }
  }

  // Verifies that startup succeeds against any warehouse state, including one holding a table of
  // each difference direction alongside entries that cannot be inspected at all (US2 scenario 4,
  // FR-005).
  @Test
  void startupSucceedsForEveryWarehouseState(@TempDir final Path tempDir) throws IOException {
    final String databasePath = tempDir.toAbsolutePath().toString();
    seedOldSchemaViewDefinitionTable(databasePath);
    seedWideOpenTypesPatientTable(databasePath);
    Files.createDirectory(tempDir.resolve("Empty.parquet"));

    assertThatNoException()
        .isThrownBy(() -> newMigrator(databasePath, true, narrowEncoders()).migrate());
  }

  // ---- helpers ----

  @Nonnull
  private SchemaMigrator newMigrator(
      @Nonnull final String databasePath, final boolean schemaAutoMerge) {
    return newMigrator(databasePath, schemaAutoMerge, fhirEncoders);
  }

  /**
   * Builds a migrator running against nominated encoders rather than the ambient ones, so that a
   * test can put the running encoder behind or ahead of a table's schema without depending on the
   * encoding configuration the test profile happens to use.
   */
  @Nonnull
  private SchemaMigrator newMigrator(
      @Nonnull final String databasePath,
      final boolean schemaAutoMerge,
      @Nonnull final FhirEncoders encoders) {
    return new SchemaMigrator(sparkSession, encoders, databasePath, schemaAutoMerge);
  }

  /**
   * Writes a ViewDefinition Delta table from encoder output with the top-level {@code url} and
   * {@code version} columns dropped, simulating a table written by an older encoder version.
   */
  private void seedOldSchemaViewDefinitionTable(@Nonnull final String databasePath) {
    encoderDataset()
        .drop("url", "version")
        .write()
        .format("delta")
        .mode(SaveMode.ErrorIfExists)
        .save(databasePath + "/ViewDefinition.parquet");
  }

  /**
   * Writes a Patient Delta table from encoder output with the top-level {@code gender} column
   * dropped, simulating a table written by an older encoder version.
   */
  private void seedOldSchemaPatientTable(@Nonnull final String databasePath) {
    sparkSession
        .createDataset(List.of(patient()), fhirEncoders.of("Patient"))
        .toDF()
        .drop("gender")
        .write()
        .format("delta")
        .mode(SaveMode.ErrorIfExists)
        .save(databasePath + "/Patient.parquet");
  }

  /**
   * Writes a Patient Delta table at the ambient encoder's schema. Paired with a migrator built on
   * {@link #narrowEncoders()}, this simulates a warehouse written before {@code
   * pathling.encoding.openTypes} was narrowed: the table carries extension value fields the running
   * encoder no longer emits.
   */
  private void seedWideOpenTypesPatientTable(@Nonnull final String databasePath) {
    FhirEncoderFixtures.seedTable(
        sparkSession,
        fhirEncoders,
        "Patient",
        List.of(patient()),
        databasePath + "/Patient.parquet");
  }

  /** The ambient encoders with Period and Quantity dropped from their open types. */
  @Nonnull
  private FhirEncoders narrowEncoders() {
    return FhirEncoderFixtures.narrow(fhirEncoders);
  }

  /** A minimal Patient, sufficient to give the encoder something to write. */
  @Nonnull
  private static Patient patient() {
    final Patient patient = new Patient();
    patient.setId("test-patient");
    return patient;
  }

  /** Sets the writability of a directory, used to provoke migration write failures. */
  private static void setWritable(@Nonnull final Path directory, final boolean writable) {
    assertThat(directory.toFile().setWritable(writable, false)).isTrue();
  }

  /** Writes a ViewDefinition Delta table whose schema matches the current encoder output. */
  private void seedCurrentSchemaViewDefinitionTable(@Nonnull final String databasePath) {
    encoderDataset()
        .write()
        .format("delta")
        .mode(SaveMode.ErrorIfExists)
        .save(databasePath + "/ViewDefinition.parquet");
  }

  /** Encodes a single minimal ViewDefinition using the current encoder. */
  @Nonnull
  private Dataset<Row> encoderDataset() {
    final ViewDefinitionResource viewDefinition = new ViewDefinitionResource();
    viewDefinition.setId("test-view");
    viewDefinition.setName(new StringType("test_view"));
    viewDefinition.setResource(new CodeType("Patient"));
    viewDefinition.setStatus(new CodeType("active"));
    return sparkSession
        .createDataset(List.of(viewDefinition), fhirEncoders.of("ViewDefinition"))
        .toDF();
  }

  /** Reads the Delta table for a resource type. */
  @Nonnull
  private Dataset<Row> readTable(
      @Nonnull final String databasePath, @Nonnull final String resourceCode) {
    return sparkSession.read().format("delta").load(databasePath + "/" + resourceCode + ".parquet");
  }

  /** Returns the latest Delta log version of a table. */
  private long latestTableVersion(
      @Nonnull final String databasePath, @Nonnull final String resourceCode) {
    final DeltaTable table =
        DeltaTable.forPath(sparkSession, databasePath + "/" + resourceCode + ".parquet");
    return table.history(1).select("version").first().getLong(0);
  }
}
