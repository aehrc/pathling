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

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.LayoutDetector;
import au.csiro.pathling.library.io.source.StorageLayout;
import au.csiro.pathling.library.io.source.UnsupportedLayoutException;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.LogCapture;
import ca.uhn.fhir.parser.IParser;
import ch.qos.logback.classic.Level;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.functions;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for {@link WarehouseMigrator}, which converts warehouse tables written in the layout
 * that preceded Parquet on FHIR at startup, overwriting each in place.
 *
 * <p>Legacy-layout tables are written with the legacy encoders, which remain available to the
 * server, and Parquet on FHIR tables with {@link ResourceEncoding}.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class WarehouseMigratorTest {

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  @Autowired private IParser jsonParser;

  // A legacy table is overwritten in the Parquet on FHIR layout, holding the same resources.
  @Test
  void migratesLegacyTableToParquetOnFhir(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    final List<Patient> patients = List.of(patient("p1", "Smith"), patient("p2", "Jones"));
    writeLegacyTable(databasePath, "Patient", patients);

    newMigrator(databasePath).migrate();

    final Dataset<Row> migrated = readTable(databasePath, "Patient");
    assertThat(LayoutDetector.detect(migrated.schema()).getLayout())
        .isEqualTo(StorageLayout.PARQUET_ON_FHIR);
    assertThat(json(ResourceEncoding.decode(pathlingContext, "Patient", migrated)))
        .containsExactlyElementsOf(json(patients));
  }

  // The table is overwritten from a lazy read of itself. Every row of a table spread over several
  // files must survive, which holds because the overwrite only marks the files it replaces as
  // removed rather than deleting them while they are being read.
  @Test
  void keepsEveryRowOfATableSpreadOverSeveralFiles(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    final List<Patient> patients =
        IntStream.range(0, 500).mapToObj(i -> patient("p" + i, "Family" + i)).toList();
    legacyDataset("Patient", patients)
        .repartition(8)
        .write()
        .format("delta")
        .save(tablePath(databasePath, "Patient"));

    newMigrator(databasePath).migrate();

    final Dataset<Row> migrated = readTable(databasePath, "Patient");
    assertThat(migrated.count()).isEqualTo(500);
    assertThat(migrated.select("id").distinct().count()).isEqualTo(500);
  }

  // The table as it was before the migration is kept as a shallow clone beside it, which holds a
  // log of its own and shares the table's data files rather than copying them.
  @Test
  void keepsAShallowCloneOfTheLegacyTable(@TempDir final Path tempDir) throws IOException {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeLegacyTable(
        databasePath, "Patient", List.of(patient("p1", "Smith"), patient("p2", "Jones")));

    newMigrator(databasePath).migrate();

    final Dataset<Row> clone = sparkSession.read().format("delta").load(clonePath(databasePath));
    assertThat(LegacyLayoutConversion.isLegacy(clone.schema())).isTrue();
    assertThat(clone.count()).isEqualTo(2);
    assertThat(dataFiles(Path.of(clonePath(databasePath)))).isEmpty();
  }

  // The clone stays readable once the table's own log no longer reaches the version before the
  // migration, as happens when Delta removes expired log files at the checkpoint the migration
  // writes. Time travel on the table cannot recover the data at that point; the clone can.
  @Test
  void cloneSurvivesTheRemovalOfTheTablesEarlierLog(@TempDir final Path tempDir)
      throws IOException {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeLegacyTable(databasePath, "Patient", List.of(patient("p1", "Smith")));
    final long versionBefore = latestVersion(databasePath, "Patient");

    newMigrator(databasePath).migrate();
    final Path log = Path.of(tablePath(databasePath, "Patient"), "_delta_log");
    for (long version = 0; version <= versionBefore; version++) {
      Files.delete(log.resolve("%020d.json".formatted(version)));
    }
    DeltaLog.clearCache();

    final Dataset<Row> clone = sparkSession.read().format("delta").load(clonePath(databasePath));
    assertThat(clone.select("id").as(Encoders.STRING()).collectAsList()).containsExactly("p1");
  }

  // A clone left by an earlier attempt that stopped before the table was overwritten is replaced,
  // so the clone always holds the table as it was immediately before its migration.
  @Test
  void replacesACloneLeftByAnEarlierAttempt(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeLegacyTable(databasePath, "Patient", List.of(patient("p1", "Smith")));
    legacyDataset("Patient", List.of(patient("stale", "Old")))
        .write()
        .format("delta")
        .save(clonePath(databasePath));

    newMigrator(databasePath).migrate();

    final Dataset<Row> clone = sparkSession.read().format("delta").load(clonePath(databasePath));
    assertThat(clone.select("id").as(Encoders.STRING()).collectAsList()).containsExactly("p1");
  }

  // The clone is not a resource table: a second run neither migrates nor rewrites it.
  @Test
  void aSecondRunLeavesTheCloneAlone(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeLegacyTable(databasePath, "Patient", List.of(patient("p1", "Smith")));
    newMigrator(databasePath).migrate();
    final long cloneVersion =
        DeltaTable.forPath(sparkSession, clonePath(databasePath))
            .history(1)
            .select("version")
            .first()
            .getLong(0);

    newMigrator(databasePath).migrate();

    assertThat(
            DeltaTable.forPath(sparkSession, clonePath(databasePath))
                .history(1)
                .select("version")
                .first()
                .getLong(0))
        .isEqualTo(cloneVersion);
  }

  // A table already in the Parquet on FHIR layout is not written, so its version is unchanged.
  @Test
  void leavesParquetOnFhirTableUntouched(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    ResourceEncoding.encode(pathlingContext, "Patient", List.of(patient("p1", "Smith")))
        .write()
        .format("delta")
        .save(tablePath(databasePath, "Patient"));
    final long versionBefore = latestVersion(databasePath, "Patient");

    newMigrator(databasePath).migrate();

    assertThat(latestVersion(databasePath, "Patient")).isEqualTo(versionBefore);
  }

  // An empty legacy table becomes an empty table in the Parquet on FHIR layout, which still has
  // the id column the server selects and filters on.
  @Test
  void migratesEmptyLegacyTable(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeLegacyTable(databasePath, "Patient", List.of());

    newMigrator(databasePath).migrate();

    final Dataset<Row> migrated = readTable(databasePath, "Patient");
    assertThat(LegacyLayoutConversion.isLegacy(migrated.schema())).isFalse();
    assertThat(migrated.schema().fieldNames()).contains("id");
    assertThat(migrated.count()).isZero();
  }

  // The server's custom ViewDefinition type is migrated like any standard resource type.
  @Test
  void migratesLegacyViewDefinitionTable(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    final ViewDefinitionResource viewDefinition = new ViewDefinitionResource();
    viewDefinition.setId("test-view");
    viewDefinition.setName(new StringType("test_view"));
    viewDefinition.setResource(new CodeType("Patient"));
    viewDefinition.setStatus(new CodeType("active"));
    writeLegacyTable(databasePath, "ViewDefinition", List.of(viewDefinition));

    newMigrator(databasePath).migrate();

    final Dataset<Row> migrated = readTable(databasePath, "ViewDefinition");
    assertThat(LegacyLayoutConversion.isLegacy(migrated.schema())).isFalse();
    final List<IBaseResource> decoded =
        ResourceEncoding.decode(pathlingContext, "ViewDefinition", migrated);
    assertThat(decoded).hasSize(1);
    final ViewDefinitionResource result = (ViewDefinitionResource) decoded.get(0);
    assertThat(result.getIdPart()).isEqualTo("test-view");
    assertThat(result.getName().getValue()).isEqualTo("test_view");
    assertThat(result.getResource().getValue()).isEqualTo("Patient");
  }

  // A table mixing the two layouts cannot be converted, so the migration fails with an error
  // naming the table and carrying the library's refusal as its cause, and the table is left as it
  // was.
  @Test
  void failsOnMixedLayoutTable(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    legacyDataset("Patient", List.of(patient("p1", "Smith")))
        .withColumn("resourceType", functions.lit("Patient"))
        .write()
        .format("delta")
        .save(tablePath(databasePath, "Patient"));
    final long versionBefore = latestVersion(databasePath, "Patient");

    assertThatThrownBy(() -> newMigrator(databasePath).migrate())
        .hasMessageContaining("Patient")
        .hasMessageContaining(tablePath(databasePath, "Patient"))
        .hasCauseInstanceOf(UnsupportedLayoutException.class);
    assertThat(latestVersion(databasePath, "Patient")).isEqualTo(versionBefore);
  }

  // A failure that Spark raises as a checked exception, such as a data file missing from the
  // table, also fails the migration with an error naming the table.
  @Test
  void namesTheTableWhenADataFileIsMissing(@TempDir final Path tempDir) throws IOException {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeLegacyTable(databasePath, "Patient", List.of(patient("p1", "Smith")));
    try (final var files = Files.list(Path.of(tablePath(databasePath, "Patient")))) {
      for (final Path file : files.filter(f -> f.toString().endsWith(".parquet")).toList()) {
        Files.delete(file);
      }
    }

    assertThatThrownBy(() -> newMigrator(databasePath).migrate())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to migrate the Patient table")
        .hasMessageContaining(tablePath(databasePath, "Patient"));
  }

  // Entries that are not resource tables - stray files, directories that are not Delta tables,
  // tables whose name is not a supported resource type, and the server's other directories - are
  // skipped and left untouched.
  @Test
  void skipsEntriesThatAreNotResourceTables(@TempDir final Path tempDir) throws IOException {
    final String databasePath = tempDir.toAbsolutePath().toString();
    Files.writeString(tempDir.resolve("stray-file.txt"), "not a table");
    Files.writeString(tempDir.resolve("Patient.parquet.txt"), "not a table");
    Files.createDirectory(tempDir.resolve("Observation.parquet"));
    Files.createDirectory(tempDir.resolve("jobs"));
    // A legacy-layout table whose name is not a supported resource type.
    writeLegacyTable(databasePath, "NotAResource", "Patient", List.of(patient("p1", "Smith")));
    final long versionBefore = latestVersion(databasePath, "NotAResource");

    assertThatNoException().isThrownBy(() -> newMigrator(databasePath).migrate());

    assertThat(latestVersion(databasePath, "NotAResource")).isEqualTo(versionBefore);
    assertThat(LegacyLayoutConversion.isLegacy(readTable(databasePath, "NotAResource").schema()))
        .isTrue();
  }

  // An empty or missing database directory has nothing to migrate.
  @Test
  void toleratesEmptyOrMissingDatabaseDirectory(@TempDir final Path tempDir) {
    final String emptyPath = tempDir.toAbsolutePath().toString();
    final String missingPath = tempDir.resolve("does-not-exist").toAbsolutePath().toString();

    assertThatNoException().isThrownBy(() -> newMigrator(emptyPath).migrate());
    assertThatNoException().isThrownBy(() -> newMigrator(missingPath).migrate());
  }

  // A second run over a migrated warehouse finds nothing to do and writes nothing.
  @Test
  void secondRunWritesNothing(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeLegacyTable(databasePath, "Patient", List.of(patient("p1", "Smith")));
    newMigrator(databasePath).migrate();
    final long versionAfterFirstRun = latestVersion(databasePath, "Patient");

    newMigrator(databasePath).migrate();

    assertThat(latestVersion(databasePath, "Patient")).isEqualTo(versionAfterFirstRun);
  }

  // A migration interrupted after some tables were converted continues where it stopped: the
  // converted table is left alone and the remaining legacy table is converted.
  @Test
  void continuesAfterPartialMigration(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeLegacyTable(databasePath, "Patient", List.of(patient("p1", "Smith")));
    newMigrator(databasePath).migrate();
    final long patientVersion = latestVersion(databasePath, "Patient");
    final Observation observation = new Observation();
    observation.setId("o1");
    observation.setStatus(Observation.ObservationStatus.FINAL);
    writeLegacyTable(databasePath, "Observation", List.of(observation));

    newMigrator(databasePath).migrate();

    assertThat(latestVersion(databasePath, "Patient")).isEqualTo(patientVersion);
    final Dataset<Row> observations = readTable(databasePath, "Observation");
    assertThat(LegacyLayoutConversion.isLegacy(observations.schema())).isFalse();
    assertThat(observations.count()).isEqualTo(1);
  }

  // Each migrated table is reported with a warning naming the table, the clone that holds the data
  // as it was before the migration, and what the conversion could not recover.
  @Test
  void warnsWithLossesAndTheCloneLocation(@TempDir final Path tempDir) {
    final String databasePath = tempDir.toAbsolutePath().toString();
    writeLegacyTable(databasePath, "Patient", List.of(patient("p1", "Smith")));

    try (final LogCapture logCapture = LogCapture.forClass(WarehouseMigrator.class)) {
      newMigrator(databasePath).migrate();

      assertThat(logCapture.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage())
                    .contains("Patient")
                    .contains(tablePath(databasePath, "Patient"))
                    .contains(clonePath(databasePath))
                    .contains(LegacyLayoutConversion.LOSSES);
              });
      // The row count of the migrated table is reported once the migration completes.
      assertThat(logCapture.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.INFO);
                assertThat(event.getFormattedMessage()).contains("Patient").contains("1 resource");
              });
    }
  }

  // ---- helpers ----

  @Nonnull
  private WarehouseMigrator newMigrator(@Nonnull final String databasePath) {
    return new WarehouseMigrator(pathlingContext, databasePath);
  }

  @Nonnull
  private static Patient patient(@Nonnull final String id, @Nonnull final String family) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.setGender(AdministrativeGender.FEMALE);
    patient.addName().setFamily(family).addGiven("Jane");
    patient.getBirthDateElement().setValueAsString("1970-01-02");
    return patient;
  }

  /** Builds a dataset of the given resources in the legacy layout. */
  @Nonnull
  private Dataset<Row> legacyDataset(
      @Nonnull final String resourceType, @Nonnull final List<? extends IBaseResource> resources) {
    return sparkSession.createDataset(List.copyOf(resources), fhirEncoders.of(resourceType)).toDF();
  }

  /** Writes a table of the given resources in the legacy layout. */
  private void writeLegacyTable(
      @Nonnull final String databasePath,
      @Nonnull final String resourceType,
      @Nonnull final List<? extends IBaseResource> resources) {
    writeLegacyTable(databasePath, resourceType, resourceType, resources);
  }

  /** Writes a table of the given resources in the legacy layout, under the given table name. */
  private void writeLegacyTable(
      @Nonnull final String databasePath,
      @Nonnull final String tableName,
      @Nonnull final String resourceType,
      @Nonnull final List<? extends IBaseResource> resources) {
    legacyDataset(resourceType, resources)
        .write()
        .format("delta")
        .mode(SaveMode.ErrorIfExists)
        .save(tablePath(databasePath, tableName));
  }

  /** Serialises resources as FHIR JSON in id order, so that their content can be compared. */
  @Nonnull
  private List<String> json(@Nonnull final List<? extends IBaseResource> resources) {
    return resources.stream()
        .sorted(Comparator.comparing(resource -> resource.getIdElement().getIdPart()))
        .map(jsonParser::encodeResourceToString)
        .toList();
  }

  @Nonnull
  private Dataset<Row> readTable(
      @Nonnull final String databasePath, @Nonnull final String tableName) {
    return sparkSession.read().format("delta").load(tablePath(databasePath, tableName));
  }

  private long latestVersion(@Nonnull final String databasePath, @Nonnull final String tableName) {
    return DeltaTable.forPath(sparkSession, tablePath(databasePath, tableName))
        .history(1)
        .select("version")
        .first()
        .getLong(0);
  }

  @Nonnull
  private static String tablePath(
      @Nonnull final String databasePath, @Nonnull final String tableName) {
    return databasePath + "/" + tableName + ".parquet";
  }

  @Nonnull
  private static String clonePath(@Nonnull final String databasePath) {
    return databasePath + "/Patient.legacy-layout";
  }

  /** Lists the Parquet data files directly within a table directory. */
  @Nonnull
  private static List<Path> dataFiles(@Nonnull final Path table) throws IOException {
    try (final var files = Files.list(table)) {
      return files.filter(file -> file.toString().endsWith(".parquet")).toList();
    }
  }
}
