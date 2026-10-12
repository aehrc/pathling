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
package au.csiro.pathling.library.io.sink;

import static au.csiro.pathling.library.io.LayoutFixtures.CONDITION_TABLE;
import static au.csiro.pathling.library.io.LayoutFixtures.PATIENT_TABLE;
import static au.csiro.pathling.library.io.LayoutFixtures.newConditions;
import static au.csiro.pathling.library.io.LayoutFixtures.newPatients;
import static au.csiro.pathling.library.io.LayoutFixtures.previousConditions;
import static au.csiro.pathling.library.io.LayoutFixtures.previousPatients;
import static au.csiro.pathling.library.io.LayoutFixtures.writeDelta;
import static au.csiro.pathling.library.io.LayoutFixtures.writeParquet;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.LayoutDetector;
import au.csiro.pathling.library.io.source.StorageLayout;
import au.csiro.pathling.library.io.source.UnsupportedLayoutException;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that a write into a table holding the layout written by earlier releases is refused before
 * anything is written, rather than failing on a storage-layer schema error or widening the table to
 * carry both layouts (US7, FR-037a, T042a).
 *
 * <p>A write that replaces the table, an overwrite, is not refused: it leaves one layout behind,
 * and it is the way to re-import into the same location. Neither is a write that fails or does
 * nothing where the table exists.
 *
 * @author Piotr Szul
 */
class SinkDetectionTest {

  @TempDir static Path warehouse;

  @TempDir Path directory;

  private static SparkSession spark;
  private static PathlingContext context;

  /** Set up Spark, with a warehouse in a temporary directory, and the Pathling context. */
  @BeforeAll
  static void setUp() {
    spark =
        TestHelpers.sparkBuilder()
            .config("spark.sql.warehouse.dir", warehouse.resolve("spark-warehouse").toString())
            .getOrCreate();
    context = PathlingContext.create(spark);
  }

  /** Stop Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  // Delta.

  /**
   * An append or a merge into a previous-layout Delta table is refused with the read path's
   * message, and the table keeps its version, schema and contents.
   */
  @ParameterizedTest
  @ValueSource(strings = {"append", "merge"})
  void deltaWriteIntoPreviousLayoutIsRefusedBeforeAnythingIsMerged(@Nonnull final String mode) {
    writeDelta(previousPatients(spark, "p1"), directory, PATIENT_TABLE);
    final List<Object> before = deltaState(directory.resolve(PATIENT_TABLE));

    final UnsupportedLayoutException error =
        assertThrows(
            UnsupportedLayoutException.class,
            () -> newPatientSource("n1").write().saveMode(mode).delta(directory.toString()));

    assertRefusalOfPreviousPatients(error, before);
    assertEquals(before, deltaState(directory.resolve(PATIENT_TABLE)));
  }

  /** An overwrite replaces a previous-layout Delta table, leaving the new layout alone. */
  @Test
  void deltaOverwriteReplacesAPreviousLayoutTable() {
    writeDelta(previousPatients(spark, "p1"), directory, PATIENT_TABLE);

    newPatientSource("n1").write().saveMode("overwrite").delta(directory.toString());

    final Dataset<Row> table = context.read().delta(directory.toString()).read("Patient");
    assertEquals(List.of("n1"), ids(table));
    assertEquals(StorageLayout.PARQUET_ON_FHIR, LayoutDetector.detect(table.schema()).getLayout());
  }

  /** An append or a merge into a Delta table in the new layout is not refused. */
  @ParameterizedTest
  @ValueSource(strings = {"append", "merge"})
  void deltaWriteIntoNewLayoutSucceeds(@Nonnull final String mode) {
    writeDelta(newPatients(context, "n1"), directory, PATIENT_TABLE);

    newPatientSource("n2").write().saveMode(mode).delta(directory.toString());

    assertEquals(
        List.of("n1", "n2"), ids(context.read().delta(directory.toString()).read("Patient")));
  }

  /**
   * A refusal of one resource type's table comes before any other type is written, so a write of
   * several types into a mix of tables leaves every one of them as it was.
   */
  @Test
  void refusalOfOneTablePrecedesWritingAnyOther() {
    writeDelta(newPatients(context, "n1"), directory, PATIENT_TABLE);
    writeDelta(previousConditions(spark, "c1"), directory, CONDITION_TABLE);
    final List<Object> patientsBefore = deltaState(directory.resolve(PATIENT_TABLE));
    final List<Object> conditionsBefore = deltaState(directory.resolve(CONDITION_TABLE));
    final DatasetSource source =
        context
            .read()
            .datasets()
            .dataset("Patient", newPatients(context, "n2"))
            .dataset("Condition", newConditions(context, "c2"));

    assertThrows(
        UnsupportedLayoutException.class,
        () -> source.write().saveMode("append").delta(directory.toString()));

    assertEquals(patientsBefore, deltaState(directory.resolve(PATIENT_TABLE)));
    assertEquals(conditionsBefore, deltaState(directory.resolve(CONDITION_TABLE)));
  }

  // Catalog tables.

  /**
   * An append to a previous-layout catalog table is refused before anything is written, whether the
   * table is a Delta table, which would otherwise be widened, or a Parquet one, which would
   * otherwise fail on the difference between the schemas.
   */
  @ParameterizedTest
  @ValueSource(strings = {"delta", "parquet"})
  void catalogAppendToPreviousLayoutIsRefused(@Nonnull final String format) {
    final String database = database();
    previousPatients(spark, "p1").write().format(format).saveAsTable(database + ".Patient");
    final List<String> idsBefore = ids(spark.table(database + ".Patient"));
    final long versionsBefore = "delta".equals(format) ? catalogVersions(database) : 0;

    final UnsupportedLayoutException error =
        assertThrows(
            UnsupportedLayoutException.class,
            () -> newPatientSource("n1").write().saveMode("append").tables(database, format));

    assertEquals("Patient", error.getResourceType());
    assertEquals(idsBefore, ids(spark.table(database + ".Patient")));
    assertTrue(
        List.of(spark.table(database + ".Patient").schema().fieldNames()).contains("id_versioned"));
    assertFalse(
        List.of(spark.table(database + ".Patient").schema().fieldNames()).contains("resourceType"));
    if ("delta".equals(format)) {
      assertEquals(versionsBefore, catalogVersions(database));
    }
  }

  /** A merge into a previous-layout Delta catalog table is refused before anything is merged. */
  @Test
  void catalogMergeIntoPreviousLayoutIsRefused() {
    final String database = database();
    previousPatients(spark, "p1").write().format("delta").saveAsTable(database + ".Patient");
    final long versionsBefore = catalogVersions(database);

    assertThrows(
        UnsupportedLayoutException.class,
        () -> newPatientSource("n1").write().saveMode("merge").tables(database, "delta"));

    assertEquals(versionsBefore, catalogVersions(database));
    assertEquals(List.of("p1"), ids(spark.table(database + ".Patient")));
  }

  /** An append to a catalog table in the new layout is not refused. */
  @Test
  void catalogAppendToNewLayoutSucceeds() {
    final String database = database();
    newPatients(context, "n1").write().format("delta").saveAsTable(database + ".Patient");

    newPatientSource("n2").write().saveMode("append").tables(database, "delta");

    assertEquals(List.of("n1", "n2"), ids(spark.table(database + ".Patient")));
  }

  // Parquet files.

  /**
   * An append beside previous-layout Parquet files is refused before any file is written, so the
   * directory does not come to hold both layouts.
   */
  @Test
  void parquetAppendBesidePreviousLayoutIsRefused() throws IOException {
    writeParquet(previousPatients(spark, "p1"), directory, PATIENT_TABLE);
    final List<Path> filesBefore = files(directory);

    final UnsupportedLayoutException error =
        assertThrows(
            UnsupportedLayoutException.class,
            () -> newPatientSource("n1").write().saveMode("append").parquet(directory.toString()));

    assertEquals("Patient", error.getResourceType());
    assertEquals(filesBefore, files(directory));
  }

  /**
   * An append beside the partition files a previous write renamed is refused too, since they are
   * what the Parquet source reads as the type.
   */
  @Test
  void parquetAppendBesideRenamedPreviousLayoutFilesIsRefused() throws IOException {
    context
        .read()
        .datasets()
        .dataset("Patient", previousPatients(spark, "p1"))
        .write()
        .parquet(directory.toString());
    final List<Path> filesBefore = files(directory);
    assertTrue(
        filesBefore.stream()
            .anyMatch(
                file ->
                    file.getFileName()
                        .toString()
                        .matches("Patient\\.\\d+-[0-9a-f-]{36}\\.parquet")),
        filesBefore::toString);

    assertThrows(
        UnsupportedLayoutException.class,
        () -> newPatientSource("n1").write().saveMode("append").parquet(directory.toString()));

    assertEquals(filesBefore, files(directory));
  }

  /**
   * An append beside a partition file named as earlier releases named them, {@code
   * Patient.00000.parquet}, is refused too.
   */
  @Test
  void parquetAppendBesidePreviousLayoutFilesNamedByEarlierReleasesIsRefused() throws IOException {
    placeAsEarlierReleaseFile(previousPatients(spark, "p1"), "Patient.00000.parquet");
    final List<Path> filesBefore = files(directory);

    assertThrows(
        UnsupportedLayoutException.class,
        () -> newPatientSource("n1").write().saveMode("append").parquet(directory.toString()));

    assertEquals(filesBefore, files(directory));
  }

  /**
   * An overwrite replaces previous-layout Parquet files, whether named by this release or an
   * earlier one, so the directory then reads as the new layout alone.
   */
  @Test
  void parquetOverwriteReplacesPreviousLayoutFiles() throws IOException {
    context
        .read()
        .datasets()
        .dataset("Patient", previousPatients(spark, "p1"))
        .write()
        .parquet(directory.toString());
    placeAsEarlierReleaseFile(previousPatients(spark, "p0"), "Patient.00000.parquet");

    newPatientSource("n1", "n2").write().saveMode("overwrite").parquet(directory.toString());

    final Dataset<Row> patients = context.read().parquet(directory.toString()).read("Patient");
    assertEquals(List.of("n1", "n2"), ids(patients));
    assertFalse(List.of(patients.schema().fieldNames()).contains("id_versioned"));
  }

  /** An append beside new-layout Parquet files is not refused, and keeps the rows of both. */
  @Test
  void parquetAppendBesideNewLayoutSucceeds() {
    newPatientSource("n1").write().parquet(directory.toString());

    assertDoesNotThrow(
        () -> newPatientSource("n2").write().saveMode("append").parquet(directory.toString()));

    assertEquals(
        List.of("n1", "n2"), ids(context.read().parquet(directory.toString()).read("Patient")));
  }

  // The source boundary.

  /**
   * Previous-layout data is refused where it is read, so a write from a source holding it to any
   * sink, NDJSON included, is refused before anything is written, whichever of the source's types
   * holds it and so whatever order the types are written in.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void previousLayoutSourceIsRefusedBeforeAnySinkWrites(final boolean previousPatients) {
    final Path source = directory.resolve("source");
    writeParquet(
        previousPatients ? previousPatients(spark, "p1") : newPatients(context, "n1"),
        source,
        PATIENT_TABLE);
    writeParquet(
        previousPatients ? newConditions(context, "c1") : previousConditions(spark, "c1"),
        source,
        CONDITION_TABLE);
    final Path target = directory.resolve("target");

    assertThrows(
        UnsupportedLayoutException.class,
        () -> context.read().parquet(source.toString()).write().ndjson(target.toString()));

    assertFalse(Files.exists(target));
  }

  // Helpers.

  /**
   * Returns the version count, the schema and the contents of a Delta table, to compare before and
   * after a write.
   */
  @Nonnull
  private static List<Object> deltaState(@Nonnull final Path table) {
    final DeltaTable delta = DeltaTable.forPath(spark, table.toString());
    return List.of(delta.history().count(), delta.toDF().schema().json(), ids(delta.toDF()));
  }

  private static long catalogVersions(@Nonnull final String database) {
    return DeltaTable.forName(spark, database + ".Patient").history().count();
  }

  private static void assertRefusalOfPreviousPatients(
      @Nonnull final UnsupportedLayoutException error, @Nonnull final List<Object> before) {
    assertEquals("Patient", error.getResourceType());
    assertEquals(StorageLayout.PREVIOUS, error.getDetection().getLayout());
    assertTrue(
        error
            .getMessage()
            .startsWith(
                "The Patient data is stored in the layout written by earlier releases of Pathling"),
        error.getMessage());
    assertTrue(((String) before.get(1)).contains("id_versioned"));
  }

  /**
   * Writes a dataset as a single Parquet file in the directory, under a name an earlier release
   * would have given one of its partition files.
   */
  private void placeAsEarlierReleaseFile(
      @Nonnull final Dataset<Row> dataset, @Nonnull final String fileName) throws IOException {
    final Path written = Files.createTempDirectory(directory.getParent(), "written");
    dataset.coalesce(1).write().mode("overwrite").parquet(written.toString());
    try (final Stream<Path> parts = Files.list(written)) {
      Files.move(
          parts
              .filter(file -> file.getFileName().toString().endsWith(".parquet"))
              .findFirst()
              .orElseThrow(),
          directory.resolve(fileName));
    }
  }

  @Nonnull
  private static DatasetSource newPatientSource(@Nonnull final String... ids) {
    return context.read().datasets().dataset("Patient", newPatients(context, ids));
  }

  @Nonnull
  private static List<String> ids(@Nonnull final Dataset<Row> dataset) {
    return dataset.select("id").as(Encoders.STRING()).collectAsList().stream().sorted().toList();
  }

  @Nonnull
  private static List<Path> files(@Nonnull final Path root) throws IOException {
    try (final Stream<Path> walk = Files.walk(root)) {
      return walk.filter(Files::isRegularFile).sorted().toList();
    }
  }

  @Nonnull
  private static String database() {
    final String database = "db_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql("CREATE DATABASE " + database);
    return database;
  }
}
