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
package au.csiro.pathling.library.io.source;

import static au.csiro.pathling.library.io.LayoutFixtures.PATIENT_TABLE;
import static au.csiro.pathling.library.io.LayoutFixtures.newPatients;
import static au.csiro.pathling.library.io.LayoutFixtures.previousPatients;
import static au.csiro.pathling.library.io.LayoutFixtures.writeDelta;
import static au.csiro.pathling.library.io.LayoutFixtures.writeParquet;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests layout detection as data is read through the Parquet, Delta and catalog sources (US7,
 * FR-037 to FR-040, T041 to T044).
 *
 * <p>The previous-layout data is written by the previous encoder, and the conforming data by
 * encoding FHIR JSON, so that each source is exercised on what is actually stored.
 *
 * @author Piotr Szul
 */
class FileSourceDetectionTest {

  /** The opening of the message refusing the previous encoder's Patients. */
  private static final String PREVIOUS_PATIENT_MESSAGE_START =
      "The Patient data is stored in the layout written by earlier releases of Pathling, "
          + "not the Parquet on FHIR layout this release expects. "
          + "Fields identifying the earlier layout: ";

  /** The remedy the message closes with, for data in the previous layout alone. */
  private static final String REMEDY =
      ". Re-import the data from its source with this release, into a new location or "
          + "by overwriting this one; "
          + "data in the earlier layout cannot be converted in place.";

  /** The remedy for data mixing the two layouts. */
  private static final String MIXED_REMEDY =
      "so it mixes the two layouts, as appending new data to earlier data does. "
          + "Re-import the data from its source with this release, into a new location or "
          + "by overwriting this one, because appending to or merging into this data is "
          + "refused, and data in the "
          + "earlier layout cannot be converted in place. Where the data is a directory of "
          + "files, the directory holds files of the earlier layout: remove them, or "
          + "re-import their data from its source.";

  @TempDir static Path root;

  private static SparkSession spark;
  private static PathlingContext context;

  /** The data each test reads, in each layout and through each kind of source. */
  private static Path previousParquet;

  private static Path previousDelta;
  private static Path newParquet;
  private static Path newDelta;
  private static String previousDatabase;
  private static String newDatabase;

  /** The kinds of source that classify the layout of what they read. */
  enum Kind {
    PARQUET,
    DELTA,
    CATALOG
  }

  /** Set up Spark with a warehouse of its own, and write the data in each layout. */
  @BeforeAll
  static void setUp() {
    spark =
        TestHelpers.sparkBuilder()
            .config("spark.sql.warehouse.dir", root.resolve("spark-warehouse").toString())
            .getOrCreate();
    context = PathlingContext.create(spark);

    previousParquet = root.resolve("previous-parquet");
    writeParquet(previousPatients(spark, "p1"), previousParquet, PATIENT_TABLE);
    previousDelta = root.resolve("previous-delta");
    writeDelta(previousPatients(spark, "p1"), previousDelta, PATIENT_TABLE);
    newParquet = root.resolve("new-parquet");
    writeParquet(newPatients(context, "n1"), newParquet, PATIENT_TABLE);
    newDelta = root.resolve("new-delta");
    writeDelta(newPatients(context, "n1"), newDelta, PATIENT_TABLE);

    previousDatabase = database();
    previousPatients(spark, "p1")
        .write()
        .format("delta")
        .saveAsTable(previousDatabase + ".Patient");
    newDatabase = database();
    newPatients(context, "n1").write().format("delta").saveAsTable(newDatabase + ".Patient");
  }

  /** Stop Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  // T041: conforming, sparse and unclassifiable data is read unchanged.

  /** Data in the Parquet on FHIR layout is read, and is the data that was written. */
  @ParameterizedTest
  @EnumSource(Kind.class)
  void conformingDataIsRead(@Nonnull final Kind kind) {
    final Dataset<Row> patients = source(kind, false, true).read("Patient");

    assertEquals(
        List.of("n1"),
        patients.select("id").as(org.apache.spark.sql.Encoders.STRING()).collectAsList());
  }

  /** A sparse but conforming schema, carrying only the root resource type, is read (FR-039). */
  @ParameterizedTest
  @EnumSource(Kind.class)
  void sparseConformingDataIsRead(@Nonnull final Kind kind) {
    final Dataset<Row> sparse = spark.sql("SELECT 'Patient' AS resourceType, 's1' AS id");

    assertEquals(1, read(kind, sparse).count());
  }

  /** A schema carrying no marker of either layout is read (FR-039). */
  @ParameterizedTest
  @EnumSource(Kind.class)
  void unclassifiableDataIsRead(@Nonnull final Kind kind) {
    final Dataset<Row> unclassifiable = spark.sql("SELECT 'u1' AS id, 'male' AS gender");

    assertEquals(1, read(kind, unclassifiable).count());
  }

  // T042: data in the previous layout is refused at read time with the actionable message.

  /**
   * Data written by the previous encoder is refused when it is read, with a message naming the
   * resource type, the detected layout, the expected layout, the evidence and the remedy.
   */
  @ParameterizedTest
  @EnumSource(Kind.class)
  void previousLayoutIsRefusedAtReadTime(@Nonnull final Kind kind) {
    final QueryableDataSource source = source(kind, true, true);
    // The refusal is made when the data is read, not when the source is created.
    assertEquals(Set.of("Patient"), source.getResourceTypes());

    final UnsupportedLayoutException error =
        assertThrows(UnsupportedLayoutException.class, () -> source.read("Patient"));

    assertEquals(
        PREVIOUS_PATIENT_MESSAGE_START
            + "id_versioned, meta.security._fid, meta.tag._fid, meta._fid, text._fid and 51 more"
            + REMEDY,
        error.getMessage());
    assertEquals("Patient", error.getResourceType());
    assertSame(StorageLayout.PREVIOUS, error.getDetection().getLayout());
  }

  /** Every read of a refused type is refused, not only the first. */
  @ParameterizedTest
  @EnumSource(Kind.class)
  void everyReadOfARefusedTypeIsRefused(@Nonnull final Kind kind) {
    final QueryableDataSource source = source(kind, true, true);

    assertThrows(UnsupportedLayoutException.class, () -> source.read("Patient"));
    assertThrows(UnsupportedLayoutException.class, () -> source.read("Patient"));
  }

  /** Operations that resolve every type, such as caching, are refused too. */
  @ParameterizedTest
  @EnumSource(Kind.class)
  void operationsOverEveryTypeAreRefused(@Nonnull final Kind kind) {
    final QueryableDataSource source = source(kind, true, true);

    assertThrows(UnsupportedLayoutException.class, () -> readThroughCache(source));
  }

  /**
   * A directory holding files of both layouts, as an append of new data beside earlier files
   * produces, is refused with the remedy for mixed data.
   */
  @Test
  void directoryMixingBothLayoutsIsRefused() {
    final Path mixed = root.resolve("mixed-parquet");
    writeParquet(previousPatients(spark, "p1"), mixed, PATIENT_TABLE);
    writeParquet(newPatients(context, "n1"), mixed, PATIENT_TABLE);

    final QueryableDataSource source = context.read().parquet(mixed.toString());

    final UnsupportedLayoutException error =
        assertThrows(UnsupportedLayoutException.class, () -> source.read("Patient"));

    assertTrue(error.getDetection().isMixed());
    assertTrue(error.getMessage().endsWith(MIXED_REMEDY), error.getMessage());
    assertTrue(
        error
            .getMessage()
            .contains("It also carries fields of the Parquet on FHIR layout (resourceType), "),
        error.getMessage());
  }

  // T043: the opt-out permits the read, and is scoped to one source.

  /** With detection disabled, data in the previous layout is read as stored. */
  @ParameterizedTest
  @EnumSource(Kind.class)
  void optOutPermitsTheRead(@Nonnull final Kind kind) {
    final Dataset<Row> patients = source(kind, true, false).read("Patient");

    assertTrue(List.of(patients.schema().fieldNames()).contains("id_versioned"));
    assertEquals(1, patients.count());
  }

  /**
   * Disabling detection for one source leaves another source over the same data, and the builder it
   * came from, detecting.
   */
  @ParameterizedTest
  @EnumSource(Kind.class)
  void optOutIsScopedToOneSource(@Nonnull final Kind kind) {
    final QueryableDataSource optedOut = source(kind, true, false);
    final QueryableDataSource detecting = source(kind, true, true);

    assertDoesNotThrow(() -> optedOut.read("Patient").count());
    assertThrows(UnsupportedLayoutException.class, () -> detecting.read("Patient"));
  }

  /** The opt-out survives the operations that build a new source from an existing one. */
  @ParameterizedTest
  @EnumSource(Kind.class)
  void optOutSurvivesDerivedSources(@Nonnull final Kind kind) {
    final QueryableDataSource source = source(kind, true, false);

    assertEquals(1, source.cache().read("Patient").count());
    assertEquals(1, source.filterByResourceType("Patient"::equals).read("Patient").count());
  }

  /** The builder detects by default, and disabling detection returns a new builder. */
  @Test
  void builderDetectsByDefault() {
    final DataSourceBuilder builder = context.read();

    assertTrue(builder.layoutDetection());
    assertNotSame(builder, builder.withLayoutDetection(false));
    assertTrue(builder.layoutDetection());
  }

  // T044: detection adds no file access to the read.

  /**
   * A read with detection enabled performs exactly the file access that the same read with it
   * disabled performs, because detection classifies the schema the read has already resolved
   * (FR-038).
   */
  @ParameterizedTest
  @EnumSource(
      value = Kind.class,
      names = {"PARQUET", "DELTA"})
  void detectionAddsNoFileAccess(@Nonnull final Kind kind) {
    // A first read warms whatever the session caches, so that both measured reads start alike.
    measureRead(kind, false);

    final List<Long> withoutDetection = measureRead(kind, false);
    final List<Long> withDetection = measureRead(kind, true);

    assertTrue(withDetection.get(0) > 0, "The measurement observed no file access.");
    assertEquals(withoutDetection, withDetection);
  }

  // Helpers.

  /**
   * Builds a source of the given kind over the Patients of either layout.
   *
   * @param kind the kind of source
   * @param previous whether to read the previous layout's data rather than the new layout's
   * @param layoutDetection whether the source classifies the layout of what it reads
   * @return the source
   */
  @Nonnull
  private static QueryableDataSource source(
      @Nonnull final Kind kind, final boolean previous, final boolean layoutDetection) {
    final DataSourceBuilder builder = context.read().withLayoutDetection(layoutDetection);
    return switch (kind) {
      case PARQUET -> builder.parquet((previous ? previousParquet : newParquet).toString());
      case DELTA -> builder.delta((previous ? previousDelta : newDelta).toString());
      case CATALOG -> builder.tables(previous ? previousDatabase : newDatabase);
    };
  }

  /**
   * Caches a source and reads the patients from it. A file source resolves every type as it is
   * cached, and a catalog source as the type is read, so either call may be refused.
   */
  @Nonnull
  private static Dataset<Row> readThroughCache(@Nonnull final QueryableDataSource source) {
    return source.cache().read("Patient");
  }

  /** Writes the data where a detecting source of the given kind reads it, and reads it back. */
  @Nonnull
  private static Dataset<Row> read(@Nonnull final Kind kind, @Nonnull final Dataset<Row> data) {
    final Path directory = root.resolve(UUID.randomUUID().toString());
    final Function<DataSourceBuilder, QueryableDataSource> open =
        switch (kind) {
          case PARQUET -> {
            writeParquet(data, directory, PATIENT_TABLE);
            yield builder -> builder.parquet(directory.toString());
          }
          case DELTA -> {
            writeDelta(data, directory, PATIENT_TABLE);
            yield builder -> builder.delta(directory.toString());
          }
          case CATALOG -> {
            final String database = database();
            data.write().format("delta").saveAsTable(database + ".Patient");
            yield builder -> builder.tables(database);
          }
        };
    return open.apply(context.read()).read("Patient");
  }

  /**
   * Reads the previous layout's Patients through a new source and returns the local file system's
   * bytes read, read operations and large read operations over the read.
   */
  @Nonnull
  private static List<Long> measureRead(@Nonnull final Kind kind, final boolean layoutDetection) {
    // Delta keeps the snapshot of a table it has read, so it is dropped, or the read would find
    // the schema without touching a file and there would be nothing to compare.
    DeltaLog.clearCache();
    final List<Long> before = fileStatistics();
    try {
      source(kind, true, layoutDetection).read("Patient");
    } catch (final UnsupportedLayoutException ignored) {
      // The refusal is the expected outcome with detection enabled; only the access is measured.
    }
    final List<Long> after = fileStatistics();
    return List.of(
        after.get(0) - before.get(0), after.get(1) - before.get(1), after.get(2) - before.get(2));
  }

  @Nonnull
  private static List<Long> fileStatistics() {
    final StorageStatistics statistics = FileSystem.getGlobalStorageStatistics().get("file");
    if (statistics == null) {
      return List.of(0L, 0L, 0L);
    }
    return List.of(
        value(statistics, "bytesRead"),
        value(statistics, "readOps"),
        value(statistics, "largeReadOps"));
  }

  private static long value(
      @Nonnull final StorageStatistics statistics, @Nonnull final String key) {
    final Long value = statistics.getLong(key);
    return value == null ? 0L : value;
  }

  @Nonnull
  private static String database() {
    final String database = "db_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql("CREATE DATABASE " + database);
    return database;
  }
}
