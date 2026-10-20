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

package au.csiro.pathling.library;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.DefaultTerminologyServiceFactory;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.local.LocalTerminologyServiceFactory;
import au.csiro.pathling.terminology.store.ManifestEntry;
import au.csiro.pathling.terminology.store.TerminologyStoreSchema;
import au.csiro.pathling.terminology.store.TerminologyStoreWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that {@link PathlingContext} selects the terminology backend from the configured mode:
 * server mode uses the remote factory, local mode uses the local factory, and local mode validates
 * the store eagerly at context creation so failures surface immediately with a clear message.
 *
 * @author John Grimes
 */
class PathlingContextLocalModeTest {

  private static final String MAP_URL = "http://example.org/ConceptMap/legacy";

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark = TestHelpers.spark();
  }

  @AfterAll
  static void stopSpark() {
    LocalTerminologyServiceFactory.reset();
  }

  @AfterEach
  void resetLocalFactory() {
    LocalTerminologyServiceFactory.reset();
  }

  private String createStore(final Path dir, final int formatVersion) {
    final String store = dir.resolve("tx-store").toString();
    new TerminologyStoreWriter(spark, store)
        .writeManifest(
            List.of(
                new ManifestEntry(
                    formatVersion,
                    "code_system",
                    "http://snomed.info/sct",
                    "http://snomed.info/sct/900000000000207008/version/20250101",
                    "rf2.zip",
                    Instant.now(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null)),
            SaveMode.Append);
    return store;
  }

  private static TerminologyConfiguration localConfiguration(final String storagePath) {
    return TerminologyConfiguration.builder()
        .mode(TerminologyMode.LOCAL)
        .local(LocalTerminologyConfiguration.builder().storagePath(storagePath).build())
        .build();
  }

  @Test
  void serverModeUsesDefaultFactory() {
    final PathlingContext context =
        PathlingContext.builder(spark)
            .terminologyConfiguration(TerminologyConfiguration.builder().build())
            .build();

    assertInstanceOf(
        DefaultTerminologyServiceFactory.class, context.getTerminologyServiceFactory());
  }

  @Test
  void localModeUsesLocalFactory(@TempDir final Path dir) {
    final String store = createStore(dir, TerminologyStoreSchema.STORE_FORMAT_VERSION);

    final PathlingContext context =
        PathlingContext.builder(spark).terminologyConfiguration(localConfiguration(store)).build();

    assertInstanceOf(LocalTerminologyServiceFactory.class, context.getTerminologyServiceFactory());
  }

  @Test
  void localModeFailsWhenStoreMissing(@TempDir final Path dir) {
    final String missing = dir.resolve("does-not-exist").toString();

    final IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                PathlingContext.builder(spark)
                    .terminologyConfiguration(localConfiguration(missing))
                    .build());
    assertTrue(e.getMessage().contains(missing), "error should name the store path");
  }

  @Test
  void localModeFailsWhenStoreFormatTooNew(@TempDir final Path dir) {
    final String store = createStore(dir, TerminologyStoreSchema.STORE_FORMAT_VERSION + 1);

    final IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                PathlingContext.builder(spark)
                    .terminologyConfiguration(localConfiguration(store))
                    .build());
    assertTrue(
        e.getMessage().contains("format version"), "error should mention the format version");
  }

  @Test
  void localModeMigratesAStoreWrittenByAnEarlierRelease(@TempDir final Path dir) {
    // A store holding a ConceptMap in the layout of format version 1 is migrated when the context
    // is built, and translation then goes through the migrated map.
    final String store = createFormatOneStoreWithConceptMap(dir);

    final PathlingContext context =
        PathlingContext.builder(spark).terminologyConfiguration(localConfiguration(store)).build();

    final List<Translation> translations =
        context
            .getTerminologyServiceFactory()
            .build()
            .translate(new Coding("http://example.org/source", "a", null), MAP_URL, false, null);
    assertEquals(1, translations.size());
    assertEquals("x", translations.get(0).getConcept().getCode());
  }

  @Test
  void localModeFailsWhenMigrationFails(@TempDir final Path dir) {
    // A store that needs migrating but cannot be written fails context creation with a message
    // that names the store and says that migration is what failed.
    final String store = createFormatOneStoreWithConceptMap(dir);
    setDirectoriesWritable(Path.of(store), false);
    try {
      final IllegalStateException e =
          assertThrows(
              IllegalStateException.class,
              () ->
                  PathlingContext.builder(spark)
                      .terminologyConfiguration(localConfiguration(store))
                      .build());
      assertTrue(e.getMessage().contains(store), e.getMessage());
      assertTrue(e.getMessage().contains("Unable to migrate"), e.getMessage());
    } finally {
      setDirectoriesWritable(Path.of(store), true);
    }
  }

  /**
   * Writes a store as Pathling 9.9.0 did: a six-column manifest at format version 1 and a {@code
   * concept_map} table holding each ConceptMap as a single JSON document.
   */
  private String createFormatOneStoreWithConceptMap(final Path dir) {
    final String store = dir.resolve("tx-store").toString();
    final TerminologyStoreWriter writer = new TerminologyStoreWriter(spark, store);
    final StructType manifestSchema =
        new StructType()
            .add(TerminologyStoreSchema.COLUMN_STORE_FORMAT_VERSION, DataTypes.IntegerType, false)
            .add(TerminologyStoreSchema.COLUMN_ENTRY_TYPE, DataTypes.StringType, false)
            .add(TerminologyStoreSchema.COLUMN_CANONICAL_URL, DataTypes.StringType, false)
            .add(TerminologyStoreSchema.COLUMN_VERSION, DataTypes.StringType, true)
            .add(TerminologyStoreSchema.COLUMN_SOURCE, DataTypes.StringType, true)
            .add(TerminologyStoreSchema.COLUMN_IMPORTED_AT, DataTypes.TimestampType, true);
    final String json =
        """
        {"resourceType":"ConceptMap","url":"%s","version":"1","status":"active",
         "group":[{"source":"http://example.org/source","target":"http://example.org/target",
         "element":[{"code":"a","target":[{"code":"x","equivalence":"equivalent"}]}]}]}
        """
            .formatted(MAP_URL);
    writer.writeTable(
        spark.createDataFrame(
            List.of(RowFactory.create(MAP_URL, "1", json)),
            TerminologyStoreSchema.resourceTableSchema()),
        "concept_map",
        SaveMode.Overwrite,
        List.of());
    writer.writeTable(
        spark.createDataFrame(
            List.of(
                RowFactory.create(
                    1, "concept_map", MAP_URL, "1", "map.json", Timestamp.from(Instant.now()))),
            manifestSchema),
        TerminologyStoreSchema.MANIFEST,
        SaveMode.Append,
        List.of());
    return store;
  }

  private static void setDirectoriesWritable(final Path root, final boolean writable) {
    try (Stream<Path> paths = Files.walk(root)) {
      for (final Path path : paths.filter(Files::isDirectory).toList()) {
        Files.setPosixFilePermissions(
            path, PosixFilePermissions.fromString(writable ? "rwx------" : "r-x------"));
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  void localModeWithoutStoragePathFailsValidation() {
    // Configuration validation rejects local mode without a storage path before any store access.
    final TerminologyConfiguration config =
        TerminologyConfiguration.builder().mode(TerminologyMode.LOCAL).build();

    assertThrows(
        RuntimeException.class,
        () -> PathlingContext.builder(spark).terminologyConfiguration(config).build());
  }
}
