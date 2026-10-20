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

package au.csiro.pathling.terminology.store;

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT_MAPPING;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.ENTRY_TYPE_CONCEPT_MAP;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.MANIFEST;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.STORE_FORMAT_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.tablePath;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * Tests the migration of a terminology store to the current format version. The format 1 store is a
 * fixture written by Pathling 9.9.0 (see {@code terminology-store/format-1/generate.py}), holding a
 * SNOMED CT edition, a FHIR CodeSystem and ValueSet, and three ConceptMaps, one of which carries
 * content a concept map relation cannot. The expected result of migrating a ConceptMap is what a
 * fresh import of the same source writes.
 *
 * @author John Grimes
 */
class TerminologyStoreMigratorTest {

  private static final String FAULTED_MAP = "http://example.org/ConceptMap/faulted";
  private static final String SNOMED_VERSION =
      "http://snomed.info/sct/900000000000207008/version/20240601";

  /** The sources the format 1 fixture was imported from, in import order. */
  private static final List<String> FHIR_SOURCES =
      List.of(
          "/conceptmap/sct-to-icd10.ConceptMap.json",
          "/fhir-import/bundle-mixed.json",
          "/terminology-store/format-1/faulted.ConceptMap.json");

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir final Path warehouse) {
    spark =
        SparkSession.builder()
            .appName("TerminologyStoreMigratorTest")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", warehouse.toString())
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  // --- Migrating ConceptMaps. ---

  @Test
  void migratesConceptMapsToTheMappingsAFreshImportWrites(@TempDir final Path dir) {
    // The mappings of each migrated ConceptMap are exactly those a fresh import of its source
    // writes, so translation through a migrated map behaves as through a re-imported one.
    final String store = formatOneStore(dir);
    final String fresh = freshImport(dir);

    migrate(store);

    assertEquals(sortedRows(fresh, CONCEPT_MAPPING), sortedRows(store, CONCEPT_MAPPING));
    assertDoesNotThrow(() -> TerminologyStoreReader.open(store, Map.of()));
  }

  @Test
  void stampsMigratedConceptMapsWithTheCurrentFormatAndKeepsTheirProvenance(
      @TempDir final Path dir) {
    // A migrated ConceptMap entry is current, records the same fault a fresh import records, and
    // keeps the source and import time of the import that originally wrote it.
    final String store = formatOneStore(dir);
    final Map<String, ManifestEntry> before = conceptMapEntries(manifest(store));
    final Map<String, ManifestEntry> fresh = conceptMapEntries(manifest(freshImport(dir)));

    migrate(store);

    final Map<String, ManifestEntry> after = conceptMapEntries(manifest(store));
    assertEquals(before.keySet(), after.keySet());
    for (final String url : after.keySet()) {
      final ManifestEntry entry = after.get(url);
      assertEquals(STORE_FORMAT_VERSION, entry.getStoreFormatVersion(), url);
      assertEquals(before.get(url).getVersion(), entry.getVersion(), url);
      assertEquals(before.get(url).getSource(), entry.getSource(), url);
      assertEquals(before.get(url).getImportedAt(), entry.getImportedAt(), url);
      assertEquals(fresh.get(url).getFault(), entry.getFault(), url);
    }
    assertNotNull(after.get(FAULTED_MAP).getFault(), "the faulted map should record its fault");
  }

  @Test
  void leavesEntriesOtherThanConceptMapsAsTheyWere(@TempDir final Path dir) {
    // Code systems and value sets did not change layout, so their entries are not rewritten; the
    // SNOMED CT edition in particular stays at format 1, since its simple map targets were never
    // stored and cannot be recovered.
    final String store = formatOneStore(dir);
    final List<ManifestEntry> before = otherEntries(manifest(store));

    migrate(store);

    assertEquals(before, otherEntries(manifest(store)));
    assertTrue(before.stream().allMatch(entry -> entry.getStoreFormatVersion() == 1));
  }

  @Test
  void removesTheFormerConceptMapTable(@TempDir final Path dir) {
    final String store = formatOneStore(dir);
    assertTrue(Files.isDirectory(Path.of(store, "concept_map")));

    migrate(store);

    assertFalse(Files.exists(Path.of(store, "concept_map")));
  }

  @Test
  void removesALeftoverFormerTableOnceNothingRecordsContentInIt(@TempDir final Path dir) {
    // A migration interrupted after the manifest was written, or a store whose maps were all
    // re-imported before it was migrated, leaves the former table with nothing recording content
    // in it. Opening the store again removes it.
    final String store = formatOneStore(dir);
    migrate(store);
    copyTree(
        resource("/terminology-store/format-1/store/concept_map"), Path.of(store, "concept_map"));
    final long commits = manifestCommits(store);

    migrate(store);

    assertFalse(Files.exists(Path.of(store, "concept_map")));
    assertEquals(commits, manifestCommits(store));
  }

  @Test
  void keepsAConceptMapReimportedBeforeTheStoreWasMigrated(@TempDir final Path dir)
      throws IOException {
    // Importing into a format 1 store succeeds and writes the map in the current layout. The map is
    // re-imported with its first target changed from "x"; its stale copy in the former table must
    // not overwrite it when the rest of the store migrates.
    final String store = formatOneStore(dir);
    final Path changed = dir.resolve("faulted-changed.json");
    Files.writeString(
        changed,
        Files.readString(resource("/terminology-store/format-1/faulted.ConceptMap.json"))
            .replace("\"code\": \"x\"", "\"code\": \"changed\""));
    new FhirTerminologyImporter(spark, store).importFrom(changed.toString(), false, null);

    migrate(store);

    final String faultedId = TerminologyStoreSchema.conceptMapId(FAULTED_MAP, null);
    final List<String> targets =
        spark
            .read()
            .format("delta")
            .load(tablePath(store, CONCEPT_MAPPING))
            .filter(col(TerminologyStoreSchema.COLUMN_CONCEPT_MAP_ID).equalTo(faultedId))
            .select(TerminologyStoreSchema.COLUMN_TARGET_CODE)
            .collectAsList()
            .stream()
            .map(row -> row.getString(0))
            .sorted()
            .toList();
    assertEquals(List.of("changed", "y"), targets);
    assertEquals(
        changed.toString(), conceptMapEntries(manifest(store)).get(FAULTED_MAP).getSource());
  }

  // --- Stores with nothing to migrate. ---

  @Test
  void writesNothingToACurrentStore(@TempDir final Path dir) {
    // A current store is opened read-only in many deployments, so migration must not write to it.
    final String store = freshImport(dir);
    final long commits = manifestCommits(store);

    withReadOnly(Path.of(store), () -> assertDoesNotThrow(() -> migrate(store)));

    assertEquals(commits, manifestCommits(store));
  }

  @Test
  void writesNothingToAFormatOneStoreWithoutConceptMaps(@TempDir final Path dir) {
    // A format 1 store without ConceptMaps opened before migration existed; it must still open
    // without write access.
    final String store = dir.resolve("store").toString();
    writeFormatOneManifestRow(store, "value_set", "http://example.org/vs", "1.0.0");
    final long commits = manifestCommits(store);

    withReadOnly(Path.of(store), () -> assertDoesNotThrow(() -> migrate(store)));

    assertEquals(commits, manifestCommits(store));
  }

  @Test
  void leavesAMissingStoreToTheReader(@TempDir final Path dir) {
    // Opening reports a missing store; migration has nothing to say about it.
    assertDoesNotThrow(() -> migrate(dir.resolve("missing").toString()));
  }

  @Test
  void leavesANewerStoreToTheReader(@TempDir final Path dir) {
    // A store written by a later version may hold format 1 ConceptMap entries alongside newer
    // ones only in theory, but it is never this version's to rewrite.
    final String store = formatOneStore(dir);
    final ManifestEntry future =
        new ManifestEntry(
            STORE_FORMAT_VERSION + 1,
            "code_system",
            "http://example.org/future",
            "1",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    new TerminologyStoreWriter(spark, store).upsertManifestEntry(future);
    final long commits = manifestCommits(store);

    assertDoesNotThrow(() -> migrate(store));

    assertEquals(commits, manifestCommits(store));
    assertTrue(Files.isDirectory(Path.of(store, "concept_map")));
  }

  // --- SNOMED CT editions imported by an earlier release. ---

  @Test
  void warnsAboutSnomedEditionsImportedByAnEarlierRelease(@TempDir final Path dir) {
    // The warning names the edition and is repeated on every open until it is re-imported, since
    // the edition stays at format 1 after migration.
    final String store = formatOneStore(dir);

    final List<String> first = warnings(() -> migrate(store));
    final List<String> second = warnings(() -> migrate(store));

    for (final List<String> warnings : List.of(first, second)) {
      assertEquals(1, warnings.size(), warnings.toString());
      assertTrue(warnings.get(0).contains(SNOMED_VERSION), warnings.get(0));
      assertTrue(warnings.get(0).contains("re-import"), warnings.get(0));
    }
  }

  @Test
  void doesNotWarnAboutCurrentSnomedEditions(@TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    new TerminologyStoreWriter(spark, store)
        .upsertManifestEntry(
            ManifestEntry.forImport(
                "code_system",
                "http://snomed.info/sct",
                SNOMED_VERSION,
                ImportProvenance.of("rf2.zip", null),
                Instant.now()));

    assertEquals(List.of(), warnings(() -> migrate(store)));
  }

  // --- Failure. ---

  @Test
  void failsWithAnActionableErrorWhenTheStoreCannotBeWrittenAndCompletesOnRetry(
      @TempDir final Path dir) {
    // A store without write access cannot be migrated; the error says so and what to do, and the
    // store is left as it was, so a retry with write access completes the migration.
    final String store = formatOneStore(dir);
    final List<String> before = sortedRows(store, MANIFEST);

    final TerminologyStoreException e =
        withReadOnly(
            Path.of(store),
            () -> assertThrows(TerminologyStoreException.class, () -> migrate(store)));

    assertTrue(e.getMessage().contains(store), e.getMessage());
    assertTrue(e.getMessage().contains("format version 1 to 2"), e.getMessage());
    assertTrue(e.getMessage().contains("write access"), e.getMessage());
    assertTrue(e.getMessage().contains("re-import"), e.getMessage());
    assertEquals(before, sortedRows(store, MANIFEST));

    migrate(store);

    assertDoesNotThrow(() -> TerminologyStoreReader.open(store, Map.of()));
    assertEquals(sortedRows(freshImport(dir), CONCEPT_MAPPING), sortedRows(store, CONCEPT_MAPPING));
  }

  @Test
  void failsWithoutWritingWhenAConceptMapIsMissingFromTheFormerTable(@TempDir final Path dir) {
    // A manifest entry whose ConceptMap content is absent cannot be migrated; the store is left
    // untouched rather than migrated without it.
    final String store = formatOneStore(dir);
    writeFormatOneManifestRow(store, ENTRY_TYPE_CONCEPT_MAP, "http://example.org/lost", "1");
    final long commits = manifestCommits(store);

    final TerminologyStoreException e =
        assertThrows(TerminologyStoreException.class, () -> migrate(store));

    assertTrue(e.getMessage().contains("http://example.org/lost"), e.getMessage());
    assertEquals(commits, manifestCommits(store));
    assertFalse(Files.exists(Path.of(store, CONCEPT_MAPPING)));
    assertTrue(Files.isDirectory(Path.of(store, "concept_map")));
  }

  @Test
  void completesAMigrationInterruptedAfterTheMappingsWereWritten(@TempDir final Path dir) {
    // With only the manifest read-only, the mappings are written and the manifest write then fails
    // inside a Spark task. The failure is reported as a failed migration, and opening the store
    // again with write access completes it.
    final String store = formatOneStore(dir);
    final List<String> before = sortedRows(store, MANIFEST);

    final TerminologyStoreException e =
        withReadOnly(
            Path.of(store, MANIFEST),
            () -> assertThrows(TerminologyStoreException.class, () -> migrate(store)));

    assertTrue(e.getMessage().contains(store), e.getMessage());
    assertTrue(e.getMessage().contains("format version 1 to 2"), e.getMessage());
    assertTrue(e.getMessage().contains("write access"), e.getMessage());
    assertTrue(Files.isDirectory(Path.of(store, CONCEPT_MAPPING)));
    assertEquals(before, sortedRows(store, MANIFEST));

    migrate(store);

    assertDoesNotThrow(() -> TerminologyStoreReader.open(store, Map.of()));
    assertEquals(sortedRows(freshImport(dir), CONCEPT_MAPPING), sortedRows(store, CONCEPT_MAPPING));
  }

  // --- Helpers. ---

  private static void migrate(@Nonnull final String store) {
    TerminologyStoreMigrator.migrate(spark, store, Map.of());
  }

  /** Copies the format 1 fixture into a scratch directory, so that a test may change it. */
  @Nonnull
  private static String formatOneStore(@Nonnull final Path dir) {
    final Path target = dir.resolve("store");
    copyTree(resource("/terminology-store/format-1/store"), target);
    return target.toString();
  }

  private static void copyTree(@Nonnull final Path source, @Nonnull final Path target) {
    try (Stream<Path> paths = Files.walk(source)) {
      for (final Path path : paths.toList()) {
        Files.copy(path, target.resolve(source.relativize(path).toString()));
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Imports the sources of the format 1 fixture's FHIR content into a new current store. */
  @Nonnull
  private static String freshImport(@Nonnull final Path dir) {
    final String store = dir.resolve("fresh-" + System.nanoTime()).toString();
    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store);
    for (final String source : FHIR_SOURCES) {
      importer.importFrom(resource(source).toString(), false, null);
    }
    return store;
  }

  @Nonnull
  private static Path resource(@Nonnull final String name) {
    try {
      return Path.of(
          Objects.requireNonNull(TerminologyStoreMigratorTest.class.getResource(name), name)
              .toURI());
    } catch (final URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }

  @Nonnull
  private static List<String> sortedRows(@Nonnull final String store, @Nonnull final String table) {
    return spark.read().format("delta").load(tablePath(store, table)).collectAsList().stream()
        .map(Row::toString)
        .sorted()
        .toList();
  }

  @Nonnull
  private static List<ManifestEntry> manifest(@Nonnull final String store) {
    return TerminologyStoreReader.unchecked(store, Map.of()).readManifest();
  }

  @Nonnull
  private static Map<String, ManifestEntry> conceptMapEntries(
      @Nonnull final List<ManifestEntry> manifest) {
    return manifest.stream()
        .filter(entry -> ENTRY_TYPE_CONCEPT_MAP.equals(entry.getEntryType()))
        .collect(Collectors.toMap(ManifestEntry::getCanonicalUrl, Function.identity()));
  }

  @Nonnull
  private static List<ManifestEntry> otherEntries(@Nonnull final List<ManifestEntry> manifest) {
    return manifest.stream()
        .filter(entry -> !ENTRY_TYPE_CONCEPT_MAP.equals(entry.getEntryType()))
        .sorted(Comparator.comparing(ManifestEntry::getCanonicalUrl))
        .toList();
  }

  private static long manifestCommits(@Nonnull final String store) {
    return DeltaTable.forPath(spark, tablePath(store, MANIFEST)).history().count();
  }

  /** Appends a manifest row in the six-column schema that Pathling 9.9.0 wrote. */
  private static void writeFormatOneManifestRow(
      @Nonnull final String store,
      @Nonnull final String entryType,
      @Nonnull final String url,
      @Nonnull final String version) {
    final StructType schema =
        new StructType()
            .add(TerminologyStoreSchema.COLUMN_STORE_FORMAT_VERSION, DataTypes.IntegerType, false)
            .add(TerminologyStoreSchema.COLUMN_ENTRY_TYPE, DataTypes.StringType, false)
            .add(TerminologyStoreSchema.COLUMN_CANONICAL_URL, DataTypes.StringType, false)
            .add(TerminologyStoreSchema.COLUMN_VERSION, DataTypes.StringType, true)
            .add(TerminologyStoreSchema.COLUMN_SOURCE, DataTypes.StringType, true)
            .add(TerminologyStoreSchema.COLUMN_IMPORTED_AT, DataTypes.TimestampType, true);
    new TerminologyStoreWriter(spark, store)
        .writeTable(
            spark.createDataFrame(
                List.of(
                    RowFactory.create(
                        1, entryType, url, version, "legacy.json", Timestamp.from(Instant.now()))),
                schema),
            MANIFEST,
            SaveMode.Append,
            List.of());
  }

  /** Runs an action with every directory of a store read-only, restoring write access after. */
  private static <T> T withReadOnly(
      @Nonnull final Path store, @Nonnull final java.util.function.Supplier<T> action) {
    setDirectoriesWritable(store, false);
    try {
      return action.get();
    } finally {
      setDirectoriesWritable(store, true);
    }
  }

  private static void withReadOnly(@Nonnull final Path store, @Nonnull final Runnable action) {
    withReadOnly(
        store,
        () -> {
          action.run();
          return null;
        });
  }

  private static void setDirectoriesWritable(@Nonnull final Path root, final boolean writable) {
    try (Stream<Path> paths = Files.walk(root)) {
      for (final Path path : paths.filter(Files::isDirectory).toList()) {
        Files.setPosixFilePermissions(
            path, PosixFilePermissions.fromString(writable ? "rwx------" : "r-x------"));
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Returns the messages of the warnings the migrator logs while an action runs. */
  @Nonnull
  private static List<String> warnings(@Nonnull final Runnable action) {
    final Logger logger = (Logger) LoggerFactory.getLogger(TerminologyStoreMigrator.class);
    final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      action.run();
    } finally {
      logger.detachAppender(appender);
    }
    return appender.list.stream()
        .filter(event -> event.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }
}
