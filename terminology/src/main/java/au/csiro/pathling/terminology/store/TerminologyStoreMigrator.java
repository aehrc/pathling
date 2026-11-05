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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CANONICAL_URL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_MAP_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_RESOURCE_JSON;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT_MAPPING;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.ENTRY_TYPE_CONCEPT_MAP;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.STORE_FORMAT_VERSION;

import au.csiro.pathling.terminology.store.FhirTerminologyImporter.StagedConceptMap;
import au.csiro.pathling.terminology.store.TerminologyStoreWriter.CanonicalVersion;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

/**
 * Migrates a terminology store written in an earlier format version to the current one, in place.
 * It runs on the driver before the store is opened, and applies an ordered list of steps, each of
 * which brings the manifest entries of one format version up to the next. A step writes only where
 * it has entries to migrate, or content of an earlier layout to remove, so a current store, or one
 * whose only earlier entries need no change, is never written and may be read-only.
 *
 * <p>Each step orders its writes so that an interrupted migration is completed by running it again:
 * content first, then the manifest entries that record it, then the removal of what it replaces.
 *
 * @author John Grimes
 */
@Slf4j
public final class TerminologyStoreMigrator {

  private static final String SNOMED_URI = "http://snomed.info/sct";

  /** The table in which format version 1 stored each ConceptMap as a single JSON document. */
  private static final String FORMAT_ONE_CONCEPT_MAP_TABLE = "concept_map";

  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  /**
   * The migration steps in order: the step at index {@code i} migrates the entries of format
   * version {@code i + 1} to version {@code i + 2}. Adding a format version means adding its step
   * here.
   */
  private static final List<Step> STEPS = List.of(TerminologyStoreMigrator::migrateFormatOne);

  /** One migration step, from one format version to the next. */
  @FunctionalInterface
  private interface Step {

    /**
     * Applies the step.
     *
     * @param store the store being migrated
     * @param manifest the manifest of the store before the step
     * @return the manifest of the store after the step
     * @throws IOException if the store cannot be read or written
     */
    @Nonnull
    List<ManifestEntry> apply(@Nonnull Store store, @Nonnull List<ManifestEntry> manifest)
        throws IOException;
  }

  /** The store being migrated, with the means to read and write it. */
  private record Store(
      @Nonnull SparkSession spark,
      @Nonnull String storagePath,
      @Nonnull TerminologyStoreReader reader,
      @Nonnull TerminologyStoreWriter writer) {}

  private TerminologyStoreMigrator() {
    // Utility class.
  }

  /**
   * Migrates the store at a path to the current format version. A store that cannot be read, or
   * that was written by a later version of Pathling, is left to {@link TerminologyStoreReader#open}
   * to report.
   *
   * @param spark the Spark session used to write the store
   * @param storagePath the root path of the store
   * @param hadoopConfiguration a snapshot of the Hadoop configuration, used to read the store
   * @throws TerminologyStoreException if the store needs migrating and cannot be migrated
   */
  public static void migrate(
      @Nonnull final SparkSession spark,
      @Nonnull final String storagePath,
      @Nonnull final Map<String, String> hadoopConfiguration) {
    final TerminologyStoreReader reader =
        TerminologyStoreReader.unchecked(storagePath, hadoopConfiguration);
    List<ManifestEntry> manifest;
    try {
      manifest = reader.readManifest();
    } catch (final TerminologyStoreException e) {
      // Opening the store reports why it cannot be read.
      return;
    }
    if (manifest.stream().anyMatch(entry -> entry.getStoreFormatVersion() > STORE_FORMAT_VERSION)) {
      return;
    }
    final Store store =
        new Store(spark, storagePath, reader, new TerminologyStoreWriter(spark, storagePath));
    for (int i = 0; i < STEPS.size(); i++) {
      try {
        manifest = STEPS.get(i).apply(store, manifest);
      } catch (final Exception e) {
        // Spark and Delta are written in Scala and may throw checked exceptions undeclared, such as
        // a SparkException from a failed write task, so every exception is caught here.
        throw new TerminologyStoreException(
            "Unable to migrate the terminology store at '"
                + storagePath
                + "' from format version "
                + (i + 1)
                + " to "
                + (i + 2)
                + ": "
                + e.getMessage()
                + ". Retry with write access to the store to complete the migration, or re-import"
                + " its content into a new store.",
            e);
      }
    }
  }

  /**
   * Migrates format version 1 to 2, which replaced the table holding each ConceptMap as a single
   * JSON document with one row per mapping. Each format 1 ConceptMap is flattened from its stored
   * JSON as an import of it would be, and its manifest entry is stamped with format version 2 and
   * the fault an import would record, keeping its provenance. The former table is removed last.
   *
   * <p>Code systems and value sets did not change layout, so their entries keep format version 1. A
   * SNOMED CT edition imported in format 1 lacks the targets of its simple map reference sets,
   * which were not stored and cannot be recovered, so it is reported on every migration until it is
   * re-imported.
   */
  @Nonnull
  private static List<ManifestEntry> migrateFormatOne(
      @Nonnull final Store store, @Nonnull final List<ManifestEntry> manifest) throws IOException {
    warnAboutFormatOneSnomedEditions(store.storagePath(), manifest);
    final Map<CanonicalVersion, ManifestEntry> conceptMaps =
        manifest.stream()
            .filter(TerminologyStoreMigrator::isFormatOneConceptMap)
            .collect(
                Collectors.toMap(
                    TerminologyStoreMigrator::key,
                    Function.identity(),
                    (first, second) -> first,
                    LinkedHashMap::new));
    if (conceptMaps.isEmpty()) {
      // A migration interrupted before its last step, or a store whose maps were all re-imported
      // before it was migrated, leaves the former table with nothing recording content in it.
      removeFormatOneConceptMapTable(store);
      return manifest;
    }
    log.info(
        "Migrating {} ConceptMap(s) in the terminology store at {} from format version 1 to 2",
        conceptMaps.size(),
        store.storagePath());

    final Map<CanonicalVersion, StagedConceptMap> staged = new LinkedHashMap<>();
    try (ConceptMapStaging staging = ConceptMapStaging.create()) {
      store
          .reader()
          .readTableIfPresent(
              FORMAT_ONE_CONCEPT_MAP_TABLE,
              row -> {
                final CanonicalVersion key =
                    new CanonicalVersion(
                        row.getString(COLUMN_CANONICAL_URL), row.getString(COLUMN_VERSION));
                // A row whose map has since been re-imported in the current layout is stale.
                if (conceptMaps.containsKey(key) && !staged.containsKey(key)) {
                  staged.put(key, stage(key, row, staging));
                }
              });
      requireAllStaged(conceptMaps, staged);
      staging.sealForReading();
      store
          .writer()
          .writePartitions(
              FhirTerminologyImporter.conceptMappings(store.spark(), staging, staged.values()),
              CONCEPT_MAPPING,
              COLUMN_CONCEPT_MAP_ID,
              staged.values().stream().map(StagedConceptMap::conceptMapId).toList());
    }

    // The upsert replaces every entry sharing a key with the entries written, so any other entry
    // under the key of a migrated map is written back unchanged.
    store
        .writer()
        .upsertManifestEntries(
            manifest.stream()
                .filter(entry -> conceptMaps.containsKey(key(entry)))
                .map(
                    entry ->
                        isFormatOneConceptMap(entry)
                            ? entry
                                .withStoreFormatVersion(2)
                                .withFault(staged.get(key(entry)).fault())
                            : entry)
                .toList());
    removeFormatOneConceptMapTable(store);
    log.info(
        "Migrated {} ConceptMap(s) in the terminology store at {} to format version 2",
        conceptMaps.size(),
        store.storagePath());
    return store.reader().readManifest();
  }

  /** Flattens the stored JSON of a format 1 ConceptMap into the staging. */
  @Nonnull
  private static StagedConceptMap stage(
      @Nonnull final CanonicalVersion key,
      @Nonnull final TerminologyStoreRow row,
      @Nonnull final ConceptMapStaging staging) {
    try (JsonParser parser = JSON_FACTORY.createParser(row.getString(COLUMN_RESOURCE_JSON))) {
      return FhirTerminologyImporter.flattenConceptMap(parser, key.url(), key.version(), staging);
    } catch (final IOException | RuntimeException e) {
      throw new TerminologyStoreException(
          "the stored ConceptMap " + key.url() + " cannot be read: " + e.getMessage(), e);
    }
  }

  /** Rejects a migration that would lose a ConceptMap whose stored JSON is missing. */
  private static void requireAllStaged(
      @Nonnull final Map<CanonicalVersion, ManifestEntry> conceptMaps,
      @Nonnull final Map<CanonicalVersion, StagedConceptMap> staged) {
    final List<String> missing =
        conceptMaps.keySet().stream()
            .filter(key -> !staged.containsKey(key))
            .map(CanonicalVersion::url)
            .sorted()
            .toList();
    if (!missing.isEmpty()) {
      throw new TerminologyStoreException(
          "the manifest records ConceptMaps whose content is missing from the store: "
              + String.join(", ", missing));
    }
  }

  /**
   * Removes the former ConceptMap table, if present, once nothing records content in it. A failure
   * is reported but not raised, since the table is never read again, and a read-only store must
   * still open.
   */
  private static void removeFormatOneConceptMapTable(@Nonnull final Store store) {
    final Path table =
        new Path(
            TerminologyStoreSchema.tablePath(store.storagePath(), FORMAT_ONE_CONCEPT_MAP_TABLE));
    try {
      final FileSystem fs = table.getFileSystem(store.spark().sessionState().newHadoopConf());
      if (fs.exists(table) && !fs.delete(table, true)) {
        throw new IOException("the file system did not delete it");
      }
    } catch (final IOException e) {
      log.warn(
          "Unable to remove the former ConceptMap table at {}, which is no longer read and may be"
              + " deleted: {}",
          table,
          e.getMessage());
    }
  }

  private static void warnAboutFormatOneSnomedEditions(
      @Nonnull final String storagePath, @Nonnull final List<ManifestEntry> manifest) {
    final List<String> editions =
        manifest.stream()
            .filter(entry -> entry.getStoreFormatVersion() == 1)
            .filter(entry -> "code_system".equals(entry.getEntryType()))
            .filter(entry -> SNOMED_URI.equals(entry.getCanonicalUrl()))
            .map(entry -> String.valueOf(entry.getVersion()))
            .sorted()
            .toList();
    if (!editions.isEmpty()) {
      log.warn(
          "The terminology store at {} holds SNOMED CT versions imported by Pathling 9.9.0 or"
              + " earlier, which did not store the targets of simple map reference sets: {}."
              + " Translation through those reference sets finds nothing until the versions are"
              + " re-imported.",
          storagePath,
          String.join(", ", editions));
    }
  }

  private static boolean isFormatOneConceptMap(@Nonnull final ManifestEntry entry) {
    return entry.getStoreFormatVersion() == 1
        && ENTRY_TYPE_CONCEPT_MAP.equals(entry.getEntryType());
  }

  @Nonnull
  private static CanonicalVersion key(@Nonnull final ManifestEntry entry) {
    return new CanonicalVersion(entry.getCanonicalUrl(), entry.getVersion());
  }
}
