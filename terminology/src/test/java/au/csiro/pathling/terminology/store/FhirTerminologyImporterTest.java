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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CLOSURE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ACTIVE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ANCESTOR_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DESCENDANT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_PROPERTY_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TERM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VALUE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.DESCRIPTION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.test.FhirFixtures;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the FHIR terminology importer against the animal-species fixtures: a single JSON file, a
 * directory of resources, and a FHIR NPM package all load their CodeSystem, ValueSet, and
 * ConceptMap content with canonical URLs and versions captured, and an invalid resource is rejected
 * without touching the store.
 *
 * @author John Grimes
 */
class FhirTerminologyImporterTest {

  private static SparkSession spark;

  @BeforeAll
  static void setUp(@TempDir final Path warehouse) {
    spark =
        SparkSession.builder()
            .appName("FhirTerminologyImporterTest")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", warehouse.toString())
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  void importsASingleCodeSystemFile(@TempDir final Path storeDir) {
    final String store = storeDir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store).importFrom(FhirFixtures.codeSystemFile().toString());

    final TerminologyStoreReader reader = TerminologyStoreReader.open(store, Map.of());
    final List<ManifestEntry> manifest = reader.readManifest();
    assertEquals(1, manifest.size());
    assertEquals("code_system", manifest.get(0).getEntryType());
    assertEquals(FhirFixtures.ANIMAL_SPECIES, manifest.get(0).getCanonicalUrl());
    assertEquals(FhirFixtures.VERSION, manifest.get(0).getVersion());

    // Concepts carry their display, keyed by dense identifier.
    final Map<String, String> display = new HashMap<>();
    final Map<String, Integer> dense = new HashMap<>();
    reader.readTable(
        CONCEPT,
        row -> {
          display.put(row.getString(COLUMN_CODE), row.getString(COLUMN_DISPLAY));
          dense.put(row.getString(COLUMN_CODE), row.getInt(COLUMN_DENSE_ID));
        });
    assertEquals("Dog", display.get(FhirFixtures.DOG));
    assertEquals("Whale", display.get(FhirFixtures.WHALE));
    assertEquals(9, display.size());

    // The nested hierarchy becomes a transitive closure: organism subsumes dog.
    final Set<String> closurePairs = new HashSet<>();
    final Map<Integer, String> codeByDense = new HashMap<>();
    dense.forEach((code, id) -> codeByDense.put(id, code));
    reader.readTable(
        CLOSURE,
        row ->
            closurePairs.add(
                codeByDense.get(row.getInt(COLUMN_ANCESTOR_DENSE_ID))
                    + "->"
                    + codeByDense.get(row.getInt(COLUMN_DESCENDANT_DENSE_ID))));
    assertTrue(closurePairs.contains(FhirFixtures.ORGANISM + "->" + FhirFixtures.DOG));
    assertTrue(closurePairs.contains(FhirFixtures.MAMMAL + "->" + FhirFixtures.DOG));

    // Scalar properties are captured with their type.
    final Map<String, String> dogLegs = new HashMap<>();
    reader.readTable(
        PROPERTY,
        row -> {
          if ("legs".equals(row.getString(COLUMN_PROPERTY_CODE))
              && dense.get(FhirFixtures.DOG).equals(row.getInt(COLUMN_CONCEPT_DENSE_ID))) {
            dogLegs.put("legs", row.getString(COLUMN_VALUE));
          }
        });
    assertEquals("4", dogLegs.get("legs"));

    // The designation is stored as a description.
    final Set<String> dogTerms = new HashSet<>();
    reader.readTable(
        DESCRIPTION,
        row -> {
          if (dense.get(FhirFixtures.DOG).equals(row.getInt(COLUMN_CONCEPT_DENSE_ID))) {
            dogTerms.add(row.getString(COLUMN_TERM));
          }
        });
    assertTrue(dogTerms.contains("Canine"));
  }

  @Test
  void importsADirectoryOfResources(@TempDir final Path storeDir) {
    final String store = storeDir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store).importFrom(FhirFixtures.jsonDirectory().toString());

    final Map<String, Set<String>> byType = manifestByType(store);
    assertTrue(byType.get("code_system").contains(FhirFixtures.ANIMAL_SPECIES));
    assertTrue(byType.get("value_set").contains(FhirFixtures.VS_MAMMALS_ENUMERATED));
    assertTrue(byType.get("value_set").contains(FhirFixtures.VS_EXPANSION_ONLY));
    assertTrue(byType.get("concept_map").contains(FhirFixtures.CONCEPT_MAP));
  }

  @Test
  void importsAFhirPackage(@TempDir final Path storeDir) {
    final String store = storeDir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store).importFrom(FhirFixtures.packageArchive().toString());

    final Map<String, Set<String>> byType = manifestByType(store);
    assertTrue(byType.get("code_system").contains(FhirFixtures.ANIMAL_SPECIES));
    assertTrue(byType.get("value_set").contains(FhirFixtures.VS_MAMMALS_ENUMERATED));
    assertTrue(byType.get("concept_map").contains(FhirFixtures.CONCEPT_MAP));
  }

  @Test
  void rejectsResourceWithoutCanonicalUrl(@TempDir final Path dir) throws Exception {
    final Path invalid = dir.resolve("codesystem-no-url.json");
    Files.writeString(
        invalid,
        "{\"resourceType\":\"CodeSystem\",\"status\":\"active\",\"content\":\"complete\"}");
    final String store = dir.resolve("store").toString();

    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store);
    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class, () -> importer.importFrom(invalid.toString()));
    assertTrue(e.getMessage().toLowerCase().contains("canonical url"));
    // Nothing was written to the store.
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
  }

  @Test
  void rejectsSourceWithNoImportableResources(@TempDir final Path dir) throws Exception {
    final Path patient = dir.resolve("patient.json");
    Files.writeString(patient, "{\"resourceType\":\"Patient\",\"id\":\"example\"}");
    final String store = dir.resolve("store").toString();

    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store);
    assertThrows(TerminologyImportException.class, () -> importer.importFrom(patient.toString()));
  }

  // --- Streaming import (feature 024). ---

  @Test
  void importsTheNestedFixtureEquivalentlyAcrossSourceShapes(@TempDir final Path dir)
      throws Exception {
    // The bare file, a directory, and a package of the same CodeSystem produce equivalent stores.
    final Set<String> fileClosure = importNestedAndReadClosure(dir, "file");
    assertTrue(fileClosure.contains("A->D"), "root A subsumes grandchild D");
    assertTrue(fileClosure.contains("A->B"));
    assertTrue(fileClosure.contains("C->D"));

    final Path dirSource = dir.resolve("dir");
    Files.createDirectories(dirSource);
    Files.copy(
        FhirPackageFixtures.resource("nested-hierarchy.json"), dirSource.resolve("nested.json"));
    final String dirStore = dir.resolve("dir-store").toString();
    new FhirTerminologyImporter(spark, dirStore).importFrom(dirSource.toString());
    assertEquals(fileClosure, closurePairs(dirStore));

    final Path archive =
        FhirPackageFixtures.buildPackage(dir, "nested.tgz", "nested-hierarchy.json");
    final String pkgStore = dir.resolve("pkg-store").toString();
    new FhirTerminologyImporter(spark, pkgStore).importFrom(archive.toString());
    assertEquals(fileClosure, closurePairs(pkgStore));
  }

  @Test
  void streamingImportPreservesConceptDetail(@TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store)
        .importFrom(FhirPackageFixtures.resource("nested-hierarchy.json").toString());

    final TerminologyStoreReader reader = TerminologyStoreReader.open(store, Map.of());
    final Map<String, String> display = new HashMap<>();
    final Map<String, Boolean> active = new HashMap<>();
    reader.readTable(
        CONCEPT,
        row -> {
          display.put(row.getString(COLUMN_CODE), row.getString(COLUMN_DISPLAY));
          active.put(row.getString(COLUMN_CODE), row.getBoolean(COLUMN_ACTIVE));
        });
    assertEquals(4, display.size());
    // The display falls back to the code, and an inactive property clears the active flag.
    assertEquals("C", display.get("C"));
    assertEquals(Boolean.FALSE, active.get("C"));
    assertEquals(Boolean.TRUE, active.get("A"));
  }

  @Test
  void rejectsCodeSystemMissingUrlDuringPreScan(@TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store);

    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class,
            () ->
                importer.importFrom(
                    FhirPackageFixtures.resource("codesystem-no-url.json").toString()));
    assertTrue(e.getMessage().toLowerCase().contains("canonical url"));
    // The pre-scan failed before any write, so the store was never created.
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
  }

  @Test
  void reportsPartialVersionOnMidStreamCorruptionAndRepairsOnReRun(@TempDir final Path dir)
      throws Exception {
    final String store = dir.resolve("store").toString();
    // A package whose second CodeSystem is corrupt: the first has already been written, so the
    // failure is reported as a possibly-partial version that a re-run repairs.
    final Path corruptPackage =
        FhirPackageFixtures.buildPackage(
            dir, "corrupt.tgz", "simple-valid.json", "corrupt-concepts.json");
    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store);

    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class, () -> importer.importFrom(corruptPackage.toString()));
    final String message = e.getMessage();
    assertTrue(message.contains("http://example.org/fhir/CodeSystem/corrupt"), message);
    assertTrue(message.toLowerCase().contains("partial"), message);
    assertTrue(message.toLowerCase().contains("re-run"), message);

    // Re-running with a corrected source repairs the store.
    final Path fixedPackage =
        FhirPackageFixtures.buildPackage(
            dir, "fixed.tgz", "simple-valid.json", "corrupt-concepts-fixed.json");
    new FhirTerminologyImporter(spark, store).importFrom(fixedPackage.toString());

    final Map<String, Set<String>> byType = manifestByType(store);
    assertTrue(byType.get("code_system").contains("http://example.org/fhir/CodeSystem/corrupt"));
    final Set<String> corruptCodes = new HashSet<>();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(CONCEPT, row -> corruptCodes.add(row.getString(COLUMN_CODE)));
    // Both the leading valid CodeSystem's codes and the repaired CodeSystem's codes are present.
    assertTrue(corruptCodes.contains("A"));
    assertTrue(corruptCodes.contains("B"));
  }

  @Test
  void rejectsAnOversizedWholeResourceWithAnActionableError(@TempDir final Path dir)
      throws Exception {
    final Path guardPackage = FhirPackageFixtures.buildGuardPackage(dir);
    final String store = dir.resolve("store").toString();
    // A tiny limit makes the padded ValueSet exceed the whole-resource guard.
    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store, 100L);

    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class, () -> importer.importFrom(guardPackage.toString()));
    assertTrue(e.getMessage().contains("ValueSet"), e.getMessage());
    assertTrue(e.getMessage().toLowerCase().contains("limit"), e.getMessage());
    // The guard fired during validation, before any write.
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
  }

  private Set<String> importNestedAndReadClosure(final Path dir, final String suffix) {
    final String store = dir.resolve("store-" + suffix).toString();
    new FhirTerminologyImporter(spark, store)
        .importFrom(FhirPackageFixtures.resource("nested-hierarchy.json").toString());
    return closurePairs(store);
  }

  private Set<String> closurePairs(final String store) {
    final TerminologyStoreReader reader = TerminologyStoreReader.open(store, Map.of());
    final Map<Integer, String> codeByDense = new HashMap<>();
    reader.readTable(
        CONCEPT, row -> codeByDense.put(row.getInt(COLUMN_DENSE_ID), row.getString(COLUMN_CODE)));
    final Set<String> pairs = new HashSet<>();
    reader.readTable(
        CLOSURE,
        row ->
            pairs.add(
                codeByDense.get(row.getInt(COLUMN_ANCESTOR_DENSE_ID))
                    + "->"
                    + codeByDense.get(row.getInt(COLUMN_DESCENDANT_DENSE_ID))));
    return pairs;
  }

  private Map<String, Set<String>> manifestByType(final String store) {
    final TerminologyStoreReader reader = TerminologyStoreReader.open(store, Map.of());
    final Map<String, Set<String>> byType = new HashMap<>();
    for (final ManifestEntry entry : reader.readManifest()) {
      byType
          .computeIfAbsent(entry.getEntryType(), k -> new HashSet<>())
          .add(entry.getCanonicalUrl());
    }
    return byType;
  }
}
