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
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CODE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ACTIVE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ANCESTOR_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CANONICAL_URL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_COUNT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DESCENDANT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_EQUIVALENCE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_PROPERTY_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_RESOURCE_JSON;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TERM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VALUE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT_MAPPING;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.DESCRIPTION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.MANIFEST;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.PROPERTY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.RELATIONSHIP;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.VALUE_SET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.test.FhirFixtures;
import ca.uhn.fhir.context.FhirContext;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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

  private WireMockServer registry;

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

  @BeforeEach
  void startRegistry() {
    registry = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    registry.start();
  }

  @AfterEach
  void stopRegistry() {
    registry.stop();
  }

  // The import path bounds the Parquet row-group size while writing, so the many concurrent Delta
  // writers together stay within a modest driver heap, and restores the caller's configuration
  // afterwards rather than leaving the bound in place.

  @Test
  void boundsParquetRowGroupSizeAndRestoresUnsetKey() {
    final Configuration conf = new Configuration(false);
    final String previous =
        FhirTerminologyImporter.applyBoundedParquetRowGroup(
            conf, FhirTerminologyImporter.IMPORT_PARQUET_BLOCK_SIZE_BYTES);

    // The key was unset, so there is no prior value and the bounded size is now in effect.
    assertNull(previous);
    assertEquals(
        FhirTerminologyImporter.IMPORT_PARQUET_BLOCK_SIZE_BYTES,
        conf.getInt("parquet.block.size", -1));

    FhirTerminologyImporter.restoreParquetRowGroup(conf, previous);

    // Restoring an originally-unset key leaves it unset rather than pinned to the bound.
    assertNull(conf.get("parquet.block.size"));
  }

  @Test
  void restoresCallerParquetRowGroupSize() {
    final Configuration conf = new Configuration(false);
    conf.set("parquet.block.size", "999");

    final String previous =
        FhirTerminologyImporter.applyBoundedParquetRowGroup(
            conf, FhirTerminologyImporter.IMPORT_PARQUET_BLOCK_SIZE_BYTES);

    // The caller's value is captured and the bounded size takes effect for the duration.
    assertEquals("999", previous);
    assertEquals(
        FhirTerminologyImporter.IMPORT_PARQUET_BLOCK_SIZE_BYTES,
        conf.getInt("parquet.block.size", -1));

    FhirTerminologyImporter.restoreParquetRowGroup(conf, previous);

    // The caller's original value is put back exactly.
    assertEquals("999", conf.get("parquet.block.size"));
  }

  @Test
  void importsASingleCodeSystemFile(@TempDir final Path storeDir) {
    final String store = storeDir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store)
        .importFrom(FhirFixtures.codeSystemFile().toString(), false, null);

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
    new FhirTerminologyImporter(spark, store)
        .importFrom(FhirFixtures.jsonDirectory().toString(), false, null);

    final Map<String, Set<String>> byType = manifestByType(store);
    assertTrue(byType.get("code_system").contains(FhirFixtures.ANIMAL_SPECIES));
    assertTrue(byType.get("value_set").contains(FhirFixtures.VS_MAMMALS_ENUMERATED));
    assertTrue(byType.get("value_set").contains(FhirFixtures.VS_EXPANSION_ONLY));
    assertTrue(byType.get("concept_map").contains(FhirFixtures.CONCEPT_MAP));
  }

  @Test
  void importsAFhirPackage(@TempDir final Path storeDir) {
    final String store = storeDir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store)
        .importFrom(FhirFixtures.packageArchive().toString(), false, null);

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
            TerminologyImportException.class,
            () -> importer.importFrom(invalid.toString(), false, null));
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
    assertThrows(
        TerminologyImportException.class,
        () -> importer.importFrom(patient.toString(), false, null));
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
    new FhirTerminologyImporter(spark, dirStore).importFrom(dirSource.toString(), false, null);
    assertEquals(fileClosure, closurePairs(dirStore));

    final Path archive =
        FhirPackageFixtures.buildPackage(dir, "nested.tgz", "nested-hierarchy.json");
    final String pkgStore = dir.resolve("pkg-store").toString();
    new FhirTerminologyImporter(spark, pkgStore).importFrom(archive.toString(), false, null);
    assertEquals(fileClosure, closurePairs(pkgStore));
  }

  @Test
  void streamingImportPreservesConceptDetail(@TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store)
        .importFrom(FhirPackageFixtures.resource("nested-hierarchy.json").toString(), false, null);

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
                    FhirPackageFixtures.resource("codesystem-no-url.json").toString(),
                    false,
                    null));
    assertTrue(e.getMessage().toLowerCase().contains("canonical url"));
    // The pre-scan failed before any write, so the store was never created.
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
  }

  @Test
  void rejectsMidStreamCorruptionBeforeAnyWriteAndImportsOnReRun(@TempDir final Path dir)
      throws Exception {
    final String store = dir.resolve("store").toString();
    // A package whose second CodeSystem is corrupt. Every resource is read before anything is
    // written, so the valid CodeSystem that precedes it is not written either.
    final Path corruptPackage =
        FhirPackageFixtures.buildPackage(
            dir, "corrupt.tgz", "simple-valid.json", "corrupt-concepts.json");
    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store);

    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class,
            () -> importer.importFrom(corruptPackage.toString(), false, null));
    final String message = e.getMessage();
    assertTrue(message.contains("http://example.org/fhir/CodeSystem/corrupt"), message);
    assertTrue(Files.notExists(Path.of(store)), "nothing was written to the store");

    // Re-running with a corrected source imports both CodeSystems.
    final Path fixedPackage =
        FhirPackageFixtures.buildPackage(
            dir, "fixed.tgz", "simple-valid.json", "corrupt-concepts-fixed.json");
    new FhirTerminologyImporter(spark, store).importFrom(fixedPackage.toString(), false, null);

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
            TerminologyImportException.class,
            () -> importer.importFrom(guardPackage.toString(), false, null));
    assertTrue(e.getMessage().contains("ValueSet"), e.getMessage());
    assertTrue(e.getMessage().toLowerCase().contains("limit"), e.getMessage());
    // The guard fired during validation, before any write.
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
  }

  @Test
  void importsACodeSystemWrappedInABundleThroughTheStreamingPath(@TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store)
        .importFrom(FhirPackageFixtures.resource("bundle-codesystem.json").toString(), false, null);

    final Map<String, Set<String>> byType = manifestByType(store);
    assertTrue(byType.get("code_system").contains("http://example.org/fhir/CodeSystem/bundled"));
    // The wrapped CodeSystem's nesting hierarchy is queryable, proving it flowed through the same
    // streaming flattener and stage loader as a standalone CodeSystem.
    assertTrue(closurePairs(store).contains("A->B"));
  }

  @Test
  void flatParentPropertiesYieldTheSameClosureAsNesting(@TempDir final Path dir) {
    // The flat-parent fixture declares the same A/B/C/D hierarchy through parent properties,
    // including a dangling reference and a duplicate concept, yet answers the same closure.
    final Set<String> nested = importFixtureClosure(dir, "nested-hierarchy.json", "nested");
    final Set<String> flatParent =
        importFixtureClosure(dir, "flat-parent-props.json", "flat-parent");
    assertEquals(nested, flatParent);
    assertTrue(flatParent.contains("A->D"), "a grandparent subsumes a grandchild");
  }

  @Test
  void flatChildPropertiesYieldTheSameClosureAsNesting(@TempDir final Path dir) {
    final Set<String> nested = importFixtureClosure(dir, "nested-hierarchy.json", "nested");
    final Set<String> flatChild = importFixtureClosure(dir, "flat-child-props.json", "flat-child");
    assertEquals(nested, flatChild);
  }

  @Test
  void mixedNestingAndPropertyEdgesDeduplicateTheOverlap(@TempDir final Path dir) {
    final Set<String> mixed = importFixtureClosure(dir, "mixed-nesting-parent.json", "mixed");
    // B is-a A comes from both nesting and a parent property, and C is-a A from a property; the
    // overlapping B is-a A edge is not double-counted.
    assertEquals(Set.of("A->B", "A->C"), mixed);
  }

  // --- Streaming ConceptMaps. ---

  @Test
  void importsAConceptMapAsOneRowPerMapping(@TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store)
        .importFrom(conceptMapFixture().toString(), false, null);

    final ManifestEntry entry = manifest(store).get(0);
    assertEquals("concept_map", entry.getEntryType());
    assertEquals(FhirFixtures.CONCEPT_MAP, entry.getCanonicalUrl());
    assertEquals(FhirFixtures.VERSION, entry.getVersion());
    // The fixture can be read in full as a concept map relation.
    assertNull(entry.getFault());
    // Each mapping is a row of its own, keyed by the map's URL and version.
    assertEquals(
        Set.of(
            mapping(FhirFixtures.CONCEPT_MAP, FhirFixtures.VERSION, "dog", "pet", "equivalent"),
            mapping(FhirFixtures.CONCEPT_MAP, FhirFixtures.VERSION, "cat", "pet", "equivalent"),
            mapping(
                FhirFixtures.CONCEPT_MAP, FhirFixtures.VERSION, "whale", "aquatic", "relatedto"),
            mapping(FhirFixtures.CONCEPT_MAP, FhirFixtures.VERSION, "sparrow", "pet", "wider")),
        mappings(store));
  }

  @Test
  void importsAConceptMapLargerThanTheWholeResourceLimit(@TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    // The fixture is far larger than this limit, which would reject it if it were read whole.
    new FhirTerminologyImporter(spark, store, 100L)
        .importFrom(conceptMapFixture().toString(), false, null);

    assertEquals(4, mappings(store).size());
  }

  @Test
  void recordsInTheManifestWhyAConceptMapCannotBeReadAsARelation(@TempDir final Path dir)
      throws Exception {
    final Path source = dir.resolve("products.json");
    Files.writeString(
        source,
        "{\"resourceType\":\"ConceptMap\",\"url\":\"http://example.org/cm\",\"version\":\"1\","
            + "\"group\":[{\"source\":\"http://s\",\"target\":\"http://t\",\"element\":"
            + "[{\"code\":\"a\",\"target\":[{\"code\":\"b\",\"equivalence\":\"equivalent\","
            + "\"product\":[{\"property\":\"p\",\"value\":\"v\"}]}]}]}]}");
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store).importFrom(source.toString(), false, null);

    assertEquals("the mapping for source code 'a' has products", manifest(store).get(0).getFault());
    // The fault does not stop the mapping from being stored for translate.
    assertEquals(
        Set.of(mapping("http://example.org/cm", "1", "a", "b", "equivalent")), mappings(store));
  }

  @Test
  void reimportingAConceptMapReplacesItsFault(@TempDir final Path dir) throws Exception {
    final Path faulty = dir.resolve("faulty.json");
    Files.writeString(
        faulty, conceptMapJson("1", "a", "b").replace(",\"equivalence\":\"equivalent\"", ""));
    final Path corrected = dir.resolve("corrected.json");
    Files.writeString(corrected, conceptMapJson("1", "a", "b"));
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store).importFrom(faulty.toString(), false, null);
    assertEquals(
        "the mapping for source code 'a' has no equivalence", manifest(store).get(0).getFault());
    new FhirTerminologyImporter(spark, store).importFrom(corrected.toString(), false, null);

    assertEquals(1, manifest(store).size());
    assertNull(manifest(store).get(0).getFault());
  }

  @Test
  void reimportingAConceptMapReplacesOnlyThatVersionsMappings(@TempDir final Path dir)
      throws Exception {
    final String store = dir.resolve("store").toString();
    final Path first = dir.resolve("first.json");
    Files.writeString(first, conceptMapJson("1", "a", "b"));
    final Path other = dir.resolve("other.json");
    Files.writeString(other, conceptMapJson("2", "c", "d"));
    final Path revised = dir.resolve("revised.json");
    Files.writeString(revised, conceptMapJson("1", "a", "z"));

    new FhirTerminologyImporter(spark, store).importFrom(first.toString(), false, null);
    new FhirTerminologyImporter(spark, store).importFrom(other.toString(), false, null);
    new FhirTerminologyImporter(spark, store).importFrom(revised.toString(), false, null);

    // The revised version 1 replaced its predecessor outright, and version 2 was left alone.
    assertEquals(
        Set.of(
            mapping("http://example.org/cm", "1", "a", "z", "equivalent"),
            mapping("http://example.org/cm", "2", "c", "d", "equivalent")),
        mappings(store));
  }

  @Test
  void rejectsAConceptMapWithAnUnrecognisedEquivalenceBeforeWritingIt(@TempDir final Path dir)
      throws Exception {
    final Path source = dir.resolve("bad.json");
    Files.writeString(source, conceptMapJson("1", "a", "b").replace("equivalent", "sort-of"));
    final String store = dir.resolve("store").toString();
    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store);
    final String path = source.toString();

    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class, () -> importer.importFrom(path, false, null));

    assertTrue(e.getMessage().contains("http://example.org/cm"), e.getMessage());
    assertTrue(e.getMessage().contains("sort-of"), e.getMessage());
    // The map was rejected while it was being read, before anything was written.
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
  }

  // --- Streaming Bundles. ---

  @Test
  void importsEveryImportableMemberOfABundleLargerThanTheWholeResourceLimit(
      @TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    // The limit admits the bundled ValueSet but not the Bundle as a whole, nor its ConceptMap.
    new FhirTerminologyImporter(spark, store, 400L)
        .importFrom(FhirPackageFixtures.resource("bundle-mixed.json").toString(), false, null);

    assertBundleMixedImported(store);
  }

  @Test
  void importsABundleFromAPackage(@TempDir final Path dir) throws Exception {
    final Path archive =
        FhirPackageFixtures.buildPackage(
            dir, "bundle.tgz", "bundle-mixed.json", "nested-hierarchy.json");
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store).importFrom(archive.toString(), false, null);

    assertBundleMixedImported(store);
    // The entry after the Bundle is read from the right place in the archive stream.
    assertTrue(
        manifestByType(store)
            .get("code_system")
            .contains("http://example.org/fhir/CodeSystem/nested"));
  }

  @Test
  void rejectsAnOversizedValueSetInABundleBeforeAnyWrite(@TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store, 100L);
    final String path = FhirPackageFixtures.resource("bundle-mixed.json").toString();

    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class, () -> importer.importFrom(path, false, null));

    assertTrue(e.getMessage().contains("http://example.org/fhir/ValueSet/bundled"), e.getMessage());
    assertTrue(e.getMessage().toLowerCase().contains("limit"), e.getMessage());
    // The CodeSystem that precedes the ValueSet in the Bundle was not written either.
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
  }

  @Test
  void rejectsABundleMemberWithoutACanonicalUrlBeforeAnyWrite(@TempDir final Path dir)
      throws Exception {
    final Path source = dir.resolve("bundle.json");
    Files.writeString(
        source,
        "{\"resourceType\":\"Bundle\",\"entry\":[{\"resource\":"
            + FhirPackageFixtures.read("simple-valid.json")
            + "},{\"resource\":{\"resourceType\":\"ConceptMap\",\"status\":\"active\"}}]}");
    final String store = dir.resolve("store").toString();
    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store);
    final String path = source.toString();

    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class, () -> importer.importFrom(path, false, null));

    assertTrue(e.getMessage().toLowerCase().contains("canonical url"), e.getMessage());
    assertTrue(e.getMessage().contains("bundle.json#entry[1]"), e.getMessage());
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
  }

  @Test
  void rejectsABundleWithNoImportableMembers(@TempDir final Path dir) throws Exception {
    final Path source = dir.resolve("bundle.json");
    Files.writeString(
        source,
        "{\"resourceType\":\"Bundle\",\"entry\":[{\"resource\":{\"resourceType\":\"Patient\"}}]}");
    final String store = dir.resolve("store").toString();
    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store);
    final String path = source.toString();

    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class, () -> importer.importFrom(path, false, null));
    assertTrue(e.getMessage().contains("No importable"), e.getMessage());
  }

  // --- Batched writes (issue 2849). ---

  @Test
  void writesEachTableOnceForASourceOfManyResources(@TempDir final Path dir) throws Exception {
    // A source of many small resources is written with one Delta commit per table rather than one
    // per resource, which is what makes a package of thousands of resources quick to import.
    final Path source =
        bundle(
            dir,
            "many.json",
            codeSystemJson("http://example.org/cs1", "1", "A", "B"),
            codeSystemJson("http://example.org/cs2", "1", "A", "C"),
            codeSystemJson("http://example.org/cs3", "1", "X", "Y"),
            valueSetJson("http://example.org/vs1", "1", "first"),
            valueSetJson("http://example.org/vs2", null, "first"),
            conceptMapJson("http://example.org/cm1", "1", "a", "b"),
            conceptMapJson("http://example.org/cm2", "1", "a", "c"));
    final String store = dir.resolve("store").toString();
    final List<String> tables =
        List.of(
            CODE_SYSTEM,
            CONCEPT,
            DESCRIPTION,
            PROPERTY,
            RELATIONSHIP,
            CLOSURE,
            CONCEPT_MAPPING,
            VALUE_SET,
            MANIFEST);

    new FhirTerminologyImporter(spark, store).importFrom(source.toString(), false, null);
    for (final String table : tables) {
      assertEquals(1, commits(store, table), table);
    }

    // Re-importing the same source replaces its content with at most one more commit per table.
    // Delta makes no commit where there is nothing to replace, as with the empty description
    // table, so a table may gain none.
    new FhirTerminologyImporter(spark, store).importFrom(source.toString(), false, null);
    for (final String table : tables) {
      assertTrue(commits(store, table) <= 2, table);
    }
    assertEquals(2, commits(store, CONCEPT));
    assertEquals(7, manifest(store).size());
  }

  @Test
  void keepsTheContentOfEachResourceInABatchSeparate(@TempDir final Path dir) throws Exception {
    // Two CodeSystems share the code A but not its child, and two ConceptMaps share a source code
    // but only one of them can be read as a relation.
    final Path source =
        bundle(
            dir,
            "shared.json",
            codeSystemJson("http://example.org/cs1", "1", "A", "B"),
            codeSystemJson("http://example.org/cs2", "1", "A", "C"),
            valueSetJson("http://example.org/vs1", "1", "one"),
            valueSetJson("http://example.org/vs2", "1", "two"),
            conceptMapJson("http://example.org/cm1", "1", "a", "b")
                .replace(",\"equivalence\":\"equivalent\"", ""),
            conceptMapJson("http://example.org/cm2", "1", "a", "z"));
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store).importFrom(source.toString(), false, null);

    // Neither the shared code nor the hierarchy leaks from one CodeSystem into the other.
    final String cs1 = TerminologyStoreSchema.systemVersionId("http://example.org/cs1", "1");
    final String cs2 = TerminologyStoreSchema.systemVersionId("http://example.org/cs2", "1");
    assertEquals(Map.of(cs1, Set.of("A", "B"), cs2, Set.of("A", "C")), codesBySystem(store));
    assertEquals(Map.of(cs1, Set.of("A->B"), cs2, Set.of("A->C")), closureBySystem(store));
    assertEquals(Map.of(cs1, 2L, cs2, 2L), conceptCountBySystem(store));

    // Each ValueSet is stored as its own resource.
    final Map<String, String> valueSetNames = valueSetNames(store);
    assertEquals(
        Map.of("http://example.org/vs1|1", "one", "http://example.org/vs2|1", "two"),
        valueSetNames);

    // Each map keeps its own mappings, and the fault of one is not attributed to the other.
    assertEquals(
        Set.of(
            mapping("http://example.org/cm1", "1", "a", "b", "relatedto"),
            mapping("http://example.org/cm2", "1", "a", "z", "equivalent")),
        mappings(store));
    final Map<String, String> faults = new HashMap<>();
    for (final ManifestEntry entry : manifest(store)) {
      if ("concept_map".equals(entry.getEntryType())) {
        faults.put(entry.getCanonicalUrl(), String.valueOf(entry.getFault()));
      }
    }
    assertEquals(
        Map.of(
            "http://example.org/cm1",
            "the mapping for source code 'a' has no equivalence",
            "http://example.org/cm2",
            "null"),
        faults);
  }

  @Test
  void theLastOccurrenceOfARepeatedResourceWins(@TempDir final Path dir) throws Exception {
    // Each resource appears twice with the same URL and version; the later occurrence is the one
    // stored, as it was when each resource was written in turn.
    final Path source =
        bundle(
            dir,
            "repeated.json",
            codeSystemJson("http://example.org/cs", "1", "A", "B"),
            valueSetJson("http://example.org/vs", null, "earlier"),
            conceptMapJson("http://example.org/cm", "1", "a", "b"),
            codeSystemJson("http://example.org/cs", "1", "A", "C"),
            valueSetJson("http://example.org/vs", null, "later"),
            conceptMapJson("http://example.org/cm", "1", "a", "z"));
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store).importFrom(source.toString(), false, null);

    final String cs = TerminologyStoreSchema.systemVersionId("http://example.org/cs", "1");
    assertEquals(Map.of(cs, Set.of("A", "C")), codesBySystem(store));
    assertEquals(Map.of(cs, Set.of("A->C")), closureBySystem(store));
    assertEquals(Map.of("http://example.org/vs|null", "later"), valueSetNames(store));
    assertEquals(
        Set.of(mapping("http://example.org/cm", "1", "a", "z", "equivalent")), mappings(store));
    assertEquals(3, manifest(store).size());
  }

  @Test
  void anEmptyValueSetVersionRepeatsAnAbsentOne(@TempDir final Path dir) throws Exception {
    // HAPI reads an empty version as absent, so these two ValueSets are stored under the same key
    // and the later one alone must be kept.
    final Path source =
        bundle(
            dir,
            "repeated.json",
            valueSetJson("http://example.org/vs", "", "earlier"),
            valueSetJson("http://example.org/vs", null, "later"));
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store).importFrom(source.toString(), false, null);

    assertEquals(Map.of("http://example.org/vs|null", "later"), valueSetNames(store));
    assertEquals(1, manifest(store).size());
  }

  @Test
  void reimportingABatchReplacesOnlyTheVersionsItHolds(@TempDir final Path dir) throws Exception {
    final String store = dir.resolve("store").toString();
    final Path first =
        bundle(
            dir,
            "first.json",
            codeSystemJson("http://example.org/x", "1", "A", "B"),
            codeSystemJson("http://example.org/y", "1", "P", "Q"),
            valueSetJson("http://example.org/vs", "1", "versioned"),
            valueSetJson("http://example.org/vs", null, "unversioned"),
            conceptMapJson("http://example.org/cm", "1", "a", "b"),
            conceptMapJson("http://example.org/cm", "2", "c", "d"));
    final Path second =
        bundle(
            dir,
            "second.json",
            codeSystemJson("http://example.org/x", "1", "A", "C"),
            codeSystemJson("http://example.org/z", "1", "M", "N"),
            valueSetJson("http://example.org/vs", null, "revised"),
            conceptMapJson("http://example.org/cm", "1", "a", "z"));

    new FhirTerminologyImporter(spark, store).importFrom(first.toString(), false, null);
    new FhirTerminologyImporter(spark, store).importFrom(second.toString(), false, null);

    // The versions in the second source replaced their predecessors; the others were left alone.
    assertEquals(
        Map.of(
            TerminologyStoreSchema.systemVersionId("http://example.org/x", "1"),
            Set.of("A", "C"),
            TerminologyStoreSchema.systemVersionId("http://example.org/y", "1"),
            Set.of("P", "Q"),
            TerminologyStoreSchema.systemVersionId("http://example.org/z", "1"),
            Set.of("M", "N")),
        codesBySystem(store));
    assertEquals(
        Map.of("http://example.org/vs|1", "versioned", "http://example.org/vs|null", "revised"),
        valueSetNames(store));
    assertEquals(
        Set.of(
            mapping("http://example.org/cm", "1", "a", "z", "equivalent"),
            mapping("http://example.org/cm", "2", "c", "d", "equivalent")),
        mappings(store));
    assertEquals(7, manifest(store).size());
  }

  @Test
  void importsAndReimportsResourcesWhoseUrlAndVersionContainQuotes(@TempDir final Path dir)
      throws Exception {
    // Canonical URLs and versions are spliced into the predicates that replace stored versions,
    // so a quote or a backslash in either must not break or widen the replacement.
    final String url = "http://example.org/o'brien\\\\x";
    final String version = "1'0";
    final String storedUrl = "http://example.org/o'brien\\x";
    final Path first =
        bundle(
            dir,
            "first.json",
            codeSystemJson(url, version, "A", "B"),
            valueSetJson(url, version, "first"),
            conceptMapJson(url, version, "a", "b"));
    final Path second =
        bundle(
            dir,
            "second.json",
            codeSystemJson(url, version, "A", "C"),
            valueSetJson(url, version, "second"),
            conceptMapJson(url, version, "a", "z"));
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store).importFrom(first.toString(), false, null);
    new FhirTerminologyImporter(spark, store).importFrom(second.toString(), false, null);

    assertEquals(
        Map.of(TerminologyStoreSchema.systemVersionId(storedUrl, version), Set.of("A", "C")),
        codesBySystem(store));
    assertEquals(Map.of(storedUrl + "|" + version, "second"), valueSetNames(store));
    assertEquals(Set.of(mapping(storedUrl, version, "a", "z", "equivalent")), mappings(store));
    assertEquals(3, manifest(store).size());
  }

  @Test
  void importsAValueSetBeyondTheDefaultJsonStringLimit(@TempDir final Path dir) throws Exception {
    // A ValueSet is staged as a single JSON string, which here is longer than the 20 MB that Spark
    // reads by default; it must be stored whole rather than read back as null.
    final String description = "x".repeat(25 * 1024 * 1024);
    final Path source = dir.resolve("large.json");
    Files.writeString(
        source,
        valueSetJson("http://example.org/vs", "1", "large")
            .replace("\"status\"", "\"description\":\"" + description + "\",\"status\""));
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store).importFrom(source.toString(), false, null);

    final List<Integer> lengths = new ArrayList<>();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(VALUE_SET, row -> lengths.add(row.getString(COLUMN_RESOURCE_JSON).length()));
    assertEquals(1, lengths.size());
    assertTrue(lengths.get(0) > description.length(), "the whole ValueSet is stored");
  }

  @Test
  void reportsAPossiblyPartialImportWhenAWriteFailsAndRepairsOnReRun(@TempDir final Path dir)
      throws Exception {
    final Path source =
        bundle(
            dir,
            "source.json",
            codeSystemJson("http://example.org/cs", "1", "A", "B"),
            conceptMapJson("http://example.org/cm", "1", "a", "b"));
    final Path storeDir = dir.resolve("store");
    final String store = storeDir.toString();
    // A plain file where the concept mapping table belongs makes that write fail after the
    // CodeSystem tables have been written.
    Files.createDirectories(storeDir);
    final Path blocker = storeDir.resolve(CONCEPT_MAPPING);
    Files.writeString(blocker, "not a table");
    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store);

    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class,
            () -> importer.importFrom(source.toString(), false, null));
    final String message = e.getMessage().toLowerCase();
    assertTrue(message.contains("partial"), message);
    assertTrue(message.contains("re-run"), message);
    // The manifest is written last, so nothing is recorded as imported.
    assertTrue(Files.notExists(storeDir.resolve(MANIFEST)));

    // Re-running once the obstruction is gone completes the import.
    Files.delete(blocker);
    new FhirTerminologyImporter(spark, store).importFrom(source.toString(), false, null);
    assertEquals(2, manifest(store).size());
    assertEquals(
        Set.of(mapping("http://example.org/cm", "1", "a", "b", "equivalent")), mappings(store));
  }

  /** Writes a collection Bundle holding the given resources to a file. */
  private static Path bundle(final Path dir, final String name, final String... resources)
      throws IOException {
    final StringBuilder json =
        new StringBuilder("{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":[");
    for (int i = 0; i < resources.length; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"resource\":").append(resources[i]).append('}');
    }
    json.append("]}");
    final Path file = dir.resolve(name);
    Files.writeString(file, json.toString());
    return file;
  }

  /** A CodeSystem whose one root concept has one nested child. */
  private static String codeSystemJson(
      final String url, final String version, final String root, final String child) {
    return "{\"resourceType\":\"CodeSystem\",\"url\":\""
        + url
        + "\",\"version\":\""
        + version
        + "\",\"status\":\"active\",\"content\":\"complete\",\"concept\":[{\"code\":\""
        + root
        + "\",\"concept\":[{\"code\":\""
        + child
        + "\"}]}]}";
  }

  /** A ValueSet with the given name, and no version when the version is null. */
  private static String valueSetJson(final String url, final String version, final String name) {
    return "{\"resourceType\":\"ValueSet\",\"url\":\""
        + url
        + "\","
        + (version == null ? "" : "\"version\":\"" + version + "\",")
        + "\"name\":\""
        + name
        + "\",\"status\":\"active\"}";
  }

  /** The number of Delta commits made to a table of a store. */
  private static long commits(final String store, final String table) throws IOException {
    try (Stream<Path> files = Files.list(Path.of(store, table, "_delta_log"))) {
      return files.filter(file -> file.getFileName().toString().matches("\\d{20}\\.json")).count();
    }
  }

  /** The codes of each stored CodeSystem version, keyed by system version. */
  private static Map<String, Set<String>> codesBySystem(final String store) {
    final Map<String, Set<String>> codes = new HashMap<>();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(
            CONCEPT,
            row ->
                codes
                    .computeIfAbsent(row.getString(COLUMN_SYSTEM_VERSION_ID), k -> new HashSet<>())
                    .add(row.getString(COLUMN_CODE)));
    return codes;
  }

  /** The closure of each stored CodeSystem version as code pairs, keyed by system version. */
  private static Map<String, Set<String>> closureBySystem(final String store) {
    final TerminologyStoreReader reader = TerminologyStoreReader.open(store, Map.of());
    final Map<String, String> codeByDense = new HashMap<>();
    reader.readTable(
        CONCEPT,
        row ->
            codeByDense.put(
                row.getString(COLUMN_SYSTEM_VERSION_ID) + "#" + row.getInt(COLUMN_DENSE_ID),
                row.getString(COLUMN_CODE)));
    final Map<String, Set<String>> pairs = new HashMap<>();
    reader.readTable(
        CLOSURE,
        row -> {
          final String system = row.getString(COLUMN_SYSTEM_VERSION_ID);
          pairs
              .computeIfAbsent(system, k -> new HashSet<>())
              .add(
                  codeByDense.get(system + "#" + row.getInt(COLUMN_ANCESTOR_DENSE_ID))
                      + "->"
                      + codeByDense.get(system + "#" + row.getInt(COLUMN_DESCENDANT_DENSE_ID)));
        });
    return pairs;
  }

  /** The concept count recorded for each stored CodeSystem version, keyed by system version. */
  private static Map<String, Long> conceptCountBySystem(final String store) {
    final Map<String, Long> counts = new HashMap<>();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(
            CODE_SYSTEM,
            row ->
                counts.put(
                    row.getString(COLUMN_SYSTEM_VERSION_ID), row.getLong(COLUMN_CONCEPT_COUNT)));
    return counts;
  }

  /** The name inside each stored ValueSet, keyed by {@code url|version}. */
  private static Map<String, String> valueSetNames(final String store) {
    final Map<String, String> names = new HashMap<>();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(
            VALUE_SET,
            row -> {
              final String json = row.getString(COLUMN_RESOURCE_JSON);
              final ValueSet valueSet =
                  FhirContext.forR4Cached().newJsonParser().parseResource(ValueSet.class, json);
              names.put(
                  row.getString(COLUMN_CANONICAL_URL) + "|" + row.getString(COLUMN_VERSION),
                  valueSet.getName());
            });
    return names;
  }

  private void assertBundleMixedImported(final String store) {
    final Map<String, Set<String>> byType = manifestByType(store);
    assertTrue(byType.get("code_system").contains("http://example.org/fhir/CodeSystem/bundled"));
    assertEquals(Set.of("http://example.org/fhir/ValueSet/bundled"), byType.get("value_set"));
    assertEquals(Set.of("http://example.org/fhir/ConceptMap/bundled"), byType.get("concept_map"));
    assertTrue(closurePairs(store).contains("A->B"));
    assertEquals(
        Set.of(
            mapping("http://example.org/fhir/ConceptMap/bundled", "1.0.0", "A", "X", "equivalent"),
            mapping("http://example.org/fhir/ConceptMap/bundled", "1.0.0", "B", "Y", "wider")),
        mappings(store));
  }

  private static Path conceptMapFixture() {
    return FhirFixtures.jsonDirectory().resolve("conceptmap-species-to-category.json");
  }

  /** A one-mapping ConceptMap with the given version, source code and target code. */
  private static String conceptMapJson(
      final String version, final String sourceCode, final String targetCode) {
    return conceptMapJson("http://example.org/cm", version, sourceCode, targetCode);
  }

  /** A one-mapping ConceptMap with the given URL, version, source code and target code. */
  private static String conceptMapJson(
      final String url, final String version, final String sourceCode, final String targetCode) {
    return "{\"resourceType\":\"ConceptMap\",\"url\":\""
        + url
        + "\",\"version\":\""
        + version
        + "\",\"group\":[{\"source\":\"http://s\",\"target\":\"http://t\",\"element\":[{\"code\":\""
        + sourceCode
        + "\",\"target\":[{\"code\":\""
        + targetCode
        + "\",\"equivalence\":\"equivalent\"}]}]}]}";
  }

  private static String mapping(
      final String url,
      final String version,
      final String sourceCode,
      final String targetCode,
      final String equivalence) {
    return url + "|" + version + ": " + sourceCode + " -> " + targetCode + " (" + equivalence + ")";
  }

  private Set<String> mappings(final String store) {
    final Set<String> rows = new HashSet<>();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(
            CONCEPT_MAPPING,
            row ->
                rows.add(
                    mapping(
                        row.getString(COLUMN_CANONICAL_URL),
                        row.getString(COLUMN_VERSION),
                        row.getString(COLUMN_SOURCE_CODE),
                        row.getString(COLUMN_TARGET_CODE),
                        row.getString(COLUMN_EQUIVALENCE))));
    return rows;
  }

  // --- Provenance and registry verification (feature 059). ---

  @Test
  void packageImportRecordsVerifiedProvenanceOnEveryRow(@TempDir final Path dir) throws Exception {
    final Path archive =
        FhirPackageFixtures.buildPackage(
            dir, "fixtures.tgz", "nested-hierarchy.json", "valueset-simple.json");
    RegistryStub.stubListing(
        registry,
        FhirPackageFixtures.PACKAGE_NAME,
        FhirPackageFixtures.PACKAGE_VERSION,
        FhirPackageFixtures.sha1Hex(archive));
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store)
        .importFrom(archive.toString(), true, registry.baseUrl());

    final List<ManifestEntry> manifest = manifest(store);
    assertEquals(2, manifest.size());
    for (final ManifestEntry entry : manifest) {
      assertEquals(FhirPackageFixtures.sha256Hex(archive), entry.getSourceSha256());
      assertEquals(FhirPackageFixtures.PACKAGE_NAME, entry.getPackageName());
      assertEquals(FhirPackageFixtures.PACKAGE_VERSION, entry.getPackageVersion());
      assertEquals(PackageVerification.VERIFIED, entry.getPackageVerification());
      assertEquals(registry.baseUrl(), entry.getPackageRegistry());
    }
  }

  @Test
  void mismatchFailsBeforeAnyWriteIntoAFreshStore(@TempDir final Path dir) throws Exception {
    final Path archive =
        FhirPackageFixtures.buildPackage(dir, "fixtures.tgz", "nested-hierarchy.json");
    stubMismatch(archive);
    final String store = dir.resolve("store").toString();
    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store);

    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class,
            () -> importer.importFrom(archive.toString(), true, registry.baseUrl()));

    assertTrue(e.getMessage().contains("does not match the registry checksum"), e.getMessage());
    // The store was never created: the check ran before anything was written.
    assertTrue(Files.notExists(Path.of(store)));
  }

  @Test
  void mismatchLeavesAnExistingStoreUnchanged(@TempDir final Path dir) throws Exception {
    final Path first = FhirPackageFixtures.buildPackage(dir, "first.tgz", "nested-hierarchy.json");
    final String store = dir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store).importFrom(first.toString(), false, null);
    final List<ManifestEntry> before = manifest(store);

    final Path tampered =
        FhirPackageFixtures.buildPackage(dir, "tampered.tgz", "simple-valid.json");
    stubMismatch(tampered);
    final FhirTerminologyImporter importer = new FhirTerminologyImporter(spark, store);
    assertThrows(
        TerminologyImportException.class,
        () -> importer.importFrom(tampered.toString(), true, registry.baseUrl()));

    assertEquals(before, manifest(store));
  }

  @Test
  void noShasumImportsAsUnverifiedWithNullRegistry(@TempDir final Path dir) throws Exception {
    final Path archive =
        FhirPackageFixtures.buildPackage(dir, "fixtures.tgz", "nested-hierarchy.json");
    RegistryStub.stubListing(
        registry, FhirPackageFixtures.PACKAGE_NAME, FhirPackageFixtures.PACKAGE_VERSION, null);
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store)
        .importFrom(archive.toString(), true, registry.baseUrl());

    final ManifestEntry entry = manifest(store).get(0);
    assertEquals(PackageVerification.UNVERIFIED, entry.getPackageVerification());
    assertNull(entry.getPackageRegistry());
    // The package is still identified and hashed even though it could not be verified.
    assertEquals(FhirPackageFixtures.PACKAGE_NAME, entry.getPackageName());
    assertEquals(FhirPackageFixtures.sha256Hex(archive), entry.getSourceSha256());
  }

  @Test
  void unreachableRegistryImportsAsUnverified(@TempDir final Path dir) throws Exception {
    final Path archive =
        FhirPackageFixtures.buildPackage(dir, "fixtures.tgz", "nested-hierarchy.json");
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store)
        .importFrom(archive.toString(), true, "http://127.0.0.1:" + closedPort());

    final ManifestEntry entry = manifest(store).get(0);
    assertEquals(PackageVerification.UNVERIFIED, entry.getPackageVerification());
    assertNull(entry.getPackageRegistry());
  }

  @Test
  void packageWithoutPackageJsonImportsAsUnverifiedWithNullIdentity(@TempDir final Path dir)
      throws Exception {
    final Path archive =
        FhirPackageFixtures.buildPackageWithJson(dir, "anon.tgz", null, "nested-hierarchy.json");
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store)
        .importFrom(archive.toString(), true, registry.baseUrl());

    final ManifestEntry entry = manifest(store).get(0);
    assertEquals(PackageVerification.UNVERIFIED, entry.getPackageVerification());
    assertNull(entry.getPackageName());
    assertNull(entry.getPackageVersion());
    assertNotNull(entry.getSourceSha256());
    // An unidentifiable package is never looked up.
    assertEquals(0, registry.getAllServeEvents().size());
  }

  @Test
  void skippedVerificationMakesNoRequestAndRecordsSkipped(@TempDir final Path dir)
      throws Exception {
    final Path archive =
        FhirPackageFixtures.buildPackage(dir, "fixtures.tgz", "nested-hierarchy.json");
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store)
        .importFrom(archive.toString(), false, registry.baseUrl());

    final ManifestEntry entry = manifest(store).get(0);
    assertEquals(PackageVerification.SKIPPED, entry.getPackageVerification());
    assertNull(entry.getPackageRegistry());
    // The identity and hash are recorded even when the check is declined.
    assertEquals(FhirPackageFixtures.PACKAGE_NAME, entry.getPackageName());
    assertEquals(FhirPackageFixtures.PACKAGE_VERSION, entry.getPackageVersion());
    assertEquals(FhirPackageFixtures.sha256Hex(archive), entry.getSourceSha256());
    assertEquals(0, registry.getAllServeEvents().size());
  }

  @Test
  void jsonFileAndDirectorySourcesRecordNullStatusAndMakeNoRequest(@TempDir final Path dir)
      throws Exception {
    final Path file = FhirPackageFixtures.resource("nested-hierarchy.json");
    final String fileStore = dir.resolve("file-store").toString();
    new FhirTerminologyImporter(spark, fileStore)
        .importFrom(file.toString(), true, registry.baseUrl());

    final ManifestEntry fromFile = manifest(fileStore).get(0);
    // A single file is hashed but carries no package identity or verification status.
    assertEquals(FhirPackageFixtures.sha256Hex(file), fromFile.getSourceSha256());
    assertNull(fromFile.getPackageVerification());
    assertNull(fromFile.getPackageName());

    final Path dirSource = dir.resolve("source");
    Files.createDirectories(dirSource);
    Files.copy(file, dirSource.resolve("nested.json"));
    final String dirStore = dir.resolve("dir-store").toString();
    new FhirTerminologyImporter(spark, dirStore)
        .importFrom(dirSource.toString(), true, registry.baseUrl());

    final ManifestEntry fromDirectory = manifest(dirStore).get(0);
    assertNull(fromDirectory.getSourceSha256());
    assertNull(fromDirectory.getPackageVerification());
    assertEquals(0, registry.getAllServeEvents().size());
  }

  @Test
  void verificationOptionHasNoEffectOnNonPackageSources(@TempDir final Path dir) {
    final Path file = FhirPackageFixtures.resource("nested-hierarchy.json");
    final String verifying = dir.resolve("verifying").toString();
    final String skipping = dir.resolve("skipping").toString();

    new FhirTerminologyImporter(spark, verifying)
        .importFrom(file.toString(), true, registry.baseUrl());
    new FhirTerminologyImporter(spark, skipping).importFrom(file.toString(), false, null);

    final ManifestEntry verified = manifest(verifying).get(0);
    final ManifestEntry skipped = manifest(skipping).get(0);
    assertEquals(verified.getSourceSha256(), skipped.getSourceSha256());
    assertNull(verified.getPackageVerification());
    assertNull(skipped.getPackageVerification());
    assertEquals(0, registry.getAllServeEvents().size());
  }

  // --- Source fingerprint (feature 059, user story 2). ---

  @Test
  void jsonFileSourceRecordsTheFileSha256(@TempDir final Path dir) throws Exception {
    final Path file = FhirPackageFixtures.resource("nested-hierarchy.json");
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store).importFrom(file.toString(), true, registry.baseUrl());

    final ManifestEntry entry = manifest(store).get(0);
    assertEquals(FhirPackageFixtures.sha256Hex(file), entry.getSourceSha256());
  }

  @Test
  void directorySourceRecordsNullsEvenWithAPackageJson(@TempDir final Path dir) throws Exception {
    final Path source = dir.resolve("source");
    Files.createDirectories(source);
    Files.writeString(source.resolve("package.json"), FhirPackageFixtures.DEFAULT_PACKAGE_JSON);
    Files.copy(
        FhirPackageFixtures.resource("nested-hierarchy.json"), source.resolve("nested.json"));
    final String store = dir.resolve("store").toString();

    new FhirTerminologyImporter(spark, store)
        .importFrom(source.toString(), true, registry.baseUrl());

    final ManifestEntry entry = manifest(store).get(0);
    // A directory is not an archive, so its resources are not the content of any one file: a
    // package.json sitting beside them names nothing that was imported as a package.
    assertNull(entry.getSourceSha256());
    assertNull(entry.getPackageName());
    assertNull(entry.getPackageVersion());
    assertNull(entry.getPackageVerification());
    assertNull(entry.getPackageRegistry());
    assertEquals(0, registry.getAllServeEvents().size());
  }

  @Test
  void reimportReplacesTheRowsProvenance(@TempDir final Path dir) throws Exception {
    final Path archive =
        FhirPackageFixtures.buildPackage(dir, "fixtures.tgz", "nested-hierarchy.json");
    final String store = dir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store).importFrom(archive.toString(), false, null);
    assertEquals(PackageVerification.SKIPPED, manifest(store).get(0).getPackageVerification());

    RegistryStub.stubListing(
        registry,
        FhirPackageFixtures.PACKAGE_NAME,
        FhirPackageFixtures.PACKAGE_VERSION,
        FhirPackageFixtures.sha1Hex(archive));
    new FhirTerminologyImporter(spark, store)
        .importFrom(archive.toString(), true, registry.baseUrl());

    // The manifest row was replaced, so it describes the most recent import rather than the first.
    final List<ManifestEntry> manifest = manifest(store);
    assertEquals(1, manifest.size());
    assertEquals(PackageVerification.VERIFIED, manifest.get(0).getPackageVerification());
    assertEquals(registry.baseUrl(), manifest.get(0).getPackageRegistry());
  }

  /** Stubs a listing whose checksum belongs to a differently compressed copy of the archive. */
  private void stubMismatch(final Path archive) throws IOException {
    RegistryStub.stubListing(
        registry,
        FhirPackageFixtures.PACKAGE_NAME,
        FhirPackageFixtures.PACKAGE_VERSION,
        FhirPackageFixtures.sha1Hex(FhirPackageFixtures.recompress(archive)));
  }

  private static int closedPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private List<ManifestEntry> manifest(final String store) {
    return TerminologyStoreReader.open(store, Map.of()).readManifest();
  }

  private Set<String> importFixtureClosure(
      final Path dir, final String fixtureName, final String suffix) {
    final String store = dir.resolve("store-" + suffix).toString();
    new FhirTerminologyImporter(spark, store)
        .importFrom(FhirPackageFixtures.resource(fixtureName).toString(), false, null);
    return closurePairs(store);
  }

  private Set<String> importNestedAndReadClosure(final Path dir, final String suffix) {
    final String store = dir.resolve("store-" + suffix).toString();
    new FhirTerminologyImporter(spark, store)
        .importFrom(FhirPackageFixtures.resource("nested-hierarchy.json").toString(), false, null);
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
