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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.test.FhirFixtures;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
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
            TerminologyImportException.class,
            () -> importer.importFrom(corruptPackage.toString(), false, null));
    final String message = e.getMessage();
    assertTrue(message.contains("http://example.org/fhir/CodeSystem/corrupt"), message);
    assertTrue(message.toLowerCase().contains("partial"), message);
    assertTrue(message.toLowerCase().contains("re-run"), message);

    // Re-running with a corrected source repairs the store.
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
