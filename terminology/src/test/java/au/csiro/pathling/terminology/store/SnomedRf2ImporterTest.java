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
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_COUNT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DEFINED;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DESCENDANT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DIRECT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_REFERENCED_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_REFSET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ROLE_GROUP;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SNOMED_EDITION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SNOMED_EFFECTIVE_TIME;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TYPE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_URL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.DESCRIPTION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.REFSET_MEMBER;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.RELATIONSHIP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.test.Rf2Mini;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the SNOMED CT RF2 importer against the rf2-mini fixture: the store tables carry the
 * expected content, dense identifiers are contiguous, edition and version are detected (and
 * overridable), and a source that is not a snapshot release is rejected without touching the store.
 *
 * @author John Grimes
 */
class SnomedRf2ImporterTest {

  private static SparkSession spark;
  private static TerminologyStoreReader reader;
  private static Map<String, Integer> denseByCode;
  private static Map<Integer, String> codeByDense;

  @BeforeAll
  static void setUp(@TempDir final Path warehouse, @TempDir final Path storeDir) {
    spark =
        SparkSession.builder()
            .appName("SnomedRf2ImporterTest")
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

    final String store = storeDir.resolve("store").toString();
    new SnomedRf2Importer(spark, store).importFrom(Rf2Mini.baseRelease().toString(), null);
    reader = TerminologyStoreReader.open(store, Map.of());

    denseByCode = new HashMap<>();
    codeByDense = new HashMap<>();
    reader.readTable(
        CONCEPT,
        row -> {
          final String code = row.getString(COLUMN_CODE);
          final int dense = row.getInt(COLUMN_DENSE_ID);
          denseByCode.put(code, dense);
          codeByDense.put(dense, code);
        });
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  void recordsTheCodeSystemAndManifest() {
    final List<ManifestEntry> manifest = reader.readManifest();
    assertEquals(1, manifest.size());
    assertEquals("code_system", manifest.get(0).getEntryType());
    assertEquals(Rf2Mini.SNOMED_URI, manifest.get(0).getCanonicalUrl());
    assertEquals(Rf2Mini.VERSION_20230601, manifest.get(0).getVersion());

    final Map<String, String> codeSystem = new HashMap<>();
    reader.readTable(
        CODE_SYSTEM,
        row -> {
          codeSystem.put(COLUMN_URL, row.getString(COLUMN_URL));
          codeSystem.put(COLUMN_VERSION, row.getString(COLUMN_VERSION));
          codeSystem.put(COLUMN_SNOMED_EDITION, row.getString(COLUMN_SNOMED_EDITION));
          codeSystem.put(COLUMN_SNOMED_EFFECTIVE_TIME, row.getString(COLUMN_SNOMED_EFFECTIVE_TIME));
          codeSystem.put(
              "hierarchy_meaning", row.getString(TerminologyStoreSchema.COLUMN_HIERARCHY_MEANING));
          codeSystem.put(COLUMN_CONCEPT_COUNT, String.valueOf(row.getLong(COLUMN_CONCEPT_COUNT)));
        });
    assertEquals(Rf2Mini.SNOMED_URI, codeSystem.get(COLUMN_URL));
    assertEquals(Rf2Mini.VERSION_20230601, codeSystem.get(COLUMN_VERSION));
    assertEquals(Rf2Mini.CORE_MODULE, codeSystem.get(COLUMN_SNOMED_EDITION));
    assertEquals("20230601", codeSystem.get(COLUMN_SNOMED_EFFECTIVE_TIME));
    assertEquals("is-a", codeSystem.get("hierarchy_meaning"));
    assertEquals(
        String.valueOf(Rf2Mini.CONCEPT_COUNT_20230601), codeSystem.get(COLUMN_CONCEPT_COUNT));
  }

  @Test
  void assignsContiguousDenseIdentifiers() {
    assertEquals(Rf2Mini.CONCEPT_COUNT_20230601, denseByCode.size());
    final Set<Integer> expected =
        IntStream.range(0, Rf2Mini.CONCEPT_COUNT_20230601).boxed().collect(Collectors.toSet());
    assertEquals(expected, new HashSet<>(denseByCode.values()));
  }

  @Test
  void capturesConceptStatusAndDisplay() {
    final Map<String, Boolean> active = new HashMap<>();
    final Map<String, Boolean> defined = new HashMap<>();
    final Map<String, String> display = new HashMap<>();
    reader.readTable(
        CONCEPT,
        row -> {
          final String code = row.getString(COLUMN_CODE);
          active.put(code, row.getBoolean(COLUMN_ACTIVE));
          defined.put(code, row.getBoolean(COLUMN_DEFINED));
          display.put(code, row.getString(COLUMN_DISPLAY));
        });
    assertTrue(active.get(Rf2Mini.DIABETES));
    assertFalse(active.get(Rf2Mini.DIABETES_INACTIVE));
    assertTrue(defined.get(Rf2Mini.DIABETES));
    assertFalse(defined.get(Rf2Mini.GESTATIONAL_DIABETES));
    assertEquals("Diabetes mellitus", display.get(Rf2Mini.DIABETES));
    assertEquals("Type 2 diabetes mellitus", display.get(Rf2Mini.TYPE2_DIABETES));
  }

  @Test
  void storesAttributeRelationshipsWithRoleGroups() {
    final Set<String> findingSiteEdges = new HashSet<>();
    reader.readTable(
        RELATIONSHIP,
        row -> {
          if (Rf2Mini.FINDING_SITE.equals(row.getString(COLUMN_TYPE_CODE))) {
            final String source = codeByDense.get(row.getInt(COLUMN_SOURCE_DENSE_ID));
            final String target = codeByDense.get(row.getInt(COLUMN_TARGET_DENSE_ID));
            findingSiteEdges.add(source + "->" + target + "@" + row.getInt(COLUMN_ROLE_GROUP));
          }
        });
    // Diabetes and its type children point at the pancreas in role group 1.
    assertTrue(
        findingSiteEdges.contains(Rf2Mini.DIABETES + "->" + Rf2Mini.PANCREAS_STRUCTURE + "@1"));
    assertTrue(
        findingSiteEdges.contains(
            Rf2Mini.TYPE1_DIABETES + "->" + Rf2Mini.PANCREAS_STRUCTURE + "@1"));
  }

  @Test
  void computesTheTransitiveClosure() {
    final Set<String> directPairs = new HashSet<>();
    final Set<String> allPairs = new HashSet<>();
    reader.readTable(
        CLOSURE,
        row -> {
          final String ancestor = codeByDense.get(row.getInt(COLUMN_ANCESTOR_DENSE_ID));
          final String descendant = codeByDense.get(row.getInt(COLUMN_DESCENDANT_DENSE_ID));
          allPairs.add(ancestor + "->" + descendant);
          if (row.getBoolean(COLUMN_DIRECT)) {
            directPairs.add(ancestor + "->" + descendant);
          }
        });
    // Direct edge DIABETES -> TYPE1.
    assertTrue(directPairs.contains(Rf2Mini.DIABETES + "->" + Rf2Mini.TYPE1_DIABETES));
    // Transitive: DISORDER is an ancestor of the deeply nested complication concept.
    assertTrue(allPairs.contains(Rf2Mini.DISORDER + "->" + Rf2Mini.TYPE2_WITH_COMPLICATION));
    assertFalse(directPairs.contains(Rf2Mini.DISORDER + "->" + Rf2Mini.TYPE2_WITH_COMPLICATION));
    // The root finding subsumes diabetes transitively.
    assertTrue(allPairs.contains(Rf2Mini.ROOT_FINDING + "->" + Rf2Mini.DIABETES));
    // No self pairs.
    assertFalse(allPairs.stream().anyMatch(p -> p.split("->")[0].equals(p.split("->")[1])));
  }

  @Test
  void loadsSimpleAndAssociationReferenceSets() {
    final Set<String> simpleMembers = new HashSet<>();
    final Map<String, String> associationTargets = new HashMap<>();
    reader.readTable(
        REFSET_MEMBER,
        row -> {
          final String refset = row.getString(COLUMN_REFSET_CODE);
          final String referenced = codeByDense.get(row.getInt(COLUMN_REFERENCED_DENSE_ID));
          if (Rf2Mini.SIMPLE_REFSET.equals(refset)) {
            simpleMembers.add(referenced);
          } else if (Rf2Mini.SAME_AS_REFSET.equals(refset)) {
            associationTargets.put(referenced, row.getString(COLUMN_TARGET_CODE));
          }
        });
    assertEquals(
        new TreeSet<>(
            List.of(Rf2Mini.TYPE1_DIABETES, Rf2Mini.TYPE2_DIABETES, Rf2Mini.GESTATIONAL_DIABETES)),
        new TreeSet<>(simpleMembers));
    assertEquals(Rf2Mini.TYPE2_DIABETES, associationTargets.get(Rf2Mini.DIABETES_INACTIVE));
  }

  @Test
  void appliesTheEditionUriOverride(@TempDir final Path storeDir) {
    final String store = storeDir.resolve("store").toString();
    final String override = "http://snomed.info/sct/32506021000036107/version/20240101";
    new SnomedRf2Importer(spark, store).importFrom(Rf2Mini.baseRelease().toString(), override);

    final TerminologyStoreReader overrideReader = TerminologyStoreReader.open(store, Map.of());
    final Map<String, String> codeSystem = new HashMap<>();
    overrideReader.readTable(
        CODE_SYSTEM,
        row -> {
          codeSystem.put(COLUMN_VERSION, row.getString(COLUMN_VERSION));
          codeSystem.put(COLUMN_SNOMED_EDITION, row.getString(COLUMN_SNOMED_EDITION));
        });
    assertEquals(override, codeSystem.get(COLUMN_VERSION));
    assertEquals("32506021000036107", codeSystem.get(COLUMN_SNOMED_EDITION));
  }

  @Test
  void detectsDerivedEditionFromModuleDependencyRefset(@TempDir final Path work) throws Exception {
    // Arrange: copy the base release, move a handful of concepts into a derived edition module,
    // and add a module dependency reference set declaring that module's dependency on the core and
    // model modules, alongside a content-less leaf module (mirroring the International ICD-10
    // mapping module). The concept content remains majority core module and the mapping module is
    // also at the top of the dependency graph, so detection must select the concept-bearing module
    // that depends on the other concept-bearing modules.
    final String extensionModule = "32506021000036107";
    final String modelModule = "900000000000012004";
    final String mappingModule = "449080006";
    final String moduleDependencyRefset = "900000000000534007";
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    reassignConceptModules(release, extensionModule, 5);
    final Path metadata = release.resolve("Snapshot").resolve("Refset").resolve("Metadata");
    Files.createDirectories(metadata);
    final String header =
        "id\teffectiveTime\tactive\tmoduleId\trefsetId\treferencedComponentId"
            + "\tsourceEffectiveTime\ttargetEffectiveTime\n";
    final StringBuilder refset = new StringBuilder(header);
    int member = 0;
    for (final String[] edge :
        new String[][] {
          {extensionModule, Rf2Mini.CORE_MODULE},
          {extensionModule, modelModule},
          {Rf2Mini.CORE_MODULE, modelModule},
          {mappingModule, Rf2Mini.CORE_MODULE},
          {mappingModule, modelModule}
        }) {
      refset
          .append(
              String.join(
                  "\t",
                  "m" + ++member,
                  "20230601",
                  "1",
                  edge[0],
                  moduleDependencyRefset,
                  edge[1],
                  "20230601",
                  "20230601"))
          .append("\n");
    }
    Files.writeString(
        metadata.resolve("der2_ssRefset_ModuleDependencySnapshot_AU1000036_20230601.txt"),
        refset.toString());

    // Act: import with detection (no override).
    final String store = work.resolve("store").toString();
    new SnomedRf2Importer(spark, store).importFrom(release.toString(), null);

    // Assert: the edition is the derived module at the top of the dependency graph.
    final TerminologyStoreReader derivedReader = TerminologyStoreReader.open(store, Map.of());
    final Map<String, String> codeSystem = new HashMap<>();
    derivedReader.readTable(
        CODE_SYSTEM,
        row -> {
          codeSystem.put(COLUMN_VERSION, row.getString(COLUMN_VERSION));
          codeSystem.put(COLUMN_SNOMED_EDITION, row.getString(COLUMN_SNOMED_EDITION));
        });
    assertEquals(
        "http://snomed.info/sct/" + extensionModule + "/version/20230601",
        codeSystem.get(COLUMN_VERSION));
    assertEquals(extensionModule, codeSystem.get(COLUMN_SNOMED_EDITION));
  }

  @Test
  void combinesMultipleDescriptionAndLanguageFiles(@TempDir final Path work) throws Exception {
    // Arrange: copy the base release and split its description file into a description and a text
    // definition file, and its language reference set into two files, as real releases ship (for
    // example sct2_Description plus sct2_TextDefinition, and one language file per dialect). All
    // files must be read; earlier revisions kept only one file of each kind.
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    splitFile(
        release.resolve("Snapshot").resolve("Terminology"),
        "sct2_Description_",
        "sct2_TextDefinition_Snapshot-en_INT_20230601.txt");
    splitFile(
        release.resolve("Snapshot").resolve("Refset").resolve("Language"),
        "der2_cRefset_Language",
        "der2_cRefset_LanguageSnapshot-en-XX_INT_20230601.txt");

    // Act: import the split release.
    final String store = work.resolve("store").toString();
    new SnomedRf2Importer(spark, store).importFrom(release.toString(), null);

    // Assert: the description table and displays match the unsplit base import exactly.
    final TerminologyStoreReader splitReader = TerminologyStoreReader.open(store, Map.of());
    final AtomicInteger splitDescriptions = new AtomicInteger();
    splitReader.readTable(DESCRIPTION, row -> splitDescriptions.incrementAndGet());
    final AtomicInteger baseDescriptions = new AtomicInteger();
    reader.readTable(DESCRIPTION, row -> baseDescriptions.incrementAndGet());
    assertEquals(baseDescriptions.get(), splitDescriptions.get());

    final Map<String, String> display = new HashMap<>();
    splitReader.readTable(
        CONCEPT, row -> display.put(row.getString(COLUMN_CODE), row.getString(COLUMN_DISPLAY)));
    assertEquals("Diabetes mellitus", display.get(Rf2Mini.DIABETES));
    assertEquals("Type 2 diabetes mellitus", display.get(Rf2Mini.TYPE2_DIABETES));
  }

  /**
   * Moves the second half of the data rows of the single file in {@code directory} whose name
   * starts with {@code prefix} into a new sibling file named {@code newName}, preserving the header
   * in both.
   */
  private static void splitFile(final Path directory, final String prefix, final String newName)
      throws Exception {
    final Path original;
    try (final Stream<Path> files = Files.list(directory)) {
      original =
          files
              .filter(f -> f.getFileName().toString().startsWith(prefix))
              .findFirst()
              .orElseThrow();
    }
    final List<String> lines = Files.readAllLines(original);
    final int splitPoint = 1 + (lines.size() - 1) / 2;
    final List<String> first = new ArrayList<>(lines.subList(0, splitPoint));
    final List<String> second = new ArrayList<>();
    second.add(lines.get(0));
    second.addAll(lines.subList(splitPoint, lines.size()));
    Files.write(original, first);
    Files.write(directory.resolve(newName), second);
  }

  @Test
  void rejectsNonSnapshotSourceWithoutTouchingStore(@TempDir final Path emptyDir) throws Exception {
    // A directory with a Full release layout but no Snapshot directory.
    final Path full = emptyDir.resolve("Full").resolve("Terminology");
    Files.createDirectories(full);
    Files.writeString(
        full.resolve("sct2_Concept_Full_INT_20230601.txt"),
        "id\teffectiveTime\tactive\tmoduleId\tdefinitionStatusId\n");

    final String store = emptyDir.resolve("store").toString();
    final SnomedRf2Importer importer = new SnomedRf2Importer(spark, store);
    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class, () -> importer.importFrom(emptyDir.toString(), null));
    assertTrue(e.getMessage().toLowerCase().contains("snapshot"));
    // Nothing was written to the store.
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
  }

  @Test
  void importsFromZipArchive(@TempDir final Path work) throws Exception {
    // Arrange: package the rf2-mini base release into a .zip archive, as real releases ship.
    final Path archive = work.resolve("rf2.zip");
    zipDirectory(Rf2Mini.baseRelease(), archive);
    final String store = work.resolve("zip-store").toString();

    // Act: import directly from the archive without extracting it first.
    new SnomedRf2Importer(spark, store).importFrom(archive.toString(), null);

    // Assert: the archive yielded the same concept set as the equivalent directory import.
    final TerminologyStoreReader zipReader = TerminologyStoreReader.open(store, Map.of());
    final AtomicInteger conceptCount = new AtomicInteger();
    zipReader.readTable(CONCEPT, row -> conceptCount.incrementAndGet());
    assertEquals(denseByCode.size(), conceptCount.get());
  }

  /**
   * Rewrites the module of the first {@code count} active concepts in the release's concept file to
   * {@code module}, giving a derived edition module some content of its own.
   */
  private static void reassignConceptModules(
      final Path release, final String module, final int count) throws Exception {
    final Path terminology = release.resolve("Snapshot").resolve("Terminology");
    try (final Stream<Path> files = Files.list(terminology)) {
      final Path conceptFile =
          files
              .filter(f -> f.getFileName().toString().startsWith("sct2_Concept_"))
              .findFirst()
              .orElseThrow();
      final List<String> lines = Files.readAllLines(conceptFile);
      int reassigned = 0;
      for (int i = 1; i < lines.size() && reassigned < count; i++) {
        final String[] fields = lines.get(i).split("\t", -1);
        if ("1".equals(fields[2])) {
          fields[3] = module;
          lines.set(i, String.join("\t", fields));
          reassigned++;
        }
      }
      Files.write(conceptFile, lines);
    }
  }

  /** Copies every regular file beneath {@code source} into {@code target}, preserving layout. */
  private static void copyDirectory(final Path source, final Path target) throws Exception {
    try (final Stream<Path> files = Files.walk(source)) {
      for (final Path file : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
        final Path destination = target.resolve(source.relativize(file).toString());
        Files.createDirectories(destination.getParent());
        Files.copy(file, destination);
      }
    }
  }

  /** Writes every regular file beneath {@code directory} into a zip archive at {@code archive}. */
  private static void zipDirectory(final Path directory, final Path archive) throws Exception {
    try (final OutputStream fileOut = Files.newOutputStream(archive);
        final ZipOutputStream zipOut = new ZipOutputStream(fileOut);
        final Stream<Path> files = Files.walk(directory)) {
      for (final Path file : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
        zipOut.putNextEntry(new ZipEntry(directory.relativize(file).toString().replace('\\', '/')));
        Files.copy(file, zipOut);
        zipOut.closeEntry();
      }
    }
  }
}
