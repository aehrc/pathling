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
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * Verifies the SNOMED CT RF2 importer against the rf2-mini fixture: the store tables carry the
 * expected content, dense identifiers are contiguous, edition and version are detected (and
 * overridable), every file joined against the concept dictionary reports how much of it resolved,
 * more than one file matching a single-valued role is rejected, and a source that is not a snapshot
 * release is rejected without touching the store.
 *
 * @author John Grimes
 */
class SnomedRf2ImporterTest {

  // File names within the rf2-mini base release, and the prefixes that identify their roles.
  private static final String CONCEPT_FILE = "sct2_Concept_Snapshot_INT_20230601.txt";
  private static final String DESCRIPTION_FILE = "sct2_Description_Snapshot-en_INT_20230601.txt";
  private static final String RELATIONSHIP_FILE = "sct2_Relationship_Snapshot_INT_20230601.txt";
  private static final String SIMPLE_REFSET_FILE = "der2_Refset_SimpleSnapshot_INT_20230601.txt";
  private static final String ASSOCIATION_REFSET_FILE =
      "der2_cRefset_AssociationSnapshot_INT_20230601.txt";
  private static final String LANGUAGE_REFSET_FILE =
      "der2_cRefset_LanguageSnapshot-en_INT_20230601.txt";
  private static final String CONCEPT_PREFIX = "sct2_Concept_";
  private static final String DESCRIPTION_PREFIX = "sct2_Description_";
  private static final String RELATIONSHIP_PREFIX = "sct2_Relationship_";
  private static final String LANGUAGE_PREFIX = "der2_cRefset_Language";
  private static final String SIMPLE_REFSET_PREFIX = "der2_Refset_Simple";

  private static final String MODULE_DEPENDENCY_REFSET = "900000000000534007";
  private static final String MODEL_MODULE = "900000000000012004";
  private static final String SYNONYM_TYPE = "900000000000013009";
  private static final String CASE_INSENSITIVE = "900000000000448009";
  private static final String STATED_RELATIONSHIP = "900000000000011006";
  private static final String SOME_MODIFIER = "900000000000451002";

  /** A concept code the fixture does not ship, used to make a reference dangle deliberately. */
  private static final String ABSENT_CODE = "9999999999";

  /**
   * The number of Spark jobs one import of the base release into a fresh store runs. Measured three
   * times on the importer as it stood before the per-file resolution reporting was added, and
   * asserted exactly so that any additional pass over an RF2 file fails the build.
   */
  private static final int BASELINE_SPARK_JOBS = 79;

  /** The per-file resolution line specified in {@code contracts/import-diagnostics.md}. */
  private static final Pattern RESOLUTION_LINE =
      Pattern.compile(
          "^(?<path>.+?): (?<resolved>\\d+) of (?<input>\\d+)"
              + " active rows resolved against the concept dictionary\\.$");

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
    final String mappingModule = "449080006";
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    reassignConceptModules(release, extensionModule, 5);
    writeModuleDependencyRefset(
        release,
        "der2_ssRefset_ModuleDependencySnapshot_AU1000036_20230601.txt",
        new String[][] {
          {extensionModule, Rf2Mini.CORE_MODULE},
          {extensionModule, MODEL_MODULE},
          {Rf2Mini.CORE_MODULE, MODEL_MODULE},
          {mappingModule, Rf2Mini.CORE_MODULE},
          {mappingModule, MODEL_MODULE}
        });

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
        terminologyDirectory(release),
        DESCRIPTION_PREFIX,
        "sct2_TextDefinition_Snapshot-en_INT_20230601.txt");
    splitFile(
        languageRefsetDirectory(release),
        LANGUAGE_PREFIX,
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

  // --- Per-file resolution reporting (User Story 1). ---

  @Test
  void reportsResolutionCountsForEveryResolvedFile(@TempDir final Path work) throws Throwable {
    // Every file joined against the concept dictionary reports its active input row count and the
    // count that resolved. The base release resolves fully except for four is-a rows whose
    // destination is 138875005 ("SNOMED CT Concept"), a concept the fixture does not ship.
    final List<ILoggingEvent> events = new ArrayList<>();
    captureImportLog(
        events,
        () ->
            new SnomedRf2Importer(spark, work.resolve("store").toString())
                .importFrom(Rf2Mini.baseRelease().toString(), null));

    assertEquals(
        Map.of(
            DESCRIPTION_FILE, "401 of 401",
            RELATIONSHIP_FILE, "199 of 203",
            SIMPLE_REFSET_FILE, "3 of 3",
            ASSOCIATION_REFSET_FILE, "4 of 4"),
        resolutionCounts(events));

    // Each line names the source file path, not merely its name, and is reported informationally.
    final String releaseRoot = Rf2Mini.baseRelease().toString();
    assertEquals(4, resolutionLines(events).size());
    assertTrue(resolutionLines(events).stream().allMatch(line -> line.contains(releaseRoot)));
    assertEquals(List.of(), eventsAbove(events, Level.INFO));
  }

  @Test
  void reportsNoResolutionCountForFilesNotJoinedToConcepts(@TempDir final Path work)
      throws Throwable {
    // The concept file, the language reference sets and the Module Dependency reference set are not
    // resolved against the concept dictionary, so none of them produces a line.
    final String moduleDependencyFile = "der2_ssRefset_ModuleDependencySnapshot_INT_20230601.txt";
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    writeModuleDependencyRefset(
        release, moduleDependencyFile, new String[][] {{Rf2Mini.CORE_MODULE, MODEL_MODULE}});

    final List<ILoggingEvent> events = new ArrayList<>();
    captureImportLog(
        events,
        () ->
            new SnomedRf2Importer(spark, work.resolve("store").toString())
                .importFrom(release.toString(), null));

    final Set<String> reported = resolutionCounts(events).keySet();
    assertFalse(reported.contains(CONCEPT_FILE));
    assertFalse(reported.contains(LANGUAGE_REFSET_FILE));
    assertFalse(reported.contains(moduleDependencyFile));
    assertEquals(
        Set.of(DESCRIPTION_FILE, RELATIONSHIP_FILE, SIMPLE_REFSET_FILE, ASSOCIATION_REFSET_FILE),
        reported);
  }

  @Test
  void reportsTheShortfallOfADerivedPackageWithoutFailing(@TempDir final Path work)
      throws Throwable {
    // The shape of a derived package imported without its declared dependency: two bookkeeping
    // concepts, and a full complement of descriptions, relationships and reference set members
    // referencing concepts the package does not ship. The import must still succeed, and the
    // shortfall must be readable straight off the log.
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    truncateFile(terminologyDirectory(release), CONCEPT_PREFIX, 2);

    final List<ILoggingEvent> events = new ArrayList<>();
    captureImportLog(
        events,
        () ->
            new SnomedRf2Importer(spark, work.resolve("store").toString())
                .importFrom(release.toString(), null));

    // The import succeeded, on the two concepts that remain.
    assertTrue(
        events.stream()
            .anyMatch(
                event -> "Writing 2 concepts to the store".equals(event.getFormattedMessage())));
    // Unresolved rows are information, not an alarm: nothing above informational severity.
    assertEquals(List.of(), eventsAbove(events, Level.INFO));

    // Every affected file resolves less than it took in, and the reference sets resolve nothing.
    final Map<String, String> reported = resolutionCounts(events);
    assertEquals("0 of 3", reported.get(SIMPLE_REFSET_FILE));
    assertEquals("0 of 4", reported.get(ASSOCIATION_REFSET_FILE));
    assertResolvedBelowInput(reported, DESCRIPTION_FILE, 401);
    assertResolvedBelowInput(reported, RELATIONSHIP_FILE, 203);
  }

  @Test
  void excludesInactiveRowsFromTheInputCount(@TempDir final Path work) throws Throwable {
    // Inactive rows are excluded by design rather than by failure, so counting them as input would
    // report a permanent shortfall on every healthy release.
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    appendRows(
        terminologyDirectory(release),
        DESCRIPTION_PREFIX,
        List.of(
            descriptionRow("5900001", "0", Rf2Mini.DIABETES, "Retired synonym"),
            descriptionRow("5900002", "0", Rf2Mini.DIABETES, "Another retired synonym")));

    final List<ILoggingEvent> events = new ArrayList<>();
    captureImportLog(
        events,
        () ->
            new SnomedRf2Importer(spark, work.resolve("store").toString())
                .importFrom(release.toString(), null));

    // The two added rows are inactive, so the input figure does not grow beyond the base release's.
    assertEquals("401 of 401", resolutionCounts(events).get(DESCRIPTION_FILE));
  }

  @Test
  void reportsEachDescriptionFileSeparately(@TempDir final Path work) throws Throwable {
    // A release shipping several description files must let the offending file be identified, so
    // the figures are reported per file rather than aggregated into one line for the table.
    final String secondDescriptionFile = "sct2_Description_Snapshot-en-XX_INT_20230601.txt";
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    splitFile(terminologyDirectory(release), DESCRIPTION_PREFIX, secondDescriptionFile);

    final List<ILoggingEvent> events = new ArrayList<>();
    captureImportLog(
        events,
        () ->
            new SnomedRf2Importer(spark, work.resolve("store").toString())
                .importFrom(release.toString(), null));

    // The 401 active rows are split across the two files, each reported on its own line.
    final Map<String, String> reported = resolutionCounts(events);
    assertEquals("200 of 200", reported.get(DESCRIPTION_FILE));
    assertEquals("201 of 201", reported.get(secondDescriptionFile));
  }

  @Test
  void dropsRelationshipRowsWhoseDestinationIsAbsent(@TempDir final Path work) throws Throwable {
    // A relationship resolves only when both its source and its destination are present, so a row
    // with a present source and an absent destination is dropped.
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    appendRows(
        terminologyDirectory(release),
        RELATIONSHIP_PREFIX,
        List.of(relationshipRow("r900001", Rf2Mini.DIABETES, ABSENT_CODE)));

    final List<ILoggingEvent> events = new ArrayList<>();
    captureImportLog(
        events,
        () ->
            new SnomedRf2Importer(spark, work.resolve("store").toString())
                .importFrom(release.toString(), null));

    // One more active row went in, and the resolved figure is unchanged.
    assertEquals("199 of 204", resolutionCounts(events).get(RELATIONSHIP_FILE));
  }

  @Test
  void rejectsReleaseWithNoActiveConceptsBeforeCounting(@TempDir final Path work) throws Exception {
    // The existing guard still fires, and it fires before any resolution counting happens.
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    deactivateRows(terminologyDirectory(release), CONCEPT_PREFIX);

    final SnomedRf2Importer importer =
        new SnomedRf2Importer(spark, work.resolve("store").toString());
    final List<ILoggingEvent> events = new ArrayList<>();
    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class,
            () -> captureImportLog(events, () -> importer.importFrom(release.toString(), null)));
    assertEquals(
        "The release contains no active concepts, so no edition can be detected.", e.getMessage());
    assertEquals(Map.of(), resolutionCounts(events));
  }

  @Test
  void importAddsNoSparkJobs(@TempDir final Path work) throws Exception {
    // The counts are collected by metric aggregation attached to the write actions the import
    // already runs, so no action, and therefore no Spark job, is added.
    final AtomicInteger jobs = new AtomicInteger();
    final SparkListener listener =
        new SparkListener() {
          @Override
          public void onJobEnd(final SparkListenerJobEnd jobEnd) {
            jobs.incrementAndGet();
          }
        };
    spark.sparkContext().addSparkListener(listener);
    try {
      new SnomedRf2Importer(spark, work.resolve("store").toString())
          .importFrom(Rf2Mini.baseRelease().toString(), null);
      spark.sparkContext().listenerBus().waitUntilEmpty();
    } finally {
      spark.sparkContext().removeSparkListener(listener);
    }
    assertEquals(BASELINE_SPARK_JOBS, jobs.get());
  }

  // --- Ambiguous file discovery (User Story 2). ---

  @Test
  void rejectsTwoConceptFiles(@TempDir final Path work) throws Exception {
    // Two release trees dropped into one directory: the concept role is single-valued, so the
    // import must fail rather than silently keep one tree's content.
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    final String duplicate = "sct2_Concept_Snapshot_INT_20240601.txt";
    duplicateFile(terminologyDirectory(release), CONCEPT_PREFIX, duplicate);

    final String store = work.resolve("store").toString();
    final SnomedRf2Importer importer = new SnomedRf2Importer(spark, store);
    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class, () -> importer.importFrom(release.toString(), null));
    assertAmbiguousDiscovery(e, "concept", release, CONCEPT_FILE, duplicate);
    // Nothing was written to the store, because the failure precedes any content being read.
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
  }

  @Test
  void rejectsTwoRelationshipFiles(@TempDir final Path work) throws Exception {
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    final String duplicate = "sct2_Relationship_Snapshot_INT_20240601.txt";
    duplicateFile(terminologyDirectory(release), RELATIONSHIP_PREFIX, duplicate);

    final String store = work.resolve("store").toString();
    final SnomedRf2Importer importer = new SnomedRf2Importer(spark, store);
    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class, () -> importer.importFrom(release.toString(), null));
    assertAmbiguousDiscovery(e, "relationship", release, RELATIONSHIP_FILE, duplicate);
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
  }

  @Test
  void rejectsTwoModuleDependencyFiles(@TempDir final Path work) throws Exception {
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    final String first = "der2_ssRefset_ModuleDependencySnapshot_INT_20230601.txt";
    final String second = "der2_ssRefset_ModuleDependencySnapshot_INT_20240601.txt";
    final String[][] edges = {{Rf2Mini.CORE_MODULE, MODEL_MODULE}};
    writeModuleDependencyRefset(release, first, edges);
    writeModuleDependencyRefset(release, second, edges);

    final String store = work.resolve("store").toString();
    final SnomedRf2Importer importer = new SnomedRf2Importer(spark, store);
    final TerminologyImportException e =
        assertThrows(
            TerminologyImportException.class, () -> importer.importFrom(release.toString(), null));
    assertAmbiguousDiscovery(e, "module dependency", release, first, second);
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
  }

  @Test
  void acceptsSeveralFilesForMultiValuedRoles(@TempDir final Path work) throws Exception {
    // Descriptions, text definitions, language reference sets and other reference sets are all
    // legitimately multi-valued, so several files of each must continue to import.
    final Path release = work.resolve("release");
    copyDirectory(Rf2Mini.baseRelease(), release);
    splitFile(
        terminologyDirectory(release),
        DESCRIPTION_PREFIX,
        "sct2_TextDefinition_Snapshot-en_INT_20230601.txt");
    splitFile(
        languageRefsetDirectory(release),
        LANGUAGE_PREFIX,
        "der2_cRefset_LanguageSnapshot-en-XX_INT_20230601.txt");
    splitFile(
        contentRefsetDirectory(release),
        SIMPLE_REFSET_PREFIX,
        "der2_Refset_SimpleSnapshot-XX_INT_20230601.txt");

    final String store = work.resolve("store").toString();
    new SnomedRf2Importer(spark, store).importFrom(release.toString(), null);

    // Discovery accepted every multi-valued role, and the whole concept set landed.
    final TerminologyStoreReader multiReader = TerminologyStoreReader.open(store, Map.of());
    final AtomicInteger concepts = new AtomicInteger();
    multiReader.readTable(CONCEPT, row -> concepts.incrementAndGet());
    assertEquals(Rf2Mini.CONCEPT_COUNT_20230601, concepts.get());
  }

  @Test
  void reportsAMissingConceptFileUnchanged(@TempDir final Path work) throws Exception {
    // The existing error for a source with no snapshot concept file, and its full or delta release
    // hint, are both untouched by the ambiguity checks.
    final Path emptySource = work.resolve("empty");
    Files.createDirectories(emptySource);
    final SnomedRf2Importer importer =
        new SnomedRf2Importer(spark, work.resolve("store").toString());
    final TerminologyImportException missing =
        assertThrows(
            TerminologyImportException.class,
            () -> importer.importFrom(emptySource.toString(), null));
    assertEquals(
        "No SNOMED CT snapshot concept file was found under " + emptySource + ".",
        missing.getMessage());

    final Path fullSource = work.resolve("full");
    final Path fullTerminology = fullSource.resolve("Full").resolve("Terminology");
    Files.createDirectories(fullTerminology);
    Files.writeString(
        fullTerminology.resolve("sct2_Concept_Full_INT_20230601.txt"),
        "id\teffectiveTime\tactive\tmoduleId\tdefinitionStatusId\n");
    final TerminologyImportException full =
        assertThrows(
            TerminologyImportException.class,
            () -> importer.importFrom(fullSource.toString(), null));
    assertEquals(
        "No SNOMED CT snapshot concept file was found under "
            + fullSource
            + ". Only snapshot releases are supported; this appears to be a full or delta release.",
        full.getMessage());
  }

  // --- Log capture. ---

  /**
   * Runs {@code action} with the importer's log events captured at {@code INFO} into {@code
   * events}, restoring the logger's previous level and detaching the appender whether or not the
   * action throws. Events are collected even on failure, so a test can assert what was logged
   * before an expected exception.
   */
  private static void captureImportLog(final List<ILoggingEvent> events, final Executable action)
      throws Throwable {
    final Logger logger = (Logger) LoggerFactory.getLogger(SnomedRf2Importer.class);
    final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    final Level previousLevel = logger.getLevel();
    logger.setLevel(Level.INFO);
    logger.addAppender(appender);
    try {
      action.execute();
    } finally {
      events.addAll(appender.list);
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
    }
  }

  /** Returns the formatted per-file resolution lines among {@code events}, in the order logged. */
  private static List<String> resolutionLines(final List<ILoggingEvent> events) {
    return events.stream()
        .map(ILoggingEvent::getFormattedMessage)
        .filter(message -> RESOLUTION_LINE.matcher(message).matches())
        .toList();
  }

  /**
   * Returns the per-file resolution figures among {@code events}, keyed by the reported file's name
   * and valued as {@code "{resolved} of {input}"}.
   */
  private static Map<String, String> resolutionCounts(final List<ILoggingEvent> events) {
    final Map<String, String> counts = new LinkedHashMap<>();
    for (final String line : resolutionLines(events)) {
      final Matcher matcher = RESOLUTION_LINE.matcher(line);
      assertTrue(matcher.matches());
      final String path = matcher.group("path");
      final String name = path.substring(path.lastIndexOf('/') + 1);
      counts.put(name, matcher.group("resolved") + " of " + matcher.group("input"));
    }
    return counts;
  }

  /** Returns the formatted messages of {@code events} logged above {@code level}. */
  private static List<String> eventsAbove(final List<ILoggingEvent> events, final Level level) {
    return events.stream()
        .filter(event -> event.getLevel().toInt() > level.toInt())
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  /**
   * Asserts that {@code file} took in {@code expectedInput} active rows and resolved fewer than
   * that.
   */
  private static void assertResolvedBelowInput(
      final Map<String, String> counts, final String file, final int expectedInput) {
    final String reported = counts.get(file);
    assertTrue(reported != null, () -> "No resolution line was reported for " + file);
    final String[] figures = reported.split(" of ");
    assertEquals(expectedInput, Integer.parseInt(figures[1]));
    assertTrue(
        Integer.parseInt(figures[0]) < expectedInput,
        () -> file + " reported " + reported + ", expecting a shortfall");
  }

  /**
   * Asserts that {@code e} is the ambiguous discovery failure for {@code role}, naming {@code
   * source}, both candidate file names in a stable order, and the advice to concatenate.
   */
  private static void assertAmbiguousDiscovery(
      final TerminologyImportException e,
      final String role,
      final Path source,
      final String firstFile,
      final String secondFile) {
    final String message = e.getMessage();
    assertTrue(
        message.startsWith(
            "Multiple SNOMED CT snapshot " + role + " files were found under " + source + ": "),
        () -> "Unexpected message: " + message);
    assertTrue(message.contains("A single " + role + " file is expected."), message);
    assertTrue(
        message.endsWith(
            "If you are combining releases, concatenate them into one file rather than placing"
                + " both release trees in the same directory."),
        message);
    // Both candidates are named, in a stable order.
    final String earlier = firstFile.compareTo(secondFile) <= 0 ? firstFile : secondFile;
    final String later = firstFile.compareTo(secondFile) <= 0 ? secondFile : firstFile;
    assertTrue(message.contains(earlier), message);
    assertTrue(message.contains(later), message);
    assertTrue(message.indexOf(earlier) < message.indexOf(later), message);
  }

  // --- Fixture shaping. ---

  /** Returns the Snapshot Terminology directory of a release. */
  private static Path terminologyDirectory(final Path release) {
    return release.resolve("Snapshot").resolve("Terminology");
  }

  /** Returns the Snapshot language reference set directory of a release. */
  private static Path languageRefsetDirectory(final Path release) {
    return release.resolve("Snapshot").resolve("Refset").resolve("Language");
  }

  /** Returns the Snapshot content reference set directory of a release. */
  private static Path contentRefsetDirectory(final Path release) {
    return release.resolve("Snapshot").resolve("Refset").resolve("Content");
  }

  /** Returns the single file in {@code directory} whose name starts with {@code prefix}. */
  private static Path fileStartingWith(final Path directory, final String prefix) throws Exception {
    try (final Stream<Path> files = Files.list(directory)) {
      return files
          .filter(file -> file.getFileName().toString().startsWith(prefix))
          .findFirst()
          .orElseThrow();
    }
  }

  /**
   * Moves the second half of the data rows of the single file in {@code directory} whose name
   * starts with {@code prefix} into a new sibling file named {@code newName}, preserving the header
   * in both.
   */
  private static void splitFile(final Path directory, final String prefix, final String newName)
      throws Exception {
    final Path original = fileStartingWith(directory, prefix);
    final List<String> lines = Files.readAllLines(original);
    final int splitPoint = 1 + (lines.size() - 1) / 2;
    final List<String> first = new ArrayList<>(lines.subList(0, splitPoint));
    final List<String> second = new ArrayList<>();
    second.add(lines.get(0));
    second.addAll(lines.subList(splitPoint, lines.size()));
    Files.write(original, first);
    Files.write(directory.resolve(newName), second);
  }

  /**
   * Truncates the single file in {@code directory} whose name starts with {@code prefix} to its
   * header plus its first {@code dataRows} data rows, so that the rows of other files referencing
   * the rest have nothing to resolve against.
   */
  private static void truncateFile(final Path directory, final String prefix, final int dataRows)
      throws Exception {
    final Path file = fileStartingWith(directory, prefix);
    final List<String> lines = Files.readAllLines(file);
    Files.write(file, lines.subList(0, Math.min(lines.size(), dataRows + 1)));
  }

  /** Appends {@code rows} to the single file in {@code directory} starting with {@code prefix}. */
  private static void appendRows(final Path directory, final String prefix, final List<String> rows)
      throws Exception {
    final Path file = fileStartingWith(directory, prefix);
    final List<String> lines = new ArrayList<>(Files.readAllLines(file));
    lines.addAll(rows);
    Files.write(file, lines);
  }

  /**
   * Marks every data row of the single file in {@code directory} starting with {@code prefix} as
   * inactive, giving a release with no active content of that kind.
   */
  private static void deactivateRows(final Path directory, final String prefix) throws Exception {
    final Path file = fileStartingWith(directory, prefix);
    final List<String> lines = new ArrayList<>(Files.readAllLines(file));
    for (int i = 1; i < lines.size(); i++) {
      final String[] fields = lines.get(i).split("\t", -1);
      fields[2] = "0";
      lines.set(i, String.join("\t", fields));
    }
    Files.write(file, lines);
  }

  /**
   * Copies the single file in {@code directory} starting with {@code prefix} to a sibling named
   * {@code newName}, producing two files filling the same role.
   */
  private static void duplicateFile(final Path directory, final String prefix, final String newName)
      throws Exception {
    Files.copy(fileStartingWith(directory, prefix), directory.resolve(newName));
  }

  /**
   * Writes a Module Dependency reference set into {@code release} under {@code fileName}, declaring
   * each {@code {module, dependency}} pair in {@code edges} as an active member.
   */
  private static void writeModuleDependencyRefset(
      final Path release, final String fileName, final String[][] edges) throws Exception {
    final Path metadata = release.resolve("Snapshot").resolve("Refset").resolve("Metadata");
    Files.createDirectories(metadata);
    final StringBuilder refset =
        new StringBuilder(
            "id\teffectiveTime\tactive\tmoduleId\trefsetId\treferencedComponentId"
                + "\tsourceEffectiveTime\ttargetEffectiveTime\n");
    int member = 0;
    for (final String[] edge : edges) {
      refset
          .append(
              String.join(
                  "\t",
                  "m" + ++member,
                  "20230601",
                  "1",
                  edge[0],
                  MODULE_DEPENDENCY_REFSET,
                  edge[1],
                  "20230601",
                  "20230601"))
          .append("\n");
    }
    Files.writeString(metadata.resolve(fileName), refset.toString());
  }

  /** Builds an RF2 description row for the fixture's column layout. */
  private static String descriptionRow(
      final String id, final String active, final String conceptId, final String term) {
    return String.join(
        "\t",
        id,
        "20230601",
        active,
        Rf2Mini.CORE_MODULE,
        conceptId,
        "en",
        SYNONYM_TYPE,
        term,
        CASE_INSENSITIVE);
  }

  /** Builds an active RF2 is-a relationship row for the fixture's column layout. */
  private static String relationshipRow(
      final String id, final String sourceId, final String destinationId) {
    return String.join(
        "\t",
        id,
        "20230601",
        "1",
        Rf2Mini.CORE_MODULE,
        sourceId,
        destinationId,
        "0",
        Rf2Mini.IS_A,
        STATED_RELATIONSHIP,
        SOME_MODIFIER);
  }

  /**
   * Rewrites the module of the first {@code count} active concepts in the release's concept file to
   * {@code module}, giving a derived edition module some content of its own.
   */
  private static void reassignConceptModules(
      final Path release, final String module, final int count) throws Exception {
    final Path conceptFile = fileStartingWith(terminologyDirectory(release), CONCEPT_PREFIX);
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
