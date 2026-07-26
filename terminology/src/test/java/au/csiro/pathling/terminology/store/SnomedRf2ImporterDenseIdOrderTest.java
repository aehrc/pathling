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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyService.Property;
import au.csiro.pathling.terminology.local.LocalTerminologyService;
import au.csiro.pathling.terminology.local.index.CodeSystemIndexes;
import au.csiro.pathling.terminology.local.index.ConceptDictionary;
import au.csiro.pathling.terminology.local.index.HierarchyIndex;
import au.csiro.pathling.test.Rf2Mini;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.ContainerPointer;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

/**
 * Verifies the dense identifier ordering option: it is off by default, the pre-order variant
 * assigns every concept exactly one identifier following a depth-first traversal of the is-a
 * hierarchy, repeated imports of the same release assign identical identifiers, and the hierarchy
 * answers every query identically under both orderings.
 *
 * <p>Three stores are imported once for the whole class, because an import is expensive: one under
 * the default ordering, and two under the pre-order, so that reproducibility can be checked.
 *
 * @author John Grimes
 */
class SnomedRf2ImporterDenseIdOrderTest {

  // Codes for the purpose-built non-tree release. They are equal length so that string order, which
  // is what the importer sorts by, matches numeric order. The branch sorts before the deep parent,
  // so the traversal descends the branch and reaches the shallow parent first, even though the
  // shallow parent's code sorts after the deep parent's.
  private static final String DIAMOND_ROOT = "100000";
  private static final String DIAMOND_BRANCH = "200000";
  private static final String DIAMOND_DEEP_PARENT = "500000";
  private static final String DIAMOND_SHALLOW_PARENT = "600000";
  private static final String DIAMOND_CHILD = "900000";
  private static final String DIAMOND_TIME = "20240101";
  private static final String DIAMOND_VERSION =
      "http://snomed.info/sct/900000000000207008/version/20240101";

  private static SparkSession spark;
  private static Map<String, Integer> codeOrderIds;
  private static Map<String, Integer> preOrderIds;
  private static Map<String, Integer> repeatedPreOrderIds;
  private static CodeSystemIndexes codeOrderIndexes;
  private static CodeSystemIndexes preOrderIndexes;
  private static LocalTerminologyService codeOrderService;
  private static LocalTerminologyService preOrderService;
  private static Path diamondRelease;
  private static Path diamondStore;

  @BeforeAll
  static void setUp(
      @TempDir final Path warehouse, @TempDir final Path storeDir, @TempDir final Path diamondDir) {
    diamondRelease = diamondDir.resolve("release");
    diamondStore = diamondDir.resolve("stores");
    spark =
        SparkSession.builder()
            .appName("SnomedRf2ImporterDenseIdOrderTest")
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

    final String release = Rf2Mini.baseRelease().toString();
    final String codeOrderStore = storeDir.resolve("code-order").toString();
    final String preOrderStore = storeDir.resolve("pre-order").toString();
    final String repeatedStore = storeDir.resolve("pre-order-again").toString();

    // The two-argument overload takes no ordering, which is how an existing caller reaches it.
    new SnomedRf2Importer(spark, codeOrderStore).importFrom(release, null);
    new SnomedRf2Importer(spark, preOrderStore).importFrom(release, null, DenseIdOrder.PRE_ORDER);
    new SnomedRf2Importer(spark, repeatedStore).importFrom(release, null, DenseIdOrder.PRE_ORDER);

    codeOrderIds = readDenseIds(codeOrderStore);
    preOrderIds = readDenseIds(preOrderStore);
    repeatedPreOrderIds = readDenseIds(repeatedStore);
    codeOrderIndexes = loadIndexes(codeOrderStore);
    preOrderIndexes = loadIndexes(preOrderStore);
    codeOrderService = serviceOver(codeOrderStore);
    preOrderService = serviceOver(preOrderStore);
  }

  @AfterAll
  static void tearDown() {
    closeQuietly(codeOrderService);
    closeQuietly(preOrderService);
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  void assignsCodeOrderIdentifiersByDefault() {
    // Without the option, identifiers ascend with the concept code, exactly as before the option
    // existed, so an existing store and an existing import are unaffected.
    assertEquals(Rf2Mini.CONCEPT_COUNT_20230601, codeOrderIds.size());
    final List<String> codesInAscendingOrder =
        new ArrayList<>(new TreeMap<>(codeOrderIds).keySet());
    for (int position = 0; position < codesInAscendingOrder.size(); position++) {
      assertEquals(
          position,
          codeOrderIds.get(codesInAscendingOrder.get(position)),
          "Code "
              + codesInAscendingOrder.get(position)
              + " did not take its code-order identifier");
    }
  }

  @Test
  void assignsEveryConceptExactlyOneIdentifierUnderThePreOrder() {
    assertEquals(Rf2Mini.CONCEPT_COUNT_20230601, preOrderIds.size());
    final boolean[] seen = new boolean[Rf2Mini.CONCEPT_COUNT_20230601];
    for (final int dense : preOrderIds.values()) {
      assertTrue(
          dense >= 0 && dense < Rf2Mini.CONCEPT_COUNT_20230601,
          "Dense identifier " + dense + " is outside the dictionary");
      assertFalse(seen[dense], "Dense identifier " + dense + " was assigned twice");
      seen[dense] = true;
    }
    // The fixture's code order is not already a pre-order, so the two orderings must differ.
    assertNotEquals(codeOrderIds, preOrderIds);
  }

  @Test
  void placesEachSubtreeInAContiguousIntervalUnderThePreOrder() {
    // This is the property the whole change exists for: a concept precedes its own descendants, and
    // its subtree occupies one unbroken interval, so a descendant set compresses into a single run
    // instead of many scattered chunks. The fixture's is-a graph is a tree, so every subtree is
    // exactly contiguous.
    final HierarchyIndex hierarchy = preOrderIndexes.hierarchy();
    for (final Map.Entry<String, Integer> entry : preOrderIds.entrySet()) {
      final int ancestor = entry.getValue();
      final RoaringBitmap descendants = hierarchy.descendantsOf(ancestor);
      if (descendants.isEmpty()) {
        continue;
      }
      assertEquals(
          ancestor + 1,
          descendants.first(),
          "The subtree of " + entry.getKey() + " does not start immediately after it");
      assertEquals(
          ancestor + descendants.getCardinality(),
          descendants.last(),
          "The subtree of " + entry.getKey() + " is not a contiguous interval");
    }
  }

  @Test
  void assignsIdenticalIdentifiersWhenTheSameSourceIsImportedTwice() {
    assertEquals(preOrderIds, repeatedPreOrderIds);
  }

  @Test
  void representsAContiguousSubtreeAsASingleRun() {
    // The two changes only pay off together: a contiguous subtree is what run-length encoding can
    // compress, and the index has to ask for that encoding to get it. The largest subtree in the
    // fixture is contiguous under the pre-order, so it must be held as a run rather than as a list
    // of its members.
    final HierarchyIndex hierarchy = preOrderIndexes.hierarchy();
    RoaringBitmap descendants = new RoaringBitmap();
    for (final int dense : preOrderIds.values()) {
      final RoaringBitmap candidate = hierarchy.descendantsOf(dense);
      if (candidate.getCardinality() > descendants.getCardinality()) {
        descendants = candidate;
      }
    }
    assertTrue(descendants.getCardinality() > 1, "The fixture has no subtree to compress");

    final ContainerPointer pointer = descendants.getContainerPointer();
    boolean sawRun = false;
    while (pointer.getContainer() != null) {
      sawRun |= pointer.isRunContainer();
      pointer.advance();
    }
    assertTrue(sawRun, "The root's contiguous subtree was not run-length encoded");
  }

  @Test
  void answersEveryHierarchyQueryIdenticallyUnderBothOrderings() {
    // Dense identifiers are internal, so the two stores are compared through concept codes. Every
    // hierarchy relation the terminology functions rest on must agree.
    final HierarchyIndex codeOrder = codeOrderIndexes.hierarchy();
    final HierarchyIndex preOrder = preOrderIndexes.hierarchy();
    for (final String code : codeOrderIds.keySet()) {
      final int underCodeOrder = codeOrderIds.get(code);
      final int underPreOrder = preOrderIds.get(code);
      assertEquals(
          codeOrderCodes(codeOrder.descendantsOf(underCodeOrder)),
          preOrderCodes(preOrder.descendantsOf(underPreOrder)),
          "Descendants of " + code + " differ between the orderings");
      assertEquals(
          codeOrderCodes(codeOrder.ancestorsOf(underCodeOrder)),
          preOrderCodes(preOrder.ancestorsOf(underPreOrder)),
          "Ancestors of " + code + " differ between the orderings");
      assertEquals(
          codeOrderCodes(codeOrder.childrenOf(underCodeOrder)),
          preOrderCodes(preOrder.childrenOf(underPreOrder)),
          "Children of " + code + " differ between the orderings");
      assertEquals(
          codeOrderCodes(codeOrder.parentsOf(underCodeOrder)),
          preOrderCodes(preOrder.parentsOf(underPreOrder)),
          "Parents of " + code + " differ between the orderings");
    }
    for (final String ancestor : codeOrderIds.keySet()) {
      for (final String descendant : codeOrderIds.keySet()) {
        assertEquals(
            codeOrder.subsumes(codeOrderIds.get(ancestor), codeOrderIds.get(descendant)),
            preOrder.subsumes(preOrderIds.get(ancestor), preOrderIds.get(descendant)),
            "Subsumption of (" + ancestor + ", " + descendant + ") differs between the orderings");
      }
    }
  }

  @Test
  void carriesTheSameConceptMetadataUnderBothOrderings() {
    // Every other index addresses concepts by the same dense identifiers, so a permutation applied
    // to
    // only some of them would silently corrupt the store.
    final ConceptDictionary codeOrder = codeOrderIndexes.dictionary();
    final ConceptDictionary preOrder = preOrderIndexes.dictionary();
    assertEquals(codeOrder.size(), preOrder.size());
    for (final String code : codeOrderIds.keySet()) {
      final int underCodeOrder = codeOrderIds.get(code);
      final int underPreOrder = preOrderIds.get(code);
      assertEquals(code, preOrder.code(underPreOrder));
      assertEquals(codeOrder.display(underCodeOrder), preOrder.display(underPreOrder));
      assertEquals(codeOrder.isActive(underCodeOrder), preOrder.isActive(underPreOrder));
      assertEquals(codeOrder.isDefined(underCodeOrder), preOrder.isDefined(underPreOrder));
      assertEquals(codeOrder.moduleId(underCodeOrder), preOrder.moduleId(underPreOrder));
      assertEquals(codeOrder.effectiveTime(underCodeOrder), preOrder.effectiveTime(underPreOrder));
    }
  }

  @Test
  void returnsIdenticalResultsFromAllSevenTerminologyFunctionsUnderBothOrderings() {
    // The hierarchy-level comparison above tests set membership, which cannot see a difference in
    // the
    // order results are returned in. This compares what the functions actually return, as ordered
    // lists, because a lookup answer is a sequence and a caller can observe its order. Dense
    // identifiers are internal, so nothing about how the store was imported may show through here.
    for (final String code : new TreeSet<>(codeOrderIds.keySet())) {
      final Coding coding = new Coding().setSystem(Rf2Mini.SNOMED_URI).setCode(code);

      // member_of, over an implicit value set of the concept's own descendants.
      final String valueSet = Rf2Mini.SNOMED_URI + "?fhir_vs=isa/" + Rf2Mini.DIABETES;
      assertEquals(
          codeOrderService.validateCode(valueSet, coding),
          preOrderService.validateCode(valueSet, coding),
          "member_of differs for " + code);

      // translate, over the fixture's SAME AS association reference set.
      final String conceptMap = Rf2Mini.SNOMED_URI + "?fhir_cm=" + Rf2Mini.SAME_AS_REFSET;
      assertEquals(
          codeOrderService.translate(coding, conceptMap, false, null),
          preOrderService.translate(coding, conceptMap, false, null),
          "translate differs for " + code);

      // subsumes and subsumed_by, which are the two directions of the same call.
      final Coding other = new Coding().setSystem(Rf2Mini.SNOMED_URI).setCode(Rf2Mini.DIABETES);
      assertEquals(
          codeOrderService.subsumes(other, coding),
          preOrderService.subsumes(other, coding),
          "subsumes differs for " + code);
      assertEquals(
          codeOrderService.subsumes(coding, other),
          preOrderService.subsumes(coding, other),
          "subsumed_by differs for " + code);

      // display, property_of and designation are all served by lookup. property_of is asked for the
      // parent and child properties specifically, because those are the multi-valued ones derived
      // from the hierarchy and so the only ones whose order could follow the dense identifiers.
      for (final String property :
          List.of("display", "parent", "child", "designation", "moduleId", "inactive")) {
        assertEquals(
            codeOrderService.lookup(coding, property),
            preOrderService.lookup(coding, property),
            "lookup of " + property + " differs for " + code);
      }
      assertEquals(
          codeOrderService.lookup(coding, null),
          preOrderService.lookup(coding, null),
          "an unfiltered lookup differs for " + code);
    }
  }

  @Test
  void emitsMultiParentPropertiesInTheSameOrderUnderBothOrderings() {
    // The rf2-mini hierarchy is a tree, so no concept there has two parents and the two orderings
    // cannot disagree about the order a parent list comes back in. This uses a purpose-built
    // release
    // where a concept's two parents sit at different depths, so their pre-order positions bear no
    // relation to their codes - which is the case a full edition is full of, and the case that once
    // let the internal ordering show through in a lookup result.
    final Path release = writeDiamondRelease();
    final String codeOrder = diamondStore.resolve("code-order").toString();
    final String preOrder = diamondStore.resolve("pre-order").toString();
    new SnomedRf2Importer(spark, codeOrder).importFrom(release.toString(), DIAMOND_VERSION);
    new SnomedRf2Importer(spark, preOrder)
        .importFrom(release.toString(), DIAMOND_VERSION, DenseIdOrder.PRE_ORDER);

    // The two parents of the multi-parent concept are ordered differently by dense identifier under
    // the two orderings, which is what makes this a real test rather than a tautology.
    final Map<String, Integer> underCodeOrder = readDenseIds(codeOrder);
    final Map<String, Integer> underPreOrder = readDenseIds(preOrder);
    assertNotEquals(
        underCodeOrder.get(DIAMOND_SHALLOW_PARENT) < underCodeOrder.get(DIAMOND_DEEP_PARENT),
        underPreOrder.get(DIAMOND_SHALLOW_PARENT) < underPreOrder.get(DIAMOND_DEEP_PARENT),
        "The release does not order the two parents differently under the two orderings");

    try (final LocalTerminologyService codeOrderService = serviceOver(codeOrder);
        final LocalTerminologyService preOrderService = serviceOver(preOrder)) {
      final Coding coding = new Coding().setSystem(Rf2Mini.SNOMED_URI).setCode(DIAMOND_CHILD);
      assertEquals(
          codeOrderService.lookup(coding, "parent"),
          preOrderService.lookup(coding, "parent"),
          "The parent property list differs between the orderings");
      assertEquals(
          List.of(DIAMOND_DEEP_PARENT, DIAMOND_SHALLOW_PARENT),
          codeOrderService.lookup(coding, "parent").stream()
              .map(property -> ((Property) property).getValue().primitiveValue())
              .toList(),
          "The parent property list is not in ascending code order");
    }
  }

  /**
   * Writes a minimal RF2 snapshot release whose hierarchy is not a tree. The root has two children,
   * one of which has a child of its own, and a fifth concept is a child of both the shallow branch
   * and the deep one. Codes are chosen so that the deep parent sorts before the shallow one, while
   * a depth-first traversal reaches the shallow one first.
   *
   * @return the release directory
   */
  @Nonnull
  private static Path writeDiamondRelease() {
    final Path terminology = diamondRelease.resolve("Snapshot").resolve("Terminology");
    final String module = Rf2Mini.CORE_MODULE;
    final StringBuilder concepts = new StringBuilder("id\teffectiveTime\tactive\tmoduleId\t");
    concepts.append("definitionStatusId\r\n");
    final StringBuilder descriptions =
        new StringBuilder(
            "id\teffectiveTime\tactive\tmoduleId\tconceptId\tlanguageCode\ttypeId\tterm\t"
                + "caseSignificanceId\r\n");
    final StringBuilder relationships =
        new StringBuilder(
            "id\teffectiveTime\tactive\tmoduleId\tsourceId\tdestinationId\trelationshipGroup\t"
                + "typeId\tcharacteristicTypeId\tmodifierId\r\n");
    int identifier = 0;
    for (final String code :
        List.of(
            DIAMOND_ROOT,
            DIAMOND_DEEP_PARENT,
            DIAMOND_SHALLOW_PARENT,
            DIAMOND_BRANCH,
            DIAMOND_CHILD)) {
      concepts.append(
          String.join("\t", code, DIAMOND_TIME, "1", module, "900000000000074008") + "\r\n");
      descriptions.append(
          String.join(
                  "\t",
                  "d" + ++identifier,
                  DIAMOND_TIME,
                  "1",
                  module,
                  code,
                  "en",
                  "900000000000003001",
                  "Concept " + code + " (finding)",
                  "900000000000448009")
              + "\r\n");
    }
    // Edges, as child to parent: the branch and the deep parent hang off the root, the shallow
    // parent hangs off the branch, and the child has both the shallow and the deep parent.
    for (final String[] edge :
        new String[][] {
          {DIAMOND_BRANCH, DIAMOND_ROOT},
          {DIAMOND_DEEP_PARENT, DIAMOND_ROOT},
          {DIAMOND_SHALLOW_PARENT, DIAMOND_BRANCH},
          {DIAMOND_CHILD, DIAMOND_SHALLOW_PARENT},
          {DIAMOND_CHILD, DIAMOND_DEEP_PARENT}
        }) {
      relationships.append(
          String.join(
                  "\t",
                  "r" + ++identifier,
                  DIAMOND_TIME,
                  "1",
                  module,
                  edge[0],
                  edge[1],
                  "0",
                  "116680003",
                  "900000000000011006",
                  "900000000000451002")
              + "\r\n");
    }
    try {
      Files.createDirectories(terminology);
      Files.writeString(
          terminology.resolve("sct2_Concept_Snapshot_INT_" + DIAMOND_TIME + ".txt"),
          concepts.toString());
      Files.writeString(
          terminology.resolve("sct2_Description_Snapshot-en_INT_" + DIAMOND_TIME + ".txt"),
          descriptions.toString());
      Files.writeString(
          terminology.resolve("sct2_Relationship_Snapshot_INT_" + DIAMOND_TIME + ".txt"),
          relationships.toString());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return diamondRelease;
  }

  @Nonnull
  private static LocalTerminologyService serviceOver(@Nonnull final String storagePath) {
    return new LocalTerminologyService(
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(LocalTerminologyConfiguration.builder().storagePath(storagePath).build())
            .build(),
        Map.of());
  }

  private static void closeQuietly(@Nullable final LocalTerminologyService service) {
    if (service != null) {
      service.close();
    }
  }

  @Nonnull
  private static Map<String, Integer> readDenseIds(@Nonnull final String storagePath) {
    final Map<String, Integer> denseByCode = new HashMap<>();
    TerminologyStoreReader.open(storagePath, Map.of())
        .readTable(
            CONCEPT,
            row -> denseByCode.put(row.getString(COLUMN_CODE), row.getInt(COLUMN_DENSE_ID)));
    return denseByCode;
  }

  @Nonnull
  private static CodeSystemIndexes loadIndexes(@Nonnull final String storagePath) {
    return CodeSystemIndexes.load(
        TerminologyStoreReader.open(storagePath, Map.of()),
        TerminologyStoreSchema.systemVersionId(Rf2Mini.SNOMED_URI, Rf2Mini.VERSION_20230601));
  }

  @Nonnull
  private static Set<String> codeOrderCodes(@Nonnull final RoaringBitmap bitmap) {
    return codesOf(bitmap, codeOrderIndexes.dictionary());
  }

  @Nonnull
  private static Set<String> preOrderCodes(@Nonnull final RoaringBitmap bitmap) {
    return codesOf(bitmap, preOrderIndexes.dictionary());
  }

  /** Translates a bitmap of dense identifiers into the concept codes it addresses. */
  @Nonnull
  private static Set<String> codesOf(
      @Nonnull final RoaringBitmap bitmap, @Nonnull final ConceptDictionary dictionary) {
    final Set<String> codes = new TreeSet<>();
    bitmap.forEach((IntConsumer) dense -> codes.add(dictionary.code(dense)));
    return codes;
  }
}
