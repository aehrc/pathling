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

import au.csiro.pathling.terminology.local.index.CodeSystemIndexes;
import au.csiro.pathling.terminology.local.index.ConceptDictionary;
import au.csiro.pathling.terminology.local.index.HierarchyIndex;
import au.csiro.pathling.test.Rf2Mini;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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

  private static SparkSession spark;
  private static Map<String, Integer> codeOrderIds;
  private static Map<String, Integer> preOrderIds;
  private static Map<String, Integer> repeatedPreOrderIds;
  private static CodeSystemIndexes codeOrderIndexes;
  private static CodeSystemIndexes preOrderIndexes;

  @BeforeAll
  static void setUp(@TempDir final Path warehouse, @TempDir final Path storeDir) {
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

    // The two-argument overload takes no ordering at all, which is how an existing caller reaches
    // it.
    new SnomedRf2Importer(spark, codeOrderStore).importFrom(release, null);
    new SnomedRf2Importer(spark, preOrderStore).importFrom(release, null, DenseIdOrder.PRE_ORDER);
    new SnomedRf2Importer(spark, repeatedStore).importFrom(release, null, DenseIdOrder.PRE_ORDER);

    codeOrderIds = readDenseIds(codeOrderStore);
    preOrderIds = readDenseIds(preOrderStore);
    repeatedPreOrderIds = readDenseIds(repeatedStore);
    codeOrderIndexes = loadIndexes(codeOrderStore);
    preOrderIndexes = loadIndexes(preOrderStore);
  }

  @AfterAll
  static void tearDown() {
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
