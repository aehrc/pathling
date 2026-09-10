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

package au.csiro.pathling.benchmark;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.terminology.local.CodeSystemEntry;
import au.csiro.pathling.terminology.local.index.ConceptDictionary;
import au.csiro.pathling.terminology.local.index.HierarchyIndex;
import au.csiro.pathling.terminology.store.SnomedRf2Importer;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

/**
 * Correctness tests for the hierarchy index memory harness. A memory figure taken with an incorrect
 * permutation, or with a container histogram that does not measure what it claims, is worse than no
 * figure at all, so everything the measurement rests on is asserted here against the {@code
 * rf2-mini} fixture before the harness is pointed at a full SNOMED CT edition.
 *
 * @author John Grimes
 */
class HierarchyIndexMemoryHarnessTest {

  private static SparkSession spark;
  private static TerminologyStoreReader reader;
  private static String systemVersionId;
  private static ConceptDictionary dictionary;
  private static HierarchyIndex index;
  private static HierarchyMaps maps;

  @BeforeAll
  static void setUp() {
    spark =
        SparkSession.builder()
            .appName("HierarchyIndexMemoryHarnessTest")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
    final String storagePath;
    try {
      storagePath = Files.createTempDirectory("rf2-mini-harness").resolve("store").toString();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    new SnomedRf2Importer(spark, storagePath).importFrom(fixturePath(), null);

    reader = TerminologyStoreReader.open(storagePath, Map.of());
    final List<CodeSystemEntry> catalogue = CodeSystemEntry.loadCatalogue(reader);
    assertEquals(1, catalogue.size(), "The base fixture release holds one code system version");
    systemVersionId = catalogue.get(0).getSystemVersionId();
    dictionary = ConceptDictionary.load(reader, systemVersionId);
    index = HierarchyIndex.load(reader, systemVersionId);
    maps = HierarchyMaps.load(reader, systemVersionId);
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * Resolves the fixture from the sibling terminology module's source tree. The classpath resource
   * is not usable here because it resides inside the terminology test-jar, from which Spark cannot
   * read a directory.
   */
  private static String fixturePath() {
    return Path.of(
            "..", "terminology", "src", "test", "resources", "rf2-mini", "international-20230601")
        .toAbsolutePath()
        .normalize()
        .toString();
  }

  // The harness works from its own copy of the four maps, because the production index keeps them
  // private and the measurement is not allowed to change production code. These two tests pin that
  // copy to the production index, so a figure taken from the copy is a figure about the real thing.

  @Test
  void replicaAnswersEveryLookupAsTheProductionIndexDoes() {
    final Map<String, Map<Integer, RoaringBitmap>> named = maps.byName();
    for (int dense = 0; dense < dictionary.size(); dense++) {
      assertEquals(
          index.descendantsOf(dense),
          bitmapAt(named, HierarchyMaps.DESCENDANTS, dense),
          "Descendants differ at dense identifier " + dense);
      assertEquals(
          index.ancestorsOf(dense),
          bitmapAt(named, HierarchyMaps.ANCESTORS, dense),
          "Ancestors differ at dense identifier " + dense);
      assertEquals(
          index.childrenOf(dense),
          bitmapAt(named, HierarchyMaps.CHILDREN, dense),
          "Children differ at dense identifier " + dense);
      assertEquals(
          index.parentsOf(dense),
          bitmapAt(named, HierarchyMaps.PARENTS, dense),
          "Parents differ at dense identifier " + dense);
    }
  }

  @Test
  void identityRemapCostsTheSameAsTheIndexAsLoaded() {
    // Variant A is built by remapping through the identity, so that it carries the same
    // construction
    // artefacts as the reordered variants. That is only a fair baseline if the remap reproduces the
    // loaded index's footprint exactly. The maps are reloaded rather than shared with the other
    // tests,
    // because measuring a map is only comparable between maps that have been traversed the same
    // number
    // of times.
    final HierarchyMaps loaded = HierarchyMaps.load(reader, systemVersionId);
    final long asLoaded = HierarchyVariantFootprint.measure(loaded).getTotalRetainedBytes();
    final long identityRemapped =
        HierarchyVariantFootprint.measure(loaded.remap(DenseIdOrdering.identity(dictionary.size())))
            .getTotalRetainedBytes();
    assertEquals(asLoaded, identityRemapped);
  }

  @Test
  void preOrderPermutationIsABijectionOverTheWholeDictionary() {
    final int conceptCount = dictionary.size();
    final int[] permutation = DenseIdOrdering.preOrder(maps, conceptCount);

    assertEquals(conceptCount, permutation.length);
    final boolean[] seen = new boolean[conceptCount];
    for (final int assigned : permutation) {
      assertTrue(assigned >= 0 && assigned < conceptCount, "Assigned identifier out of range");
      assertFalse(seen[assigned], "Identifier " + assigned + " was assigned twice");
      seen[assigned] = true;
    }
    // The fixture's concept code order is not already a pre-order, so a permutation identical to
    // the
    // identity would mean the traversal did nothing.
    assertFalse(
        Arrays.equals(DenseIdOrdering.identity(conceptCount), permutation),
        "The pre-order permutation is indistinguishable from the identity");
  }

  @Test
  void conceptsWithNoIsARelationshipKeepCodeOrderAfterTheTraversal() {
    final int conceptCount = dictionary.size();
    final int[] permutation = DenseIdOrdering.preOrder(maps, conceptCount);

    final List<Integer> unreached = new ArrayList<>();
    for (int dense = 0; dense < conceptCount; dense++) {
      if (!maps.children().containsKey(dense) && !maps.parents().containsKey(dense)) {
        unreached.add(dense);
      }
    }
    assertFalse(
        unreached.isEmpty(),
        "The fixture must contain concepts outside the is-a graph for this test to mean anything");

    // Every concept outside the graph takes one of the highest identifiers, and they keep their
    // existing relative order, which is concept code order.
    final int firstUnreachedId = conceptCount - unreached.size();
    for (int position = 0; position < unreached.size(); position++) {
      assertEquals(
          firstUnreachedId + position,
          permutation[unreached.get(position)],
          "Concept " + unreached.get(position) + " was not appended in code order");
    }
  }

  @Test
  void remappingPreservesEachMapsTotalCardinality() {
    final int[] permutation = DenseIdOrdering.preOrder(maps, dictionary.size());
    final HierarchyMaps remapped = maps.remap(permutation);

    final Map<String, Map<Integer, RoaringBitmap>> before = maps.byName();
    final Map<String, Map<Integer, RoaringBitmap>> after = remapped.byName();
    assertEquals(before.keySet(), after.keySet());
    for (final String name : before.keySet()) {
      assertEquals(
          before.get(name).size(), after.get(name).size(), "Entry count differs for map " + name);
      assertEquals(
          totalCardinality(before.get(name)),
          totalCardinality(after.get(name)),
          "Total cardinality differs for map " + name);
    }
  }

  @Test
  void subsumptionIsUnchangedByTheRemapping() {
    final int conceptCount = dictionary.size();
    final int[] permutation = DenseIdOrdering.preOrder(maps, conceptCount);
    final HierarchyMaps remapped = maps.remap(permutation);

    for (int ancestor = 0; ancestor < conceptCount; ancestor++) {
      for (int descendant = 0; descendant < conceptCount; descendant++) {
        assertEquals(
            index.subsumes(ancestor, descendant),
            remapped.subsumes(permutation[ancestor], permutation[descendant]),
            "Subsumption differs for the pair (" + ancestor + ", " + descendant + ")");
      }
    }
  }

  @Test
  void traversalTerminatesWhenAConceptIsReachableByMoreThanOnePath() {
    // 0 is the root; 1 and 2 are its children; 3 is a child of both, so it is reachable by two
    // paths.
    // 4 has no is-a edge at all. The hierarchy is a directed acyclic graph, not a tree.
    final HierarchyMaps diamond =
        HierarchyMaps.of(
            edges(Map.of(0, bitmapOf(1, 2, 3), 1, bitmapOf(3), 2, bitmapOf(3))),
            edges(Map.of(1, bitmapOf(0), 2, bitmapOf(0), 3, bitmapOf(0, 1, 2))),
            edges(Map.of(0, bitmapOf(1, 2), 1, bitmapOf(3), 2, bitmapOf(3))),
            edges(Map.of(1, bitmapOf(0), 2, bitmapOf(0), 3, bitmapOf(1, 2))));

    final int[] permutation = DenseIdOrdering.preOrder(diamond, 5);

    // Pre-order from root 0: 0, then its smallest child 1, then 1's child 3, then 2. The isolated
    // concept 4 is appended last. Concept 3 is visited once, despite having two parents.
    assertArrayEquals(new int[] {0, 1, 3, 2, 4}, permutation);
  }

  @Test
  void histogramCountsRunContainersOnlyWhereOptimisationWasRequested() {
    // The production index builds every bitmap by repeated addition and never asks for
    // optimisation,
    // so no run container exists in it at any ordering.
    for (final Map.Entry<String, Map<Integer, RoaringBitmap>> entry : maps.byName().entrySet()) {
      final HierarchyMapFootprint footprint =
          HierarchyMapFootprint.measure(entry.getKey(), entry.getValue());
      assertEquals(
          0,
          footprint.getRunContainers(),
          "Unoptimised map " + entry.getKey() + " holds run containers");
      assertTrue(
          footprint.getArrayContainers() + footprint.getBitmapContainers() > 0,
          "Map " + entry.getKey() + " holds no containers at all");
    }

    // A contiguous range is the case run-length encoding exists for, so asking for optimisation
    // must
    // produce a run container and shrink the map.
    final Map<Integer, RoaringBitmap> contiguous = new HashMap<>();
    final RoaringBitmap range = new RoaringBitmap();
    for (int value = 0; value < 20_000; value++) {
      range.add(value);
    }
    contiguous.put(0, range);
    final HierarchyMapFootprint before = HierarchyMapFootprint.measure("contiguous", contiguous);
    assertEquals(0, before.getRunContainers());

    assertTrue(range.runOptimize(), "A contiguous range must be worth run-length encoding");
    final HierarchyMapFootprint after = HierarchyMapFootprint.measure("contiguous", contiguous);
    assertTrue(after.getRunContainers() > 0, "Optimisation produced no run container");
    assertTrue(
        after.getRetainedBytes() < before.getRetainedBytes(),
        "Run-length encoding a contiguous range did not reduce retained heap");
  }

  private static RoaringBitmap bitmapAt(
      final Map<String, Map<Integer, RoaringBitmap>> named, final String name, final int dense) {
    final RoaringBitmap bitmap = named.get(name).get(dense);
    return bitmap == null ? new RoaringBitmap() : bitmap;
  }

  private static long totalCardinality(final Map<Integer, RoaringBitmap> map) {
    return map.values().stream().mapToLong(RoaringBitmap::getLongCardinality).sum();
  }

  private static RoaringBitmap bitmapOf(final int... values) {
    final RoaringBitmap bitmap = new RoaringBitmap();
    for (final int value : values) {
      bitmap.add(value);
    }
    return bitmap;
  }

  /** Copies a literal map into the mutable, default-capacity form the harness works with. */
  private static Map<Integer, RoaringBitmap> edges(final Map<Integer, RoaringBitmap> literal) {
    return new HashMap<>(literal);
  }
}
