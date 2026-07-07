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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ANCESTOR_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DESCENDANT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DIRECT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_DENSE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the transitive closure builder computes the full ancestor/descendant closure of a
 * hierarchy: the correct depth of transitive pairs, deduplication across multiple parents, correct
 * direct-edge flags, and isolation between system versions.
 *
 * @author John Grimes
 */
class TransitiveClosureBuilderTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .appName("TransitiveClosureBuilderTest")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
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

  private Dataset<Row> edges(final List<Row> rows) {
    final StructType schema =
        new StructType()
            .add(COLUMN_SYSTEM_VERSION_ID, DataTypes.StringType, false)
            .add(COLUMN_SOURCE_DENSE_ID, DataTypes.IntegerType, false)
            .add(COLUMN_TARGET_DENSE_ID, DataTypes.IntegerType, false);
    return spark.createDataFrame(rows, schema);
  }

  /** A row of the closure output, for order-independent comparison. */
  private record Pair(String sv, int ancestor, int descendant, boolean direct) {}

  private Set<Pair> collect(final Dataset<Row> closure) {
    final Set<Pair> pairs = new HashSet<>();
    for (final Row row : closure.collectAsList()) {
      pairs.add(
          new Pair(
              row.getString(row.fieldIndex(COLUMN_SYSTEM_VERSION_ID)),
              row.getInt(row.fieldIndex(COLUMN_ANCESTOR_DENSE_ID)),
              row.getInt(row.fieldIndex(COLUMN_DESCENDANT_DENSE_ID)),
              row.getBoolean(row.fieldIndex(COLUMN_DIRECT))));
    }
    return pairs;
  }

  @Test
  void computesTransitivePairsWithDepth() {
    // A <- B <- D, with C also under A, D also under C (D has two parents B and C).
    final Dataset<Row> input =
        edges(
            List.of(
                RowFactory.create("v", 1, 0), // B is-a A
                RowFactory.create("v", 2, 0), // C is-a A
                RowFactory.create("v", 3, 1), // D is-a B
                RowFactory.create("v", 3, 2))); // D is-a C

    final Set<Pair> closure = collect(new TransitiveClosureBuilder().build(input));

    // Direct edges.
    assertTrue(closure.contains(new Pair("v", 0, 1, true)));
    assertTrue(closure.contains(new Pair("v", 0, 2, true)));
    assertTrue(closure.contains(new Pair("v", 1, 3, true)));
    assertTrue(closure.contains(new Pair("v", 2, 3, true)));
    // Transitive edge A -> D (via both B and C, deduplicated to a single non-direct pair).
    assertTrue(closure.contains(new Pair("v", 0, 3, false)));
    // Exactly five pairs, no self pairs, no duplicate A -> D.
    assertEquals(5, closure.size());
  }

  @Test
  void isolatesSystemVersions() {
    final Dataset<Row> input =
        edges(
            List.of(
                RowFactory.create("v1", 1, 0), // B is-a A in v1
                RowFactory.create("v2", 11, 10))); // Y is-a X in v2

    final Set<Pair> closure = collect(new TransitiveClosureBuilder().build(input));

    assertTrue(closure.contains(new Pair("v1", 0, 1, true)));
    assertTrue(closure.contains(new Pair("v2", 10, 11, true)));
    // No cross-version pairs are produced.
    assertFalse(closure.stream().anyMatch(p -> p.ancestor() == 0 && p.descendant() == 11));
    assertEquals(2, closure.size());
  }

  @Test
  void excludesSelfPairs() {
    final Dataset<Row> input = edges(new ArrayList<>(List.of(RowFactory.create("v", 1, 0))));
    final Set<Pair> closure = collect(new TransitiveClosureBuilder().build(input));
    assertFalse(closure.stream().anyMatch(p -> p.ancestor() == p.descendant()));
  }

  @Test
  void handlesDeepHierarchies() {
    // A chain 0 <- 1 <- ... <- 16, deeper than a real SNOMED CT is-a path. Guards against the
    // iterative plan growing with each generation, which previously made analysis cost exponential
    // in hierarchy depth and exhausted driver memory beyond a depth of six.
    final int depth = 16;
    final List<Row> rows = new ArrayList<>();
    for (int i = 1; i <= depth; i++) {
      rows.add(RowFactory.create("v", i, i - 1));
    }

    final Set<Pair> closure = collect(new TransitiveClosureBuilder().build(edges(rows)));

    // The closure of a chain of depth d contains d * (d + 1) / 2 pairs.
    assertEquals(depth * (depth + 1) / 2, closure.size());
    // The deepest concept has every other concept as an ancestor, only its parent directly.
    assertEquals(depth, closure.stream().filter(p -> p.descendant() == depth).count());
    assertTrue(closure.contains(new Pair("v", depth - 1, depth, true)));
    assertTrue(closure.contains(new Pair("v", 0, depth, false)));
  }
}
