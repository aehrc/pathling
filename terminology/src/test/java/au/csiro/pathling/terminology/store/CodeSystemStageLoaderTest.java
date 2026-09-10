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
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ANCESTOR_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_COUNT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DESCENDANT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TERM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TYPE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.DESCRIPTION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.RELATIONSHIP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the stage loader: duplicate concept codes resolve to their first occurrence with rows of
 * dropped occurrences filtered out, Coding-valued properties and property-derived edges resolve to
 * dense identifiers with dangling references dropped, edges orient by role and deduplicate against
 * nesting edges, and every table carries the system-version key.
 *
 * @author John Grimes
 */
class CodeSystemStageLoaderTest {

  private static final String URL = "http://example.org/fhir/CodeSystem/loader";
  private static final String VERSION = "1.0.0";

  private static SparkSession spark;

  @BeforeAll
  static void setUp(@TempDir final Path warehouse) {
    spark =
        SparkSession.builder()
            .appName("CodeSystemStageLoaderTest")
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
  void resolvesDuplicateConceptCodesToTheFirstOccurrence(@TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      staging.appendConcept("A", 0, true, false, "First A");
      staging.appendConcept("A", 1, true, false, "Duplicate A");
      staging.appendConcept("B", 2, true, false, "B");
      // A description keyed to the dropped duplicate occurrence must be filtered out.
      staging.appendDescription(0, "kept", null, null, null);
      staging.appendDescription(1, "dropped", null, null, null);
      staging.sealForReading();
      load(staging, store);
    }

    final Map<String, Integer> dense = new HashMap<>();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(
            CONCEPT, row -> dense.put(row.getString(COLUMN_CODE), row.getInt(COLUMN_DENSE_ID)));
    assertEquals(Set.of("A", "B"), dense.keySet());
    assertEquals(0, dense.get("A"), "the first occurrence (minimum dense id) survives");

    final AtomicLong conceptCount = new AtomicLong();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(CODE_SYSTEM, row -> conceptCount.set(row.getLong(COLUMN_CONCEPT_COUNT)));
    assertEquals(2, conceptCount.get());

    final Set<String> terms = new HashSet<>();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(DESCRIPTION, row -> terms.add(row.getString(COLUMN_TERM)));
    assertTrue(terms.contains("kept"));
    assertFalse(terms.contains("dropped"), "rows of a dropped duplicate are filtered out");
  }

  @Test
  void resolvesCodingPropertiesToRelationshipRows(@TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      staging.appendConcept("A", 0, true, false, "A");
      staging.appendConcept("B", 1, true, false, "B");
      staging.appendCodingProperty(0, "assoc", "B");
      // An unmatched target is dropped.
      staging.appendCodingProperty(0, "assoc", "ZZZ");
      staging.sealForReading();
      load(staging, store);
    }

    final Set<String> relationships = new HashSet<>();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(
            RELATIONSHIP,
            row ->
                relationships.add(
                    row.getInt(COLUMN_SOURCE_DENSE_ID)
                        + ":"
                        + row.getString(COLUMN_TYPE_CODE)
                        + ":"
                        + row.getInt(COLUMN_TARGET_DENSE_ID)));
    assertEquals(Set.of("0:assoc:1"), relationships);
  }

  @Test
  void attachesTheSystemVersionToEveryConceptRow(@TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      staging.appendConcept("A", 0, true, false, "A");
      staging.sealForReading();
      load(staging, store);
    }
    final String expected = TerminologyStoreSchema.systemVersionId(URL, VERSION);
    final Set<String> systemVersions = new HashSet<>();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(CONCEPT, row -> systemVersions.add(row.getString(COLUMN_SYSTEM_VERSION_ID)));
    assertEquals(Set.of(expected), systemVersions);
  }

  @Test
  void resolvesAndOrientsPropertyEdgesDeduplicatingAgainstNesting(@TempDir final Path dir) {
    final String store = dir.resolve("store").toString();
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      staging.appendConcept("A", 0, true, false, "A");
      staging.appendConcept("B", 1, true, false, "B");
      staging.appendConcept("C", 2, true, false, "C");
      // Nesting edge B is-a A.
      staging.appendDenseEdge(1, 0);
      // A parent property on C referencing A: C is-a A.
      staging.appendCodeEdge(2, "child", "A");
      // A child property on A referencing B: B is-a A, duplicating the nesting edge.
      staging.appendCodeEdge(0, "parent", "B");
      // A dangling reference is dropped.
      staging.appendCodeEdge(1, "child", "ZZZ");
      staging.sealForReading();
      load(staging, store);
    }

    final Map<Integer, String> codeByDense = new HashMap<>();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(
            CONCEPT,
            row -> codeByDense.put(row.getInt(COLUMN_DENSE_ID), row.getString(COLUMN_CODE)));
    final Map<String, AtomicInteger> pairCounts = new HashMap<>();
    TerminologyStoreReader.open(store, Map.of())
        .readTable(
            CLOSURE,
            row -> {
              final String pair =
                  codeByDense.get(row.getInt(COLUMN_ANCESTOR_DENSE_ID))
                      + "->"
                      + codeByDense.get(row.getInt(COLUMN_DESCENDANT_DENSE_ID));
              pairCounts.computeIfAbsent(pair, k -> new AtomicInteger()).incrementAndGet();
            });
    assertTrue(pairCounts.containsKey("A->B"), "B is-a A from both nesting and a child property");
    assertTrue(pairCounts.containsKey("A->C"), "C is-a A from a parent property");
    // The duplicated B is-a A edge appears once in the closure, not twice.
    assertEquals(1, pairCounts.get("A->B").get());
  }

  private void load(final CodeSystemStaging staging, final String store) {
    final TerminologyStoreWriter writer = new TerminologyStoreWriter(spark, store);
    new CodeSystemStageLoader(spark, writer).load(staging, URL, VERSION, "is-a", store);
  }
}
