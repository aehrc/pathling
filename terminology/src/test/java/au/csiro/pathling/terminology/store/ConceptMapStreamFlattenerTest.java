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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_EQUIVALENCE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ORDINAL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.test.FhirFixtures;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the streaming ConceptMap flattener: every target of every element becomes one staging
 * row carrying its group's systems and versions and its displays, the fields of a ConceptMap may
 * appear in any order, a missing equivalence defaults to {@code relatedto}, an unrecognised
 * equivalence fails the import, and the first content that a concept map relation cannot carry is
 * recorded with the staging.
 *
 * @author John Grimes
 */
class ConceptMapStreamFlattenerTest {

  private static final JsonFactory FACTORY = new JsonFactory();

  private static SparkSession spark;

  @BeforeAll
  static void setUp(@TempDir final Path warehouse) {
    spark =
        SparkSession.builder()
            .appName("ConceptMapStreamFlattenerTest")
            .master("local[2]")
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
  void flattensEachTargetOfEachElementIntoOneRow() throws Exception {
    final String json =
        Files.readString(
            FhirFixtures.jsonDirectory().resolve("conceptmap-species-to-category.json"));

    final List<String> rows = flatten(json);

    // The four elements of the fixture each carry one target, in document order.
    assertEquals(
        List.of(
            row(
                FhirFixtures.ANIMAL_SPECIES,
                "dog",
                FhirFixtures.ANIMAL_CATEGORY,
                "pet",
                "equivalent"),
            row(
                FhirFixtures.ANIMAL_SPECIES,
                "cat",
                FhirFixtures.ANIMAL_CATEGORY,
                "pet",
                "equivalent"),
            row(
                FhirFixtures.ANIMAL_SPECIES,
                "whale",
                FhirFixtures.ANIMAL_CATEGORY,
                "aquatic",
                "relatedto"),
            row(
                FhirFixtures.ANIMAL_SPECIES,
                "sparrow",
                FhirFixtures.ANIMAL_CATEGORY,
                "pet",
                "wider")),
        rows);
  }

  @Test
  void readsFieldsInAnyOrder() throws Exception {
    // The group precedes the metadata, and within each element and target the code comes last.
    final String json =
        "{\"group\":[{\"element\":[{\"target\":[{\"equivalence\":\"narrower\",\"code\":\"t1\"}],"
            + "\"code\":\"s1\"}],\"target\":\"http://t\",\"source\":\"http://s\"}],"
            + "\"url\":\"http://x/cm\",\"resourceType\":\"ConceptMap\"}";

    assertEquals(List.of(row("http://s", "s1", "http://t", "t1", "narrower")), flatten(json));
  }

  @Test
  void carriesEachGroupsOwnSystems() throws Exception {
    // A group without a source or target system yields rows whose systems are absent.
    final String json =
        "{\"resourceType\":\"ConceptMap\",\"group\":["
            + "{\"source\":\"http://a\",\"target\":\"http://b\","
            + "\"element\":[{\"code\":\"1\",\"target\":[{\"code\":\"2\",\"equivalence\":\"equal\"}]}]},"
            + "{\"element\":[{\"code\":\"3\",\"target\":[{\"code\":\"4\",\"equivalence\":\"equal\"}]}]}"
            + "]}";

    assertEquals(
        List.of(row("http://a", "1", "http://b", "2", "equal"), row(null, "3", null, "4", "equal")),
        flatten(json));
  }

  @Test
  void writesOneRowPerTargetOfAnElementInDocumentOrder() throws Exception {
    final String json =
        "{\"resourceType\":\"ConceptMap\",\"group\":[{\"source\":\"http://s\","
            + "\"target\":\"http://t\",\"element\":[{\"code\":\"x\",\"target\":["
            + "{\"code\":\"c\",\"equivalence\":\"inexact\"},"
            + "{\"code\":\"a\",\"equivalence\":\"wider\"},"
            + "{\"code\":\"b\",\"equivalence\":\"narrower\"}]}]}]}";

    assertEquals(
        List.of(
            row("http://s", "x", "http://t", "c", "inexact"),
            row("http://s", "x", "http://t", "a", "wider"),
            row("http://s", "x", "http://t", "b", "narrower")),
        flatten(json));
  }

  @Test
  void defaultsAMissingEquivalenceToRelatedTo() throws Exception {
    final String json =
        "{\"resourceType\":\"ConceptMap\",\"group\":[{\"source\":\"http://s\","
            + "\"target\":\"http://t\",\"element\":[{\"code\":\"x\",\"target\":[{\"code\":\"y\"}]}]}]}";

    assertEquals(List.of(row("http://s", "x", "http://t", "y", "relatedto")), flatten(json));
  }

  @Test
  void skipsElementsWithoutACodeAndElementsWithoutTargets() throws Exception {
    // An element without a code cannot be looked up, and an element without targets maps nowhere.
    final String json =
        "{\"resourceType\":\"ConceptMap\",\"group\":[{\"source\":\"http://s\","
            + "\"target\":\"http://t\",\"element\":["
            + "{\"target\":[{\"code\":\"orphan\",\"equivalence\":\"equal\"}]},"
            + "{\"code\":\"unmapped\"},"
            + "{\"code\":\"x\",\"target\":[{\"code\":\"y\",\"equivalence\":\"equal\"}]}]}]}";

    assertEquals(List.of(row("http://s", "x", "http://t", "y", "equal")), flatten(json));
  }

  @Test
  void rejectsAnUnrecognisedEquivalence() {
    final String json =
        "{\"resourceType\":\"ConceptMap\",\"group\":[{\"element\":[{\"code\":\"x\","
            + "\"target\":[{\"code\":\"y\",\"equivalence\":\"sort-of\"}]}]}]}";

    final TerminologyImportException e =
        assertThrows(TerminologyImportException.class, () -> flatten(json));
    assertTrue(e.getMessage().contains("sort-of"), e.getMessage());
  }

  @Test
  void returnsTheNumberOfMappingsFlattened() throws Exception {
    final String json =
        Files.readString(
            FhirFixtures.jsonDirectory().resolve("conceptmap-species-to-category.json"));
    try (ConceptMapStaging staging = ConceptMapStaging.create();
        JsonParser parser = FACTORY.createParser(json.getBytes(StandardCharsets.UTF_8))) {
      staging.beginConceptMap("cm");
      assertEquals(4, new ConceptMapStreamFlattener(staging).flatten(parser));
    }
  }

  @Test
  void carriesDisplaysAndTheGroupsOwnVersions() throws Exception {
    // Versions are carried as written; reading a canonical's version out of its system is left to
    // the reader, as is treating a blank value as absent.
    final String json =
        "{\"resourceType\":\"ConceptMap\",\"group\":[{\"source\":\"http://s|1\","
            + "\"sourceVersion\":\"9\",\"target\":\"http://t\",\"targetVersion\":\"2\","
            + "\"element\":[{\"code\":\"a\",\"display\":\"A\",\"target\":[{\"code\":\"b\","
            + "\"display\":\"B\",\"equivalence\":\"equal\"}]},{\"code\":\"c\",\"target\":"
            + "[{\"code\":\"d\",\"equivalence\":\"equal\"}]}]}]}";

    final List<List<String>> rows =
        staged(json).orderBy(COLUMN_ORDINAL).collectAsList().stream()
            .map(
                row ->
                    Arrays.asList(
                        row.<String>getAs(COLUMN_SOURCE_VERSION),
                        row.<String>getAs(COLUMN_SOURCE_DISPLAY),
                        row.<String>getAs(COLUMN_TARGET_VERSION),
                        row.<String>getAs(COLUMN_TARGET_DISPLAY)))
            .toList();

    assertEquals(
        List.of(Arrays.asList("9", "A", "2", "B"), Arrays.asList("9", null, "2", null)), rows);
  }

  @Test
  void recordsNoFaultForContentARelationCanCarry() throws Exception {
    final String json =
        Files.readString(
            FhirFixtures.jsonDirectory().resolve("conceptmap-species-to-category.json"));

    assertNull(fault(json));
  }

  @Test
  void recordsOnlyTheFirstFault() throws Exception {
    // The first element's target carries dependsOn; the second has no equivalence.
    final String json =
        "{\"resourceType\":\"ConceptMap\",\"group\":[{\"source\":\"http://s\",\"element\":["
            + "{\"code\":\"a\",\"target\":[{\"code\":\"b\",\"equivalence\":\"equal\","
            + "\"dependsOn\":[{\"property\":\"p\",\"value\":\"v\"}]}]},"
            + "{\"code\":\"c\",\"target\":[{\"code\":\"d\"}]}]}]}";

    assertEquals(
        "the mapping for source code 'a' depends on other elements (dependsOn)", fault(json));
    // The fault does not stop either mapping from being staged for translate.
    assertEquals(
        List.of(
            row("http://s", "a", null, "b", "equal"), row("http://s", "c", null, "d", "relatedto")),
        flatten(json));
  }

  /** Flattens a ConceptMap and reads its mapping rows back in document order. */
  private static List<String> flatten(final String json) throws IOException {
    return staged(json).orderBy(COLUMN_ORDINAL).collectAsList().stream()
        .map(ConceptMapStreamFlattenerTest::row)
        .toList();
  }

  /** Flattens a ConceptMap and returns the fault recorded with the staging, or null. */
  private static String fault(final String json) throws IOException {
    try (ConceptMapStaging staging = ConceptMapStaging.create();
        JsonParser parser = FACTORY.createParser(json.getBytes(StandardCharsets.UTF_8))) {
      staging.beginConceptMap("cm");
      new ConceptMapStreamFlattener(staging).flatten(parser);
      return staging.fault();
    }
  }

  /** Flattens a ConceptMap and reads every staging row back, collected before cleanup. */
  private static Dataset<Row> staged(final String json) throws IOException {
    try (ConceptMapStaging staging = ConceptMapStaging.create()) {
      staging.beginConceptMap("cm");
      try (JsonParser parser = FACTORY.createParser(json.getBytes(StandardCharsets.UTF_8))) {
        new ConceptMapStreamFlattener(staging).flatten(parser);
      }
      staging.sealForReading();
      final Dataset<Row> rows = staging.read(spark);
      return spark.createDataFrame(rows.collectAsList(), rows.schema());
    }
  }

  private static String row(final Row row) {
    return row(
        row.getAs(COLUMN_SOURCE_SYSTEM),
        row.getAs(COLUMN_SOURCE_CODE),
        row.getAs(COLUMN_TARGET_SYSTEM),
        row.getAs(COLUMN_TARGET_CODE),
        row.getAs(COLUMN_EQUIVALENCE));
  }

  private static String row(
      final String sourceSystem,
      final String sourceCode,
      final String targetSystem,
      final String targetCode,
      final String equivalence) {
    return sourceSystem
        + "|"
        + sourceCode
        + " -> "
        + targetSystem
        + "|"
        + targetCode
        + " ("
        + equivalence
        + ")";
  }
}
