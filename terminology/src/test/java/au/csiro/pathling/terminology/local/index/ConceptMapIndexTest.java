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

package au.csiro.pathling.terminology.local.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.local.LocalTerminologyService;
import au.csiro.pathling.terminology.local.VersionResolver;
import au.csiro.pathling.terminology.store.FhirTerminologyImporter;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the concept map index over a store: a code translates to every target it maps to in
 * document order, both its system and its code must match, and where a store holds several versions
 * of a map only the latest answers, without the versions being merged.
 *
 * @author John Grimes
 */
class ConceptMapIndexTest {

  private static final String VERSIONED = "http://example.org/cm/versioned";
  private static final String ORDERED = "http://example.org/cm/ordered";
  private static final String UNICODE = "http://example.org/cm/unicode";
  private static final List<String> UNICODE_CODES = List.of("a", "é", "\uFFFD", "😀", "z");
  private static final String LARGE = "http://example.org/cm/large";
  private static final int LARGE_SIZE = 5000;

  private static SparkSession spark;
  private static ConceptMapIndex index;
  private static String store;

  @BeforeAll
  static void setUp(@TempDir final Path dir) throws Exception {
    spark =
        SparkSession.builder()
            .appName("ConceptMapIndexTest")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", dir.resolve("warehouse").toString())
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();

    final Path source = Files.createDirectories(dir.resolve("source"));
    // Version 1.10.0 is the latest by SemVer precedence, though 1.9.0 sorts after it as text.
    Files.writeString(
        source.resolve("versioned-old.json"),
        conceptMap(VERSIONED, "1.9.0", group("http://s", "http://t", element("dog", "old"))));
    Files.writeString(
        source.resolve("versioned-new.json"),
        conceptMap(VERSIONED, "1.10.0", group("http://s", "http://t", element("dog", "new"))));
    Files.writeString(
        source.resolve("ordered.json"),
        conceptMap(
            ORDERED,
            "1",
            group(
                    "http://s",
                    "http://t",
                    "{\"code\":\"x\",\"target\":[{\"code\":\"c\",\"equivalence\":\"equal\"},"
                        + "{\"code\":\"a\",\"equivalence\":\"wider\"},"
                        + "{\"code\":\"b\",\"equivalence\":\"narrower\"}]}")
                + ","
                + group("http://other", "http://t", element("x", "q"))));
    Files.writeString(
        source.resolve("unicode.json"),
        conceptMap(
            UNICODE,
            "1",
            group(
                "http://s",
                "http://t",
                String.join(
                    ",",
                    UNICODE_CODES.stream().map(code -> element(code, "to-" + code)).toList()))));
    Files.writeString(
        source.resolve("large.json"),
        conceptMap(
            LARGE,
            "1",
            group(
                "http://s",
                "http://t",
                String.join(
                    ",",
                    IntStream.range(0, LARGE_SIZE)
                        .mapToObj(i -> element("s" + i, "t" + i))
                        .toList()))));
    store = dir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store).importFrom(source.toString(), false, null);

    index =
        ConceptMapIndex.load(
            TerminologyStoreReader.open(store, Map.of()), new VersionResolver(null));
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  void aStoreHoldingOnlyConceptMapsAnswersTranslate() {
    // This store holds no CodeSystem, which must not stop the service from translating.
    final LocalTerminologyService service =
        new LocalTerminologyService(
            TerminologyConfiguration.builder()
                .mode(TerminologyMode.LOCAL)
                .local(LocalTerminologyConfiguration.builder().storagePath(store).build())
                .build(),
            Map.of());

    assertEquals(
        List.of("http://t|new (equivalent)"),
        describe(service.translate(new Coding("http://s", "dog", null), VERSIONED, false, null)));
  }

  @Test
  void returnsEveryTargetOfACodeInDocumentOrder() {
    assertEquals(
        List.of("http://t|c (equal)", "http://t|a (wider)", "http://t|b (narrower)"),
        describe(index.translate(ORDERED, "http://s", "x", false)));
  }

  @Test
  void matchesTheSourceSystemAsWellAsTheCode() {
    // The same code in another group's source system has a translation of its own.
    assertEquals(
        List.of("http://t|q (equivalent)"),
        describe(index.translate(ORDERED, "http://other", "x", false)));
    assertTrue(index.translate(ORDERED, "http://elsewhere", "x", false).isEmpty());
  }

  @Test
  void reverseTranslationMatchesTheTargetSystemAndInvertsTheEquivalence() {
    // a is the wider target of x, so x is narrower than a.
    assertEquals(
        List.of("http://s|x (narrower)"),
        describe(index.translate(ORDERED, "http://t", "a", true)));
    assertTrue(index.translate(ORDERED, "http://elsewhere", "a", true).isEmpty());
  }

  @Test
  void reverseTranslationReturnsEverySourceOfATarget() {
    // Each group maps an x into http://t, but only the second group's x maps to q.
    assertEquals(
        List.of("http://other|x (equivalent)"),
        describe(index.translate(ORDERED, "http://t", "q", true)));
  }

  @Test
  void translatesThroughTheLatestVersionOnly() {
    assertEquals(
        List.of("http://t|new (equivalent)"),
        describe(index.translate(VERSIONED, "http://s", "dog", false)));
    // The mappings of the older version are not merged into the latest.
    assertTrue(index.translate(VERSIONED, "http://t", "old", true).isEmpty());
    assertEquals(
        List.of("http://s|dog (equivalent)"),
        describe(index.translate(VERSIONED, "http://t", "new", true)));
  }

  @Test
  void unknownMapsAndCodesTranslateToNothing() {
    assertTrue(index.translate("http://example.org/cm/missing", "http://s", "x", false).isEmpty());
    assertTrue(index.translate(ORDERED, "http://s", "unmapped", false).isEmpty());
  }

  @Test
  void findsCodesOutsideAsciiInBothDirections() {
    // Codes are held as UTF-8 bytes, so codes of two, three and four bytes per character, including
    // one outside the Basic Multilingual Plane, must each be found and returned intact.
    for (final String code : UNICODE_CODES) {
      assertEquals(
          List.of("http://t|to-" + code + " (equivalent)"),
          describe(index.translate(UNICODE, "http://s", code, false)),
          code);
      assertEquals(
          List.of("http://s|" + code + " (equivalent)"),
          describe(index.translate(UNICODE, "http://t", "to-" + code, true)),
          code);
    }
  }

  @Test
  void findsEveryCodeOfAMapThatOutgrowsTheInitialCodeTable() {
    // Thousands of codes force the code table and the mapping arrays to grow many times over.
    for (int i = 0; i < LARGE_SIZE; i++) {
      assertEquals(
          List.of("http://t|t" + i + " (equivalent)"),
          describe(index.translate(LARGE, "http://s", "s" + i, false)));
    }
    assertEquals(
        List.of("http://s|s" + (LARGE_SIZE - 1) + " (equivalent)"),
        describe(index.translate(LARGE, "http://t", "t" + (LARGE_SIZE - 1), true)));
  }

  private static List<String> describe(final List<Translation> translations) {
    return translations.stream()
        .map(
            t ->
                t.getConcept().getSystem()
                    + "|"
                    + t.getConcept().getCode()
                    + " ("
                    + t.getEquivalence().toCode()
                    + ")")
        .toList();
  }

  private static String conceptMap(final String url, final String version, final String groups) {
    return "{\"resourceType\":\"ConceptMap\",\"url\":\""
        + url
        + "\",\"version\":\""
        + version
        + "\",\"status\":\"active\",\"group\":["
        + groups
        + "]}";
  }

  private static String group(final String source, final String target, final String elements) {
    return "{\"source\":\""
        + source
        + "\",\"target\":\""
        + target
        + "\",\"element\":["
        + elements
        + "]}";
  }

  private static String element(final String sourceCode, final String targetCode) {
    return "{\"code\":\""
        + sourceCode
        + "\",\"target\":[{\"code\":\""
        + targetCode
        + "\",\"equivalence\":\"equivalent\"}]}";
  }
}
