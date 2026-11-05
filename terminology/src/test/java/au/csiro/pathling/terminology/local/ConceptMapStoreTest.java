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

package au.csiro.pathling.terminology.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapLimitExceededException;
import au.csiro.pathling.terminology.store.FhirTerminologyImporter;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.ConceptMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies that a ConceptMap read back from the flattened {@code concept_mapping} rows of a store
 * is exactly what {@link ConceptMapContent#fromResource} makes of the resource that was imported:
 * the same rows in the same order, with the same versions and displays, and the same fault where
 * the content cannot be represented. Each case is imported from raw JSON, so that field order and
 * primitives carrying only an extension reach the streaming importer as they were written, and is
 * parsed by HAPI from the same JSON for the comparison.
 *
 * @author John Grimes
 */
class ConceptMapStoreTest {

  private static final int NO_LIMIT = Integer.MAX_VALUE;

  private static final String BASE = "http://example.org/cm/";

  private static final String EXTENSION_ONLY =
      "{\"extension\":[{\"url\":\"http://example.org/ext\",\"valueString\":\"x\"}]}";

  /** The JSON of each case, keyed by the last segment of its canonical URL. */
  private static final Map<String, String> CASES = new LinkedHashMap<>();

  static {
    // Representable content.
    CASES.put(
        "displays-and-versions",
        groups(
            "{\"source\":\"http://s\",\"sourceVersion\":\"1\",\"target\":\"http://t\","
                + "\"targetVersion\":\"2\",\"element\":[{\"code\":\"a\",\"display\":\"A\","
                + "\"target\":[{\"code\":\"b\",\"display\":\"B\",\"equivalence\":\"wider\"}]}]}"));
    CASES.put(
        "versions-in-canonicals",
        groups(
            "{\"source\":\"http://s|1\",\"target\":\"http://t|2\",\"element\":[{\"code\":\"a\","
                + "\"target\":[{\"code\":\"b\",\"equivalence\":\"equal\"}]}]}"));
    CASES.put(
        "explicit-version-beside-canonical-version",
        groups(
            "{\"source\":\"http://s|1\",\"sourceVersion\":\"9\",\"target\":\"http://t|2\","
                + "\"targetVersion\":\"8\",\"element\":[{\"code\":\"a\","
                + "\"target\":[{\"code\":\"b\",\"equivalence\":\"equal\"}]}]}"));
    CASES.put(
        "no-target-system",
        groups(
            "{\"source\":\"http://s\",\"element\":[{\"code\":\"a\","
                + "\"target\":[{\"code\":\"b\",\"equivalence\":\"equal\"}]}]}"));
    CASES.put(
        "unmatched",
        groups(
            "{\"source\":\"http://s\",\"target\":\"http://t\",\"element\":[{\"code\":\"a\","
                + "\"target\":[{\"code\":\"ignored\",\"display\":\"Ignored\","
                + "\"equivalence\":\"unmatched\"},{\"equivalence\":\"unmatched\"}]}]}"));
    CASES.put(
        "duplicates-keep-first-display",
        groups(
            "{\"source\":\"http://s\",\"target\":\"http://t\",\"element\":["
                + "{\"code\":\"a\",\"display\":\"First\",\"target\":[{\"code\":\"b\","
                + "\"display\":\"First\",\"equivalence\":\"equal\"}]},"
                + "{\"code\":\"a\",\"display\":\"Second\",\"target\":[{\"code\":\"b\","
                + "\"display\":\"Second\",\"equivalence\":\"equal\"}]}]}"));
    CASES.put(
        "absent-and-extension-only-primitives",
        groups(
            "{\"source\":\"http://s\",\"_sourceVersion\":"
                + EXTENSION_ONLY
                + ",\"target\":\"http://t\",\"targetVersion\":\"  \",\"element\":[{\"code\":\"a\","
                + "\"_display\":"
                + EXTENSION_ONLY
                + ",\"target\":[{\"code\":\"b\",\"display\":\" \",\"equivalence\":\"equal\"}]}]}"));
    CASES.put(
        "fields-in-reverse-order",
        groups(
            "{\"element\":[{\"target\":[{\"equivalence\":\"narrower\",\"display\":\"B\","
                + "\"code\":\"b\"}],\"display\":\"A\",\"code\":\"a\"}],\"targetVersion\":\"2\","
                + "\"target\":\"http://t\",\"sourceVersion\":\"1\",\"source\":\"http://s\"}"));
    // Many mappings across two groups, so that the rows are read back in an order other than the
    // document's and must be put back into it.
    CASES.put(
        "document-order",
        groups(
            group("http://s1", "http://t1", 0, 60),
            "{\"source\":\"http://s2\",\"target\":\"http://t2\",\"element\":[{\"code\":\"x\","
                + "\"target\":[{\"code\":\"z\",\"equivalence\":\"equal\"},"
                + "{\"code\":\"y\",\"equivalence\":\"wider\"}]}]}",
            group("http://s1", "http://t1", 60, 120)));
    CASES.put("no-groups", "{\"resourceType\":\"ConceptMap\",\"url\":\"%s\",\"version\":\"1\"}");

    // Content that a concept map relation cannot carry.
    CASES.put(
        "group-without-source",
        groups(
            "{\"target\":\"http://t\",\"element\":[{\"code\":\"a\","
                + "\"target\":[{\"code\":\"b\",\"equivalence\":\"equal\"}]}]}"));
    CASES.put(
        "group-without-source-or-elements",
        groups(group("http://s", "http://t", 0, 2), "{\"target\":\"http://t\"}"));
    CASES.put(
        "group-with-extension-only-source",
        groups(
            "{\"_source\":"
                + EXTENSION_ONLY
                + ",\"element\":[{\"code\":\"a\","
                + "\"target\":[{\"code\":\"b\",\"equivalence\":\"equal\"}]}]}"));
    CASES.put(
        "element-without-code-or-targets",
        groups("{\"source\":\"http://s\",\"element\":[{\"display\":\"Nothing\"}]}"));
    CASES.put(
        "element-with-blank-code",
        groups(
            "{\"source\":\"http://s\",\"element\":[{\"code\":\" \","
                + "\"target\":[{\"code\":\"b\",\"equivalence\":\"equal\"}]}]}"));
    CASES.put(
        "depends-on",
        groups(
            "{\"source\":\"http://s\",\"element\":[{\"code\":\"a\",\"target\":[{\"code\":\"b\","
                + "\"equivalence\":\"equal\",\"dependsOn\":[{\"property\":\"p\",\"value\":\"v\"}]}]}]}"));
    CASES.put(
        "product",
        groups(
            "{\"source\":\"http://s\",\"element\":[{\"code\":\"a\",\"target\":[{\"code\":\"b\","
                + "\"equivalence\":\"equal\",\"product\":[{\"property\":\"p\",\"value\":\"v\"}]}]}]}"));
    CASES.put(
        "unmatched-with-depends-on",
        groups(
            "{\"source\":\"http://s\",\"element\":[{\"code\":\"a\",\"target\":["
                + "{\"equivalence\":\"unmatched\",\"dependsOn\":[{\"property\":\"p\",\"value\":\"v\"}]}]}]}"));
    CASES.put(
        "no-equivalence",
        groups(
            "{\"source\":\"http://s\",\"element\":[{\"code\":\"a\",\"target\":[{\"code\":\"b\"}]}]}"));
    CASES.put(
        "extension-only-equivalence",
        groups(
            "{\"source\":\"http://s\",\"element\":[{\"code\":\"a\",\"target\":[{\"code\":\"b\","
                + "\"_equivalence\":"
                + EXTENSION_ONLY
                + "}]}]}"));
    CASES.put(
        "no-target-code",
        groups(
            "{\"source\":\"http://s\",\"element\":[{\"code\":\"a\","
                + "\"target\":[{\"display\":\"B\",\"equivalence\":\"equal\"}]}]}"));
    // Where a map has several faults, the first in the order fromResource walks it is reported.
    CASES.put(
        "group-source-checked-before-its-elements",
        groups(
            "{\"element\":[{\"code\":\"a\",\"target\":[{\"code\":\"b\"}]}],\"target\":\"http://t\"}"));
    CASES.put(
        "element-code-checked-before-its-targets",
        groups(
            "{\"source\":\"http://s\",\"element\":[{\"target\":[{\"code\":\"b\","
                + "\"product\":[{\"property\":\"p\",\"value\":\"v\"}]}]}]}"));
    CASES.put(
        "target-checks-in-order",
        groups(
            "{\"source\":\"http://s\",\"element\":[{\"code\":\"a\",\"target\":[{"
                + "\"product\":[{\"property\":\"p\",\"value\":\"v\"}],"
                + "\"dependsOn\":[{\"property\":\"p\",\"value\":\"v\"}]}]}]}"));
    CASES.put(
        "earlier-fault-wins",
        groups(
            "{\"source\":\"http://s\",\"element\":[{\"code\":\"a\",\"target\":[{\"code\":\"b\"}]},"
                + "{\"code\":\"c\",\"target\":[{\"equivalence\":\"equal\"}]}]}",
            "{\"element\":[{\"code\":\"d\",\"target\":[{\"code\":\"e\","
                + "\"equivalence\":\"equal\"}]}]}"));
  }

  private static SparkSession spark;
  private static TerminologyStoreReader reader;

  @BeforeAll
  static void setUp(@TempDir final Path dir) throws Exception {
    spark =
        SparkSession.builder()
            .appName("ConceptMapStoreTest")
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
    for (final Map.Entry<String, String> entry : CASES.entrySet()) {
      Files.writeString(source.resolve(entry.getKey() + ".json"), json(entry.getKey()));
    }
    final String store = dir.resolve("store").toString();
    new FhirTerminologyImporter(spark, store).importFrom(source.toString(), false, null);
    reader = TerminologyStoreReader.open(store, Map.of());
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  static Stream<String> cases() {
    return CASES.keySet().stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void readsWhatFromResourceMakesOfTheImportedResource(@Nonnull final String name) {
    assertEquals(expected(name, NO_LIMIT), actual(name, NO_LIMIT));
  }

  @Test
  void rejectsMoreRowsThanTheCap() {
    // The map has 120 distinct rows in its first group alone.
    final Object expected = expected("document-order", 10);

    assertTrue(
        expected.toString().startsWith(ConceptMapLimitExceededException.class.getSimpleName()),
        expected.toString());
    assertEquals(expected, actual("document-order", 10));
  }

  @Test
  void acceptsExactlyTheCap() {
    // Two of the three rows are duplicates of each other, so the content is at the cap of one.
    assertEquals(
        expected("duplicates-keep-first-display", 1), actual("duplicates-keep-first-display", 1));
  }

  @Test
  void reportsAFaultBeyondTheCapRatherThanTheCap() {
    // The fault lies in the second group, after more rows than the cap allows.
    final String name = "group-without-source-or-elements";

    assertEquals(expected(name, 1), actual(name, 1));
  }

  /** What {@link ConceptMapContent#fromResource} makes of the case, or the fault it reports. */
  @Nonnull
  private static Object expected(@Nonnull final String name, final int maxMappings) {
    final ConceptMap resource =
        (ConceptMap) FhirContext.forR4Cached().newJsonParser().parseResource(json(name));
    return outcome(() -> ConceptMapContent.fromResource(resource, maxMappings));
  }

  /** What the store reads back for the case, or the fault it reports. */
  @Nonnull
  private static Object actual(@Nonnull final String name, final int maxMappings) {
    return outcome(
        () ->
            ConceptMapStore.read(reader, BASE + name, null, new VersionResolver(null), maxMappings)
                .orElseThrow());
  }

  /** Returns the content read, or the class and message of the exception the read threw. */
  @Nonnull
  private static Object outcome(@Nonnull final Supplier<ConceptMapContent> read) {
    try {
      return read.get();
    } catch (final RuntimeException e) {
      return e.getClass().getSimpleName() + ": " + e.getMessage();
    }
  }

  @Nonnull
  private static String json(@Nonnull final String name) {
    return CASES.get(name).formatted(BASE + name);
  }

  /** A ConceptMap holding the given groups, with its URL left as a placeholder. */
  @Nonnull
  private static String groups(@Nonnull final String... groups) {
    return "{\"resourceType\":\"ConceptMap\",\"url\":\"%s\",\"version\":\"1\",\"group\":["
        + String.join(",", groups)
        + "]}";
  }

  /** A group mapping source code {@code s<i>} to target code {@code t<i>} for each i in range. */
  @Nonnull
  private static String group(
      @Nonnull final String source, @Nonnull final String target, final int from, final int to) {
    return "{\"source\":\""
        + source
        + "\",\"target\":\""
        + target
        + "\",\"element\":["
        + IntStream.range(from, to)
            .mapToObj(
                i ->
                    "{\"code\":\"s"
                        + i
                        + "\",\"target\":[{\"code\":\"t"
                        + i
                        + "\",\"equivalence\":\"equal\"}]}")
            .collect(Collectors.joining(","))
        + "]}";
  }
}
