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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the streaming flattener against the nested-hierarchy fixture: dense identifiers follow
 * document order, the display falls back to the code, an inactive property clears the active flag,
 * scalar and Coding-valued properties are split into their staging files, and a Bundle-extracted
 * CodeSystem re-encoded through the same flattener produces the same staging rows.
 *
 * @author John Grimes
 */
class CodeSystemStreamFlattenerTest {

  private static final JsonFactory FACTORY = new JsonFactory();

  private static SparkSession spark;

  @BeforeAll
  static void setUp(@TempDir final Path warehouse) {
    spark =
        SparkSession.builder()
            .appName("CodeSystemStreamFlattenerTest")
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
  void flattensNestedHierarchyIntoStagingRows() throws Exception {
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      flattenFixture(staging, "nested-hierarchy.json");
      staging.sealForReading();

      // Concepts carry dense identifiers in document (pre-order) order.
      final Map<String, Integer> dense = new HashMap<>();
      final Map<String, Boolean> active = new HashMap<>();
      final Map<String, String> display = new HashMap<>();
      for (final Row row :
          spark
              .read()
              .schema(CodeSystemStaging.conceptSchema())
              .json(staging.conceptPath())
              .collectAsList()) {
        final String code = row.getAs(TerminologyStoreSchema.COLUMN_CODE);
        dense.put(code, row.getAs(TerminologyStoreSchema.COLUMN_DENSE_ID));
        active.put(code, row.getAs(TerminologyStoreSchema.COLUMN_ACTIVE));
        display.put(code, row.getAs(TerminologyStoreSchema.COLUMN_DISPLAY));
      }
      assertEquals(0, dense.get("A"));
      assertEquals(1, dense.get("B"));
      assertEquals(2, dense.get("C"));
      assertEquals(3, dense.get("D"));
      // The display falls back to the code when absent.
      assertEquals("C", display.get("C"));
      assertEquals("Alpha", display.get("A"));
      // An inactive property clears the active flag.
      assertFalse(active.get("C"));
      assertTrue(active.get("A"));

      // The designation on A becomes a description row keyed by A's dense identifier.
      final Set<String> descriptionsOfA = new HashSet<>();
      for (final Row row :
          spark
              .read()
              .schema(CodeSystemStaging.descriptionSchema())
              .json(staging.descriptionPath())
              .collectAsList()) {
        if (dense.get("A").equals(row.getAs(TerminologyStoreSchema.COLUMN_CONCEPT_DENSE_ID))) {
          descriptionsOfA.add(row.getAs(TerminologyStoreSchema.COLUMN_TERM));
        }
      }
      assertTrue(descriptionsOfA.contains("AlphaSyn"));

      // Scalar properties land in the property file with their FHIR type; the Coding property does
      // not.
      final Map<String, String> propertyTypes = new HashMap<>();
      for (final Row row :
          spark
              .read()
              .schema(CodeSystemStaging.propertySchema())
              .json(staging.propertyPath())
              .collectAsList()) {
        propertyTypes.put(
            row.getAs(TerminologyStoreSchema.COLUMN_PROPERTY_CODE)
                + "="
                + row.getAs(TerminologyStoreSchema.COLUMN_VALUE),
            row.getAs(TerminologyStoreSchema.COLUMN_VALUE_TYPE));
      }
      assertEquals("integer", propertyTypes.get("weight=5"));
      assertEquals("boolean", propertyTypes.get("inactive=true"));
      assertFalse(propertyTypes.containsKey("assoc=C"));

      // The Coding-valued property lands in the Coding-property file, unresolved by target code.
      final Set<String> codingProperties = new HashSet<>();
      for (final Row row :
          spark
              .read()
              .schema(CodeSystemStaging.codingPropertySchema())
              .json(staging.codingPropertyPath())
              .collectAsList()) {
        codingProperties.add(
            row.getAs(TerminologyStoreSchema.COLUMN_SOURCE_DENSE_ID)
                + ":"
                + row.getAs(TerminologyStoreSchema.COLUMN_PROPERTY_CODE)
                + ":"
                + row.getAs(TerminologyStoreSchema.COLUMN_TARGET_CODE));
      }
      assertTrue(codingProperties.contains(dense.get("B") + ":assoc:C"));

      // Nesting edges connect each child to its parent by dense identifier.
      final Set<String> edges = new HashSet<>();
      for (final Row row :
          spark
              .read()
              .schema(CodeSystemStaging.denseEdgeSchema())
              .json(staging.denseEdgePath())
              .collectAsList()) {
        edges.add(
            row.getAs(TerminologyStoreSchema.COLUMN_SOURCE_DENSE_ID)
                + "->"
                + row.getAs(TerminologyStoreSchema.COLUMN_TARGET_DENSE_ID));
      }
      assertTrue(edges.contains(dense.get("B") + "->" + dense.get("A")));
      assertTrue(edges.contains(dense.get("C") + "->" + dense.get("A")));
      assertTrue(edges.contains(dense.get("D") + "->" + dense.get("C")));
      assertEquals(3, edges.size());
    }
  }

  @Test
  void reEncodesABundleWrappedCodeSystemThroughTheSamePath() throws Exception {
    // Extract the CodeSystem object from the Bundle fixture and re-encode it as a standalone
    // resource, then flatten it: the flattener accepts any parser, so the rows match.
    final String bundle = FhirPackageFixtures.read("bundle-codesystem.json");
    final int start = bundle.indexOf("{", bundle.indexOf("\"resource\""));
    final String codeSystemJson = extractBalancedObject(bundle, start);

    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      final CodeSystemStreamFlattener flattener = new CodeSystemStreamFlattener(staging);
      try (JsonParser parser =
          FACTORY.createParser(codeSystemJson.getBytes(StandardCharsets.UTF_8))) {
        assertEquals(2, flattener.flatten(parser));
      }
      staging.sealForReading();

      final Set<String> codes = new HashSet<>();
      for (final Row row :
          spark
              .read()
              .schema(CodeSystemStaging.conceptSchema())
              .json(staging.conceptPath())
              .collectAsList()) {
        codes.add(row.getAs(TerminologyStoreSchema.COLUMN_CODE));
      }
      assertEquals(Set.of("A", "B"), codes);
      assertEquals("is-a", flattener.getHierarchyMeaning());
    }
  }

  private static void flattenFixture(final CodeSystemStaging staging, final String fixtureName)
      throws Exception {
    final CodeSystemStreamFlattener flattener = new CodeSystemStreamFlattener(staging);
    try (InputStream in = Files.newInputStream(FhirPackageFixtures.resource(fixtureName));
        JsonParser parser = FACTORY.createParser(in)) {
      flattener.flatten(parser);
    }
  }

  @Test
  void recognisesParentPropertiesByCodeAndByDeclaredUri() throws Exception {
    // A parent property, whether keyed by the standard code or a declared standard URI, emits a
    // child-role edge, and the property row is retained.
    assertEquals(Set.of("0:child:Y"), codeEdgesOf(byCodeParent()));
    assertEquals(Set.of("0:child:Y"), codeEdgesOf(byUriParent()));

    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      flattenString(staging, byCodeParent());
      staging.sealForReading();
      final Set<String> propertyCodes = new HashSet<>();
      for (final Row row :
          spark
              .read()
              .schema(CodeSystemStaging.propertySchema())
              .json(staging.propertyPath())
              .collectAsList()) {
        propertyCodes.add(row.getAs(TerminologyStoreSchema.COLUMN_PROPERTY_CODE));
      }
      assertTrue(propertyCodes.contains("parent"), "the parent property row is still emitted");
    }
  }

  @Test
  void recognisesChildPropertiesAsParentRoleEdges() throws Exception {
    final String json =
        "{\"resourceType\":\"CodeSystem\",\"url\":\"u\",\"version\":\"1\",\"concept\":["
            + "{\"code\":\"X\",\"property\":[{\"code\":\"child\",\"valueCode\":\"Y\"}]}]}";
    assertEquals(Set.of("0:parent:Y"), codeEdgesOf(json));
  }

  @Test
  void doesNotEmitEdgesForNonHierarchyCodeProperties() throws Exception {
    final String json =
        "{\"resourceType\":\"CodeSystem\",\"url\":\"u\",\"version\":\"1\",\"concept\":["
            + "{\"code\":\"X\",\"property\":[{\"code\":\"habitat\",\"valueCode\":\"land\"}]}]}";
    assertEquals(Set.of(), codeEdgesOf(json));
  }

  @Test
  void emitsARunningConceptCountEveryProgressInterval() throws Exception {
    final ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(CodeSystemStreamFlattener.class);
    final ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
        new ch.qos.logback.core.read.ListAppender<>();
    appender.start();
    final ch.qos.logback.classic.Level previousLevel = logger.getLevel();
    logger.setLevel(ch.qos.logback.classic.Level.INFO);
    logger.addAppender(appender);
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      // Five flat concepts with an interval of two: a running count is logged at two and four.
      final String json =
          "{\"resourceType\":\"CodeSystem\",\"url\":\"u\",\"version\":\"1\",\"concept\":["
              + "{\"code\":\"a\"},{\"code\":\"b\"},{\"code\":\"c\"},{\"code\":\"d\"},{\"code\":\"e\"}]}";
      final CodeSystemStreamFlattener flattener = new CodeSystemStreamFlattener(staging, 2);
      try (JsonParser parser = FACTORY.createParser(json.getBytes(StandardCharsets.UTF_8))) {
        flattener.flatten(parser);
      }
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
    }
    final long progressMessages =
        appender.list.stream()
            .filter(event -> event.getFormattedMessage().startsWith("Parsed "))
            .count();
    assertEquals(2, progressMessages);
  }

  private static String byCodeParent() {
    return "{\"resourceType\":\"CodeSystem\",\"url\":\"u\",\"version\":\"1\",\"concept\":["
        + "{\"code\":\"X\",\"property\":[{\"code\":\"parent\",\"valueCode\":\"Y\"}]}]}";
  }

  private static String byUriParent() {
    return "{\"resourceType\":\"CodeSystem\",\"url\":\"u\",\"version\":\"1\","
        + "\"property\":[{\"code\":\"isa\","
        + "\"uri\":\"http://hl7.org/fhir/concept-properties#parent\",\"type\":\"code\"}],"
        + "\"concept\":[{\"code\":\"X\",\"property\":[{\"code\":\"isa\",\"valueCode\":\"Y\"}]}]}";
  }

  private Set<String> codeEdgesOf(final String json) throws Exception {
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      flattenString(staging, json);
      staging.sealForReading();
      final Set<String> edges = new HashSet<>();
      for (final Row row :
          spark
              .read()
              .schema(CodeSystemStaging.codeEdgeSchema())
              .json(staging.codeEdgePath())
              .collectAsList()) {
        edges.add(
            row.getAs(CodeSystemStaging.COLUMN_KNOWN_DENSE_ID)
                + ":"
                + row.getAs(CodeSystemStaging.COLUMN_KNOWN_ROLE)
                + ":"
                + row.getAs(CodeSystemStaging.COLUMN_OTHER_CODE));
      }
      return edges;
    }
  }

  private static void flattenString(final CodeSystemStaging staging, final String json)
      throws Exception {
    final CodeSystemStreamFlattener flattener = new CodeSystemStreamFlattener(staging);
    try (JsonParser parser = FACTORY.createParser(json.getBytes(StandardCharsets.UTF_8))) {
      flattener.flatten(parser);
    }
  }

  /** Extracts the balanced JSON object beginning at {@code start} within {@code json}. */
  private static String extractBalancedObject(final String json, final int start) {
    int depth = 0;
    boolean inString = false;
    boolean escape = false;
    for (int i = start; i < json.length(); i++) {
      final char c = json.charAt(i);
      if (escape) {
        escape = false;
      } else if (c == '\\') {
        escape = true;
      } else if (c == '"') {
        inString = !inString;
      } else if (!inString && c == '{') {
        depth++;
      } else if (!inString && c == '}') {
        depth--;
        if (depth == 0) {
          return json.substring(start, i + 1);
        }
      }
    }
    throw new IllegalStateException("Unbalanced object in fixture");
  }
}
