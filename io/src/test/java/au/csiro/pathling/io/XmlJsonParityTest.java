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

package au.csiro.pathling.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.utilities.xhtml.XhtmlNode;
import org.hl7.fhir.utilities.xhtml.XhtmlParser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that FHIR XML is stored as the same resources in JSON are stored, schema and rows, for
 * every resource type of the specification's examples and of the Synthea data the benchmark is made
 * from (FR-060, ticket 32).
 *
 * <p>The two sides are made to carry the same content. The XML is written by HAPI from the JSON,
 * and the JSON the XML route is compared with is written by HAPI from that XML, so that whatever
 * HAPI's writers change, such as a narrative's whitespace, is on both sides. The comparison is then
 * exact for every column but two. A decimal is compared by value, because XML stores its text and
 * JSON the text of a double (owner, 2026-10-22). A narrative is stored by each route as the
 * serialisation of the same XHTML that its source held: the XML route serialises what it parsed, so
 * it is compared as XHTML, at L2. On this corpus, which HAPI wrote, every narrative is also equal
 * as text, and that is asserted too, so that a change in it is seen.
 */
class XmlJsonParityTest {

  @Nonnull private static final FhirContext CONTEXT = FhirContext.forR4Cached();

  @ParameterizedTest
  @MethodSource("specificationTypes")
  void storesWhatJsonStoresForTheSpecificationExamples(@Nonnull final String resourceType) {
    final List<String> source = lines(SpecExampleRoundTripTest.corpus(resourceType));

    assertSameStored(resourceType, source);
  }

  @ParameterizedTest
  @MethodSource("syntheaTypes")
  void storesWhatJsonStoresForTheSyntheaData(@Nonnull final String resourceType) {
    final List<String> source =
        entryResources(SemanticJson.parse(text("/data/bundles/synthea.json"))).stream()
            .filter(resource -> resourceType.equals(resource.get("resourceType").asText()))
            .map(JsonNode::toString)
            .toList();

    assertSameStored(resourceType, source);
  }

  private static void assertSameStored(
      @Nonnull final String resourceType, @Nonnull final List<String> source) {
    final IParser json = CONTEXT.newJsonParser();
    final IParser xml = CONTEXT.newXmlParser();
    final List<String> documents =
        source.stream()
            .map(resource -> xml.encodeResourceToString(json.parseResource(resource)))
            .toList();
    final List<String> equivalents =
        documents.stream()
            .map(document -> json.encodeResourceToString(xml.parseResource(document)))
            .toList();

    final Dataset<Row> fromXml =
        TransformFixtures.fhirReader().xml().read(resourceType, dataset(documents));
    final Dataset<Row> fromJson =
        TransformFixtures.fhirReader().json().read(resourceType, dataset(equivalents));

    assertEquals(fromJson.schema(), fromXml.schema(), "the stored schemas differ");
    final DefinitionCanonicalStructure structure =
        DefinitionCanonicalStructure.forResource(TransformFixtures.DEFINITIONS, resourceType);
    final List<JsonNode> expected = rows(fromJson, structure);
    final List<JsonNode> actual = rows(fromXml, structure);
    final List<String> textual = new ArrayList<>();
    assertEquals(expected.size(), actual.size(), "the number of stored resources differs");
    for (int i = 0; i < expected.size(); i++) {
      final String expectedDiv = removeNarrative(expected.get(i));
      final String actualDiv = removeNarrative(actual.get(i));
      assertEquals(expected.get(i), actual.get(i), "a stored resource differs");
      assertEquals(expectedDiv == null, actualDiv == null, "a narrative is present on one side");
      if (expectedDiv != null) {
        if (!expectedDiv.equals(actualDiv)) {
          textual.add(actualDiv);
        }
        assertTrue(
            xhtml(expectedDiv).equalsDeep(xhtml(actualDiv)),
            "a narrative differs as XHTML:\n" + expectedDiv + "\n" + actualDiv);
      }
    }
    assertFalse(actual.isEmpty(), "the corpus carries no " + resourceType);
    // The corpus is written by HAPI, whose serialisation of XHTML the XML route reproduces, so
    // every narrative here is equal as text too. Another writer's XML is held to L2 alone.
    assertEquals(List.of(), textual, "narratives equal as XHTML but not as text");
  }

  /**
   * Returns the stored rows as JSON, each decimal by its value, in a stable order that does not
   * depend on the narrative.
   */
  @Nonnull
  private static List<JsonNode> rows(
      @Nonnull final Dataset<Row> stored, @Nonnull final DefinitionCanonicalStructure structure) {
    final Column row =
        functions.to_json(
            functions.struct(Stream.of(stored.columns()).map(stored::col).toArray(Column[]::new)));
    return stored.select(row).as(Encoders.STRING()).collectAsList().stream()
        .map(SemanticJson::parse)
        .peek(parsed -> byValue(parsed, structure))
        .sorted(Comparator.comparing(XmlJsonParityTest::withoutNarrative))
        .toList();
  }

  /**
   * Replaces every decimal in a stored row with the plain text of its value as a double. XML stores
   * a decimal's text and JSON the text of a double, so the two are compared by value, at the
   * precision JSON keeps (owner, 2026-10-22).
   */
  private static void byValue(
      @Nonnull final JsonNode node, @Nonnull final DefinitionCanonicalStructure structure) {
    if (!(node instanceof final ObjectNode object)) {
      return;
    }
    for (final String name : names(object)) {
      final java.util.Optional<LayoutEntry> entry =
          structure.entry(name).filter(LayoutEntry::isElement);
      if (entry.isEmpty()) {
        continue;
      }
      final JsonNode value = object.get(name);
      if (entry.get().primitiveType().filter(FHIRDefinedType.DECIMAL::equals).isPresent()) {
        object.set(name, decimal(value));
      } else if (entry.get().primitiveType().isEmpty()) {
        final DefinitionCanonicalStructure child =
            structure.elementStructure(entry.get()).orElseThrow();
        if (value.isArray()) {
          value.forEach(item -> byValue(item, child));
        } else {
          byValue(value, child);
        }
      }
    }
  }

  @Nonnull
  private static JsonNode decimal(@Nonnull final JsonNode value) {
    if (value.isArray()) {
      final com.fasterxml.jackson.databind.node.ArrayNode array =
          com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.arrayNode();
      value.forEach(item -> array.add(decimal(item)));
      return array;
    }
    return value.isTextual()
        ? TextNode.valueOf(
            BigDecimal.valueOf(Double.parseDouble(value.asText()))
                .stripTrailingZeros()
                .toPlainString())
        : value;
  }

  @Nonnull
  private static List<String> names(@Nonnull final ObjectNode object) {
    final List<String> names = new ArrayList<>();
    object.fieldNames().forEachRemaining(names::add);
    return names;
  }

  @Nonnull
  private static String withoutNarrative(@Nonnull final JsonNode row) {
    final JsonNode copy = row.deepCopy();
    removeNarrative(copy);
    return copy.toString();
  }

  /** Removes a row's narrative, returning it, or null where it has none. */
  private static String removeNarrative(@Nonnull final JsonNode row) {
    final JsonNode text = row.get("text");
    if (text instanceof final ObjectNode object && object.has("div")) {
      return object.remove("div").asText();
    }
    return null;
  }

  @Nonnull
  private static XhtmlNode xhtml(@Nonnull final String div) {
    try {
      return new XhtmlParser().parse(div, "div").getChildNodes().get(0);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nonnull
  static Stream<String> specificationTypes() {
    return SpecExampleRoundTripTest.resourceTypes();
  }

  @Nonnull
  static Stream<String> syntheaTypes() {
    return entryResources(SemanticJson.parse(text("/data/bundles/synthea.json"))).stream()
        .map(resource -> resource.get("resourceType").asText())
        .distinct()
        .sorted();
  }

  @Nonnull
  private static List<JsonNode> entryResources(@Nonnull final JsonNode bundle) {
    return StreamSupport.stream(bundle.get("entry").spliterator(), false)
        .map(entry -> entry.get("resource"))
        .filter(Objects::nonNull)
        .toList();
  }

  @Nonnull
  private static Dataset<String> dataset(@Nonnull final List<String> documents) {
    return TransformFixtures.spark().createDataset(new ArrayList<>(documents), Encoders.STRING());
  }

  @Nonnull
  private static List<String> lines(@Nonnull final java.nio.file.Path corpus) {
    try {
      return Files.readAllLines(corpus).stream().filter(line -> !line.isBlank()).toList();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nonnull
  private static String text(@Nonnull final String resource) {
    try (var stream =
        Objects.requireNonNull(XmlJsonParityTest.class.getResourceAsStream(resource))) {
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
