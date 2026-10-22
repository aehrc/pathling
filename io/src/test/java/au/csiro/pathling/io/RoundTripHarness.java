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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.LayoutFields;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Drives a corpus of FHIR JSON through the layout and back, and asserts that what comes out is
 * semantically equal to what went in (US2, FR-016).
 *
 * <p>It drives {@link FhirJsonReader} and {@link FhirJsonWriter} directly. The public API is not
 * rewired until M4, and a round trip that went through it would be testing the wiring rather than
 * the layout.
 *
 * <p>Pointing it at another corpus is one call: {@link #assertRoundTrip(String, Path)} takes the
 * resource type and a file of newline-delimited JSON.
 *
 * <p>Resources are paired by their identifier rather than by position, so that nothing here depends
 * on Spark returning rows in the order the file wrote them.
 *
 * <p>The harness carries FR-016's exception list, and strips each exception from the expected side
 * by a rule of its own, taken from the source JSON alone, rather than asking the code under test
 * what it did not store. {@link #assertRoundTrip} returns how often the counted exceptions applied,
 * so a test says what it expects each count to be. Two apply to every round trip, because nothing a
 * caller chooses avoids them — decimals compared numerically, and base64Binary whitespace. The rest
 * are opted into: primitive element ids and extensions until ticket 17, {@code contained} resources
 * under FR-006, and an inline resource held by an element declared as the abstract {@code
 * Resource}. Without the corresponding option, a corpus carrying any of these fails here, which is
 * the point. An exclusion of primitive metadata does not cascade into the containers it empties,
 * because the layout keeps them and writes them as empty objects (decisions 71 and 72).
 *
 * <p>{@code Bundle} is not the harness's concern, because it is never a resource type a corpus is
 * round-tripped as (FR-007).
 */
public final class RoundTripHarness {

  /** The element FR-006 says is never represented. */
  @Nonnull private static final String CONTAINED = "contained";

  /** Where a resource carries its narrative, as a JSON pointer. */
  @Nonnull private static final String NARRATIVE = "/text/div";

  private final boolean excludePrimitiveMetadata;

  private final boolean excludeContainedResources;

  private final boolean excludeInlineResources;

  private final boolean collapseNarrativeWhitespace;

  /** Where the stored dataset is written as Parquet and read back from, if anywhere. */
  @Nullable private final Path persistence;

  private RoundTripHarness(
      final boolean excludePrimitiveMetadata,
      final boolean excludeContainedResources,
      final boolean excludeInlineResources,
      final boolean collapseNarrativeWhitespace,
      @Nullable final Path persistence) {
    this.excludePrimitiveMetadata = excludePrimitiveMetadata;
    this.excludeContainedResources = excludeContainedResources;
    this.excludeInlineResources = excludeInlineResources;
    this.collapseNarrativeWhitespace = collapseNarrativeWhitespace;
    this.persistence = persistence;
  }

  /**
   * Returns a harness admitting no exclusion at all, which is what FR-016 asks for of a source that
   * carries no primitive id or extension content.
   *
   * @return the harness
   */
  @Nonnull
  public static RoundTripHarness unconditional() {
    return new RoundTripHarness(false, false, false, false, null);
  }

  /**
   * Returns a harness excluding the primitive id and extension content that is not written until
   * ticket 17 stores the metadata group, one of FR-016's exceptions.
   *
   * @return the harness
   */
  @Nonnull
  public static RoundTripHarness excludingPrimitiveMetadata() {
    return new RoundTripHarness(true, false, false, false, null);
  }

  /**
   * Returns a harness excluding {@code contained} resources, which FR-006 says are never
   * represented, and are dropped silently.
   *
   * <p>This is not a carve-out that a later milestone removes. A contained resource is never
   * stored, so a resource carrying one can never round-trip whole, and a corpus carrying one has to
   * say so to assert anything about the rest of it.
   *
   * @return the harness
   */
  @Nonnull
  public RoundTripHarness excludingContainedResources() {
    return new RoundTripHarness(
        excludePrimitiveMetadata,
        true,
        excludeInlineResources,
        collapseNarrativeWhitespace,
        persistence);
  }

  /**
   * Returns a harness excluding an inline resource held by an element the definitions declare as
   * the abstract {@code Resource}, such as {@code Parameters.parameter.resource}, which the layout
   * does not store (FR-016).
   *
   * <p>Such an element is recognised from the source alone: it is an object, other than the
   * resource itself and a {@code contained} resource, that carries a {@code resourceType}. Each one
   * removed is counted.
   *
   * @return the harness
   */
  @Nonnull
  public RoundTripHarness excludingInlineResources() {
    return new RoundTripHarness(
        excludePrimitiveMetadata,
        excludeContainedResources,
        true,
        collapseNarrativeWhitespace,
        persistence);
  }

  /**
   * Returns a harness that compares a resource's narrative with the runs of whitespace in it
   * collapsed, which is what HAPI's XML writer does to a narrative (decision 83).
   *
   * <p>This is an artefact of how a test writes its XML input, not of XML ingest, which keeps a
   * narrative's whitespace as the XML wrote it: HAPI's XML writer replaces the whitespace between
   * the narrative's elements with a single space. A test that writes XML from JSON with HAPI
   * therefore feeds XML ingest a narrative that differs from the JSON as written, though not as
   * rendered. The exception applies only where the two narratives differ and are equal once
   * collapsed, and each such narrative is counted, so a narrative that differs in any other way
   * still fails the round trip.
   *
   * @return the harness
   */
  @Nonnull
  public RoundTripHarness collapsingNarrativeWhitespace() {
    return new RoundTripHarness(
        excludePrimitiveMetadata,
        excludeContainedResources,
        excludeInlineResources,
        true,
        persistence);
  }

  /**
   * Returns a harness that writes the stored dataset to Parquet and reads it back before returning
   * it to JSON.
   *
   * <p>Without this the round trip never leaves memory, and what it proves is the transform and the
   * serialiser rather than the layout: a type or an order that Parquet does not preserve would pass
   * it. The schema read back is also asserted to be the schema written, less nullability, which
   * Parquet does not carry.
   *
   * @param directory the directory to write under, one subdirectory per resource type
   * @return the harness
   */
  @Nonnull
  public RoundTripHarness persistingTo(@Nonnull final Path directory) {
    return new RoundTripHarness(
        excludePrimitiveMetadata,
        excludeContainedResources,
        excludeInlineResources,
        collapseNarrativeWhitespace,
        directory);
  }

  /**
   * Round-trips every resource in a file of newline-delimited FHIR JSON, asserting that each comes
   * back semantically equal to the source subject to FR-016's exceptions.
   *
   * <p>Two of the exceptions apply to every round trip, because nothing the caller can choose
   * avoids them: a decimal comes back numerically equal rather than textually identical, and a
   * base64Binary value comes back without the whitespace FHIR permits inside it. Both are counted.
   *
   * @param resourceType the type of the resources the file carries
   * @param corpus the file
   * @return how often each exception applied
   */
  @Nonnull
  public RoundTripOutcome assertRoundTrip(
      @Nonnull final String resourceType, @Nonnull final Path corpus) {
    return assertRoundTrip(
        resourceType, corpus, TransformFixtures.reader().read(resourceType, corpus.toString()));
  }

  /**
   * Round-trips resources that reached the layout by a route other than a file of newline-delimited
   * JSON, such as a bundle or XML, asserting that each comes back semantically equal to the JSON it
   * is expected to be.
   *
   * <p>The route is the caller's: it hands over what it stored, and a file holding the JSON each of
   * those resources should come back as. Everything else is as {@link #assertRoundTrip(String,
   * Path)} does it, including the exceptions and their counts, which are taken from that file.
   *
   * @param resourceType the type of the resources
   * @param expected a file of newline-delimited JSON holding the resources as they should come back
   * @param transformed the resources as the route stored them
   * @return how often each exception applied
   */
  @Nonnull
  public RoundTripOutcome assertRoundTrip(
      @Nonnull final String resourceType,
      @Nonnull final Path expected,
      @Nonnull final Dataset<Row> transformed) {
    return compare(resourceType, expected, roundTrip(resourceType, transformed));
  }

  @Nonnull
  private RoundTripOutcome compare(
      @Nonnull final String resourceType,
      @Nonnull final Path corpus,
      @Nonnull final List<String> documents) {
    final Map<String, JsonNode> expected = byIdentifier(read(corpus), "source");
    final Map<String, JsonNode> actual = byIdentifier(documents, "output");
    assertEquals(expected.keySet(), actual.keySet(), "the round trip returned different resources");

    if (excludeContainedResources) {
      expected.values().forEach(RoundTripHarness::removeContainedResources);
    }
    final int inline =
        excludeInlineResources
            ? expected.values().stream().mapToInt(RoundTripHarness::removeInlineResources).sum()
            : 0;
    final int excluded = expected.values().stream().mapToInt(RoundTripHarness::exclude).sum();
    if (excludePrimitiveMetadata) {
      assertNoMetadataGroups(actual);
    } else {
      assertEquals(
          0,
          excluded,
          "the source carries primitive id or extension content, which an unconditional round trip"
              + " does not admit");
    }
    final DefinitionCanonicalStructure canonical =
        DefinitionCanonicalStructure.forResource(TransformFixtures.DEFINITIONS, resourceType);
    final int whitespace =
        expected.values().stream()
            .mapToInt(resource -> removeBase64Whitespace(canonical, resource))
            .sum();

    final int narrative =
        collapseNarrativeWhitespace
            ? expected.entrySet().stream()
                .mapToInt(entry -> collapseNarrative(entry.getValue(), actual.get(entry.getKey())))
                .sum()
            : 0;

    int numericOnly = 0;
    for (final Map.Entry<String, JsonNode> entry : expected.entrySet()) {
      final JsonNode output = actual.get(entry.getKey());
      SemanticJson.difference(resourceType + "[" + entry.getKey() + "]", entry.getValue(), output)
          .ifPresent(difference -> fail("The round trip was not lossless at " + difference));
      numericOnly += SemanticJson.numericOnlyMatches(entry.getValue(), output);
    }
    return new RoundTripOutcome(excluded, inline, whitespace, numericOnly, narrative);
  }

  /** Runs stored resources out of the layout, returning the documents that came out. */
  @Nonnull
  private List<String> roundTrip(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> transformed) {
    final Dataset<Row> stored =
        persistence == null ? transformed : persisted(resourceType, transformed, persistence);
    return TransformFixtures.writer().write(resourceType, stored).collectAsList();
  }

  /** Writes a stored dataset to Parquet and returns what reading it back yields. */
  @Nonnull
  private static Dataset<Row> persisted(
      @Nonnull final String resourceType,
      @Nonnull final Dataset<Row> stored,
      @Nonnull final Path directory) {
    final String location = directory.resolve(resourceType).toString();
    stored.write().mode(SaveMode.Overwrite).parquet(location);
    final Dataset<Row> persisted = TransformFixtures.spark().read().parquet(location);
    // The catalog form carries names, order and types, and not nullability, which a Parquet read
    // reports as nullable throughout.
    assertEquals(
        stored.schema().catalogString(),
        persisted.schema().catalogString(),
        "the schema read back from Parquet is not the schema written");
    return persisted;
  }

  /** Reads a file of newline-delimited JSON, ignoring the blank lines a corpus may end with. */
  @Nonnull
  private static List<String> read(@Nonnull final Path corpus) {
    try (var lines = Files.lines(corpus)) {
      return lines.filter(line -> !line.isBlank()).toList();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Parses documents and keys them by the identifier of the resource, which pairs the two sides.
   */
  @Nonnull
  private static Map<String, JsonNode> byIdentifier(
      @Nonnull final List<String> documents, @Nonnull final String side) {
    final Map<String, JsonNode> resources = new LinkedHashMap<>();
    for (final String document : documents) {
      final JsonNode resource = SemanticJson.parse(document);
      final JsonNode identifier = resource.get("id");
      assertTrue(
          identifier != null && identifier.isTextual(),
          "every resource on the " + side + " side needs an identifier to be paired by");
      assertNull(
          resources.put(identifier.asText(), resource),
          "the " + side + " side carries the identifier " + identifier.asText() + " twice");
    }
    return resources;
  }

  /**
   * Removes the primitive id and extension keys from a tree, returning how many were removed. The
   * count is what makes the exclusion explicit: a test states whether it expects the source to have
   * carried any, so an exclusion that never applied cannot be mistaken for one that did.
   *
   * <p>The removal does not cascade. An element whose entire content is primitive metadata is left
   * as an empty object here, and the layout keeps it too, because it stores the primitive as a null
   * of its declared type (decision 72), and writes it as an empty object. T078c deletes this along
   * with the carve-out it serves.
   */
  private static int exclude(@Nonnull final JsonNode node) {
    int removed = 0;
    if (node.isObject()) {
      final ObjectNode object = (ObjectNode) node;
      for (final String name : metadataGroupNames(object)) {
        object.remove(name);
        removed++;
      }
      for (final String name : names(object)) {
        removed += exclude(object.get(name));
      }
    } else if (node.isArray()) {
      for (final JsonNode item : node) {
        removed += exclude(item);
      }
    }
    return removed;
  }

  /**
   * Removes the whitespace from every base64Binary value in a tree, returning the number of values
   * changed. The definitions say which values are base64Binary, because nothing in the document
   * does.
   */
  private static int removeBase64Whitespace(
      @Nonnull final DefinitionCanonicalStructure node, @Nonnull final JsonNode value) {
    if (value.isArray()) {
      int changed = 0;
      final ArrayNode array = (ArrayNode) value;
      for (int i = 0; i < array.size(); i++) {
        final JsonNode item = array.get(i);
        if (item.isTextual()) {
          final String stripped = item.asText().replaceAll("\\s", "");
          if (!stripped.equals(item.asText())) {
            array.set(i, TextNode.valueOf(stripped));
            changed++;
          }
        }
      }
      return changed;
    }
    if (!value.isObject()) {
      return 0;
    }
    final ObjectNode object = (ObjectNode) value;
    int changed = 0;
    for (final String name : names(object)) {
      final Optional<LayoutEntry> entry = node.entry(name).filter(LayoutEntry::isElement);
      if (entry.isEmpty()) {
        continue;
      }
      final JsonNode child = object.get(name);
      if (isBase64Binary(entry.orElseThrow())) {
        if (child.isTextual()) {
          final String stripped = child.asText().replaceAll("\\s", "");
          if (!stripped.equals(child.asText())) {
            object.set(name, TextNode.valueOf(stripped));
            changed++;
          }
        } else {
          changed += removeBase64Whitespace(node, child);
        }
      } else {
        final Optional<DefinitionCanonicalStructure> structure =
            node.elementStructure(entry.orElseThrow());
        if (structure.isPresent()) {
          if (child.isArray()) {
            for (final JsonNode item : child) {
              changed += removeBase64Whitespace(structure.orElseThrow(), item);
            }
          } else {
            changed += removeBase64Whitespace(structure.orElseThrow(), child);
          }
        }
      }
    }
    return changed;
  }

  private static boolean isBase64Binary(@Nonnull final LayoutEntry entry) {
    return entry
        .getElement()
        .flatMap(ElementDefinition::getFhirType)
        .filter(FHIRDefinedType.BASE64BINARY::equals)
        .isPresent();
  }

  /**
   * Removes every {@code contained} key from a tree, which is the content FR-006 says this layout
   * never represents.
   */
  private static void removeContainedResources(@Nonnull final JsonNode node) {
    if (node.isObject()) {
      final ObjectNode object = (ObjectNode) node;
      object.remove(CONTAINED);
      names(object).forEach(name -> removeContainedResources(object.get(name)));
    } else if (node.isArray()) {
      node.forEach(RoundTripHarness::removeContainedResources);
    }
  }

  /**
   * Removes every inline resource beneath a resource, other than a contained one, returning how
   * many were removed. The resource itself carries a {@code resourceType} too, so only what is
   * beneath it is examined.
   *
   * <p>The removal does not cascade into the element that held the resource. In a corpus that is
   * not a {@code Bundle}, that element is a {@code Parameters} parameter or part, whose {@code
   * name} is required, so it is never left empty.
   */
  private static int removeInlineResources(@Nonnull final JsonNode resource) {
    int removed = 0;
    for (final String name : names((ObjectNode) resource)) {
      if (!CONTAINED.equals(name)) {
        removed += removeInlineResourcesBeneath(resource.get(name));
      }
    }
    return removed;
  }

  private static int removeInlineResourcesBeneath(@Nonnull final JsonNode node) {
    int removed = 0;
    if (node.isObject()) {
      final ObjectNode object = (ObjectNode) node;
      for (final String name : names(object)) {
        final JsonNode child = object.get(name);
        if (isResource(child)) {
          object.remove(name);
          removed++;
        } else {
          removed += removeInlineResourcesBeneath(child);
        }
      }
    } else if (node.isArray()) {
      for (final JsonNode item : node) {
        removed += removeInlineResourcesBeneath(item);
      }
    }
    return removed;
  }

  private static boolean isResource(@Nonnull final JsonNode node) {
    return node.isObject() && node.has("resourceType");
  }

  /**
   * Takes the output's narrative as the expected one where the two differ only in the runs of
   * whitespace beside a tag, returning 1 where it did and 0 otherwise. A narrative that differs in
   * any other way is left for the comparison to report.
   */
  private static int collapseNarrative(
      @Nonnull final JsonNode expected, @Nullable final JsonNode output) {
    final JsonNode expectedDiv = expected.at(NARRATIVE);
    final JsonNode outputDiv = output == null ? expectedDiv : output.at(NARRATIVE);
    if (!expectedDiv.isTextual()
        || !outputDiv.isTextual()
        || expectedDiv.asText().equals(outputDiv.asText())
        || !collapsed(expectedDiv.asText()).equals(collapsed(outputDiv.asText()))) {
      return 0;
    }
    ((ObjectNode) expected.get("text")).set("div", outputDiv);
    return 1;
  }

  @Nonnull
  private static String collapsed(@Nonnull final String text) {
    // Only whitespace beside a tag is collapsed, so that a narrative that lost whitespace within
    // its text is still reported.
    final StringBuilder collapsed = new StringBuilder(text.length());
    int start = 0;
    while (start < text.length()) {
      if (!Character.isWhitespace(text.charAt(start))) {
        collapsed.append(text.charAt(start++));
        continue;
      }
      int end = start;
      while (end < text.length() && Character.isWhitespace(text.charAt(end))) {
        end++;
      }
      final boolean besideTag =
          (start > 0 && text.charAt(start - 1) == '>')
              || (end < text.length() && text.charAt(end) == '<');
      collapsed.append(besideTag ? " " : text.substring(start, end));
      start = end;
    }
    return collapsed.toString();
  }

  @Nonnull
  private static List<String> names(@Nonnull final ObjectNode object) {
    final List<String> names = new ArrayList<>();
    object.fieldNames().forEachRemaining(names::add);
    return names;
  }

  /** Asserts that no metadata group reached the output, which nothing writes one to before M5. */
  private static void assertNoMetadataGroups(@Nonnull final Map<String, JsonNode> actual) {
    for (final Map.Entry<String, JsonNode> entry : actual.entrySet()) {
      final JsonNode copy = entry.getValue().deepCopy();
      assertEquals(
          0,
          exclude(copy),
          "the output carries primitive id or extension content, which is excluded until M5: "
              + entry.getKey());
    }
  }

  @Nonnull
  private static List<String> metadataGroupNames(@Nonnull final ObjectNode object) {
    return names(object).stream()
        .filter(name -> name.startsWith(LayoutFields.METADATA_GROUP_PREFIX))
        .toList();
  }
}
