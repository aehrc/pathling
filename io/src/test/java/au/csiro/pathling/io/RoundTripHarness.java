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

import au.csiro.pathling.io.egress.ResourceSerialiser;
import au.csiro.pathling.io.transform.NonConformantContent;
import au.csiro.pathling.io.transform.ResourceTransformer;
import au.csiro.pathling.io.transform.TransformFixtures;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.SchemaConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Drives a corpus of FHIR JSON through the layout and back, and asserts that what comes out is
 * semantically equal to what went in (US2, FR-016).
 *
 * <p>It drives {@link ResourceTransformer} and {@link ResourceSerialiser} directly. The public API
 * is not rewired until M4, and a round trip that went through it would be testing the wiring rather
 * than the layout.
 *
 * <p>Pointing it at another corpus is one call: {@link #assertRoundTrip(String, Path)} takes the
 * resource type and a file of newline-delimited JSON, which is the ingest path on which the lexical
 * form of a number survives.
 *
 * <p>Resources are paired by their identifier rather than by position, so that nothing here depends
 * on Spark returning rows in the order the file wrote them.
 *
 * <p>One exclusion is offered, and it is the carve-out FR-017 states: primitive element ids and
 * extensions are not written until M5. It is asserted rather than assumed — the count of excluded
 * keys is returned, so a test says what it expects that count to be, and an output carrying such a
 * key at all fails. The exclusion cascades into the containers it empties, because the layout omits
 * those rather than writing an empty structure; nothing else is excluded, and a corpus carrying
 * content the definitions do not describe fails here, which is the point. A corpus carrying content
 * the layout deliberately does not store — {@code contained} resources under FR-006 — also fails
 * here, and the test rather than the harness is where that is accounted for.
 */
public final class RoundTripHarness {

  /** The element FR-006 says is never represented. */
  @Nonnull private static final String CONTAINED = "contained";

  @Nonnull private final SchemaConfiguration configuration;

  private final boolean excludePrimitiveMetadata;

  private final boolean excludeContainedResources;

  private RoundTripHarness(
      @Nonnull final SchemaConfiguration configuration,
      final boolean excludePrimitiveMetadata,
      final boolean excludeContainedResources) {
    this.configuration = configuration;
    this.excludePrimitiveMetadata = excludePrimitiveMetadata;
    this.excludeContainedResources = excludeContainedResources;
  }

  /**
   * Returns a harness admitting no exclusion at all, which is what FR-016 asks for of a source that
   * carries no primitive id or extension content.
   *
   * @return the harness
   */
  @Nonnull
  public static RoundTripHarness unconditional() {
    return new RoundTripHarness(SchemaConfiguration.builder().build(), false, false);
  }

  /**
   * Returns a harness excluding the primitive id and extension content that is not written until
   * M5, under FR-017's carve-out.
   *
   * @return the harness
   */
  @Nonnull
  public static RoundTripHarness excludingPrimitiveMetadata() {
    return new RoundTripHarness(SchemaConfiguration.builder().build(), true, false);
  }

  /**
   * Returns a harness in a configuration of its own, which is how the dense mode is exercised.
   *
   * @param schemaConfiguration the configuration
   * @return the harness
   */
  @Nonnull
  public RoundTripHarness withConfiguration(
      @Nonnull final SchemaConfiguration schemaConfiguration) {
    return new RoundTripHarness(
        schemaConfiguration, excludePrimitiveMetadata, excludeContainedResources);
  }

  /**
   * Returns a harness excluding {@code contained} resources, which FR-006 says are never
   * represented and whose presence is instead reported as a finding.
   *
   * <p>This is not a carve-out that a later milestone removes. A contained resource is never
   * stored, so a resource carrying one can never round-trip whole, and a corpus carrying one has to
   * say so to assert anything about the rest of it. What makes the exclusion honest is that the
   * same content is reported by {@link #findings}, which a caller asserts as an exact set — so a
   * corpus that stopped carrying contained resources would change that assertion rather than
   * quietly passing here.
   *
   * @return the harness
   */
  @Nonnull
  public RoundTripHarness excludingContainedResources() {
    return new RoundTripHarness(configuration, excludePrimitiveMetadata, true);
  }

  /**
   * Round-trips every resource in a file of newline-delimited FHIR JSON, asserting that each comes
   * back semantically equal to the source.
   *
   * @param resourceType the type of the resources the file carries
   * @param corpus the file
   * @return the number of primitive id and extension keys the exclusion removed, which is zero
   *     where the source carried none
   */
  public int assertRoundTrip(@Nonnull final String resourceType, @Nonnull final Path corpus) {
    final Map<String, JsonNode> expected = byIdentifier(read(corpus), "source");
    final Map<String, JsonNode> actual = byIdentifier(roundTrip(resourceType, corpus), "output");
    assertEquals(expected.keySet(), actual.keySet(), "the round trip returned different resources");

    if (excludeContainedResources) {
      expected.values().forEach(RoundTripHarness::removeContainedResources);
    }
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

    for (final Map.Entry<String, JsonNode> entry : expected.entrySet()) {
      SemanticJson.difference(
              resourceType + "[" + entry.getKey() + "]",
              entry.getValue(),
              actual.get(entry.getKey()))
          .ifPresent(difference -> fail("The round trip was not lossless at " + difference));
    }
    return excluded;
  }

  /**
   * Returns the content of a corpus that this layout does not store, as values.
   *
   * @param resourceType the type of the resources the file carries
   * @param corpus the file
   * @return the findings
   */
  @Nonnull
  public List<NonConformantContent> findings(
      @Nonnull final String resourceType, @Nonnull final Path corpus) {
    final ResourceTransformer transformer = TransformFixtures.transformer(configuration);
    return transformer.findings(
        resourceType, transformer.source(TransformFixtures.spark(), corpus.toString()).schema());
  }

  /** Runs the corpus through the layout and back, returning the documents that came out. */
  @Nonnull
  private List<String> roundTrip(@Nonnull final String resourceType, @Nonnull final Path corpus) {
    final ResourceTransformer transformer = TransformFixtures.transformer(configuration);
    final Dataset<Row> stored =
        transformer.read(TransformFixtures.spark(), resourceType, corpus.toString());
    return ResourceSerialiser.of(TransformFixtures.DEFINITIONS)
        .serialise(resourceType, stored)
        .collectAsList();
  }

  /** Reads a file of newline-delimited JSON, ignoring the blank lines a corpus may end with. */
  @Nonnull
  private static List<String> read(@Nonnull final Path corpus) {
    try (var lines = Files.lines(corpus)) {
      return lines.filter(line -> !line.isBlank()).collect(Collectors.toList());
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
   * <p>The removal cascades, and it has to. An element whose entire content is primitive metadata
   * is left with nothing once the metadata is excluded, and FR-019 says the layout omits a
   * structure whose every field is null rather than writing an empty one. Keeping the emptied
   * container on this side would compare an empty object against a correctly absent one and call
   * the layout lossy. A container is dropped only where this exclusion is what emptied it, so a
   * container that was already empty in the source is still a difference. T078c deletes all of this
   * along with the carve-out it serves.
   */
  private static int exclude(@Nonnull final JsonNode node) {
    int removed = 0;
    if (node.isObject()) {
      final ObjectNode object = (ObjectNode) node;
      for (final String name : metadataGroupNames(object)) {
        object.remove(name);
        removed++;
      }
      final List<String> emptied = new ArrayList<>();
      for (final String name : names(object)) {
        final int fromChild = exclude(object.get(name));
        removed += fromChild;
        if (fromChild > 0 && isEmptyContainer(object.get(name))) {
          emptied.add(name);
        }
      }
      emptied.forEach(object::remove);
    } else if (node.isArray()) {
      final ArrayNode array = (ArrayNode) node;
      for (int i = array.size() - 1; i >= 0; i--) {
        final int fromChild = exclude(array.get(i));
        removed += fromChild;
        if (fromChild > 0 && isEmptyContainer(array.get(i))) {
          array.remove(i);
        }
      }
    }
    return removed;
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

  /** Whether a node is a structure or an array that now holds nothing. */
  private static boolean isEmptyContainer(@Nonnull final JsonNode node) {
    return (node.isObject() || node.isArray()) && node.isEmpty();
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
