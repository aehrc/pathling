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
 * key at all fails. Nothing else is excluded; a corpus carrying content the definitions do not
 * describe fails here, which is the point.
 */
public final class RoundTripHarness {

  @Nonnull private final SchemaConfiguration configuration;

  private final boolean excludePrimitiveMetadata;

  private RoundTripHarness(
      @Nonnull final SchemaConfiguration configuration, final boolean excludePrimitiveMetadata) {
    this.configuration = configuration;
    this.excludePrimitiveMetadata = excludePrimitiveMetadata;
  }

  /**
   * Returns a harness admitting no exclusion at all, which is what FR-016 asks for of a source that
   * carries no primitive id or extension content.
   *
   * @return the harness
   */
  @Nonnull
  public static RoundTripHarness unconditional() {
    return new RoundTripHarness(SchemaConfiguration.builder().build(), false);
  }

  /**
   * Returns a harness excluding the primitive id and extension content that is not written until
   * M5, under FR-017's carve-out.
   *
   * @return the harness
   */
  @Nonnull
  public static RoundTripHarness excludingPrimitiveMetadata() {
    return new RoundTripHarness(SchemaConfiguration.builder().build(), true);
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
    return new RoundTripHarness(schemaConfiguration, excludePrimitiveMetadata);
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
   */
  private static int exclude(@Nonnull final JsonNode node) {
    int removed = 0;
    if (node.isObject()) {
      final ObjectNode object = (ObjectNode) node;
      for (final String name : metadataGroupNames(object)) {
        object.remove(name);
        removed++;
      }
      for (final JsonNode child : object) {
        removed += exclude(child);
      }
    } else if (node.isArray()) {
      for (final JsonNode child : node) {
        removed += exclude(child);
      }
    }
    return removed;
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
    final List<String> names = new ArrayList<>();
    object
        .fieldNames()
        .forEachRemaining(
            name -> {
              if (name.startsWith(LayoutFields.METADATA_GROUP_PREFIX)) {
                names.add(name);
              }
            });
    return names;
  }
}
