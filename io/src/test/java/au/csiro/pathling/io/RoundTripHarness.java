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
import au.csiro.pathling.io.json.FhirJsonReader;
import au.csiro.pathling.io.json.FhirJsonWriter;
import au.csiro.pathling.io.transform.NonConformantContent;
import au.csiro.pathling.io.transform.ResourceTransformer;
import au.csiro.pathling.io.transform.TransformFixtures;
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
 * <p>The harness carries FR-016's exception list, and each exception is asserted rather than
 * assumed: {@link #assertRoundTrip} returns how often each applied, so a test says what it expects
 * each count to be. Two apply to every round trip, because nothing a caller chooses avoids them —
 * decimals compared numerically, and base64Binary whitespace. The rest are opted into: primitive
 * element ids and extensions under FR-017's carve-out until M5, {@code contained} resources under
 * FR-006, and the content the layout ignores rather than stores. Without the corresponding option,
 * a corpus carrying any of these fails here, which is the point. An exclusion of primitive metadata
 * does not cascade into the containers it empties, because the layout keeps them and writes them as
 * empty objects (decisions 71 and 72). The exclusion of ignored content does, because an element
 * with nothing left to store has no column in the layout and is absent from the output.
 *
 * <p>{@code Bundle} is not the harness's concern, because it is never a resource type a corpus is
 * round-tripped as (FR-007).
 */
public final class RoundTripHarness {

  /** The element FR-006 says is never represented. */
  @Nonnull private static final String CONTAINED = "contained";

  private final boolean excludePrimitiveMetadata;

  private final boolean excludeContainedResources;

  private final boolean excludeIgnoredContent;

  /** Where the stored dataset is written as Parquet and read back from, if anywhere. */
  @Nullable private final Path persistence;

  private RoundTripHarness(
      final boolean excludePrimitiveMetadata,
      final boolean excludeContainedResources,
      final boolean excludeIgnoredContent,
      @Nullable final Path persistence) {
    this.excludePrimitiveMetadata = excludePrimitiveMetadata;
    this.excludeContainedResources = excludeContainedResources;
    this.excludeIgnoredContent = excludeIgnoredContent;
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
    return new RoundTripHarness(false, false, false, null);
  }

  /**
   * Returns a harness excluding the primitive id and extension content that is not written until
   * M5, under FR-017's carve-out.
   *
   * @return the harness
   */
  @Nonnull
  public static RoundTripHarness excludingPrimitiveMetadata() {
    return new RoundTripHarness(true, false, false, null);
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
    return new RoundTripHarness(excludePrimitiveMetadata, true, excludeIgnoredContent, persistence);
  }

  /**
   * Returns a harness excluding the content the layout ignores rather than stores: content the
   * definitions do not describe, and elements whose shape or JSON encoding contradicts them.
   *
   * <p>What is excluded is exactly what the strictness check reports, removed from the source at
   * the paths it names, so the exclusion cannot widen beyond what is detected. Every value of a
   * reported element is removed, not only the offending one, because inference types a column from
   * every value in a file and the layout drops the element for all of them. A structure or an array
   * item emptied by the removal is removed too, which holds where the removal takes the element's
   * only content in the file and the layout therefore has no column for it. Where a sibling row
   * keeps the column, the emptied occurrence comes back as an empty object instead, which is the
   * input decision 71 puts outside the layout and `PrunedSchemaGuaranteeTest` asserts directly.
   *
   * @return the harness
   */
  @Nonnull
  public RoundTripHarness excludingIgnoredContent() {
    return new RoundTripHarness(
        excludePrimitiveMetadata, excludeContainedResources, true, persistence);
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
        excludePrimitiveMetadata, excludeContainedResources, excludeIgnoredContent, directory);
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
    final Map<String, JsonNode> expected = byIdentifier(read(corpus), "source");
    final Map<String, JsonNode> actual = byIdentifier(roundTrip(resourceType, corpus), "output");
    assertEquals(expected.keySet(), actual.keySet(), "the round trip returned different resources");

    if (excludeContainedResources) {
      expected.values().forEach(RoundTripHarness::removeContainedResources);
    }
    final int ignored = excludeIgnoredContent ? excludeIgnored(resourceType, corpus, expected) : 0;
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

    int numericOnly = 0;
    for (final Map.Entry<String, JsonNode> entry : expected.entrySet()) {
      final JsonNode output = actual.get(entry.getKey());
      SemanticJson.difference(resourceType + "[" + entry.getKey() + "]", entry.getValue(), output)
          .ifPresent(difference -> fail("The round trip was not lossless at " + difference));
      numericOnly += SemanticJson.numericOnlyMatches(entry.getValue(), output);
    }
    return new RoundTripOutcome(excluded, ignored, whitespace, numericOnly);
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
    final ResourceTransformer transformer = TransformFixtures.transformer();
    return transformer.findings(
        resourceType, TransformFixtures.inferred(corpus.toString()).schema());
  }

  /** Runs the corpus through the layout and back, returning the documents that came out. */
  @Nonnull
  private List<String> roundTrip(@Nonnull final String resourceType, @Nonnull final Path corpus) {
    final Dataset<Row> transformed =
        TransformFixtures.reader().read(resourceType, corpus.toString());
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
   * Removes from the source the content the strictness check reports as ignored, returning the
   * number of values removed.
   */
  private int excludeIgnored(
      @Nonnull final String resourceType,
      @Nonnull final Path corpus,
      @Nonnull final Map<String, JsonNode> expected) {
    final List<List<String>> paths =
        findings(resourceType, corpus).stream()
            .filter(
                finding ->
                    finding.isUndescribedContent()
                        || finding.isShapeMismatch()
                        || finding.isEncodingMismatch())
            .map(finding -> List.of(finding.getPath().split("\\.")))
            // The first segment names the resource type rather than a key of the document.
            .map(path -> path.subList(1, path.size()))
            .toList();
    int removed = 0;
    for (final JsonNode resource : expected.values()) {
      for (final List<String> path : paths) {
        removed += removePath(resource, path);
      }
    }
    return removed;
  }

  /**
   * Removes the values at a dotted path from a tree, descending into every item of an array on the
   * way, and removes the containers the removal empties. Returns the number of values removed.
   */
  private static int removePath(@Nonnull final JsonNode node, @Nonnull final List<String> path) {
    if (node.isArray()) {
      int removed = 0;
      final ArrayNode array = (ArrayNode) node;
      for (int i = array.size() - 1; i >= 0; i--) {
        final int fromItem = removePath(array.get(i), path);
        removed += fromItem;
        if (fromItem > 0 && isEmptyContainer(array.get(i))) {
          array.remove(i);
        }
      }
      return removed;
    }
    if (!node.isObject() || !node.has(path.get(0))) {
      return 0;
    }
    final ObjectNode object = (ObjectNode) node;
    final String name = path.get(0);
    if (path.size() == 1) {
      final JsonNode value = object.remove(name);
      return value.isArray() ? Math.max(1, value.size()) : 1;
    }
    final int removed = removePath(object.get(name), path.subList(1, path.size()));
    if (removed > 0 && isEmptyContainer(object.get(name))) {
      object.remove(name);
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
