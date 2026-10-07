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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Runs the M1 round trip harness over what the remaining ingest formats store: the resources a
 * bundle is exploded into, and resources read from XML (T069a).
 *
 * <p>T076 excludes bundles permanently, because a bundle is never stored (FR-007), and its corpus
 * is newline-delimited JSON, so without this nothing round-trips what M3 adds.
 *
 * <p>The expected side is derived independently of the code under test. For a bundle it is each
 * entry's resource exactly as the bundle's text carries it, with each reference to another entry
 * rewritten to the relative reference that entry's type and identifier make, which is the
 * resolution decision 83 records. The corpora carry no versioned entry, so the version that
 * decision adds does not arise here; {@code BundleTransformTest} covers it. For XML it is the JSON
 * the XML was generated from, so the comparison is between what JSON ingest would have stored and
 * what XML ingest did.
 *
 * <p>XML input is written by HAPI from the JSON rather than vendored. A file of XML written
 * independently of its JSON twin does not carry the same narrative, because whitespace inside XHTML
 * is content and pretty-printing the XML adds it, so it could not be compared with the JSON at all.
 *
 * <p>Every route admits the exclusions the JSON route does: primitive metadata until M5 and {@code
 * contained} resources permanently, since the Synthea bundle's claims carry them. What makes the
 * second honest is that the finding FR-006 requires is asserted to survive the route. The XML
 * routes also admit, and count, the whitespace HAPI's XML writer collapses in a narrative while the
 * test writes its XML input; HAPI's XML parser, which is what XML ingest uses, keeps it (decision
 * 83).
 */
class IngestFormatRoundTripTest {

  @Nonnull private static final String SYNTHEA_JSON = "/data/bundles/synthea.json";

  @Nonnull private static final String REFERENCES_JSON = "/data/bundles/references.json";

  /**
   * The element of the specification's examples that HAPI's JSON parser drops, by the type that
   * carries it: {@code ActivityDefinition.timingTiming}, whose only content is {@code _event}.
   */
  @Nonnull
  private static final Map<String, String> UNWRITABLE =
      Map.of("ActivityDefinition", "timingTiming");

  /** The prefix of a reference that names another entry of the same bundle. */
  @Nonnull private static final String URN = "urn:";

  /** A run of whitespace beside a tag of a narrative, at the start or the end of a text node. */
  @Nonnull private static final Pattern BESIDE_A_TAG = Pattern.compile(">\\s+|\\s+<");

  @ParameterizedTest
  @MethodSource("syntheaTypes")
  void roundTripsTheResourcesAJsonBundleIsExplodedInto(
      @Nonnull final String resourceType, @TempDir @Nonnull final Path directory) {
    final RoundTripOutcome outcome =
        roundTripBundle(
            resourceType,
            text(SYNTHEA_JSON),
            text(SYNTHEA_JSON),
            TransformFixtures.fhirReader().json(),
            harness(directory),
            directory);

    assertEquals(0, outcome.getPrimitiveMetadata(), "the corpus carries no primitive metadata");
  }

  @ParameterizedTest
  @MethodSource("syntheaTypes")
  void roundTripsTheResourcesAnXmlBundleIsExplodedInto(
      @Nonnull final String resourceType, @TempDir @Nonnull final Path directory) {
    final RoundTripOutcome outcome =
        roundTripBundle(
            resourceType,
            xml(text(SYNTHEA_JSON)),
            text(SYNTHEA_JSON),
            TransformFixtures.fhirReader().xml(),
            harness(directory).collapsingNarrativeWhitespace(),
            directory);

    assertEquals(0, outcome.getPrimitiveMetadata(), "the corpus carries no primitive metadata");
    assertEquals(
        expectedResources(text(SYNTHEA_JSON), resourceType).stream()
            .filter(IngestFormatRoundTripTest::hasCollapsibleNarrative)
            .count(),
        outcome.getNarrativeWhitespace(),
        "the narratives collapsed are not those whose whitespace collapsing changes");
  }

  /**
   * Asserts that a contained resource carried by an exploded resource still reaches the transform,
   * so that its presence is reported as FR-006 requires rather than lost to the parser. This is
   * what the round trips' exclusion of contained resources rests on.
   */
  @Test
  void detectsTheContainedResourcesABundleCarries() {
    final String type = "ExplanationOfBenefit";
    for (final Dataset<String> resources :
        List.of(
            BundleTransformer.json().resources(type, dataset(List.of(text(SYNTHEA_JSON)))),
            BundleTransformer.xml().resources(type, dataset(List.of(xml(text(SYNTHEA_JSON))))))) {
      final List<NonConformantContent> findings =
          TransformFixtures.transformer()
              .findings(type, TransformFixtures.spark().read().json(resources).schema());

      assertEquals(
          Set.of(type + ".contained"),
          findings.stream().map(NonConformantContent::getPath).collect(Collectors.toSet()));
      assertTrue(findings.stream().allMatch(NonConformantContent::isContainedResource));
    }
  }

  /**
   * Round-trips the bundle the previous encoder's reference resolution was tested over, which
   * carries references in extensions and in the extension of a primitive.
   */
  @ParameterizedTest
  @MethodSource("referenceTypes")
  void roundTripsTheResourcesOfABundleWithReferencesBetweenEntries(
      @Nonnull final String resourceType, @TempDir @Nonnull final Path directory) {
    final RoundTripOutcome outcome =
        roundTripBundle(
            resourceType,
            text(REFERENCES_JSON),
            text(REFERENCES_JSON),
            TransformFixtures.fhirReader().json(),
            harness(directory),
            directory);

    // Only the claim carries an extension on a primitive, and that is excluded until M5.
    assertEquals(
        "Claim".equals(resourceType) ? 1 : 0,
        outcome.getPrimitiveMetadata(),
        "the primitive metadata excluded is not what the corpus carries");
  }

  /**
   * Round-trips the structural selection of the specification's examples, written as XML from the
   * JSON the specification publishes, and compares what comes back with that JSON.
   *
   * <p>One element of the selection cannot be written as XML, because HAPI's JSON parser drops it
   * before the XML is written: a repeating primitive carried only as its metadata, with no value
   * array beside it. It is removed from the expected side, and the removal is asserted to have
   * applied exactly once, so that it is visible rather than incidental. The same loss applies to a
   * bundle written as JSON, which HAPI parses too, and decision 83 records it.
   */
  @ParameterizedTest
  @MethodSource("specificationTypes")
  void roundTripsResourcesReadFromXml(
      @Nonnull final String resourceType, @TempDir @Nonnull final Path directory) {
    final Path corpus = SpecExampleRoundTripTest.corpus(resourceType);
    final List<String> source = lines(corpus);
    final List<String> documents = source.stream().map(IngestFormatRoundTripTest::xml).toList();
    final Path expected = directory.resolve("expected.ndjson");
    write(expected, withoutUnwritable(resourceType, source));

    final Dataset<Row> transformed =
        TransformFixtures.fhirReader().xml().read(resourceType, dataset(documents));

    final RoundTripOutcome outcome =
        harness(directory)
            .collapsingNarrativeWhitespace()
            .assertRoundTrip(resourceType, expected, transformed);

    assertEquals(
        source.stream().filter(IngestFormatRoundTripTest::hasCollapsibleNarrative).count(),
        outcome.getNarrativeWhitespace(),
        "the narratives collapsed are not those whose whitespace collapsing changes");
  }

  /**
   * Whether a resource's narrative carries whitespace beside a tag that HAPI's XML writer would
   * collapse to one space. The writer keeps a run of whitespace within a text node, so that does
   * not count.
   */
  private static boolean hasCollapsibleNarrative(@Nonnull final String resource) {
    final JsonNode div = SemanticJson.parse(resource).at("/text/div");
    return div.isTextual()
        && BESIDE_A_TAG
            .matcher(div.asText())
            .results()
            .anyMatch(match -> !match.group().matches(">? <?"));
  }

  /**
   * Removes the element HAPI cannot read from the JSON of the type that carries it, asserting that
   * exactly one resource carried it.
   */
  @Nonnull
  private static List<String> withoutUnwritable(
      @Nonnull final String resourceType, @Nonnull final List<String> source) {
    if (!UNWRITABLE.containsKey(resourceType)) {
      return source;
    }
    final String element = UNWRITABLE.get(resourceType);
    final List<JsonNode> resources = source.stream().map(SemanticJson::parse).toList();
    final long removed =
        resources.stream()
            .filter(resource -> ((ObjectNode) resource).remove(element) != null)
            .count();
    assertEquals(1, removed, "the element HAPI cannot read is not where it was recorded");
    return resources.stream().map(JsonNode::toString).toList();
  }

  @Nonnull
  private static RoundTripOutcome roundTripBundle(
      @Nonnull final String resourceType,
      @Nonnull final String bundle,
      @Nonnull final String expectedFrom,
      @Nonnull final FhirFormatReader reader,
      @Nonnull final RoundTripHarness harness,
      @Nonnull final Path directory) {
    final Path expected = directory.resolve("expected.ndjson");
    final List<String> resources = expectedResources(expectedFrom, resourceType);
    assertFalse(resources.isEmpty(), "the corpus carries no " + resourceType);
    write(expected, resources);

    final Dataset<Row> transformed = reader.readBundles(resourceType, dataset(List.of(bundle)));

    return harness.assertRoundTrip(resourceType, expected, transformed);
  }

  /** The exclusions every route admits, persisting what is stored beneath a directory. */
  @Nonnull
  private static RoundTripHarness harness(@Nonnull final Path directory) {
    return RoundTripHarness.excludingPrimitiveMetadata()
        .excludingContainedResources()
        .persistingTo(directory.resolve("stored"));
  }

  /**
   * Writes a FHIR JSON document as XML with HAPI, keeping the identifier of each resource in a
   * bundle rather than taking one from its entry's full URL, and the version of each reference.
   */
  @Nonnull
  private static String xml(@Nonnull final String json) {
    final IParser reader = FhirContext.forR4Cached().newJsonParser();
    reader.setOverrideResourceIdWithBundleEntryFullUrl(false);
    final IParser writer = FhirContext.forR4Cached().newXmlParser();
    writer.setStripVersionsFromReferences(false);
    return writer.encodeResourceToString(reader.parseResource(json));
  }

  /**
   * The resources of one type that a bundle carries, as its text carries them, with each reference
   * to another entry rewritten to the relative reference of that entry.
   */
  @Nonnull
  private static List<String> expectedResources(
      @Nonnull final String bundle, @Nonnull final String resourceType) {
    final JsonNode parsed = SemanticJson.parse(bundle);
    final List<JsonNode> resources = entryResources(parsed);
    final Map<String, String> targets = new HashMap<>();
    for (final JsonNode entry : parsed.get("entry")) {
      final JsonNode resource = entry.get("resource");
      if (entry.has("fullUrl")
          && entry.get("fullUrl").asText().startsWith(URN)
          && resource != null
          && resource.has("id")) {
        targets.put(
            entry.get("fullUrl").asText(),
            resource.get("resourceType").asText() + "/" + resource.get("id").asText());
      }
    }
    final List<String> expected = new ArrayList<>();
    for (final JsonNode resource : resources) {
      if (resourceType.equals(resource.get("resourceType").asText())) {
        rewriteReferences(resource, targets);
        expected.add(resource.toString());
      }
    }
    return expected;
  }

  @Nonnull
  private static List<JsonNode> entryResources(@Nonnull final JsonNode bundle) {
    return StreamSupport.stream(bundle.get("entry").spliterator(), false)
        .map(entry -> entry.get("resource"))
        .filter(Objects::nonNull)
        .toList();
  }

  /**
   * Rewrites every {@code reference} naming another entry. Neither corpus carries an element of
   * another type under that name, which is the only case where matching by name would differ from
   * matching by type; {@code BundleTransformTest} covers that case.
   */
  private static void rewriteReferences(
      @Nonnull final JsonNode node, @Nonnull final Map<String, String> targets) {
    if (node.isObject()) {
      final ObjectNode object = (ObjectNode) node;
      final JsonNode reference = object.get("reference");
      if (reference != null && reference.isTextual() && targets.containsKey(reference.asText())) {
        object.set("reference", TextNode.valueOf(targets.get(reference.asText())));
      }
      object.elements().forEachRemaining(child -> rewriteReferences(child, targets));
    } else if (node.isArray()) {
      node.forEach(child -> rewriteReferences(child, targets));
    }
  }

  @Nonnull
  static Stream<String> syntheaTypes() {
    return types(text(SYNTHEA_JSON));
  }

  @Nonnull
  static Stream<String> referenceTypes() {
    return types(text(REFERENCES_JSON));
  }

  @Nonnull
  static Stream<String> specificationTypes() {
    return SpecExampleRoundTripTest.selection();
  }

  @Nonnull
  private static Stream<String> types(@Nonnull final String bundle) {
    return entryResources(SemanticJson.parse(bundle)).stream()
        .map(resource -> resource.get("resourceType").asText())
        .distinct()
        .sorted();
  }

  @Nonnull
  private static Dataset<String> dataset(@Nonnull final List<String> documents) {
    return TransformFixtures.spark().createDataset(documents, Encoders.STRING());
  }

  @Nonnull
  private static String text(@Nonnull final String resource) {
    try (var stream =
        Objects.requireNonNull(IngestFormatRoundTripTest.class.getResourceAsStream(resource))) {
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nonnull
  private static List<String> lines(@Nonnull final Path corpus) {
    try {
      return Files.readAllLines(corpus).stream().filter(line -> !line.isBlank()).toList();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void write(@Nonnull final Path file, @Nonnull final List<String> lines) {
    try {
      Files.write(file, lines);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
