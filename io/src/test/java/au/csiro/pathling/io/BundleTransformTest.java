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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Bundle;
import org.junit.jupiter.api.Test;

/**
 * Tests that a bundle is a transport carrier and never a resource type (FR-007): it is exploded
 * into the resources it carries, one type at a time, and references between its entries are
 * resolved as the previous encoder resolved them (T058, T068).
 *
 * <p>The reference cases are those of the previous encoder's {@code ResourceParserTest}, over the
 * same bundle, so that what this layout stores for a reference is what was stored before. Decision
 * 83 records the semantics. They assert the JSON each bundle is exploded into, which is internal to
 * the bundle routes; what is stored is asserted through the public readers (decision 84).
 */
class BundleTransformTest {

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  @Nonnull private static final String PATIENT_ID = "704c9750-f6e6-473b-ee83-fbd48e07fe3f";

  @Nonnull private static final String PATIENT_REF = "Patient/" + PATIENT_ID;

  @Nonnull
  private static final String CONDITION_REF = "Condition/2383c155-6345-e842-b7d7-3f6748ca634b";

  @Nonnull
  private static final String ENCOUNTER_URN = "urn:uuid:16dceeb0-620b-f259-8b1a-87dc65e5f78a";

  @Nonnull
  private static final String REFERENCE_RELATIVE = "Patient/2383c155-6345-e842-b7d7-000000000001";

  @Nonnull
  private static final String REFERENCE_ABSOLUTE =
      "http://foo.bar.com/Encounter/2383c155-6345-e842-b7d7-000000000002";

  @Nonnull
  private static final String REFERENCE_CONDITIONAL =
      "Organization?identifier=https://github.com/synthetichealth/synthea|"
          + "fa51267f-96dd-340c-ad1c-76080f4525f6";

  @Test
  void refusesToReturnBundlesAsAResourceType() {
    final Dataset<String> bundles = documents(references());

    final FhirJsonReader reader = TransformFixtures.reader();

    assertThrows(IllegalArgumentException.class, () -> reader.readBundles("Bundle", bundles));
  }

  @Test
  void refusesATypeTheDefinitionsDoNotDescribe() {
    final Dataset<String> bundles = documents(references());

    final FhirJsonReader reader = TransformFixtures.reader();

    assertThrows(IllegalArgumentException.class, () -> reader.readBundles("NotAResource", bundles));
  }

  @Test
  void explodesEachTypeToItsOwnTable() {
    final Dataset<String> bundles = documents(references());

    final Map<String, List<String>> idsByType =
        List.of("Patient", "Condition", "Claim").stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    type -> ids(TransformFixtures.reader().readBundles(type, bundles))));

    assertEquals(
        Map.of(
            "Patient",
            List.of(PATIENT_ID),
            "Condition",
            List.of("2383c155-6345-e842-b7d7-000000000000", "2383c155-6345-e842-b7d7-3f6748ca634b"),
            "Claim",
            List.of("47a08521-15ad-de17-f731-9b79065ddd72")),
        idsByType);
  }

  @Test
  void returnsNothingForATypeTheBundleDoesNotCarry() {
    assertTrue(
        BundleTransformer.json()
            .resources("Observation", documents(references()))
            .collectAsList()
            .isEmpty());
  }

  @Test
  void explodesEveryBundleInTheDataset() {
    final String second =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":[{\"resource\":"
            + "{\"resourceType\":\"Patient\",\"id\":\"second\"}}]}";

    final List<String> ids =
        ids(TransformFixtures.reader().readBundles("Patient", documents(references(), second)));

    assertEquals(List.of(PATIENT_ID, "second"), ids);
  }

  @Test
  void skipsEntriesThatCarryNoResource() {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"batch\",\"entry\":["
            + "{\"request\":{\"method\":\"DELETE\",\"url\":\"Patient/234\"}},"
            + "{\"resource\":{\"resourceType\":\"Patient\",\"id\":\"kept\"},"
            + "\"request\":{\"method\":\"PUT\",\"url\":\"Patient/kept\"}}]}";

    assertEquals(
        List.of("kept"), ids(TransformFixtures.reader().readBundles("Patient", documents(bundle))));
  }

  /**
   * A bundle carried inside a bundle is not exploded in turn, as the previous encoder did not
   * explode it, and since a bundle is never a resource type it is not returned either.
   */
  @Test
  void neitherReturnsNorExplodesANestedBundle() {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
            + "{\"resource\":{\"resourceType\":\"Patient\",\"id\":\"outer\"}},"
            + "{\"resource\":{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
            + "{\"resource\":{\"resourceType\":\"Patient\",\"id\":\"inner\"}}]}}]}";

    final List<String> patients =
        BundleTransformer.json().resources("Patient", documents(bundle)).collectAsList();

    assertEquals(List.of("outer"), patients.stream().map(json -> text(json, "id")).toList());
    assertFalse(patients.stream().anyMatch(json -> json.contains("\"Bundle\"")));
  }

  @Test
  void failsOnADocumentThatIsNotABundle() {
    final Dataset<String> documents =
        documents("{\"resourceType\":\"Patient\",\"id\":\"not-a-bundle\"}");

    assertThrows(
        Exception.class,
        () -> TransformFixtures.reader().readBundles("Patient", documents).collectAsList());
  }

  @Test
  void keepsTheIdentifierEachResourceCarries() {
    final List<String> ids =
        BundleTransformer.json()
            .resources("Condition", documents(references()))
            .collectAsList()
            .stream()
            .map(json -> text(json, "id"))
            .sorted()
            .toList();

    assertEquals(
        List.of("2383c155-6345-e842-b7d7-000000000000", "2383c155-6345-e842-b7d7-3f6748ca634b"),
        ids);
  }

  @Test
  void resolvesAReferenceToAnEntryOfTheBundle() {
    final JsonNode condition = conditionWithId("2383c155-6345-e842-b7d7-3f6748ca634b");
    final JsonNode claim = single("Claim");

    // A reference at the root of the resource.
    assertEquals(PATIENT_REF, condition.at("/subject/reference").asText());
    // A reference within a backbone element.
    assertEquals(CONDITION_REF, claim.at("/diagnosis/0/diagnosisReference/reference").asText());
    // A reference within an extension.
    assertEquals(PATIENT_REF, claim.at("/extension/0/valueReference/reference").asText());
    // A reference within an extension of a primitive.
    assertEquals(CONDITION_REF, claim.at("/_status/extension/0/valueReference/reference").asText());
  }

  @Test
  void keepsAReferenceToAnEntryTheBundleDoesNotCarry() {
    assertEquals(
        ENCOUNTER_URN,
        conditionWithId("2383c155-6345-e842-b7d7-3f6748ca634b")
            .at("/encounter/reference")
            .asText());
  }

  @Test
  void keepsReferencesThatAreNotUrns() {
    final JsonNode condition = conditionWithId("2383c155-6345-e842-b7d7-000000000000");

    assertEquals(REFERENCE_RELATIVE, condition.at("/subject/reference").asText());
    assertEquals(REFERENCE_ABSOLUTE, condition.at("/encounter/reference").asText());
    assertEquals(REFERENCE_CONDITIONAL, single("Claim").at("/provider/reference").asText());
  }

  /**
   * The previous encoder resolved a reference to the identifier of the resource as HAPI reports it,
   * which carries the version the resource's metadata names. Decision 83.
   */
  @Test
  void resolvesAReferenceToAVersionedEntryWithItsVersion() {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
            + "{\"fullUrl\":\"urn:uuid:p\",\"resource\":{\"resourceType\":\"Patient\",\"id\":\"p1\","
            + "\"meta\":{\"versionId\":\"3\"}}},"
            + "{\"fullUrl\":\"urn:uuid:o\",\"resource\":{\"resourceType\":\"Observation\","
            + "\"id\":\"o1\",\"status\":\"final\",\"code\":{\"text\":\"x\"},"
            + "\"subject\":{\"reference\":\"urn:uuid:p\"},"
            + "\"focus\":[{\"reference\":\"Patient/p1/_history/2\"}]}}]}";

    final JsonNode observation =
        parse(
            BundleTransformer.json()
                .resources("Observation", documents(bundle))
                .collectAsList()
                .get(0));

    assertEquals("Patient/p1/_history/3", observation.at("/subject/reference").asText());
    // A versioned reference in the source is kept as written.
    assertEquals("Patient/p1/_history/2", observation.at("/focus/0/reference").asText());
  }

  /**
   * A reference to an entry that has no identifier has nothing to be resolved to, so it is kept,
   * and the entry it names is not copied into the referring resource as a contained resource.
   */
  @Test
  void keepsAReferenceToAnEntryWithoutAnIdentifier() {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"transaction\",\"entry\":["
            + "{\"fullUrl\":\"urn:uuid:anonymous\",\"resource\":{\"resourceType\":\"Patient\","
            + "\"active\":true}},"
            + "{\"fullUrl\":\"urn:uuid:o\",\"resource\":{\"resourceType\":\"Observation\","
            + "\"id\":\"o1\",\"status\":\"final\",\"code\":{\"text\":\"x\"},"
            + "\"subject\":{\"reference\":\"urn:uuid:anonymous\"}}}]}";

    final JsonNode observation =
        parse(
            BundleTransformer.json()
                .resources("Observation", documents(bundle))
                .collectAsList()
                .get(0));

    assertEquals("urn:uuid:anonymous", observation.at("/subject/reference").asText());
    assertFalse(observation.has("contained"));
  }

  /**
   * A reference to an entry whose identifier carries only an extension has no value to be resolved
   * to, so it is kept rather than cleared.
   */
  @Test
  void keepsAReferenceToAnEntryWhoseIdentifierCarriesOnlyAnExtension() {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"transaction\",\"entry\":["
            + "{\"fullUrl\":\"urn:uuid:anonymous\",\"resource\":{\"resourceType\":\"Patient\","
            + "\"_id\":{\"extension\":[{\"url\":\"http://example.com/id\","
            + "\"valueString\":\"x\"}]},\"active\":true}},"
            + "{\"fullUrl\":\"urn:uuid:o\",\"resource\":{\"resourceType\":\"Observation\","
            + "\"id\":\"o1\",\"status\":\"final\",\"code\":{\"text\":\"x\"},"
            + "\"subject\":{\"reference\":\"urn:uuid:anonymous\"}}}]}";

    final JsonNode observation =
        parse(
            BundleTransformer.json()
                .resources("Observation", documents(bundle))
                .collectAsList()
                .get(0));

    assertEquals("urn:uuid:anonymous", observation.at("/subject/reference").asText());
  }

  /** A null row is left out, as a null XML document is, rather than failing the whole job. */
  @Test
  void skipsANullBundle() {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":[{\"resource\":"
            + "{\"resourceType\":\"Patient\",\"id\":\"kept\"}}]}";
    final Dataset<String> bundles =
        TransformFixtures.spark().createDataset(Arrays.asList(null, bundle), Encoders.STRING());

    assertEquals(List.of("kept"), ids(TransformFixtures.reader().readBundles("Patient", bundles)));
  }

  /**
   * Only a reference is resolved. An element named {@code reference} whose type is a URI, such as
   * {@code DetectedIssue.reference}, keeps its value even where it names an entry of the bundle.
   */
  @Test
  void leavesAUriNamedLikeAReferenceAlone() {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
            + "{\"fullUrl\":\"urn:uuid:p\",\"resource\":{\"resourceType\":\"Patient\","
            + "\"id\":\"p1\"}},"
            + "{\"fullUrl\":\"urn:uuid:d\",\"resource\":{\"resourceType\":\"DetectedIssue\","
            + "\"id\":\"d1\",\"status\":\"final\",\"reference\":\"urn:uuid:p\","
            + "\"patient\":{\"reference\":\"urn:uuid:p\"}}}]}";

    final JsonNode issue =
        parse(
            BundleTransformer.json()
                .resources("DetectedIssue", documents(bundle))
                .collectAsList()
                .get(0));

    assertEquals("urn:uuid:p", issue.at("/reference").asText());
    assertEquals("Patient/p1", issue.at("/patient/reference").asText());
  }

  @Test
  void storesTheResolvedReference() {
    final Dataset<Row> conditions =
        TransformFixtures.reader().readBundles("Condition", documents(references()));

    final Map<String, String> subjects =
        conditions.collectAsList().stream()
            .collect(
                Collectors.toMap(
                    row -> row.<String>getAs("id"),
                    row -> row.<Row>getAs("subject").<String>getAs("reference")));

    assertEquals(
        Map.of(
            "2383c155-6345-e842-b7d7-3f6748ca634b", PATIENT_REF,
            "2383c155-6345-e842-b7d7-000000000000", REFERENCE_RELATIVE),
        subjects);
  }

  /** An XML bundle yields the same resources as the same bundle written as JSON. */
  @Test
  void explodesAnXmlBundleAsItExplodesTheSameBundleInJson() {
    final String json = references();
    // The bundle is written as XML from a parse that keeps each resource's own identifier.
    final IParser reader = FhirContext.forR4Cached().newJsonParser();
    reader.setOverrideResourceIdWithBundleEntryFullUrl(false);
    final String xml =
        FhirContext.forR4Cached()
            .newXmlParser()
            .encodeResourceToString(reader.parseResource(Bundle.class, json));

    for (final String type : List.of("Patient", "Condition", "Claim")) {
      assertEquals(
          trees(BundleTransformer.json().resources(type, documents(json))),
          trees(BundleTransformer.xml().resources(type, documents(xml))),
          "the XML bundle yielded different " + type + " resources");
    }
  }

  @Nonnull
  private static JsonNode conditionWithId(@Nonnull final String id) {
    return BundleTransformer.json()
        .resources("Condition", documents(references()))
        .collectAsList()
        .stream()
        .map(BundleTransformTest::parse)
        .filter(condition -> id.equals(condition.get("id").asText()))
        .findFirst()
        .orElseThrow();
  }

  @Nonnull
  private static JsonNode single(@Nonnull final String resourceType) {
    final List<String> resources =
        BundleTransformer.json().resources(resourceType, documents(references())).collectAsList();
    assertEquals(1, resources.size());
    return parse(resources.get(0));
  }

  @Nonnull
  private static List<JsonNode> trees(@Nonnull final Dataset<String> resources) {
    return resources.collectAsList().stream()
        .map(BundleTransformTest::parse)
        .sorted(Comparator.comparing(resource -> resource.get("id").asText()))
        .toList();
  }

  @Nonnull
  private static List<String> ids(@Nonnull final Dataset<Row> resources) {
    return resources.collectAsList().stream().map(row -> row.<String>getAs("id")).sorted().toList();
  }

  @Nonnull
  private static String text(@Nonnull final String json, @Nonnull final String field) {
    return parse(json).get(field).asText();
  }

  @Nonnull
  private static JsonNode parse(@Nonnull final String json) {
    try {
      return MAPPER.readTree(json);
    } catch (final JsonProcessingException e) {
      throw new IllegalStateException("Not JSON: " + json, e);
    }
  }

  /** The bundle the previous encoder's reference resolution was tested over. */
  @Nonnull
  static String references() {
    try (var stream =
        Objects.requireNonNull(
            BundleTransformTest.class.getResourceAsStream("/data/bundles/references.json"))) {
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nonnull
  private static Dataset<String> documents(@Nonnull final String... documents) {
    return TransformFixtures.spark().createDataset(List.of(documents), Encoders.STRING());
  }
}
