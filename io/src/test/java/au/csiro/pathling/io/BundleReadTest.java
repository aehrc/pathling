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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
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
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Bundle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that a bundle is a transport carrier and never a resource type (FR-007): it is exploded
 * into the resources it carries, one type at a time, and the references between its entries are
 * resolved as decision 83 states (FR-062), in both formats and without HAPI (ticket 32).
 *
 * <p>The reference cases are those of the previous encoder's {@code ResourceParserTest}, over the
 * same bundle. What is asserted is what is stored, through the public readers. Each case is written
 * in JSON and run on both formats, the XML being written from the JSON by HAPI in the test, which
 * keeps each resource's own id.
 */
class BundleReadTest {

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

  /** The formats a bundle is read in, by the media type that names each. */
  @Nonnull
  static Stream<String> formats() {
    return Stream.of(FhirMediaType.JSON, FhirMediaType.XML);
  }

  @ParameterizedTest
  @MethodSource("formats")
  void refusesToReturnBundlesAsAResourceType(@Nonnull final String format) {
    final Dataset<String> bundles = bundles(format, references());
    final FhirFormatReader reader = reader(format);

    assertThrows(IllegalArgumentException.class, () -> reader.readBundles("Bundle", bundles));
    assertThrows(IllegalArgumentException.class, () -> reader.readBundles("NotAResource", bundles));
  }

  @ParameterizedTest
  @MethodSource("formats")
  void explodesEachTypeToItsOwnTable(@Nonnull final String format) {
    final Dataset<String> bundles = bundles(format, references());

    final Map<String, List<String>> idsByType =
        Stream.of("Patient", "Condition", "Claim")
            .collect(
                Collectors.toMap(
                    Function.identity(), type -> ids(reader(format).readBundles(type, bundles))));

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

  @ParameterizedTest
  @MethodSource("formats")
  void storesNothingForATypeTheBundleDoesNotCarry(@Nonnull final String format) {
    assertTrue(
        reader(format)
            .readBundles("Observation", bundles(format, references()))
            .collectAsList()
            .isEmpty());
  }

  @ParameterizedTest
  @MethodSource("formats")
  void explodesEveryBundleInTheDataset(@Nonnull final String format) {
    final String second =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":[{\"resource\":"
            + "{\"resourceType\":\"Patient\",\"id\":\"second\"}}]}";

    assertEquals(
        List.of(PATIENT_ID, "second"),
        ids(reader(format).readBundles("Patient", bundles(format, references(), second))));
  }

  @ParameterizedTest
  @MethodSource("formats")
  void skipsEntriesThatCarryNoResource(@Nonnull final String format) {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"batch\",\"entry\":["
            + "{\"request\":{\"method\":\"DELETE\",\"url\":\"Patient/234\"}},"
            + "{\"resource\":{\"resourceType\":\"Patient\",\"id\":\"kept\"},"
            + "\"request\":{\"method\":\"PUT\",\"url\":\"Patient/kept\"}}]}";

    assertEquals(
        List.of("kept"), ids(reader(format).readBundles("Patient", bundles(format, bundle))));
  }

  /**
   * A bundle carried inside a bundle is not exploded in turn, as the previous encoder did not
   * explode it, and since a bundle is never a resource type it is not returned either.
   */
  @ParameterizedTest
  @MethodSource("formats")
  void neitherReturnsNorExplodesANestedBundle(@Nonnull final String format) {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
            + "{\"resource\":{\"resourceType\":\"Patient\",\"id\":\"outer\"}},"
            + "{\"resource\":{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
            + "{\"resource\":{\"resourceType\":\"Patient\",\"id\":\"inner\"}}]}}]}";

    assertEquals(
        List.of("outer"), ids(reader(format).readBundles("Patient", bundles(format, bundle))));
  }

  @ParameterizedTest
  @MethodSource("formats")
  void failsOnADocumentThatIsNotABundle(@Nonnull final String format) {
    final String patient = "{\"resourceType\":\"Patient\",\"id\":\"not-a-bundle\"}";
    final Dataset<String> documents =
        dataset(
            format.equals(FhirMediaType.XML)
                ? xmlParser().encodeResourceToString(jsonParser().parseResource(patient))
                : patient);

    final Exception failure =
        assertThrows(
            Exception.class,
            () -> reader(format).readBundles("Patient", documents).collectAsList());

    assertTrue(
        XmlNarrativeTest.messages(failure).contains("Expected a bundle"),
        XmlNarrativeTest.messages(failure));
  }

  @Test
  void failsOnJsonBundleTextThatIsNotJson() {
    final Exception failure =
        assertThrows(
            Exception.class,
            () ->
                reader(FhirMediaType.JSON)
                    .readBundles("Patient", dataset("{\"resourceType\":\"Bundle\",\"entry\":["))
                    .collectAsList());

    assertTrue(
        XmlNarrativeTest.messages(failure).contains("Cannot read the FHIR JSON bundle starting"),
        XmlNarrativeTest.messages(failure));
  }

  /**
   * A string in a JSON bundle longer than the JSON parser allows by default, such as a large
   * attachment's data, does not fail the bundle when it is not text that resolution needs.
   */
  @Test
  void readsAJsonBundleCarryingAStringLongerThanTheParserAllows() {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
            + "{\"resource\":{\"resourceType\":\"Binary\",\"id\":\"large\","
            + "\"contentType\":\"application/octet-stream\",\"data\":\""
            + "A".repeat(20_000_004)
            + "\"}},"
            + "{\"resource\":{\"resourceType\":\"Patient\",\"id\":\"kept\"}}]}";

    assertEquals(
        List.of("kept"), ids(reader(FhirMediaType.JSON).readBundles("Patient", dataset(bundle))));
  }

  @ParameterizedTest
  @MethodSource("formats")
  void keepsTheIdentifierEachResourceCarries(@Nonnull final String format) {
    assertEquals(
        List.of("2383c155-6345-e842-b7d7-000000000000", "2383c155-6345-e842-b7d7-3f6748ca634b"),
        ids(reader(format).readBundles("Condition", bundles(format, references()))));
  }

  @ParameterizedTest
  @MethodSource("formats")
  void resolvesAReferenceToAnEntryOfTheBundle(@Nonnull final String format) {
    final Row condition = withId(format, "Condition", "2383c155-6345-e842-b7d7-3f6748ca634b");
    final Row claim = withId(format, "Claim", "47a08521-15ad-de17-f731-9b79065ddd72");

    // A reference at the root of the resource.
    assertEquals(PATIENT_REF, reference(condition, "subject"));
    assertEquals(PATIENT_REF, reference(claim, "patient"));
    // A reference within a backbone element.
    final Row diagnosis = claim.<Row>getList(claim.fieldIndex("diagnosis")).get(0);
    assertEquals(CONDITION_REF, reference(diagnosis, "diagnosisReference"));
    // A reference within an extension.
    final Row extension = claim.<Row>getList(claim.fieldIndex("extension")).get(0);
    assertEquals(PATIENT_REF, reference(extension, "valueReference"));
  }

  @ParameterizedTest
  @MethodSource("formats")
  void keepsAReferenceToAnEntryTheBundleDoesNotCarry(@Nonnull final String format) {
    assertEquals(
        ENCOUNTER_URN,
        reference(
            withId(format, "Condition", "2383c155-6345-e842-b7d7-3f6748ca634b"), "encounter"));
  }

  @ParameterizedTest
  @MethodSource("formats")
  void keepsReferencesThatAreNotUrns(@Nonnull final String format) {
    final Row condition = withId(format, "Condition", "2383c155-6345-e842-b7d7-000000000000");

    assertEquals(REFERENCE_RELATIVE, reference(condition, "subject"));
    assertEquals(REFERENCE_ABSOLUTE, reference(condition, "encounter"));
    assertEquals(
        REFERENCE_CONDITIONAL,
        reference(withId(format, "Claim", "47a08521-15ad-de17-f731-9b79065ddd72"), "provider"));
  }

  /**
   * A reference to an entry resolves to the entry's relative reference with the version its
   * metadata names, and a versioned reference in the source is kept as written (decision 83,
   * FR-062).
   */
  @ParameterizedTest
  @MethodSource("formats")
  void resolvesAReferenceToAVersionedEntryWithItsVersion(@Nonnull final String format) {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
            + "{\"fullUrl\":\"urn:uuid:p\",\"resource\":{\"resourceType\":\"Patient\",\"id\":\"p1\","
            + "\"meta\":{\"versionId\":\"3\"}}},"
            + "{\"fullUrl\":\"urn:uuid:o\",\"resource\":{\"resourceType\":\"Observation\","
            + "\"id\":\"o1\",\"status\":\"final\",\"code\":{\"text\":\"x\"},"
            + "\"subject\":{\"reference\":\"urn:uuid:p\"},"
            + "\"focus\":[{\"reference\":\"Patient/p1/_history/2\"}]}}]}";

    final Row observation = single(format, "Observation", bundle);

    assertEquals("Patient/p1/_history/3", reference(observation, "subject"));
    assertEquals(
        "Patient/p1/_history/2",
        observation
            .<Row>getList(observation.fieldIndex("focus"))
            .get(0)
            .<String>getAs("reference"));
  }

  /** A reference to an entry that has no identifier has nothing to be resolved to. */
  @ParameterizedTest
  @MethodSource("formats")
  void keepsAReferenceToAnEntryWithoutAnIdentifier(@Nonnull final String format) {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"transaction\",\"entry\":["
            + "{\"fullUrl\":\"urn:uuid:anonymous\",\"resource\":{\"resourceType\":\"Patient\","
            + "\"active\":true}},"
            + "{\"fullUrl\":\"urn:uuid:o\",\"resource\":{\"resourceType\":\"Observation\","
            + "\"id\":\"o1\",\"status\":\"final\",\"code\":{\"text\":\"x\"},"
            + "\"subject\":{\"reference\":\"urn:uuid:anonymous\"}}}]}";

    assertEquals("urn:uuid:anonymous", reference(single(format, "Observation", bundle), "subject"));
  }

  /**
   * A reference to an entry whose identifier carries only an extension has no value to be resolved
   * to, so it is kept.
   */
  @ParameterizedTest
  @MethodSource("formats")
  void keepsAReferenceToAnEntryWhoseIdentifierCarriesOnlyAnExtension(@Nonnull final String format) {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"transaction\",\"entry\":["
            + "{\"fullUrl\":\"urn:uuid:anonymous\",\"resource\":{\"resourceType\":\"Patient\","
            + "\"_id\":{\"extension\":[{\"url\":\"http://example.com/id\","
            + "\"valueString\":\"x\"}]},\"active\":true}},"
            + "{\"fullUrl\":\"urn:uuid:o\",\"resource\":{\"resourceType\":\"Observation\","
            + "\"id\":\"o1\",\"status\":\"final\",\"code\":{\"text\":\"x\"},"
            + "\"subject\":{\"reference\":\"urn:uuid:anonymous\"}}}]}";

    assertEquals("urn:uuid:anonymous", reference(single(format, "Observation", bundle), "subject"));
  }

  /**
   * An entry whose full URL is {@code urn:uuid:X} and whose resource's id is {@code X} keeps its
   * id, and a reference to it becomes {@code Type/X}. The HAPI route lost both (xml-native.md, "A
   * defect found in the current route").
   */
  @ParameterizedTest
  @MethodSource("formats")
  void keepsAnIdThatIsAlsoTheEntrysUuid(@Nonnull final String format) {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"transaction\",\"entry\":["
            + "{\"fullUrl\":\"urn:uuid:abc\",\"resource\":{\"resourceType\":\"Patient\","
            + "\"id\":\"abc\"}},"
            + "{\"fullUrl\":\"urn:uuid:def\",\"resource\":{\"resourceType\":\"Observation\","
            + "\"id\":\"def\",\"status\":\"final\",\"code\":{\"text\":\"x\"},"
            + "\"subject\":{\"reference\":\"urn:uuid:abc\"}}}]}";
    // HAPI's XML writer would write this bundle correctly, but HAPI's parser is what loses the id,
    // so the XML is written by hand rather than through a parse.
    final Dataset<String> bundles =
        dataset(
            format.equals(FhirMediaType.XML)
                ? "<Bundle xmlns=\"http://hl7.org/fhir\"><type value=\"transaction\"/>"
                    + "<entry><fullUrl value=\"urn:uuid:abc\"/><resource><Patient>"
                    + "<id value=\"abc\"/></Patient></resource></entry>"
                    + "<entry><fullUrl value=\"urn:uuid:def\"/><resource><Observation>"
                    + "<id value=\"def\"/><status value=\"final\"/><code><text value=\"x\"/></code>"
                    + "<subject><reference value=\"urn:uuid:abc\"/></subject>"
                    + "</Observation></resource></entry></Bundle>"
                : bundle);

    assertEquals(List.of("abc"), ids(reader(format).readBundles("Patient", bundles)));
    final Row observation = reader(format).readBundles("Observation", bundles).first();
    assertEquals("def", observation.<String>getAs("id"));
    assertEquals("Patient/abc", reference(observation, "subject"));
  }

  /** A null row is left out rather than failing the whole job. */
  @ParameterizedTest
  @MethodSource("formats")
  void skipsANullBundle(@Nonnull final String format) {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":[{\"resource\":"
            + "{\"resourceType\":\"Patient\",\"id\":\"kept\"}}]}";
    final Dataset<String> bundles =
        TransformFixtures.spark()
            .createDataset(Arrays.asList(null, bundles(format, bundle).first()), Encoders.STRING());

    assertEquals(List.of("kept"), ids(reader(format).readBundles("Patient", bundles)));
  }

  /**
   * Only a reference is resolved. An element named {@code reference} whose type is a URI, such as
   * {@code DetectedIssue.reference}, keeps its value even where it names an entry of the bundle.
   */
  @ParameterizedTest
  @MethodSource("formats")
  void leavesAUriNamedLikeAReferenceAlone(@Nonnull final String format) {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
            + "{\"fullUrl\":\"urn:uuid:p\",\"resource\":{\"resourceType\":\"Patient\","
            + "\"id\":\"p1\"}},"
            + "{\"fullUrl\":\"urn:uuid:d\",\"resource\":{\"resourceType\":\"DetectedIssue\","
            + "\"id\":\"d1\",\"status\":\"final\",\"reference\":\"urn:uuid:p\","
            + "\"patient\":{\"reference\":\"urn:uuid:p\"}}}]}";

    final Row issue = single(format, "DetectedIssue", bundle);

    assertEquals("urn:uuid:p", issue.<String>getAs("reference"));
    assertEquals("Patient/p1", reference(issue, "patient"));
  }

  /**
   * The resources of one type are read as any other documents of that type, so what is stored is
   * what the same resources store read one per row, with their references resolved. The bundle's
   * other types, which declare fields such as {@code status} differently, change nothing.
   */
  @ParameterizedTest
  @MethodSource("formats")
  void storesWhatTheSameResourcesStoreReadOnePerRow(@Nonnull final String format) {
    final JsonNode bundle = SemanticJson.parse(references());
    final Map<String, String> targets = new java.util.HashMap<>();
    final List<String> conditions = new java.util.ArrayList<>();
    for (final JsonNode entry : bundle.get("entry")) {
      final JsonNode resource = entry.get("resource");
      final String type = resource.get("resourceType").asText();
      targets.put(entry.get("fullUrl").asText(), type + "/" + resource.get("id").asText());
      if ("Condition".equals(type)) {
        conditions.add(resource.toString());
      }
    }
    // The references this bundle's conditions hold to its entries, resolved as the route resolves
    // them; the text of these conditions holds each URN only as a reference.
    final String[] resolved =
        conditions.stream()
            .map(
                condition ->
                    targets.entrySet().stream()
                        .reduce(
                            condition,
                            (text, target) ->
                                text.replace(
                                    "\"" + target.getKey() + "\"", "\"" + target.getValue() + "\""),
                            (first, second) -> first))
            .toArray(String[]::new);

    final Dataset<Row> fromBundle =
        reader(format).readBundles("Condition", bundles(format, references()));
    final Dataset<Row> fromLines =
        TransformFixtures.fhirReader().json().read("Condition", dataset(resolved));

    assertEquals(fromLines.schema(), fromBundle.schema());
    assertEquals(json(fromLines), json(fromBundle));
  }

  /** A bundle in the FHIR namespace under a prefix is valid FHIR, and is read. */
  @Test
  void readsAnXmlBundleWithAPrefixedFhirNamespace() {
    final String bundle =
        "<f:Bundle xmlns:f=\"http://hl7.org/fhir\"><f:type"
            + " value=\"collection\"/><f:entry><f:fullUrl"
            + " value=\"urn:uuid:1\"/><f:resource><f:Patient><f:id value=\"x1\"/><f:text><f:status"
            + " value=\"generated\"/><h:div xmlns:h=\"http://www.w3.org/1999/xhtml\">"
            + "<h:p>pre</h:p></h:div></f:text></f:Patient></f:resource></f:entry><f:entry><f:resource><f:Observation><f:id"
            + " value=\"o1\"/><f:status value=\"final\"/><f:code><f:text"
            + " value=\"t\"/></f:code><f:subject><f:reference value=\"urn:uuid:1\"/>"
            + "</f:subject></f:Observation></f:resource></f:entry></f:Bundle>";

    final Row observation =
        reader(FhirMediaType.XML).readBundles("Observation", dataset(bundle)).first();
    final Row patient = reader(FhirMediaType.XML).readBundles("Patient", dataset(bundle)).first();

    assertEquals("Patient/x1", reference(observation, "subject"));
    assertEquals(
        "<div xmlns=\"http://www.w3.org/1999/xhtml\"><p>pre</p></div>",
        patient.<Row>getAs("text").<String>getAs("div"));
  }

  /**
   * A bundle in no namespace, or with text or a second root after its root, is malformed, and fails
   * the read naming it.
   */
  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "<Bundle><id value=\"b9\"/><type value=\"collection\"/></Bundle>",
        "<Bundle xmlns=\"http://hl7.org/fhir\"><id value=\"b9\"/><type value=\"collection\"/>"
            + "</Bundle>trailing",
        "<Bundle xmlns=\"http://hl7.org/fhir\"><id value=\"b9\"/><type value=\"collection\"/>"
            + "</Bundle><Bundle xmlns=\"http://hl7.org/fhir\"/>"
      })
  void failsOnAMalformedXmlBundleNamingIt(@Nonnull final String bundle) {
    final Exception failure =
        assertThrows(
            Exception.class,
            () ->
                reader(FhirMediaType.XML).readBundles("Patient", dataset(bundle)).collectAsList());

    assertTrue(
        XmlNarrativeTest.messages(failure).contains("Bundle with id \"b9\""),
        XmlNarrativeTest.messages(failure));
  }

  /** An XML bundle yields the same stored resources as the same bundle written as JSON. */
  @Test
  void storesTheSameFromAnXmlBundleAsFromTheJson() {
    for (final String type : List.of("Patient", "Condition", "Claim")) {
      final Dataset<Row> fromJson =
          reader(FhirMediaType.JSON).readBundles(type, bundles(FhirMediaType.JSON, references()));
      final Dataset<Row> fromXml =
          reader(FhirMediaType.XML).readBundles(type, bundles(FhirMediaType.XML, references()));

      assertEquals(
          fromJson.schema(), fromXml.schema(), "the stored schemas of " + type + " differ");
      assertEquals(json(fromJson), json(fromXml), "the stored " + type + " resources differ");
    }
  }

  @Nonnull
  private static Row withId(
      @Nonnull final String format, @Nonnull final String type, @Nonnull final String id) {
    return reader(format).readBundles(type, bundles(format, references())).collectAsList().stream()
        .filter(row -> id.equals(row.getAs("id")))
        .findFirst()
        .orElseThrow();
  }

  @Nonnull
  private static Row single(
      @Nonnull final String format, @Nonnull final String type, @Nonnull final String bundle) {
    final List<Row> rows =
        reader(format).readBundles(type, bundles(format, bundle)).collectAsList();
    assertEquals(1, rows.size());
    return rows.get(0);
  }

  @Nullable
  private static String reference(@Nonnull final Row row, @Nonnull final String field) {
    return row.<Row>getAs(field).getAs("reference");
  }

  @Nonnull
  private static FhirFormatReader reader(@Nonnull final String format) {
    return TransformFixtures.fhirReader().format(format);
  }

  /** The bundles in a format, the XML written from the JSON by HAPI keeping each resource's id. */
  @Nonnull
  private static Dataset<String> bundles(
      @Nonnull final String format, @Nonnull final String... bundles) {
    return dataset(
        format.equals(FhirMediaType.XML)
            ? Stream.of(bundles)
                .map(
                    bundle ->
                        xmlParser()
                            .encodeResourceToString(
                                jsonParser().parseResource(Bundle.class, bundle)))
                .toArray(String[]::new)
            : bundles);
  }

  @Nonnull
  private static IParser jsonParser() {
    final IParser parser = FhirContext.forR4Cached().newJsonParser();
    parser.setOverrideResourceIdWithBundleEntryFullUrl(false);
    return parser;
  }

  @Nonnull
  private static IParser xmlParser() {
    final IParser parser = FhirContext.forR4Cached().newXmlParser();
    parser.setStripVersionsFromReferences(false);
    return parser;
  }

  @Nonnull
  private static List<String> ids(@Nonnull final Dataset<Row> resources) {
    return resources.collectAsList().stream().map(row -> row.<String>getAs("id")).sorted().toList();
  }

  @Nonnull
  private static List<String> json(@Nonnull final Dataset<Row> stored) {
    final Column row =
        functions.to_json(
            functions.struct(Stream.of(stored.columns()).map(stored::col).toArray(Column[]::new)));
    return stored.select(row).as(Encoders.STRING()).collectAsList().stream()
        .sorted(Comparator.naturalOrder())
        .toList();
  }

  /** The bundle the previous encoder's reference resolution was tested over. */
  @Nonnull
  static String references() {
    try (var stream =
        Objects.requireNonNull(
            BundleReadTest.class.getResourceAsStream("/data/bundles/references.json"))) {
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nonnull
  private static Dataset<String> dataset(@Nonnull final String... documents) {
    return TransformFixtures.spark().createDataset(List.of(documents), Encoders.STRING());
  }
}
