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

package au.csiro.pathling.library;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.library.io.source.LayoutDetector;
import au.csiro.pathling.library.io.source.StorageLayout;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationStatus;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that the public entry points of {@link PathlingContext} that take or return resources as
 * text — {@code encode}, {@code encodeBundle} and {@code decode} — store and read the Parquet on
 * FHIR layout, through the {@code io} module, while keeping the behaviour they had over the
 * previous layout (T070, T081, FR-043).
 *
 * @author Piotr Szul
 */
class PublicApiLayoutTest {

  private static final String EXTENSION_URL = "http://example.org/extension";

  private static final FhirContext FHIR = FhirContext.forR4();

  private static SparkSession spark;
  private static PathlingContext pathling;
  private static Path temporaryDirectory;

  @BeforeAll
  static void setUp() throws IOException {
    spark = TestHelpers.spark();
    pathling = PathlingContext.create(spark);
    temporaryDirectory = Files.createTempDirectory("pathling-public-api-layout-test-");
  }

  @AfterAll
  static void tearDown() throws IOException {
    FileUtils.deleteDirectory(temporaryDirectory.toFile());
  }

  // Encoding resources.

  @Test
  void encodeStoresTheNewLayout() {
    final Dataset<Row> encoded =
        pathling.encode(documents(json(), patient(), observation()), "Patient");

    assertEquals(
        StorageLayout.PARQUET_ON_FHIR, LayoutDetector.detect(encoded.schema()).getLayout());
    assertTrue(LayoutDetector.detect(encoded.schema()).getPreviousLayoutMarkers().isEmpty());
  }

  @Test
  void encodeOfXmlStoresTheNewLayout() {
    final Dataset<Row> encoded =
        pathling.encode(
            documents(xml(), patient(), observation()), "Patient", PathlingContext.FHIR_XML);

    assertEquals(
        StorageLayout.PARQUET_ON_FHIR, LayoutDetector.detect(encoded.schema()).getLayout());
  }

  @Test
  void encodeOfJsonKeepsOnlyTheRequestedType() {
    final Dataset<Row> mixed = documents(json(), patient(), observation(), emptyBundle());

    assertEquals(List.of("p1"), ids(pathling.encode(mixed, "Patient", PathlingContext.FHIR_JSON)));
    assertEquals(
        List.of("o1"), ids(pathling.encode(mixed, "Observation", PathlingContext.FHIR_JSON)));
  }

  @Test
  void encodeOfXmlKeepsOnlyTheRequestedType() {
    final Dataset<Row> mixed = documents(xml(), patient(), observation(), emptyBundle());

    assertEquals(List.of("p1"), ids(pathling.encode(mixed, "Patient", PathlingContext.FHIR_XML)));
    assertEquals(
        List.of("o1"), ids(pathling.encode(mixed, "Observation", PathlingContext.FHIR_XML)));
  }

  @Test
  void encodeOfJsonOfAnAbsentTypeIsEmpty() {
    final Dataset<Row> mixed = documents(json(), patient(), observation(), emptyBundle());

    assertEquals(0, pathling.encode(mixed, "Encounter", PathlingContext.FHIR_JSON).count());
  }

  @Test
  void encodeOfXmlOfAnAbsentTypeIsEmpty() {
    final Dataset<Row> mixed = documents(xml(), patient(), observation(), emptyBundle());

    assertEquals(0, pathling.encode(mixed, "Encounter", PathlingContext.FHIR_XML).count());
  }

  @Test
  void encodeTakesTheNamedColumn() {
    final Dataset<Row> documents =
        documents(json(), patient())
            .withColumnRenamed("value", "resource")
            .selectExpr("1 AS n", "*");

    assertEquals(
        List.of("p1"),
        ids(pathling.encode(documents, "Patient", PathlingContext.FHIR_JSON, "resource")));
  }

  @Test
  void encodeOfTextThatIsNotJsonFails() {
    final Dataset<Row> documents =
        spark
            .createDataset(
                List.of("not json", json().encodeResourceToString(patient())), Encoders.STRING())
            .toDF();

    assertThrows(Exception.class, () -> pathling.encode(documents, "Patient").collectAsList());
  }

  @Test
  void encodeRefusesBundleAsAType() {
    final Dataset<Row> documents = documents(json(), emptyBundle());

    assertThrows(IllegalArgumentException.class, () -> pathling.encode(documents, "Bundle"));
  }

  // Encoding bundles.

  @Test
  void encodeBundleStoresTheNewLayoutFromJson() {
    final Dataset<Row> encoded =
        pathling.encodeBundle(documents(json(), bundle()), "Patient", PathlingContext.FHIR_JSON);

    assertEquals(
        StorageLayout.PARQUET_ON_FHIR, LayoutDetector.detect(encoded.schema()).getLayout());
    assertEquals(List.of("p1"), ids(encoded));
  }

  @Test
  void encodeBundleStoresTheNewLayoutFromXml() {
    final Dataset<Row> encoded =
        pathling.encodeBundle(documents(xml(), bundle()), "Observation", PathlingContext.FHIR_XML);

    assertEquals(
        StorageLayout.PARQUET_ON_FHIR, LayoutDetector.detect(encoded.schema()).getLayout());
    assertEquals(List.of("o1"), ids(encoded));
  }

  /**
   * A URN reference to a bundle entry whose resource carries a version resolves to the versioned
   * identifier, as the previous encoder resolved it (decision 83's open question, settled by
   * FR-043).
   */
  @Test
  void encodeBundleResolvesAUrnToAVersionedEntryWithItsVersion() {
    final String bundle =
        """
        {"resourceType":"Bundle","type":"collection","entry":[
         {"fullUrl":"urn:uuid:aaaa","resource":{"resourceType":"Patient","id":"p9",
          "meta":{"versionId":"2"}}},
         {"fullUrl":"urn:uuid:bbbb","resource":{"resourceType":"Condition","id":"c9",
          "subject":{"reference":"urn:uuid:aaaa"}}}]}\
        """;
    final Dataset<Row> encoded =
        pathling.encodeBundle(
            spark.createDataset(List.of(bundle), Encoders.STRING()).toDF(), "Condition");

    assertEquals(
        List.of("Patient/p9/_history/2"),
        encoded.select("subject.reference").as(Encoders.STRING()).collectAsList());
  }

  // Decoding.

  @Test
  void decodeWritesNewLayoutResourcesAsJson() {
    final Dataset<Row> stored = newLayout("Patient", patient());

    final List<String> decoded =
        pathling.decode(stored, "Patient", PathlingContext.FHIR_JSON).collectAsList();

    assertEquals(1, decoded.size());
    final Patient patient = (Patient) json().parseResource(decoded.getFirst());
    assertEquals("p1", patient.getIdPart());
    assertEquals(AdministrativeGender.MALE, patient.getGender());
    assertEquals("Smith", patient.getNameFirstRep().getFamily());
    assertEquals(
        "a value", ((StringType) patient.getExtensionByUrl(EXTENSION_URL).getValue()).getValue());
  }

  @Test
  void decodeWritesNewLayoutResourcesAsXml() {
    final Dataset<Row> stored = newLayout("Observation", observation());

    final List<String> decoded =
        pathling.decode(stored, "Observation", PathlingContext.FHIR_XML).collectAsList();

    assertEquals(1, decoded.size());
    final Observation observation = (Observation) xml().parseResource(decoded.getFirst());
    assertEquals("o1", observation.getIdPart());
    assertEquals(
        0, observation.getValueQuantity().getValue().compareTo(new java.math.BigDecimal("1.5")));
  }

  @Test
  void decodeRoundTripsWhatEncodeStores() {
    final Dataset<Row> encoded = pathling.encode(documents(json(), patient()), "Patient");

    final String decoded =
        pathling.decode(encoded, "Patient", PathlingContext.FHIR_JSON).collectAsList().getFirst();

    assertEquals(
        json().encodeResourceToString(patient()),
        json().encodeResourceToString(json().parseResource(decoded)));
  }

  // Writing NDJSON.

  @Test
  void ndjsonSinkWritesNewLayoutResources() throws IOException {
    final Path target = temporaryDirectory.resolve("ndjson");
    pathling
        .read()
        .datasets()
        .dataset("Patient", newLayout("Patient", patient()))
        .write()
        .ndjson(target.toString());

    final List<Path> files;
    try (final Stream<Path> listing = Files.list(target)) {
      files = listing.filter(file -> file.getFileName().toString().endsWith(".ndjson")).toList();
    }
    assertFalse(files.isEmpty());
    assertTrue(
        files.stream().allMatch(file -> file.getFileName().toString().startsWith("Patient.")));
    final List<String> lines =
        files.stream().flatMap(PublicApiLayoutTest::lines).filter(line -> !line.isBlank()).toList();
    assertEquals(1, lines.size());
    assertEquals(
        json().encodeResourceToString(patient()),
        json().encodeResourceToString(json().parseResource(lines.getFirst())));
  }

  // Fixtures and helpers.

  @Nonnull
  private static Patient patient() {
    final Patient patient = new Patient();
    patient.setId("p1");
    patient.setGender(AdministrativeGender.MALE);
    patient.addName(new HumanName().setFamily("Smith").addGiven("John"));
    patient.addExtension(new Extension(EXTENSION_URL, new StringType("a value")));
    return patient;
  }

  @Nonnull
  private static Observation observation() {
    final Observation observation = new Observation();
    observation.setId("o1");
    observation.setStatus(ObservationStatus.FINAL);
    observation.getCode().setText("a code");
    observation.setValue(new Quantity().setValue(new java.math.BigDecimal("1.5")).setUnit("mg"));
    return observation;
  }

  @Nonnull
  private static Bundle emptyBundle() {
    final Bundle bundle = new Bundle();
    bundle.setId("b1");
    bundle.setType(BundleType.COLLECTION);
    return bundle;
  }

  @Nonnull
  private static Bundle bundle() {
    final Bundle bundle = new Bundle();
    bundle.setType(BundleType.COLLECTION);
    bundle.addEntry().setResource(patient());
    bundle.addEntry().setResource(observation());
    return bundle;
  }

  @Nonnull
  private static IParser json() {
    return FHIR.newJsonParser();
  }

  @Nonnull
  private static IParser xml() {
    return FHIR.newXmlParser();
  }

  @Nonnull
  private static Dataset<Row> documents(
      @Nonnull final IParser parser, @Nonnull final Resource... resources) {
    final List<String> texts = Stream.of(resources).map(parser::encodeResourceToString).toList();
    return spark.createDataset(texts, Encoders.STRING()).toDF();
  }

  /** Stores resources in the new layout through the {@code io} module directly. */
  @Nonnull
  private static Dataset<Row> newLayout(
      @Nonnull final String resourceType, @Nonnull final Resource... resources) {
    final Dataset<String> texts =
        spark.createDataset(
            Stream.of(resources).map(json()::encodeResourceToString).toList(), Encoders.STRING());
    return FhirReader.of(spark, FhirDefinitionContext.of(pathling.getFhirContext()))
        .json()
        .read(resourceType, texts);
  }

  @Nonnull
  private static List<String> ids(@Nonnull final Dataset<Row> encoded) {
    return encoded.select("id").as(Encoders.STRING()).collectAsList();
  }

  @Nonnull
  private static Stream<String> lines(@Nonnull final Path file) {
    try {
      return Files.readAllLines(file).stream();
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
