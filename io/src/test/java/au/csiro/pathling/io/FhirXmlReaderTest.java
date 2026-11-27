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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that XML ingest reaches the same stored result as the equivalent JSON (T058a, T069),
 * through the public XML reader (decision 84).
 *
 * <p>Two cases are where XML and JSON differ in form, so they are where a conversion would go
 * wrong. A primitive's extension is a child element in XML and an underscore-prefixed sibling in
 * JSON. A repeating element occurring once looks, in XML, exactly like an element that cannot
 * repeat: Spark's own XML reader would infer it as a single value, and the layout would then store
 * it as a shape mismatch rather than as an array of one. Cardinality has to come from the FHIR
 * parser and the definitions, never from the text.
 */
class FhirXmlReaderTest {

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  /** A patient with one name carrying one given name, and primitives with extensions. */
  @Nonnull
  private static final String PATIENT_XML =
      "<Patient xmlns=\"http://hl7.org/fhir\">"
          + "<id value=\"1\"/>"
          + "<identifier><system value=\"http://example.org/mrn\"/><value value=\"123\"/>"
          + "</identifier>"
          + "<active>"
          + "<extension url=\"http://example.org/reason\"><valueString value=\"unknown\"/>"
          + "</extension>"
          + "</active>"
          + "<name><family value=\"Smith\"/><given value=\"Jane\"/></name>"
          + "<gender value=\"female\"/>"
          + "<birthDate value=\"1980-01-01\">"
          + "<extension url=\"http://hl7.org/fhir/StructureDefinition/patient-birthTime\">"
          + "<valueDateTime value=\"1980-01-01T10:30:00+10:00\"/></extension>"
          + "</birthDate>"
          + "</Patient>";

  /** The same patient, written as JSON. */
  @Nonnull
  private static final String PATIENT_JSON =
      "{\"resourceType\":\"Patient\",\"id\":\"1\","
          + "\"identifier\":[{\"system\":\"http://example.org/mrn\",\"value\":\"123\"}],"
          + "\"_active\":{\"extension\":[{\"url\":\"http://example.org/reason\","
          + "\"valueString\":\"unknown\"}]},"
          + "\"name\":[{\"family\":\"Smith\",\"given\":[\"Jane\"]}],"
          + "\"gender\":\"female\",\"birthDate\":\"1980-01-01\","
          + "\"_birthDate\":{\"extension\":[{"
          + "\"url\":\"http://hl7.org/fhir/StructureDefinition/patient-birthTime\","
          + "\"valueDateTime\":\"1980-01-01T10:30:00+10:00\"}]}}";

  /** An observation carrying a decimal whose text a double does not reproduce. */
  @Nonnull
  private static final String OBSERVATION_XML =
      "<Observation xmlns=\"http://hl7.org/fhir\">"
          + "<id value=\"o1\"/><status value=\"final\"/>"
          + "<code><coding><system value=\"http://loinc.org\"/><code value=\"8302-2\"/></coding>"
          + "</code>"
          + "<valueQuantity><value value=\"1.50\"/><unit value=\"m\"/></valueQuantity>"
          + "</Observation>";

  @Test
  void storesWhatTheEquivalentJsonStores() {
    assertSameStoredResult("Patient", PATIENT_XML, PATIENT_JSON);
  }

  /**
   * A decimal is stored as the text the XML carries, so XML keeps its lexical form where JSON
   * passes it through a double (owner, 2026-10-22). That holds for a value no double or long holds.
   */
  @Test
  void storesADecimalAsItsText() {
    final List<String> values =
        List.of("1.50", "5", "0.0008823084504462283", "9999999999999999999", "1.2E3");
    final String[] documents =
        values.stream()
            .map(value -> OBSERVATION_XML.replace("o1", "o" + value).replace("1.50", value))
            .toArray(String[]::new);

    final List<String> stored =
        TransformFixtures.fhirReader()
            .xml()
            .read("Observation", documents(documents))
            .collectAsList()
            .stream()
            .map(row -> row.<Row>getAs("valueQuantity").<String>getAs("value"))
            .toList();

    assertEquals(new java.util.HashSet<>(values), new java.util.HashSet<>(stored));
  }

  /**
   * An element in another namespace is not FHIR, and is left out rather than read as the FHIR
   * element its local name matches.
   */
  @Test
  void leavesOutAnElementInAnotherNamespace() {
    final Row stored =
        TransformFixtures.fhirReader()
            .xml()
            .read(
                "Observation",
                documents(
                    "<Observation xmlns=\"http://hl7.org/fhir\" xmlns:v=\"http://example.org\"><id"
                        + " value=\"o1\"/><v:status value=\"preliminary\"/><status"
                        + " value=\"final\"/><code><text value=\"x\"/></code></Observation>"))
            .first();

    assertEquals("final", stored.<String>getAs("status"));
  }

  /**
   * One {@code given} and one {@code name} in the XML come back as arrays of one, which is what the
   * definitions declare, and not as single values.
   */
  @Test
  void storesARepeatingElementOccurringOnceAsAnArray() {
    final Dataset<Row> stored =
        TransformFixtures.fhirReader().xml().read("Patient", documents(PATIENT_XML));

    final StructType schema = stored.schema();
    final ArrayType names = assertInstanceOf(ArrayType.class, schema.apply("name").dataType());
    final StructType name = assertInstanceOf(StructType.class, names.elementType());
    assertInstanceOf(ArrayType.class, name.apply("given").dataType());
    assertInstanceOf(ArrayType.class, schema.apply("identifier").dataType());

    final Row row = stored.collectAsList().get(0);
    final List<Row> storedNames = row.getList(row.fieldIndex("name"));
    assertEquals(1, storedNames.size());
    assertEquals(
        List.of("Jane"), storedNames.get(0).getList(storedNames.get(0).fieldIndex("given")));
  }

  /**
   * The type of an XML document is known only once it is parsed, so the reader selects the
   * documents of the type asked for, as the previous encoder did, and stores only those.
   */
  @Test
  void storesOnlyTheDocumentsOfTheTypeAskedFor() {
    final Dataset<Row> stored =
        TransformFixtures.fhirReader()
            .xml()
            .read("Patient", documents(OBSERVATION_XML, PATIENT_XML, OBSERVATION_XML));

    assertEquals(
        sorted(TransformFixtures.fhirReader().xml().read("Patient", documents(PATIENT_XML))),
        sorted(stored));
  }

  @Test
  void storesNothingForANullDocument() {
    final Dataset<String> documents =
        TransformFixtures.spark()
            .createDataset(Arrays.asList(null, PATIENT_XML), Encoders.STRING());

    assertEquals(
        List.of("1"),
        TransformFixtures.fhirReader().xml().read("Patient", documents).collectAsList().stream()
            .map(row -> row.<String>getAs("id"))
            .toList());
  }

  /**
   * A blank line in a file of line-delimited XML holds no document, and is skipped as the NDJSON
   * reader skips one, rather than failing the read.
   */
  @Test
  void skipsABlankLine(@TempDir @Nonnull final Path directory) throws IOException {
    Files.writeString(
        directory.resolve("patients.xml"),
        PATIENT_XML + "\n\n" + PATIENT_XML.replace("\"1\"", "\"2\"") + "\n \t\n");

    assertEquals(
        List.of("1", "2"),
        TransformFixtures.fhirReader()
            .xml()
            .read("Patient", directory.toString())
            .collectAsList()
            .stream()
            .map(row -> row.<String>getAs("id"))
            .sorted()
            .toList());
  }

  /** A document in the FHIR namespace under a prefix is valid FHIR, and is read. */
  @Test
  void readsADocumentWithAPrefixedFhirNamespace() {
    final String prefixed =
        "<f:Patient xmlns:f=\"http://hl7.org/fhir\"><f:id value=\"1\"/>"
            + "<f:identifier><f:system value=\"http://example.org/mrn\"/><f:value value=\"123\"/>"
            + "</f:identifier>"
            + "<f:active><f:extension url=\"http://example.org/reason\">"
            + "<f:valueString value=\"unknown\"/></f:extension></f:active>"
            + "<f:name><f:family value=\"Smith\"/><f:given value=\"Jane\"/></f:name>"
            + "<f:gender value=\"female\"/>"
            + "<f:birthDate value=\"1980-01-01\">"
            + "<f:extension url=\"http://hl7.org/fhir/StructureDefinition/patient-birthTime\">"
            + "<f:valueDateTime value=\"1980-01-01T10:30:00+10:00\"/></f:extension>"
            + "</f:birthDate>"
            + "</f:Patient>";

    assertSameStoredResult("Patient", prefixed, PATIENT_JSON);
  }

  @Test
  void failsOnADocumentInNoNamespace() {
    assertFailsNaming(
        "<Patient><id value=\"n1\"/></Patient>", "Patient with id \"n1\"", "no namespace");
  }

  @Test
  void failsOnTextAfterTheRoot() {
    assertFailsNaming(
        "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"t1\"/></Patient>trailing",
        "Patient with id \"t1\"",
        "");
  }

  @Test
  void failsOnASecondRoot() {
    assertFailsNaming(
        "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"r1\"/></Patient>"
            + "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"r2\"/></Patient>",
        "Patient with id \"r1\"",
        "");
  }

  @Test
  void failsOnADocumentTypeDeclaration() {
    assertFailsNaming(
        "<!DOCTYPE Patient [<!ENTITY e \"x\">]>"
            + "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"d1\"/></Patient>",
        "document starting",
        "document type declaration");
  }

  /** Text that is not XML is named by its start, since no type or id can be read from it. */
  @Test
  void namesADocumentThatIsNotXmlByItsStart() {
    assertFailsNaming("{\"resourceType\":\"Patient\"}", "document starting \"{", "");
  }

  private static void assertFailsNaming(
      @Nonnull final String document, @Nonnull final String name, @Nonnull final String reason) {
    final Exception failure =
        assertThrows(
            Exception.class,
            () ->
                TransformFixtures.fhirReader()
                    .xml()
                    .read("Patient", documents(document))
                    .collectAsList());
    final String messages = XmlNarrativeTest.messages(failure);
    assertTrue(messages.contains(name), "the failure does not name the document: " + messages);
    assertTrue(messages.contains(reason), "the failure does not give the reason: " + messages);
  }

  private static void assertSameStoredResult(
      @Nonnull final String resourceType, @Nonnull final String xml, @Nonnull final String json) {
    final Dataset<Row> fromXml =
        TransformFixtures.fhirReader().xml().read(resourceType, documents(xml));
    final Dataset<Row> fromJson = TransformFixtures.reader().read(resourceType, documents(json));

    assertEquals(fromJson.schema(), fromXml.schema(), "the stored schemas differ");
    assertEquals(sorted(fromJson), sorted(fromXml), "the stored rows differ");
  }

  @Nonnull
  private static List<Row> sorted(@Nonnull final Dataset<Row> dataset) {
    return dataset.collectAsList().stream()
        .sorted(Comparator.comparing(row -> row.<String>getAs("id")))
        .toList();
  }

  @Nonnull
  private static JsonNode parse(@Nonnull final String json) {
    try {
      return MAPPER.readTree(json);
    } catch (final JsonProcessingException e) {
      throw new IllegalStateException("Not JSON: " + json, e);
    }
  }

  @Nonnull
  private static Dataset<String> documents(@Nonnull final String... documents) {
    return TransformFixtures.spark().createDataset(List.of(documents), Encoders.STRING());
  }
}
