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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

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

  /** The same observation, written as JSON. */
  @Nonnull
  private static final String OBSERVATION_JSON =
      "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
          + "\"code\":{\"coding\":[{\"system\":\"http://loinc.org\",\"code\":\"8302-2\"}]},"
          + "\"valueQuantity\":{\"value\":1.50,\"unit\":\"m\"}}";

  @Test
  void convertsToTheEquivalentJson() {
    assertEquals(parse(PATIENT_JSON), parse(converted("Patient", PATIENT_XML)));
    assertEquals(parse(OBSERVATION_JSON), parse(converted("Observation", OBSERVATION_XML)));
  }

  @Test
  void storesWhatTheEquivalentJsonStores() {
    assertSameStoredResult("Patient", PATIENT_XML, PATIENT_JSON);
    assertSameStoredResult("Observation", OBSERVATION_XML, OBSERVATION_JSON);
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
  void convertsNothingForANullDocument() {
    final Dataset<String> documents =
        TransformFixtures.spark().createDataset(Arrays.asList((String) null), Encoders.STRING());

    assertTrue(XmlConversion.toJson("Patient", documents).collectAsList().isEmpty());
  }

  @Test
  void failsOnADocumentThatIsNotXml() {
    final Dataset<String> documents = documents("{\"resourceType\":\"Patient\"}");

    assertThrows(
        Exception.class,
        () -> TransformFixtures.fhirReader().xml().read("Patient", documents).collectAsList());
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
  private static String converted(@Nonnull final String resourceType, @Nonnull final String xml) {
    return XmlConversion.toJson(resourceType, documents(xml)).collectAsList().get(0);
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
