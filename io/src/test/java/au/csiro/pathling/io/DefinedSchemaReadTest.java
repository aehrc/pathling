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

import au.csiro.pathling.schema.SchemaConfiguration;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests reads with a schema defined up front, for each way resources are read: JSON files, JSON
 * documents, JSON bundles, XML documents and XML bundles (T008).
 */
class DefinedSchemaReadTest {

  /** A Patient with a name, a gender, a birth date and an active flag. */
  @Nonnull
  private static final String PATIENT_JSON =
      "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"active\":true,"
          + "\"name\":[{\"family\":\"Smith\",\"given\":[\"Ann\"]}],"
          + "\"gender\":\"female\",\"birthDate\":\"1970-01-01\"}";

  @Nonnull
  private static final String PATIENT_XML =
      "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"p1\"/><active value=\"true\"/>"
          + "<name><family value=\"Smith\"/><given value=\"Ann\"/></name>"
          + "<gender value=\"female\"/><birthDate value=\"1970-01-01\"/></Patient>";

  /** A Patient carrying content that the definitions do not describe. */
  @Nonnull
  private static final String UNDESCRIBED_JSON =
      "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"foo\":\"x\","
          + "\"name\":[{\"family\":\"Jones\",\"bar\":1}]}";

  @Nonnull
  private static final String UNDESCRIBED_XML =
      "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"p2\"/><foo value=\"x\"/>"
          + "<name><family value=\"Jones\"/><bar value=\"1\"/></name></Patient>";

  /** A Patient whose birth date is absent for a reason, given in its metadata group. */
  @Nonnull
  private static final String ABSENT_REASON_JSON =
      "{\"resourceType\":\"Patient\",\"id\":\"p3\",\"_birthDate\":{\"extension\":[{\"url\":"
          + "\"http://hl7.org/fhir/StructureDefinition/data-absent-reason\","
          + "\"valueCode\":\"unknown\"}]}}";

  @Nonnull
  private static final String ABSENT_REASON_XML =
      "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"p3\"/><birthDate><extension"
          + " url=\"http://hl7.org/fhir/StructureDefinition/data-absent-reason\">"
          + "<valueCode value=\"unknown\"/></extension></birthDate></Patient>";

  @Nonnull
  private static final String OBSERVATION_JSON =
      "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
          + "\"code\":{\"text\":\"weight\"},\"effectiveDateTime\":\"2020-01-01\","
          + "\"valueQuantity\":{\"value\":1.5,\"unit\":\"kg\","
          + "\"system\":\"http://unitsofmeasure.org\",\"code\":\"kg\"}}";

  @Nonnull
  private static final String OBSERVATION_XML =
      "<Observation xmlns=\"http://hl7.org/fhir\"><id value=\"o1\"/><status value=\"final\"/>"
          + "<code><text value=\"weight\"/></code><effectiveDateTime value=\"2020-01-01\"/>"
          + "<valueQuantity><value value=\"1.5\"/><unit value=\"kg\"/>"
          + "<system value=\"http://unitsofmeasure.org\"/><code value=\"kg\"/></valueQuantity>"
          + "</Observation>";

  @TempDir Path directory;

  /** Wraps JSON resources in a collection bundle. */
  @Nonnull
  static String jsonBundle(@Nonnull final String... resources) {
    final StringBuilder entries = new StringBuilder();
    for (final String resource : resources) {
      entries.append(entries.isEmpty() ? "" : ",").append("{\"resource\":").append(resource);
      entries.append('}');
    }
    return "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":[" + entries + "]}";
  }

  /** Wraps XML resources in a collection bundle. */
  @Nonnull
  static String xmlBundle(@Nonnull final String... resources) {
    final StringBuilder entries = new StringBuilder();
    for (final String resource : resources) {
      entries.append("<entry><resource>").append(resource).append("</resource></entry>");
    }
    return "<Bundle xmlns=\"http://hl7.org/fhir\"><type value=\"collection\"/>"
        + entries
        + "</Bundle>";
  }

  @Nonnull
  private static Dataset<String> strings(@Nonnull final String... values) {
    return TransformFixtures.spark().createDataset(List.of(values), Encoders.STRING());
  }

  /** Reads one resource with a schema, by the way that a route names. */
  @Nonnull
  private Dataset<Row> read(
      @Nonnull final String route,
      @Nonnull final FhirReader reader,
      @Nonnull final String resourceType,
      @Nonnull final String json,
      @Nonnull final String xml,
      @Nonnull final StructType schema)
      throws IOException {
    return switch (route) {
      case "jsonFiles" ->
          reader
              .json()
              .read(
                  resourceType,
                  TransformFixtures.corpus(Files.createTempDirectory(directory, "corpus"), json),
                  schema);
      case "jsonDocuments" -> reader.json().read(resourceType, strings(json), schema);
      case "jsonBundles" ->
          reader.json().readBundles(resourceType, strings(jsonBundle(json)), schema);
      case "xmlDocuments" -> reader.xml().read(resourceType, strings(xml), schema);
      case "xmlBundles" -> reader.xml().readBundles(resourceType, strings(xmlBundle(xml)), schema);
      default -> throw new IllegalArgumentException(route);
    };
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"jsonFiles", "jsonDocuments", "jsonBundles", "xmlDocuments", "xmlBundles"})
  void elementsTheBatchLacksArePresentAndNull(@Nonnull final String route) throws IOException {
    final StructType schema =
        StructType.fromDDL(
            "resourceType STRING, id STRING, name ARRAY<STRUCT<family: STRING, given:"
                + " ARRAY<STRING>, prefix: ARRAY<STRING>>>, gender STRING,"
                + " maritalStatus STRUCT<text: STRING>, deceasedBoolean BOOLEAN");

    final Dataset<Row> result =
        read(route, TransformFixtures.fhirReader(), "Patient", PATIENT_JSON, PATIENT_XML, schema);

    assertEquals(schema, result.schema());
    final Row row = result.collectAsList().get(0);
    assertEquals("Patient", row.getAs("resourceType"));
    assertEquals("p1", row.getAs("id"));
    final Row name = row.<List<Row>>getAs("name").get(0);
    assertEquals("Smith", name.getAs("family"));
    assertEquals(List.of("Ann"), name.getList(name.fieldIndex("given")));
    assertNull(name.get(name.fieldIndex("prefix")));
    assertEquals("female", row.getAs("gender"));
    assertNull(row.getAs("maritalStatus"));
    assertNull(row.getAs("deceasedBoolean"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"jsonFiles", "jsonDocuments", "jsonBundles", "xmlDocuments", "xmlBundles"})
  void contentTheSchemaLacksIsDropped(@Nonnull final String route) throws IOException {
    // The resources carry content that the schema lacks and the definitions describe, and content
    // that they do not describe, at the top level and inside an element.
    final StructType schema =
        StructType.fromDDL("resourceType STRING, id STRING, name ARRAY<STRUCT<family: STRING>>");

    final Dataset<Row> described =
        read(route, TransformFixtures.fhirReader(), "Patient", PATIENT_JSON, PATIENT_XML, schema);
    final Dataset<Row> undescribed =
        read(
            route,
            TransformFixtures.fhirReader(),
            "Patient",
            UNDESCRIBED_JSON,
            UNDESCRIBED_XML,
            schema);

    assertEquals(schema, described.schema());
    assertEquals(schema, undescribed.schema());
    final Row first = described.collectAsList().get(0);
    assertEquals("p1", first.getAs("id"));
    assertEquals("Smith", first.<Row>getList(2).get(0).getAs("family"));
    final Row second = undescribed.collectAsList().get(0);
    assertEquals("p2", second.getAs("id"));
    assertEquals("Jones", second.<Row>getList(2).get(0).getAs("family"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"jsonFiles", "jsonDocuments", "jsonBundles", "xmlDocuments", "xmlBundles"})
  void annotationsFollowTheConfiguration(@Nonnull final String route) throws IOException {
    // The schema carries annotations of every kind, but only the range annotation is configured.
    final StructType schema =
        StructType.fromDDL(
            "resourceType STRING, id STRING, status STRING, effectiveDateTime STRING,"
                + " _effectiveDateTime STRUCT<__start: TIMESTAMP, __end: TIMESTAMP>,"
                + " valueQuantity STRUCT<value: STRING, _value: STRUCT<__numeric: DECIMAL(38,6)>,"
                + " unit: STRING, __canonical_code: STRING>, __custom STRING");
    final SchemaConfiguration rangeOnly =
        SchemaConfiguration.builder()
            .enableNumericAnnotation(false)
            .enableCanonicalAnnotation(false)
            .build();

    final Dataset<Row> result =
        read(
            route,
            TransformFixtures.fhirReader(),
            "Observation",
            OBSERVATION_JSON,
            OBSERVATION_XML,
            schema);

    assertEquals(
        StructType.fromDDL(
            "resourceType STRING, id STRING, status STRING, effectiveDateTime STRING,"
                + " valueQuantity STRUCT<value: STRING, unit: STRING>"),
        result.schema());
    final Dataset<Row> annotated =
        Annotator.of(TransformFixtures.DEFINITIONS, rangeOnly).annotate("Observation", result);
    assertEquals(
        "struct<resourceType:string,id:string,status:string,effectiveDateTime:string,"
            + "_effectiveDateTime:struct<__start:timestamp,__end:timestamp>,"
            + "valueQuantity:struct<value:string,unit:string>>",
        annotated.schema().catalogString());
    final Row row = annotated.collectAsList().get(0);
    assertEquals("2020-01-01", row.getAs("effectiveDateTime"));
    assertEquals("1.5", row.<Row>getAs("valueQuantity").getAs("value"));
    assertEquals("kg", row.<Row>getAs("valueQuantity").getAs("unit"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"jsonFiles", "jsonDocuments", "jsonBundles", "xmlDocuments", "xmlBundles"})
  void metadataGroupIsKeptWhereTheSchemaHasItWithTheOptionOff(@Nonnull final String route)
      throws IOException {
    final StructType schema =
        StructType.fromDDL(
            "resourceType STRING, id STRING, birthDate STRING,"
                + " _birthDate STRUCT<extension: ARRAY<STRUCT<url: STRING, valueCode: STRING>>>");

    final Dataset<Row> result =
        read(
            route,
            TransformFixtures.fhirReader(),
            "Patient",
            ABSENT_REASON_JSON,
            ABSENT_REASON_XML,
            schema);

    assertEquals(schema, result.schema());
    final Row row = result.collectAsList().get(0);
    assertNull(row.getAs("birthDate"));
    final Row extension = row.<Row>getAs("_birthDate").<Row>getList(0).get(0);
    assertEquals("http://hl7.org/fhir/StructureDefinition/data-absent-reason", extension.get(0));
    assertEquals("unknown", extension.get(1));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"jsonFiles", "jsonDocuments", "jsonBundles", "xmlDocuments", "xmlBundles"})
  void metadataGroupIsDroppedWhereTheSchemaLacksItWithTheOptionOn(@Nonnull final String route)
      throws IOException {
    final StructType schema =
        StructType.fromDDL("resourceType STRING, id STRING, birthDate STRING");

    final Dataset<Row> result =
        read(
            route,
            TransformFixtures.metadataReader(),
            "Patient",
            ABSENT_REASON_JSON,
            ABSENT_REASON_XML,
            schema);

    assertEquals(schema, result.schema());
    final Row row = result.collectAsList().get(0);
    assertEquals("p3", row.getAs("id"));
    assertNull(row.getAs("birthDate"));
  }
}
