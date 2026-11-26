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

import au.csiro.pathling.schema.SchemaConfiguration;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that a schema defined up front gives the same data whatever the resources are read from
 * (T009, FR-009, FR-021a, SC-004). A decimal is compared by value, since the JSON read takes it as
 * the text Spark makes of the number and the XML read takes the text the XML carries (research R3).
 */
class DefinedSchemaParityTest {

  @Nonnull
  private static final StructType SCHEMA =
      StructType.fromDDL(
          "resourceType STRING, id STRING, status STRING, code STRUCT<text: STRING>,"
              + " subject STRUCT<reference: STRING>, effectiveDateTime STRING,"
              + " valueQuantity STRUCT<value: STRING, unit: STRING>");

  /** The canonical annotation keeps a decimal's scale, so it is left out of value parity. */
  @Nonnull
  private static final SchemaConfiguration CONFIGURATION =
      SchemaConfiguration.builder().enableCanonicalAnnotation(false).build();

  @TempDir Path directory;

  @Nonnull
  private static String observationJson(@Nonnull final String id, @Nonnull final String value) {
    return "{\"resourceType\":\"Observation\",\"id\":\""
        + id
        + "\",\"status\":\"final\",\"code\":{\"text\":\"weight\"},"
        + "\"subject\":{\"reference\":\"Patient/p1\"},\"effectiveDateTime\":\"2020-01-01\","
        + "\"valueQuantity\":{\"value\":"
        + value
        + ",\"unit\":\"kg\"}}";
  }

  @Nonnull
  private static String observationXml(@Nonnull final String id, @Nonnull final String value) {
    return "<Observation xmlns=\"http://hl7.org/fhir\"><id value=\""
        + id
        + "\"/><status value=\"final\"/><code><text value=\"weight\"/></code>"
        + "<subject><reference value=\"Patient/p1\"/></subject>"
        + "<effectiveDateTime value=\"2020-01-01\"/><valueQuantity><value value=\""
        + value
        + "\"/><unit value=\"kg\"/></valueQuantity></Observation>";
  }

  @Nonnull
  private static Dataset<String> strings(@Nonnull final List<String> values) {
    return TransformFixtures.spark().createDataset(values, Encoders.STRING());
  }

  @Nonnull
  private static Dataset<Row> annotate(@Nonnull final Dataset<Row> resources) {
    return Annotator.of(TransformFixtures.DEFINITIONS, CONFIGURATION)
        .annotate("Observation", resources);
  }

  /**
   * Returns the rows ordered by id, with the quantity's value as a decimal so that it is compared
   * by value.
   */
  @Nonnull
  private static List<Row> byValue(@Nonnull final Dataset<Row> resources) {
    final Column value = functions.col("valueQuantity.value").cast("decimal(38,10)");
    return resources
        .withColumn("valueQuantity", functions.col("valueQuantity").withField("value", value))
        .orderBy("id")
        .collectAsList();
  }

  @Test
  void jsonXmlAndStoredGiveEqualData() {
    final List<String> json = List.of(observationJson("o1", "1.10"), observationJson("o2", "100"));
    final List<String> xml = List.of(observationXml("o1", "1.10"), observationXml("o2", "100"));
    final FhirReader reader = TransformFixtures.fhirReader();

    final Dataset<Row> fromJson =
        annotate(reader.json().read("Observation", strings(json), SCHEMA));
    final Dataset<Row> fromXml = annotate(reader.xml().read("Observation", strings(xml), SCHEMA));
    // The stored resources were written by a read without a schema, and are projected onto the
    // layout schema, as a stored source does (research R6).
    final String stored = directory.resolve("stored").toString();
    reader.json().read("Observation", strings(json)).write().parquet(stored);
    final StructType layout =
        DefinedSchemaLogic.of(TransformFixtures.spark(), TransformFixtures.DEFINITIONS)
            .layoutSchema("Observation", SCHEMA);
    final Dataset<Row> fromStored =
        annotate(Projection.project(TransformFixtures.spark().read().parquet(stored), layout));

    assertEquals(SCHEMA, layout);
    assertEquals(fromJson.schema(), fromXml.schema());
    assertEquals(fromJson.schema(), fromStored.schema());
    final List<Row> expected = byValue(fromJson);
    assertEquals(2, expected.size());
    assertEquals(expected, byValue(fromXml));
    assertEquals(expected, byValue(fromStored));
  }

  @Test
  void definedLoadWithInferredSchemaGivesInferredLoad() {
    // Integers mixed with fractions are inferred as a double, so the inferred read stores 100.0
    // where the defined read stores 100, which is equal in value (research R3).
    final List<String> json = List.of(observationJson("o1", "100"), observationJson("o2", "1.5"));
    final List<String> xml = List.of(observationXml("o1", "100"), observationXml("o2", "1.5"));
    final FhirReader reader = TransformFixtures.fhirReader();

    final Dataset<Row> inferredJson = reader.json().read("Observation", strings(json));
    final Dataset<Row> definedJson =
        reader.json().read("Observation", strings(json), inferredJson.schema());
    final Dataset<Row> inferredXml = reader.xml().read("Observation", strings(xml));
    final Dataset<Row> definedXml =
        reader.xml().read("Observation", strings(xml), inferredXml.schema());

    assertEquals(inferredJson.schema(), definedJson.schema());
    assertEquals(byValue(inferredJson), byValue(definedJson));
    assertEquals(inferredXml.schema(), definedXml.schema());
    assertEquals(byValue(inferredXml), byValue(definedXml));
  }

  @Test
  void bundlesGiveEqualDataWithAndWithoutSchema() {
    // The Observation refers to the Patient by the full URL of its entry, which the read resolves.
    final String jsonBundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
            + "{\"fullUrl\":\"urn:uuid:a\",\"resource\":{\"resourceType\":\"Patient\",\"id\":\"p1\"}},"
            + "{\"fullUrl\":\"urn:uuid:b\",\"resource\":"
            + observationJson("o1", "1.5").replace("Patient/p1", "urn:uuid:a")
            + "}]}";
    final String xmlBundle =
        "<Bundle xmlns=\"http://hl7.org/fhir\"><type value=\"collection\"/><entry><fullUrl"
            + " value=\"urn:uuid:a\"/><resource><Patient xmlns=\"http://hl7.org/fhir\"><id"
            + " value=\"p1\"/></Patient></resource></entry><entry><fullUrl"
            + " value=\"urn:uuid:b\"/><resource>"
            + observationXml("o1", "1.5").replace("Patient/p1", "urn:uuid:a")
            + "</resource></entry></Bundle>";
    final FhirReader reader = TransformFixtures.fhirReader();

    final Dataset<Row> inferredJson =
        reader.json().readBundles("Observation", strings(List.of(jsonBundle)));
    final Dataset<Row> definedJson =
        reader.json().readBundles("Observation", strings(List.of(jsonBundle)), SCHEMA);
    final Dataset<Row> inferredXml =
        reader.xml().readBundles("Observation", strings(List.of(xmlBundle)));
    final Dataset<Row> definedXml =
        reader.xml().readBundles("Observation", strings(List.of(xmlBundle)), SCHEMA);

    assertEquals(SCHEMA, definedJson.schema());
    assertEquals(SCHEMA, definedXml.schema());
    final List<Row> expected = byValue(definedJson);
    assertEquals("Patient/p1", expected.get(0).<Row>getAs("subject").getAs("reference"));
    assertEquals(expected, byValue(definedXml));
    assertEquals(expected, byValue(Projection.project(inferredJson, SCHEMA)));
    assertEquals(expected, byValue(Projection.project(inferredXml, SCHEMA)));
  }
}
