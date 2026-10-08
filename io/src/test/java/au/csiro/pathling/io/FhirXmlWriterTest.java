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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ca.uhn.fhir.context.FhirContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

/**
 * Tests that resources stored in this layout are written as the FHIR XML of what was stored, which
 * keeps the XML output the previous encoder's decoding offered (FR-043, decision 84).
 */
class FhirXmlWriterTest {

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  /** A patient with repeating elements, of which one occurs once, and no primitive metadata. */
  @Nonnull
  private static final String PATIENT_JSON =
      "{\"resourceType\":\"Patient\",\"id\":\"1\","
          + "\"identifier\":[{\"system\":\"http://example.org/mrn\",\"value\":\"123\"}],"
          + "\"active\":true,"
          + "\"name\":[{\"family\":\"Smith\",\"given\":[\"Jane\",\"Q\"]},{\"text\":\"J Smith\"}],"
          + "\"gender\":\"female\",\"birthDate\":\"1980-01-01\"}";

  @Test
  void selectsAFormatByItsMediaType() {
    final FhirWriter writer = TransformFixtures.fhirWriter();

    assertSame(writer.json(), writer.format("application/fhir+json"));
    assertSame(writer.xml(), writer.format("application/fhir+xml"));
    assertThrows(IllegalArgumentException.class, () -> writer.format("text/xml"));
  }

  @Test
  void writesTheXmlOfWhatWasStored() {
    final Dataset<Row> stored = TransformFixtures.reader().read("Patient", documents(PATIENT_JSON));

    final List<String> written =
        TransformFixtures.fhirWriter().xml().write("Patient", stored).collectAsList();

    assertEquals(1, written.size());
    assertEquals(parse(PATIENT_JSON), parse(asJson(written.get(0))));
  }

  @Test
  void storesWhatItWritesAsItWasStored() {
    final Dataset<Row> stored = TransformFixtures.reader().read("Patient", documents(PATIENT_JSON));

    final Dataset<Row> restored =
        TransformFixtures.fhirReader()
            .xml()
            .read("Patient", TransformFixtures.fhirWriter().xml().write("Patient", stored));

    assertEquals(stored.schema(), restored.schema());
    assertEquals(stored.collectAsList(), restored.collectAsList());
  }

  /**
   * XML has no way to carry the null that keeps a repeating primitive's position, which the JSON
   * writer writes until M5 stores the metadata it aligns with (decision 71). The value is kept and
   * the null is not.
   */
  @Test
  void leavesOutThePositionalNullOfARepeatingPrimitive() {
    final Dataset<Row> stored =
        TransformFixtures.reader()
            .read(
                "Patient",
                documents(
                    "{\"resourceType\":\"Patient\",\"id\":\"1\","
                        + "\"name\":[{\"given\":[null,\"B\"]}]}"));

    final JsonNode written =
        parse(asJson(TransformFixtures.fhirWriter().xml().write("Patient", stored).first()));

    assertEquals(parse("[\"B\"]"), written.at("/name/0/given"));
  }

  @Nonnull
  private static String asJson(@Nonnull final String xml) {
    final FhirContext context = FhirContext.forR4Cached();
    return context
        .newJsonParser()
        .encodeResourceToString(context.newXmlParser().parseResource(xml));
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
