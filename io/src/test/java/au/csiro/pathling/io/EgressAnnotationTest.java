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

import ca.uhn.fhir.context.FhirContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Test;

/**
 * Tests that egress leaves out every field marked as an annotation, one Pathling does not define
 * included, rather than failing on it: annotations belong to this layout and not to FHIR (decision
 * 135, part 3).
 */
class EgressAnnotationTest {

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  @Nonnull
  private static final String OBSERVATION =
      "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
          + "\"code\":{\"text\":\"x\"},\"effectiveDateTime\":\"2020-01\","
          + "\"valueQuantity\":{\"value\":1.5,\"unit\":\"kg\"}}";

  @Test
  void writesJsonWithoutAnyAnnotation() throws IOException {
    final List<String> written =
        TransformFixtures.fhirWriter().json().write("Observation", annotated()).collectAsList();

    assertEquals(MAPPER.readTree(OBSERVATION), MAPPER.readTree(written.get(0)));
  }

  @Test
  void writesXmlWithoutAnyAnnotation() throws IOException {
    final String xml =
        TransformFixtures.fhirWriter().xml().write("Observation", annotated()).first();

    final FhirContext context = FhirContext.forR4Cached();
    final JsonNode json =
        MAPPER.readTree(
            context
                .newJsonParser()
                .encodeResourceToString(context.newXmlParser().parseResource(xml)));
    assertEquals(MAPPER.readTree(OBSERVATION), json);
  }

  /**
   * Returns an observation carrying an annotation Pathling does not define at the top, one inside
   * the quantity, and a metadata group holding nothing but an annotation.
   */
  @Nonnull
  private static Dataset<Row> annotated() {
    return TransformFixtures.reader()
        .read(
            "Observation",
            TransformFixtures.spark().createDataset(List.of(OBSERVATION), Encoders.STRING()))
        .withColumn("__unknown", functions.lit("u"))
        .withColumn(
            "valueQuantity",
            functions.col("valueQuantity").withField("__unknown", functions.lit("u")))
        .withColumn("_effectiveDateTime", functions.struct(functions.lit("s").alias("__start")));
  }
}
