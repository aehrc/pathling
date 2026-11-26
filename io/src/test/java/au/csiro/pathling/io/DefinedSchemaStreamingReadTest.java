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
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Tests that a read with a schema defined up front of a streaming dataset returns a streaming
 * dataset with the layout schema, since it needs no pass over the data (T010, FR-012, research R4,
 * R5).
 */
class DefinedSchemaStreamingReadTest {

  @Nonnull
  private static final StructType SCHEMA =
      StructType.fromDDL("resourceType STRING, id STRING, gender STRING, birthDate STRING");

  @Nonnull
  private static final String PATIENT_JSON =
      "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"gender\":\"female\",\"active\":true}";

  @Nonnull
  private static final String PATIENT_XML =
      "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"p1\"/><active value=\"true\"/>"
          + "<gender value=\"female\"/></Patient>";

  /** Reads a stream holding one document, by the way that a route names. */
  @Nonnull
  private static Dataset<Row> read(
      @Nonnull final String route, @Nonnull final Dataset<String> documents) {
    final FhirReader reader = TransformFixtures.fhirReader();
    return switch (route) {
      case "jsonDocuments" -> reader.json().read("Patient", documents, SCHEMA);
      case "jsonBundles" -> reader.json().readBundles("Patient", documents, SCHEMA);
      case "xmlDocuments" -> reader.xml().read("Patient", documents, SCHEMA);
      case "xmlBundles" -> reader.xml().readBundles("Patient", documents, SCHEMA);
      default -> throw new IllegalArgumentException(route);
    };
  }

  @Nonnull
  private static String document(@Nonnull final String route) {
    return switch (route) {
      case "jsonDocuments" -> PATIENT_JSON;
      case "jsonBundles" -> DefinedSchemaReadTest.jsonBundle(PATIENT_JSON);
      case "xmlDocuments" -> PATIENT_XML;
      case "xmlBundles" -> DefinedSchemaReadTest.xmlBundle(PATIENT_XML);
      default -> throw new IllegalArgumentException(route);
    };
  }

  @ParameterizedTest
  @ValueSource(strings = {"jsonDocuments", "jsonBundles", "xmlDocuments", "xmlBundles"})
  void returnsStreamingDatasetWithLayoutSchema(@Nonnull final String route)
      throws TimeoutException {
    final MemoryStream<String> stream =
        MemoryStream.apply(Encoders.STRING(), TransformFixtures.spark());
    stream.addData(CollectionConverters.asScala(List.of(document(route))));

    final Dataset<Row> result = read(route, stream.toDS());

    assertTrue(result.isStreaming());
    assertEquals(SCHEMA, result.schema());
    final String table = "defined_" + route;
    final StreamingQuery query =
        result.writeStream().format("memory").queryName(table).outputMode("append").start();
    try {
      query.processAllAvailable();
    } finally {
      query.stop();
    }
    final List<Row> rows = TransformFixtures.spark().table(table).collectAsList();
    assertEquals(List.of(RowFactory.create("Patient", "p1", "female", null)), rows);
  }
}
