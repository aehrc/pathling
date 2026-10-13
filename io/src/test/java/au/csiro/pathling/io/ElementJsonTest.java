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
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.definition.NodeDefinition;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the JSON of the values of one element, such as an element a query returns whole (T100j).
 *
 * <p>Each value is written as a JSON object by the element's definition. A decimal is a number with
 * the text it is stored as, and a field the definition does not describe is left out.
 */
class ElementJsonTest {

  @Test
  void decimalsAreNumbersWithTheirStoredText(@TempDir @Nonnull final Path directory)
      throws IOException {
    final Dataset<Row> locations =
        read(
            directory,
            "Location",
            "{\"resourceType\":\"Location\",\"id\":\"1\",\"position\":"
                + "{\"longitude\":-83.6945691,\"latitude\":42.2565,\"altitude\":90}}",
            "{\"resourceType\":\"Location\",\"id\":\"2\",\"position\":"
                + "{\"longitude\":0.00001,\"latitude\":1.50}}");

    // The stored text is what ingest made of the source: through a double for a column with a
    // fraction, so 0.00001 is 1.0E-5 and 1.50 is 1.5, and as an integer for a column without one.
    assertEquals(
        List.of(
            "{\"longitude\":-83.6945691,\"latitude\":42.2565,\"altitude\":90}",
            "{\"longitude\":1.0E-5,\"latitude\":1.5}"),
        json(locations, "Location", "position", functions.col("position")));
  }

  @Test
  void eachValueOfAnArrayIsADocument(@TempDir @Nonnull final Path directory) throws IOException {
    // The text of a string that looks like a marked decimal is left as it is.
    final Dataset<Row> patients =
        read(
            directory,
            "Patient",
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"family\":"
                + "\"{\\\"$decimal$\\\":\\\"1\\\"}\"},{\"given\":[\"Jane\"]}]}");
    final Column names = functions.col("name");
    final Dataset<Row> rendered =
        patients.select(
            TransformFixtures.writer()
                .writeElement(element("Patient", "name"), names, type(patients, names)));

    assertEquals(
        List.of("{\"family\":\"{\\\"$decimal$\\\":\\\"1\\\"}\"}", "{\"given\":[\"Jane\"]}"),
        rendered.first().getList(0));
  }

  @Test
  void fieldsTheDefinitionDoesNotDescribeAreLeftOut(@TempDir @Nonnull final Path directory)
      throws IOException {
    final Dataset<Row> locations =
        read(
            directory,
            "Location",
            "{\"resourceType\":\"Location\",\"id\":\"1\",\"position\":"
                + "{\"longitude\":1.5,\"latitude\":2.5}}");
    final Column extended =
        functions.struct(
            functions.lit(7).alias("_fid"),
            functions.col("position.longitude").alias("longitude"),
            functions.lit(3).alias("longitude_scale"));

    assertEquals(List.of("{\"longitude\":1.5}"), json(locations, "Location", "position", extended));
  }

  @Test
  void nullValueIsNull(@TempDir @Nonnull final Path directory) throws IOException {
    final Dataset<Row> locations =
        read(directory, "Location", "{\"resourceType\":\"Location\",\"id\":\"1\",\"name\":\"x\"}");
    final Column absent =
        functions
            .lit(null)
            .cast(
                DataTypes.createStructType(
                    List.of(DataTypes.createStructField("longitude", DataTypes.StringType, true))));

    final Row row =
        locations
            .select(
                TransformFixtures.writer()
                    .writeElement(element("Location", "position"), absent, type(locations, absent)))
            .first();
    assertNull(row.get(0));
  }

  @Test
  void valuesThatAreNotStructuresAreRefused() {
    final FhirJsonWriter writer = TransformFixtures.writer();
    final Column values = functions.lit("x");

    assertThrows(
        IllegalArgumentException.class,
        () -> writer.writeElement(element("Location", "position"), values, DataTypes.StringType));
  }

  @Nonnull
  private static Dataset<Row> read(
      @Nonnull final Path directory,
      @Nonnull final String resourceType,
      @Nonnull final String... documents)
      throws IOException {
    return TransformFixtures.reader()
        .read(
            resourceType,
            TransformFixtures.corpus(Files.createDirectories(directory.resolve("in")), documents));
  }

  @Nonnull
  private static List<String> json(
      @Nonnull final Dataset<Row> resources,
      @Nonnull final String resourceType,
      @Nonnull final String elementName,
      @Nonnull final Column values) {
    return resources
        .orderBy("id")
        .select(
            TransformFixtures.writer()
                .writeElement(element(resourceType, elementName), values, type(resources, values)))
        .collectAsList()
        .stream()
        .map(row -> row.getString(0))
        .toList();
  }

  @Nonnull
  private static NodeDefinition element(
      @Nonnull final String resourceType, @Nonnull final String elementName) {
    return TransformFixtures.DEFINITIONS
        .findResourceDefinition(resourceType)
        .getChildElement(elementName)
        .orElseThrow();
  }

  @Nonnull
  private static DataType type(
      @Nonnull final Dataset<Row> resources, @Nonnull final Column values) {
    return resources.select(values).schema().fields()[0].dataType();
  }
}
