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

package au.csiro.pathling.views;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.DatasetDataSource;
import ca.uhn.fhir.parser.IParser;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Pins the unnesting constraint of FR-036 against the real engine, using the divergent-schema
 * fixture in {@code viewTests/divergent_schema/}. Its two files disagree on a leaf of a repeating
 * element: every {@code Patient.name} in the first carries a {@code family}, and none in the second
 * does. The view unnests {@code name} and projects only {@code family}.
 *
 * <p>If unnesting were reduced to a read of that single leaf, the second file would contribute no
 * rows and the loss would be silent. So the assertion is that each file's elements come back, one
 * row per element, whether or not the leaf is populated. T027a extends the fixture so that the leaf
 * is absent from one file's schema rather than merely null.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class DivergentSchemaViewTest {

  private static final String FIXTURE_DIR = "/viewTests/divergent_schema/";

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Autowired Gson gson;

  @TempDir Path tempDir;

  @Test
  void unnestingReturnsRowsFromBothFiles() throws IOException {
    // Write each batch to its own Parquet file in one directory, and read the directory back as a
    // single dataset, so that the engine reads real files rather than an in-memory union.
    final String patientDir = tempDir.resolve("Patient").toString();
    encode("batch-1.ndjson").coalesce(1).write().mode(SaveMode.Overwrite).parquet(patientDir);
    encode("batch-2.ndjson").coalesce(1).write().mode(SaveMode.Append).parquet(patientDir);
    final Dataset<Row> patients = spark.read().parquet(patientDir);
    assertThat(patients.inputFiles()).hasSize(2);

    final FhirView view = gson.fromJson(readFixture("view.json"), FhirView.class);
    final FhirViewExecutor executor =
        new FhirViewExecutor(
            fhirEncoders.getContext(), new DatasetDataSource(Map.of("Patient", patients)));
    final List<String> families =
        executor.buildQuery(view).collectAsList().stream()
            .map(row -> Objects.toString(row.get(0)))
            .toList();

    // The first file has three names, each with a family. The second has three names, none with a
    // family, and each still yields a row.
    assertThat(families)
        .containsExactlyInAnyOrder("Alpha", "Beta", "Gamma", "null", "null", "null");
  }

  @Nonnull
  private Dataset<Row> encode(@Nonnull final String fileName) throws IOException {
    final IParser parser = fhirEncoders.getContext().newJsonParser();
    final List<IBaseResource> resources =
        readFixture(fileName)
            .lines()
            .filter(line -> !line.isBlank())
            .map(line -> (IBaseResource) parser.parseResource(line))
            .toList();
    return spark.createDataset(resources, fhirEncoders.of("Patient")).toDF();
  }

  @Nonnull
  private String readFixture(@Nonnull final String fileName) throws IOException {
    try (final InputStream input = getClass().getResourceAsStream(FIXTURE_DIR + fileName)) {
      return new String(Objects.requireNonNull(input).readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
