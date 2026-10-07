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
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;
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
 * is absent from one file's schema rather than merely null, which exercises tolerant traversal over
 * divergent files and not only the unnesting shape.
 *
 * <p>The first case is pinned to the previous layout, because it needs the leaf carried as a null,
 * which only the previous layout's dense schema does. The second follows the active test layout,
 * and on the new layout the leaf is absent from the second file because that file populates none.
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
    // The files are always in the previous layout, whose dense schema carries the leaf as a null
    // in the second file, so that both files share a schema. On the new layout the second file
    // would not carry the leaf, and a read without merging would take its schema from whichever
    // file it read first; the next test covers that case.
    encode("batch-1.ndjson", TestLayout.PREVIOUS)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(patientDir);
    encode("batch-2.ndjson", TestLayout.PREVIOUS)
        .coalesce(1)
        .write()
        .mode(SaveMode.Append)
        .parquet(patientDir);
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

  @Test
  void unnestingReturnsRowsFromBothFilesWhenTheLeafIsAbsentFromOneFile() throws IOException {
    // The second file is written with the projected leaf removed from its schema entirely, rather
    // than carried as a null, as a pruned table would store it (T027a).
    final String patientDir = tempDir.resolve("Patient").toString();
    // On the new layout the second file lacks the leaf already, because it populates none.
    encode("batch-1.ndjson", TestLayout.active())
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(patientDir);
    final Dataset<Row> withoutLeaf =
        PrunedSchemaReader.write(
                encode("batch-2.ndjson", TestLayout.active()),
                tempDir.resolve("staging").toString(),
                fhirEncoders.of("Patient").schema())
            .readWithout("name.family");
    withoutLeaf.coalesce(1).write().mode(SaveMode.Append).parquet(patientDir);

    final Dataset<Row> directory = spark.read().parquet(patientDir);
    assertThat(directory.inputFiles()).hasSize(2);
    final List<Boolean> fileCarriesLeaf =
        Arrays.stream(directory.inputFiles())
            .map(file -> nameCarriesFamily(spark.read().parquet(file).schema()))
            .toList();
    assertThat(fileCarriesLeaf).containsExactlyInAnyOrder(true, false);

    // Read with the schema of the file that lacks the leaf, the engine meets an element whose
    // struct has no such field. Tolerant traversal yields an empty value for it, and each file's
    // elements still come back as one row each. The first file's values are not visible through
    // this schema; reading divergent files as one dataset is Phase 10's merge.
    final Dataset<Row> leafAbsent = spark.read().schema(withoutLeaf.schema()).parquet(patientDir);
    assertThat(nameCarriesFamily(leafAbsent.schema())).isFalse();
    assertThat(families(leafAbsent))
        .containsExactlyInAnyOrder("null", "null", "null", "null", "null", "null");

    // Read with the schemas merged, the leaf is in the dataset and missing only from the second
    // file, so the first file's values come back beside the second file's empty ones.
    final Dataset<Row> merged = spark.read().option("mergeSchema", "true").parquet(patientDir);
    assertThat(nameCarriesFamily(merged.schema())).isTrue();
    assertThat(families(merged))
        .containsExactlyInAnyOrder("Alpha", "Beta", "Gamma", "null", "null", "null");
  }

  @Nonnull
  private List<String> families(@Nonnull final Dataset<Row> patients) throws IOException {
    final FhirView view = gson.fromJson(readFixture("view.json"), FhirView.class);
    final FhirViewExecutor executor =
        new FhirViewExecutor(
            fhirEncoders.getContext(), new DatasetDataSource(Map.of("Patient", patients)));
    return executor.buildQuery(view).collectAsList().stream()
        .map(row -> Objects.toString(row.get(0)))
        .toList();
  }

  private static boolean nameCarriesFamily(@Nonnull final StructType schema) {
    final StructType name =
        (StructType) ((ArrayType) schema.apply("name").dataType()).elementType();
    return Arrays.asList(name.fieldNames()).contains("family");
  }

  @Nonnull
  private Dataset<Row> encode(@Nonnull final String fileName, @Nonnull final TestLayout layout)
      throws IOException {
    final List<String> json =
        readFixture(fileName).lines().filter(line -> !line.isBlank()).toList();
    return LayoutDatasets.fromJson(spark, fhirEncoders, layout, "Patient", json);
  }

  @Nonnull
  private String readFixture(@Nonnull final String fileName) throws IOException {
    try (final InputStream input = getClass().getResourceAsStream(FIXTURE_DIR + fileName)) {
      return new String(Objects.requireNonNull(input).readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
