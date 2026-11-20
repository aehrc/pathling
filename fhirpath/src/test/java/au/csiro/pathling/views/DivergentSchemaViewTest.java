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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.DatasetDataSource;
import au.csiro.pathling.test.layout.PofDatasets;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
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
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
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
 * <p>The resources are read in the Parquet on FHIR layout. The first case needs the leaf carried as
 * a null in the second file, so it reads both batches together, under the one fitted schema that
 * covers them, and writes each batch to its own file. The second case reads each batch on its own,
 * so the leaf is absent from the second file because that file populates none.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class DivergentSchemaViewTest {

  private static final String FIXTURE_DIR = "/viewTests/divergent_schema/";

  @Autowired SparkSession spark;

  @Autowired FhirContext fhirContext;

  @Autowired Gson gson;

  @TempDir Path tempDir;

  @Test
  void unnestingReturnsRowsFromBothFiles() throws IOException {
    // Write each batch to its own Parquet file in one directory, and read the directory back as a
    // single dataset, so that the engine reads real files rather than an in-memory union.
    final String patientDir = tempDir.resolve("Patient").toString();
    // Both batches are read together, so that the fitted schema carries the leaf, and the second
    // batch's names carry it as a null. Read on its own, the second batch would not carry the leaf,
    // and a read without merging would take its schema from whichever file it read first; the next
    // test covers that case.
    final List<String> firstBatch = lines("batch-1.ndjson");
    final Dataset<Row> both =
        PofDatasets.fromJson(
                spark,
                fhirContext,
                "Patient",
                Stream.concat(firstBatch.stream(), lines("batch-2.ndjson").stream()).toList())
            .cache();
    final Column inFirstBatch = col("id").isin(ids(firstBatch));
    both.filter(inFirstBatch).coalesce(1).write().mode(SaveMode.Overwrite).parquet(patientDir);
    both.filter(not(inFirstBatch)).coalesce(1).write().mode(SaveMode.Append).parquet(patientDir);
    final Dataset<Row> patients = spark.read().parquet(patientDir);
    assertThat(patients.inputFiles()).hasSize(2);
    assertThat(
            Arrays.stream(patients.inputFiles())
                .map(file -> nameCarriesFamily(spark.read().parquet(file).schema())))
        .containsExactly(true, true);

    final FhirView view = gson.fromJson(readFixture("view.json"), FhirView.class);
    final FhirViewExecutor executor =
        new FhirViewExecutor(fhirContext, new DatasetDataSource(Map.of("Patient", patients)));
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
    // The second file lacks the projected leaf from its schema entirely, rather than carrying it as
    // a null, as a pruned table would store it (T027a). Read on its own, the second batch's fitted
    // schema lacks the leaf, because the batch populates none.
    final String patientDir = tempDir.resolve("Patient").toString();
    encode("batch-1.ndjson").coalesce(1).write().mode(SaveMode.Overwrite).parquet(patientDir);
    final Dataset<Row> withoutLeaf = encode("batch-2.ndjson");
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
        new FhirViewExecutor(fhirContext, new DatasetDataSource(Map.of("Patient", patients)));
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
  private Dataset<Row> encode(@Nonnull final String fileName) throws IOException {
    return PofDatasets.fromJson(spark, fhirContext, "Patient", lines(fileName));
  }

  @Nonnull
  private List<String> lines(@Nonnull final String fileName) throws IOException {
    return readFixture(fileName).lines().filter(line -> !line.isBlank()).toList();
  }

  @Nonnull
  private Object[] ids(@Nonnull final List<String> json) {
    final IParser parser = fhirContext.newJsonParser();
    return json.stream()
        .map(document -> parser.parseResource(document).getIdElement().getIdPart())
        .toArray();
  }

  @Nonnull
  private String readFixture(@Nonnull final String fileName) throws IOException {
    try (final InputStream input = getClass().getResourceAsStream(FIXTURE_DIR + fileName)) {
      return new String(Objects.requireNonNull(input).readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
