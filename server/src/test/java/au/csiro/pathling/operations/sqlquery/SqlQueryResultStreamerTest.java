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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.test.SpringBootUnitTest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * Tests for {@link SqlQueryResultStreamer} covering each output format. Uses the shared Spark
 * session to materialise a small Dataset and a Spring {@link MockHttpServletResponse} to capture
 * written bytes and headers.
 */
@SpringBootUnitTest
class SqlQueryResultStreamerTest {

  @Autowired private SparkSession spark;

  private final SqlQueryResultStreamer streamer = new SqlQueryResultStreamer();

  @Test
  void streamsNdjsonWithUtf8Encoding() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    streamer.stream(twoRowDataset(), SqlQueryOutputFormat.NDJSON, false, response);

    assertThat(response.getContentType()).startsWith("application/x-ndjson");
    assertThat(response.getCharacterEncoding()).isEqualToIgnoringCase("UTF-8");
    assertThat(response.getStatus()).isEqualTo(200);
    final String body = new String(response.getContentAsByteArray(), StandardCharsets.UTF_8);
    assertThat(body)
        .contains("\"id\":1")
        .contains("\"id\":2")
        .contains("\"name\":\"alice\"")
        .contains("\"name\":\"bob\"");
    // NDJSON: each row terminated by a newline.
    assertThat(body.split("\n")).hasSize(2);
  }

  @Test
  void streamsJsonAsArray() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    streamer.stream(twoRowDataset(), SqlQueryOutputFormat.JSON, false, response);

    assertThat(response.getContentType()).startsWith("application/json");
    final String body = new String(response.getContentAsByteArray(), StandardCharsets.UTF_8);
    assertThat(body).startsWith("[").endsWith("]").contains("\"alice\"").contains("\"bob\"");
  }

  @Test
  void streamsCsvWithoutHeaderByDefault() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    streamer.stream(twoRowDataset(), SqlQueryOutputFormat.CSV, false, response);

    assertThat(response.getContentType()).startsWith("text/csv");
    final String body = new String(response.getContentAsByteArray(), StandardCharsets.UTF_8);
    assertThat(body).doesNotContain("id,name").contains("alice").contains("bob");
  }

  @Test
  void streamsCsvWithHeaderWhenRequested() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    streamer.stream(twoRowDataset(), SqlQueryOutputFormat.CSV, true, response);

    final String body = new String(response.getContentAsByteArray(), StandardCharsets.UTF_8);
    assertThat(body).startsWith("id,name");
  }

  @Test
  void streamsFhirParametersResource() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    streamer.stream(twoRowDataset(), SqlQueryOutputFormat.FHIR, false, response);

    assertThat(response.getContentType()).startsWith("application/fhir+json");
    final String body = new String(response.getContentAsByteArray(), StandardCharsets.UTF_8);
    assertThat(body).contains("\"resourceType\":\"Parameters\"").contains("\"name\":\"row\"");
  }

  @Test
  void streamsParquetWithoutSettingCharacterEncoding() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    streamer.stream(twoRowDataset(), SqlQueryOutputFormat.PARQUET, false, response);

    assertThat(response.getContentType()).isEqualTo("application/vnd.apache.parquet");
    // PARQUET is binary — no UTF-8 charset should be set.
    assertThat(response.getCharacterEncoding()).isNotEqualToIgnoringCase("UTF-8");
    // Parquet files start with the magic bytes "PAR1".
    final byte[] bytes = response.getContentAsByteArray();
    assertThat(bytes).isNotEmpty();
    assertThat(new String(bytes, 0, 4, StandardCharsets.US_ASCII)).isEqualTo("PAR1");
  }

  @Test
  void parquetWithVoidColumnRejectedBeforeCreatingTempDirectory() throws IOException {
    // A NullType (VOID) column cannot be written to Parquet. The streamer must reject it with an
    // InvalidUserInputError (mapped to a 400) rather than letting Spark fail deep in the writer.
    final Set<Path> tempDirsBefore = sqlQueryParquetTempDirs();
    final MockHttpServletResponse response = new MockHttpServletResponse();

    assertThatThrownBy(
            () ->
                streamer.stream(voidColumnDataset(), SqlQueryOutputFormat.PARQUET, false, response))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("'foo'")
        .hasMessageContaining("CAST")
        .hasMessageContaining("output format");

    // The validation fires before any temporary directory is created, so no filesystem work
    // happens for a rejected request.
    assertThat(sqlQueryParquetTempDirs()).isEqualTo(tempDirsBefore);
  }

  @Test
  void csvWithVoidColumnStillSucceeds() {
    // Non-Parquet formats are unaffected by the new validation.
    final MockHttpServletResponse response = new MockHttpServletResponse();

    assertThatCode(
            () -> streamer.stream(voidColumnDataset(), SqlQueryOutputFormat.CSV, true, response))
        .doesNotThrowAnyException();

    assertThat(response.getStatus()).isEqualTo(200);
    final String body = new String(response.getContentAsByteArray(), StandardCharsets.UTF_8);
    assertThat(body).startsWith("id,foo");
  }

  @Test
  void parquetWithFullyTypedDatasetStillSucceeds() {
    // A fully typed dataset must still be written to Parquet exactly as before.
    final MockHttpServletResponse response = new MockHttpServletResponse();

    assertThatCode(
            () -> streamer.stream(twoRowDataset(), SqlQueryOutputFormat.PARQUET, false, response))
        .doesNotThrowAnyException();

    final byte[] bytes = response.getContentAsByteArray();
    assertThat(bytes).isNotEmpty();
    assertThat(new String(bytes, 0, 4, StandardCharsets.US_ASCII)).isEqualTo("PAR1");
  }

  /** Lists the streamer's Parquet temporary directories currently present under the temp root. */
  private Set<Path> sqlQueryParquetTempDirs() throws IOException {
    final Path tempRoot = Path.of(System.getProperty("java.io.tmpdir"));
    try (final Stream<Path> entries = Files.list(tempRoot)) {
      return entries
          .filter(p -> p.getFileName().toString().startsWith("sqlquery-parquet-"))
          .collect(Collectors.toSet());
    }
  }

  private Dataset<Row> voidColumnDataset() {
    // A literal NULL projection produces a NullType (VOID) column, reproducing the failure mode.
    return spark.range(2).toDF("id").selectExpr("id", "NULL AS foo");
  }

  private Dataset<Row> twoRowDataset() {
    final StructType schema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    final List<Row> rows = List.of(RowFactory.create(1, "alice"), RowFactory.create(2, "bob"));
    return spark.createDataFrame(rows, schema);
  }
}
