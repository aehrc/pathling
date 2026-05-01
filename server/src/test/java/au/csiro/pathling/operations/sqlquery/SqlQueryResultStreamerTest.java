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

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * Tests for {@link SqlQueryResultStreamer} covering each output format. Uses a real local
 * SparkSession to materialise a small Dataset and a Spring {@link MockHttpServletResponse} to
 * capture written bytes and headers.
 */
@TestInstance(Lifecycle.PER_CLASS)
class SqlQueryResultStreamerTest {

  private SparkSession spark;
  private SqlQueryResultStreamer streamer;

  @BeforeAll
  void setUpAll() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("SqlQueryResultStreamerTest")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", false)
            .config("spark.sql.shuffle.partitions", 1)
            .getOrCreate();
    streamer = new SqlQueryResultStreamer();
  }

  @AfterAll
  void tearDownAll() {
    if (spark != null) {
      spark.stop();
    }
  }

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
    assertThat(body).startsWith("[").endsWith("]");
    assertThat(body).contains("\"alice\"").contains("\"bob\"");
  }

  @Test
  void streamsCsvWithoutHeaderByDefault() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    streamer.stream(twoRowDataset(), SqlQueryOutputFormat.CSV, false, response);

    assertThat(response.getContentType()).startsWith("text/csv");
    final String body = new String(response.getContentAsByteArray(), StandardCharsets.UTF_8);
    assertThat(body).doesNotContain("id,name");
    assertThat(body).contains("alice").contains("bob");
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
