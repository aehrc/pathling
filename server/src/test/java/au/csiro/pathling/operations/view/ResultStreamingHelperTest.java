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

package au.csiro.pathling.operations.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ResultStreamingHelper#streamFhirJson} — covers the typed Spark→FHIR mapping,
 * NULL handling, and rejection of unsupported column types.
 */
class ResultStreamingHelperTest {

  private ResultStreamingHelper helper;

  @BeforeEach
  void setUp() {
    helper = new ResultStreamingHelper(new Gson());
  }

  // ---------------------------------------------------------------------------
  // Top-level envelope.
  // ---------------------------------------------------------------------------

  @Test
  void emitsParametersResourceEnvelope() throws Exception {
    final StructType schema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, false)
            });
    final List<Row> rows = List.of(RowFactory.create("alice"));

    final JsonObject result = streamAndParse(rows, schema);
    assertThat(result.get("resourceType").getAsString()).isEqualTo("Parameters");
    assertThat(result.has("parameter")).isTrue();
  }

  @Test
  void emitsOneRowParameterPerResultRow() throws Exception {
    final StructType schema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false)
            });
    final List<Row> rows =
        List.of(RowFactory.create(1), RowFactory.create(2), RowFactory.create(3));

    final JsonArray parameters = streamAndParse(rows, schema).getAsJsonArray("parameter");
    assertThat(parameters.size()).isEqualTo(3);
    for (int i = 0; i < parameters.size(); i++) {
      assertThat(parameters.get(i).getAsJsonObject().get("name").getAsString()).isEqualTo("row");
    }
  }

  // ---------------------------------------------------------------------------
  // Type mapping.
  // ---------------------------------------------------------------------------

  @Test
  void mapsBooleanColumnToValueBoolean() throws Exception {
    final JsonObject part = onlyPart("active", DataTypes.BooleanType, true);
    assertThat(part.get("valueBoolean").getAsBoolean()).isTrue();
  }

  @Test
  void mapsIntegerColumnToValueInteger() throws Exception {
    final JsonObject part = onlyPart("age", DataTypes.IntegerType, 42);
    assertThat(part.get("valueInteger").getAsInt()).isEqualTo(42);
  }

  @Test
  void mapsLongColumnToValueDecimal() throws Exception {
    final JsonObject part = onlyPart("count", DataTypes.LongType, 1234567890123L);
    assertThat(part.get("valueDecimal").getAsLong()).isEqualTo(1234567890123L);
  }

  @Test
  void mapsDoubleColumnToValueDecimal() throws Exception {
    final JsonObject part = onlyPart("score", DataTypes.DoubleType, 3.14);
    assertThat(part.get("valueDecimal").getAsDouble()).isEqualTo(3.14);
  }

  @Test
  void mapsStringColumnToValueString() throws Exception {
    final JsonObject part = onlyPart("name", DataTypes.StringType, "alice");
    assertThat(part.get("valueString").getAsString()).isEqualTo("alice");
  }

  @Test
  void mapsDateColumnToValueDate() throws Exception {
    final JsonObject part = onlyPart("birth_date", DataTypes.DateType, Date.valueOf("1990-05-15"));
    assertThat(part.get("valueDate").getAsString()).isEqualTo("1990-05-15");
  }

  @Test
  void mapsTimestampColumnToValueInstant() throws Exception {
    final JsonObject part =
        onlyPart(
            "created_at",
            DataTypes.TimestampType,
            Timestamp.from(java.time.Instant.parse("2026-01-15T10:30:00Z")));
    assertThat(part.get("valueInstant").getAsString()).isEqualTo("2026-01-15T10:30:00Z");
  }

  @Test
  void mapsBinaryColumnToValueBase64Binary() throws Exception {
    final byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
    final JsonObject part = onlyPart("blob", DataTypes.BinaryType, bytes);
    assertThat(part.get("valueBase64Binary").getAsString())
        .isEqualTo(java.util.Base64.getEncoder().encodeToString(bytes));
  }

  // ---------------------------------------------------------------------------
  // NULL handling.
  // ---------------------------------------------------------------------------

  @Test
  void omitsPartForNullValue() throws Exception {
    final StructType schema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("nickname", DataTypes.StringType, true)
            });
    final List<Row> rows = List.of(RowFactory.create(7, null));

    final JsonObject row =
        streamAndParse(rows, schema).getAsJsonArray("parameter").get(0).getAsJsonObject();
    final JsonArray parts = row.getAsJsonArray("part");

    assertThat(parts.size()).isEqualTo(1);
    assertThat(parts.get(0).getAsJsonObject().get("name").getAsString()).isEqualTo("id");
  }

  // ---------------------------------------------------------------------------
  // Unsupported types.
  // ---------------------------------------------------------------------------

  @Test
  void rejectsArrayColumn() {
    final StructType schema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField(
                  "tags", DataTypes.createArrayType(DataTypes.StringType), true)
            });
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final Iterator<Row> rows = List.<Row>of().iterator();

    assertThatThrownBy(() -> helper.streamFhirJson(out, rows, schema))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContaining("tags");
  }

  @Test
  void rejectsStructColumn() {
    final StructType nested =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("inner", DataTypes.StringType, true)
            });
    final StructType schema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("payload", nested, true)
            });
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final Iterator<Row> rows = List.<Row>of().iterator();

    assertThatThrownBy(() -> helper.streamFhirJson(out, rows, schema))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContaining("payload");
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private JsonObject streamAndParse(final List<Row> rows, final StructType schema)
      throws Exception {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final Iterator<Row> iterator = rows.iterator();
    helper.streamFhirJson(out, iterator, schema);
    return JsonParser.parseString(out.toString(StandardCharsets.UTF_8)).getAsJsonObject();
  }

  private JsonObject onlyPart(
      final String columnName, final org.apache.spark.sql.types.DataType type, final Object value)
      throws Exception {
    final StructType schema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField(columnName, type, true)
            });
    final List<Row> rows = List.of(RowFactory.create(value));
    final JsonObject root = streamAndParse(rows, schema);
    final JsonArray parameters = root.getAsJsonArray("parameter");
    assertThat(parameters.size()).isEqualTo(1);
    final JsonArray parts = parameters.get(0).getAsJsonObject().getAsJsonArray("part");
    assertThat(parts.size()).isEqualTo(1);
    final JsonObject part = parts.get(0).getAsJsonObject();
    assertThat(part.get("name").getAsString()).isEqualTo(columnName);
    return part;
  }
}
