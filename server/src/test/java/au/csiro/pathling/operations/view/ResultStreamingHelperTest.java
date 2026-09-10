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

import au.csiro.pathling.views.ViewDefinitionGson;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Tests for {@link ResultStreamingHelper} - covers the typed Spark to FHIR mapping, the NDJSON,
 * JSON and CSV output forms, NULL handling, the serialisation of the {@code java.time} values Spark
 * materialises for {@code TIMESTAMP_NTZ} and the interval types, and rejection of column types the
 * FHIR Parameters format cannot express.
 *
 * @author John Grimes
 */
class ResultStreamingHelperTest {

  private ResultStreamingHelper helper;

  @BeforeEach
  void setUp() {
    helper = new ResultStreamingHelper(ViewDefinitionGson.create());
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
  // NDJSON streaming.
  // ---------------------------------------------------------------------------

  @Test
  void streamsNdjsonOneLinePerRow() throws Exception {
    final StructType schema = idNameSchema();
    final List<Row> rows = List.of(RowFactory.create(1, "alice"), RowFactory.create(2, "bob"));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    helper.streamNdjson(out, rows.iterator(), schema);

    final String body = out.toString(StandardCharsets.UTF_8);
    final String[] lines = body.split("\n");
    assertThat(lines).hasSize(2);
    assertThat(lines[0]).contains("\"id\":1").contains("\"name\":\"alice\"");
    assertThat(lines[1]).contains("\"id\":2").contains("\"name\":\"bob\"");
  }

  @Test
  void streamsNdjsonOmitsNullValuesPerSpec() throws Exception {
    final StructType schema = idNameSchema();
    final List<Row> rows = List.of(RowFactory.create(1, null));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    helper.streamNdjson(out, rows.iterator(), schema);

    assertThat(out.toString(StandardCharsets.UTF_8)).contains("\"id\":1").doesNotContain("name");
  }

  // ---------------------------------------------------------------------------
  // CSV header + body streaming.
  // ---------------------------------------------------------------------------

  @Test
  void writeCsvHeaderEmitsCommaSeparatedColumnNames() throws Exception {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    helper.writeCsvHeader(out, List.of("id", "name", "active"));

    assertThat(out.toString(StandardCharsets.UTF_8)).startsWith("id,name,active");
  }

  @Test
  void streamsCsvOneRecordPerRow() throws Exception {
    final StructType schema = idNameSchema();
    final List<Row> rows = List.of(RowFactory.create(1, "alice"), RowFactory.create(2, "bob"));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    helper.streamCsv(out, rows.iterator(), schema);

    final String body = out.toString(StandardCharsets.UTF_8);
    assertThat(body).contains("1,alice").contains("2,bob");
  }

  @Test
  void streamsCsvQuotesValueContainingDelimiter() throws Exception {
    final StructType schema = idNameSchema();
    final List<Row> rows = List.of(RowFactory.create(1, "smith, john"));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    helper.streamCsv(out, rows.iterator(), schema);

    assertThat(out.toString(StandardCharsets.UTF_8)).contains("\"smith, john\"");
  }

  // ---------------------------------------------------------------------------
  // Single-document JSON.
  // ---------------------------------------------------------------------------

  @Test
  void writeJsonEmitsArrayOfObjects() throws Exception {
    final StructType schema = idNameSchema();
    final List<Row> rows = List.of(RowFactory.create(1, "alice"), RowFactory.create(2, "bob"));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    helper.writeJson(out, rows.iterator(), schema);

    final JsonArray array =
        JsonParser.parseString(out.toString(StandardCharsets.UTF_8)).getAsJsonArray();
    assertThat(array.size()).isEqualTo(2);
    final JsonObject first = array.get(0).getAsJsonObject();
    assertThat(first.get("id").getAsInt()).isEqualTo(1);
    assertThat(first.get("name").getAsString()).isEqualTo("alice");
  }

  @Test
  void writeJsonEmitsEmptyArrayForNoRows() throws Exception {
    final StructType schema = idNameSchema();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    helper.writeJson(out, List.<Row>of().iterator(), schema);

    assertThat(out.toString(StandardCharsets.UTF_8)).isEqualTo("[]");
  }

  // ---------------------------------------------------------------------------
  // java.time-backed values.
  //
  // Spark materialises TIMESTAMP_NTZ as java.time.LocalDateTime, day-time intervals as
  // java.time.Duration and year-month intervals as java.time.Period. Gson has no built-in
  // adapter for any of these, and the module system blocks its reflective fallback, so these
  // cases fail until ISO-8601 adapters are registered in ViewDefinitionGson.create(). The
  // expected strings are the JDK toString() forms, which are what CSV has always emitted.
  // ---------------------------------------------------------------------------

  /** Verifies NDJSON renders each java.time value as its canonical ISO-8601 string. */
  @Test
  void serialisesJavaTimeValuesAsIso8601StringsInNdjson() throws Exception {
    final StructType schema = javaTimeSchema();
    final List<Row> rows =
        javaTimeRows(
            LocalDateTime.parse("2020-01-01T12:00:00"), Duration.ofHours(1), Period.ofYears(1));

    final JsonObject json = onlyNdjsonObject(rows, schema);

    assertJavaTimeMembers(json, "2020-01-01T12:00", "PT1H", "P1Y");
  }

  /** Verifies the single-document JSON array carries the same strings as the NDJSON output. */
  @Test
  void serialisesJavaTimeValuesAsIso8601StringsInJson() throws Exception {
    final StructType schema = javaTimeSchema();
    final List<Row> rows =
        javaTimeRows(
            LocalDateTime.parse("2020-01-01T12:00:00"), Duration.ofHours(1), Period.ofYears(1));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    helper.writeJson(out, rows.iterator(), schema);

    final JsonArray array =
        JsonParser.parseString(out.toString(StandardCharsets.UTF_8)).getAsJsonArray();
    assertThat(array.size()).isEqualTo(1);
    assertJavaTimeMembers(array.get(0).getAsJsonObject(), "2020-01-01T12:00", "PT1H", "P1Y");
  }

  /** Verifies sub-second precision is preserved rather than truncated to whole minutes. */
  @Test
  void serialisesLocalDateTimeWithFractionalSecondsAsIso8601String() throws Exception {
    final StructType schema = javaTimeSchema();
    final List<Row> rows =
        javaTimeRows(
            LocalDateTime.parse("2020-01-01T12:00:00.123"), Duration.ofHours(1), Period.ofYears(1));

    final JsonObject json = onlyNdjsonObject(rows, schema);

    assertJavaTimeMembers(json, "2020-01-01T12:00:00.123", "PT1H", "P1Y");
  }

  /**
   * Verifies negative intervals keep the JDK sign form, where the sign sits on the field rather
   * than in front of the period designator: {@code PT-1H}, not {@code -PT1H}.
   */
  @Test
  void serialisesNegativeIntervalsUsingJdkSignForm() throws Exception {
    final StructType schema = javaTimeSchema();
    final List<Row> rows =
        javaTimeRows(
            LocalDateTime.parse("2020-01-01T12:00:00"), Duration.ofHours(-1), Period.ofYears(-1));

    final JsonObject json = onlyNdjsonObject(rows, schema);

    assertJavaTimeMembers(json, "2020-01-01T12:00", "PT-1H", "P-1Y");
  }

  /**
   * Verifies a DATE column holding a {@link LocalDate} serialises as an ISO-8601 date. Spark only
   * materialises this Java type when {@code spark.sql.datetime.java8API.enabled} is set, and {@link
   * RowFactory} reproduces that pairing directly because it applies no conversion of its own.
   */
  @Test
  void serialisesLocalDateAsIso8601String() throws Exception {
    final StructType schema = schemaOf(nullableField("birth_date", DataTypes.DateType));
    final List<Row> rows = List.of(RowFactory.create(LocalDate.parse("2020-01-01")));

    final JsonObject json = onlyNdjsonObject(rows, schema);

    assertThat(json.get("birth_date").getAsString()).isEqualTo("2020-01-01");
  }

  /**
   * Verifies a TIMESTAMP column holding an {@link Instant} serialises as an ISO-8601 instant, again
   * only reachable with Spark's Java 8 datetime API enabled.
   */
  @Test
  void serialisesInstantAsIso8601String() throws Exception {
    final StructType schema = schemaOf(nullableField("created_at", DataTypes.TimestampType));
    final List<Row> rows = List.of(RowFactory.create(Instant.parse("2020-01-01T00:00:00Z")));

    final JsonObject json = onlyNdjsonObject(rows, schema);

    assertThat(json.get("created_at").getAsString()).isEqualTo("2020-01-01T00:00:00Z");
  }

  /** Verifies a null TIMESTAMP_NTZ value still omits the member entirely, as it did before. */
  @Test
  void omitsMemberForNullTimestampNtzValueInNdjson() throws Exception {
    final StructType schema =
        schemaOf(
            nullableField("id", DataTypes.IntegerType),
            nullableField("ts_ntz", DataTypes.TimestampNTZType));
    final List<Row> rows = List.of(RowFactory.create(1, null));

    final JsonObject json = onlyNdjsonObject(rows, schema);

    assertThat(json.get("id").getAsInt()).isEqualTo(1);
    assertThat(json.has("ts_ntz")).isFalse();
  }

  /**
   * Verifies ARRAY&lt;TIMESTAMP_NTZ&gt; elements serialise as canonical strings inside the nested
   * JSON array. Spark hands array columns to the helper as a Scala sequence, so the value is built
   * that way here to reach the array branch of convertValue.
   */
  @Test
  void serialisesTimestampNtzInsideArrayAsIso8601Strings() throws Exception {
    final StructType schema =
        schemaOf(
            nullableField("timestamps", DataTypes.createArrayType(DataTypes.TimestampNTZType)));
    final scala.collection.Seq<Object> values =
        CollectionConverters.asScala(
                List.<Object>of(
                    LocalDateTime.parse("2020-01-01T12:00:00"),
                    LocalDateTime.parse("2021-06-30T08:15:30")))
            .toSeq();
    final List<Row> rows = List.of(RowFactory.create(values));

    final JsonObject json = onlyNdjsonObject(rows, schema);

    final JsonArray timestamps = json.getAsJsonArray("timestamps");
    assertThat(timestamps.size()).isEqualTo(2);
    assertThat(timestamps.get(0).getAsString()).isEqualTo("2020-01-01T12:00");
    assertThat(timestamps.get(1).getAsString()).isEqualTo("2021-06-30T08:15:30");
  }

  /** Verifies a TIMESTAMP_NTZ field inside a struct column serialises as a canonical string. */
  @Test
  void serialisesTimestampNtzInsideStructAsIso8601String() throws Exception {
    final StructType nested = schemaOf(nullableField("recorded", DataTypes.TimestampNTZType));
    final StructType schema = schemaOf(nullableField("event", nested));
    final Row nestedRow = RowFactory.create(LocalDateTime.parse("2020-01-01T12:00:00"));
    final List<Row> rows = List.of(RowFactory.create(nestedRow));

    final JsonObject json = onlyNdjsonObject(rows, schema);

    assertThat(json.getAsJsonObject("event").get("recorded").getAsString())
        .isEqualTo("2020-01-01T12:00");
  }

  /**
   * Verifies CSV already emits the canonical strings, so this assertion holds both before and after
   * the JSON adapters are registered. The CSV printer calls toString() on the value, which yields
   * the same form the adapters produce.
   */
  @Test
  void streamsCsvWithIso8601StringsForJavaTimeValues() throws Exception {
    final StructType schema = javaTimeSchema();
    final List<Row> rows =
        javaTimeRows(
            LocalDateTime.parse("2020-01-01T12:00:00"), Duration.ofHours(1), Period.ofYears(1));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    helper.streamCsv(out, rows.iterator(), schema);

    assertThat(out.toString(StandardCharsets.UTF_8).trim()).isEqualTo("2020-01-01T12:00,PT1H,P1Y");
  }

  /**
   * Verifies the FHIR Parameters format is unaffected: a TIMESTAMP_NTZ column still maps to
   * valueDateTime carrying the same canonical string.
   */
  @Test
  void mapsTimestampNtzColumnToValueDateTime() throws Exception {
    final JsonObject part =
        onlyPart("ts_ntz", DataTypes.TimestampNTZType, LocalDateTime.parse("2020-01-01T12:00:00"));
    assertThat(part.get("valueDateTime").getAsString()).isEqualTo("2020-01-01T12:00");
  }

  /**
   * Verifies interval columns are still rejected by the FHIR Parameters format, which has no
   * primitive able to carry them.
   */
  @Test
  void rejectsDayTimeIntervalColumnInFhirJson() {
    final StructType schema = schemaOf(nullableField("dt", DataTypes.createDayTimeIntervalType()));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final Iterator<Row> rows = List.of(RowFactory.create(Duration.ofHours(1))).iterator();

    assertThatThrownBy(() -> helper.streamFhirJson(out, rows, schema))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContaining("dt");
  }

  // ---------------------------------------------------------------------------
  // Misc value conversion.
  // ---------------------------------------------------------------------------

  @Test
  void convertValueReturnsNullForNullInput() {
    assertThat(helper.convertValue(null, DataTypes.StringType)).isNull();
  }

  @Test
  void convertValueForCsvJsonifiesNestedRow() {
    final StructType nested =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("inner", DataTypes.StringType, true)
            });
    final Row nestedRow = RowFactory.create("hello");
    final Object converted = helper.convertValueForCsv(nestedRow, nested);
    assertThat(converted).isInstanceOf(String.class);
    assertThat((String) converted).contains("\"inner\"").contains("\"hello\"");
  }

  @Test
  void convertValueForCsvPassesThroughScalars() {
    assertThat(helper.convertValueForCsv("alice", DataTypes.StringType)).isEqualTo("alice");
    assertThat(helper.convertValueForCsv(42, DataTypes.IntegerType)).isEqualTo(42);
    assertThat(helper.convertValueForCsv(null, DataTypes.StringType)).isNull();
  }

  @Test
  void rowToListPreservesColumnOrder() {
    final StructType schema = idNameSchema();
    final Row row = RowFactory.create(7, "alice");
    assertThat(helper.rowToList(row, schema)).containsExactly(7, "alice");
  }

  private static StructType idNameSchema() {
    return DataTypes.createStructType(
        new org.apache.spark.sql.types.StructField[] {
          DataTypes.createStructField("id", DataTypes.IntegerType, false),
          DataTypes.createStructField("name", DataTypes.StringType, true)
        });
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

  /** Builds a struct type from the supplied fields. */
  private static StructType schemaOf(final org.apache.spark.sql.types.StructField... fields) {
    return DataTypes.createStructType(fields);
  }

  /** Builds a nullable struct field. */
  private static org.apache.spark.sql.types.StructField nullableField(
      final String name, final org.apache.spark.sql.types.DataType type) {
    return DataTypes.createStructField(name, type, true);
  }

  /** Schema with a TIMESTAMP_NTZ, a day-time interval and a year-month interval column. */
  private static StructType javaTimeSchema() {
    return schemaOf(
        nullableField("ts_ntz", DataTypes.TimestampNTZType),
        nullableField("dt", DataTypes.createDayTimeIntervalType()),
        nullableField("ym", DataTypes.createYearMonthIntervalType()));
  }

  /** Builds a single row matching the column order of {@link #javaTimeSchema()}. */
  private static List<Row> javaTimeRows(
      final LocalDateTime timestamp, final Duration dayTime, final Period yearMonth) {
    return List.of(RowFactory.create(timestamp, dayTime, yearMonth));
  }

  /** Streams the rows as NDJSON, asserts a single line was written, and returns it parsed. */
  private JsonObject onlyNdjsonObject(final List<Row> rows, final StructType schema)
      throws Exception {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.streamNdjson(out, rows.iterator(), schema);
    final String[] lines = out.toString(StandardCharsets.UTF_8).split("\n");
    assertThat(lines).hasSize(1);
    return JsonParser.parseString(lines[0]).getAsJsonObject();
  }

  /** Asserts the three java.time columns carry the expected canonical strings. */
  private static void assertJavaTimeMembers(
      final JsonObject json, final String timestamp, final String dayTime, final String yearMonth) {
    assertThat(json.get("ts_ntz").getAsString()).isEqualTo(timestamp);
    assertThat(json.get("dt").getAsString()).isEqualTo(dayTime);
    assertThat(json.get("ym").getAsString()).isEqualTo(yearMonth);
  }
}
