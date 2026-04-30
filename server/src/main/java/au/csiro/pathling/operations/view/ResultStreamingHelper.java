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

import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Shared utility methods for streaming query results in different formats (NDJSON, CSV, JSON). Used
 * by both {@link ViewExecutionHelper} and the {@code $sqlquery-run} operation.
 */
public class ResultStreamingHelper {

  @Nonnull private final Gson gson;

  /**
   * Constructs a new ResultStreamingHelper.
   *
   * @param gson the Gson instance for JSON serialisation
   */
  public ResultStreamingHelper(@Nonnull final Gson gson) {
    this.gson = gson;
  }

  /**
   * Writes the CSV header row.
   *
   * @param outputStream the output stream to write to
   * @param columnNames the column names for the header
   * @throws IOException if writing fails
   */
  public void writeCsvHeader(
      @Nonnull final OutputStream outputStream, @Nonnull final List<String> columnNames)
      throws IOException {
    final OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
    final CSVPrinter printer =
        new CSVPrinter(
            writer,
            CSVFormat.DEFAULT
                .builder()
                .setHeader(columnNames.toArray(String[]::new))
                .setSkipHeaderRecord(false)
                .build());
    printer.flush();
  }

  /**
   * Streams results as NDJSON.
   *
   * @param outputStream the output stream to write to
   * @param iterator the row iterator
   * @param schema the result schema
   * @throws IOException if writing fails
   */
  public void streamNdjson(
      @Nonnull final OutputStream outputStream,
      @Nonnull final Iterator<Row> iterator,
      @Nonnull final StructType schema)
      throws IOException {
    while (iterator.hasNext()) {
      final Row row = iterator.next();
      final String json = rowToJson(row, schema);
      outputStream.write(json.getBytes(StandardCharsets.UTF_8));
      outputStream.write('\n');
      outputStream.flush();
    }
  }

  /**
   * Streams results as CSV. The header is written separately for TTFB optimisation.
   *
   * @param outputStream the output stream to write to
   * @param iterator the row iterator
   * @param schema the result schema
   * @throws IOException if writing fails
   */
  public void streamCsv(
      @Nonnull final OutputStream outputStream,
      @Nonnull final Iterator<Row> iterator,
      @Nonnull final StructType schema)
      throws IOException {
    final OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
    final CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT);

    while (iterator.hasNext()) {
      final Row row = iterator.next();
      printer.printRecord(rowToList(row, schema));
      printer.flush();
    }
  }

  /**
   * Writes results as a single JSON document containing an array. Unlike NDJSON, this cannot be
   * streamed row-by-row as it requires proper JSON array formatting.
   *
   * @param outputStream the output stream to write to
   * @param iterator the row iterator
   * @param schema the result schema
   * @throws IOException if writing fails
   */
  public void writeJson(
      @Nonnull final OutputStream outputStream,
      @Nonnull final Iterator<Row> iterator,
      @Nonnull final StructType schema)
      throws IOException {
    final List<Map<String, Object>> rows = new ArrayList<>();
    while (iterator.hasNext()) {
      final Row row = iterator.next();
      rows.add(rowToMap(row, schema));
    }
    final String json = gson.toJson(rows);
    outputStream.write(json.getBytes(StandardCharsets.UTF_8));
  }

  /** Converts a Spark Row to a Map for JSON serialisation. */
  @Nonnull
  public Map<String, Object> rowToMap(@Nonnull final Row row, @Nonnull final StructType schema) {
    final Map<String, Object> map = new LinkedHashMap<>();
    final StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      final String name = fields[i].name();
      final Object value = row.get(i);
      if (value != null) {
        map.put(name, convertValue(value, fields[i].dataType()));
      }
    }
    return map;
  }

  /** Converts a Spark Row to a JSON string. */
  @Nonnull
  public String rowToJson(@Nonnull final Row row, @Nonnull final StructType schema) {
    return gson.toJson(rowToMap(row, schema));
  }

  /** Converts a value for JSON serialisation. */
  @Nullable
  public Object convertValue(
      @Nullable final Object value, @Nonnull final org.apache.spark.sql.types.DataType dataType) {
    if (value == null) {
      return null;
    }
    if (value instanceof final Row nestedRow && dataType instanceof final StructType structType) {
      final Map<String, Object> nested = new LinkedHashMap<>();
      final StructField[] fields = structType.fields();
      for (int i = 0; i < fields.length; i++) {
        final Object nestedValue = nestedRow.get(i);
        if (nestedValue != null) {
          nested.put(fields[i].name(), convertValue(nestedValue, fields[i].dataType()));
        }
      }
      return nested;
    }
    if (value instanceof final scala.collection.Seq<?> seq) {
      final List<?> list = CollectionConverters.asJava(seq);
      if (dataType instanceof final ArrayType arrayType) {
        return list.stream().map(item -> convertValue(item, arrayType.elementType())).toList();
      }
      return list;
    }
    return value;
  }

  /**
   * Streams results as a FHIR {@code Parameters} resource in JSON, with one repeating {@code row}
   * parameter per result row. Each column is rendered as a part with a typed {@code value[x]}
   * matched to the Spark column type. NULL values are represented by omitting the part.
   *
   * <p>The output is written row-by-row so memory stays bounded regardless of result size.
   *
   * @param outputStream the output stream to write to
   * @param iterator the row iterator
   * @param schema the result schema
   * @throws IOException if writing fails
   * @throws UnprocessableEntityException if any column has a Spark type that cannot be mapped to a
   *     FHIR primitive
   */
  public void streamFhirJson(
      @Nonnull final OutputStream outputStream,
      @Nonnull final Iterator<Row> iterator,
      @Nonnull final StructType schema)
      throws IOException {

    rejectUnsupportedColumnTypes(schema);

    final OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
    try (final JsonWriter json = new JsonWriter(writer)) {
      json.beginObject();
      json.name("resourceType").value("Parameters");
      json.name("parameter").beginArray();
      while (iterator.hasNext()) {
        final Row row = iterator.next();
        writeFhirRow(json, row, schema);
      }
      json.endArray();
      json.endObject();
    }
    outputStream.flush();
  }

  /** Walks the result schema once and rejects any column whose type cannot be FHIR-encoded. */
  private void rejectUnsupportedColumnTypes(@Nonnull final StructType schema) {
    for (final StructField field : schema.fields()) {
      final DataType dt = field.dataType();
      if (dt instanceof ArrayType || dt instanceof MapType || dt instanceof StructType) {
        throw new UnprocessableEntityException(
            "Column '"
                + field.name()
                + "' of type "
                + dt.simpleString()
                + " cannot be expressed as a FHIR primitive; use a different _format");
      }
    }
  }

  private void writeFhirRow(
      @Nonnull final JsonWriter json, @Nonnull final Row row, @Nonnull final StructType schema)
      throws IOException {
    json.beginObject();
    json.name("name").value("row");
    json.name("part").beginArray();
    final StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      final Object value = row.get(i);
      if (value == null) {
        // Per spec, SQL NULL is represented by omitting the part.
        continue;
      }
      writeFhirPart(json, fields[i], value);
    }
    json.endArray();
    json.endObject();
  }

  private void writeFhirPart(
      @Nonnull final JsonWriter json, @Nonnull final StructField field, @Nonnull final Object value)
      throws IOException {
    json.beginObject();
    json.name("name").value(field.name());

    final DataType dt = field.dataType();
    if (dt instanceof BooleanType) {
      json.name("valueBoolean").value((Boolean) value);
    } else if (dt instanceof IntegerType || dt instanceof ShortType || dt instanceof ByteType) {
      json.name("valueInteger").value(((Number) value).intValue());
    } else if (dt instanceof LongType
        || dt instanceof DecimalType
        || dt instanceof DoubleType
        || dt instanceof FloatType) {
      json.name("valueDecimal").value(toBigDecimal((Number) value));
    } else if (dt instanceof StringType) {
      json.name("valueString").value(value.toString());
    } else if (dt instanceof DateType) {
      json.name("valueDate").value(formatDate(value));
    } else if (dt instanceof TimestampType) {
      json.name("valueInstant").value(formatInstant(value));
    } else if (dt instanceof TimestampNTZType) {
      json.name("valueDateTime").value(formatLocalDateTime(value));
    } else if (dt instanceof BinaryType) {
      json.name("valueBase64Binary").value(Base64.getEncoder().encodeToString((byte[]) value));
    } else {
      throw new UnprocessableEntityException(
          "Column '"
              + field.name()
              + "' of type "
              + dt.simpleString()
              + " cannot be expressed as a FHIR primitive");
    }

    json.endObject();
  }

  @Nonnull
  private BigDecimal toBigDecimal(@Nonnull final Number value) {
    if (value instanceof final BigDecimal bd) {
      return bd;
    }
    if (value instanceof final java.math.BigInteger bi) {
      return new BigDecimal(bi);
    }
    if (value instanceof final Double d) {
      return BigDecimal.valueOf(d);
    }
    if (value instanceof final Float f) {
      return BigDecimal.valueOf(f.doubleValue());
    }
    return BigDecimal.valueOf(value.longValue());
  }

  @Nonnull
  private String formatDate(@Nonnull final Object value) {
    if (value instanceof final LocalDate ld) {
      return ld.toString();
    }
    if (value instanceof final java.sql.Date sqlDate) {
      return sqlDate.toLocalDate().toString();
    }
    return value.toString();
  }

  @Nonnull
  private String formatInstant(@Nonnull final Object value) {
    if (value instanceof final Instant instant) {
      return instant.toString();
    }
    if (value instanceof final java.sql.Timestamp ts) {
      return ts.toInstant().toString();
    }
    return value.toString();
  }

  @Nonnull
  private String formatLocalDateTime(@Nonnull final Object value) {
    if (value instanceof final LocalDateTime ldt) {
      return ldt.toString();
    }
    return value.toString();
  }

  /** Converts a Spark Row to a list of values for CSV output. */
  @Nonnull
  public List<Object> rowToList(@Nonnull final Row row, @Nonnull final StructType schema) {
    final List<Object> values = new ArrayList<>();
    final StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      final Object value = row.get(i);
      values.add(convertValueForCsv(value, fields[i].dataType()));
    }
    return values;
  }

  /** Converts a value for CSV output. */
  @Nullable
  public Object convertValueForCsv(
      @Nullable final Object value, @Nonnull final org.apache.spark.sql.types.DataType dataType) {
    if (value == null) {
      return null;
    }
    if (value instanceof Row || value instanceof scala.collection.Seq<?>) {
      return gson.toJson(convertValue(value, dataType));
    }
    return value;
  }
}
