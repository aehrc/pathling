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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import au.csiro.pathling.schema.PrimitiveTypes;
import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests the converter table: what each FHIR primitive type stores from JSON inference, and what it
 * returns to JSON (decision 68).
 *
 * <p>Each type is converted in both cardinalities, because the converter is applied through an
 * array for a repeating element and the split between a scalar and an array column is where this
 * codebase has historically broken.
 */
class PrimitiveConverterTest {

  /** One value of every primitive type, as it appears in JSON, and what is stored for it. */
  @Nonnull
  static Stream<Arguments> primitives() {
    return Stream.of(
        Arguments.of("boolean", "true", DataTypes.BooleanType, true),
        Arguments.of("integer", "7", DataTypes.IntegerType, 7),
        Arguments.of("unsignedInt", "0", DataTypes.IntegerType, 0),
        Arguments.of("positiveInt", "5", DataTypes.IntegerType, 5),
        Arguments.of("integer64", "3000000000", DataTypes.LongType, 3000000000L),
        Arguments.of("decimal", "1.50", DataTypes.StringType, "1.5"),
        Arguments.of("date", "\"1980-01-01\"", DataTypes.StringType, "1980-01-01"),
        Arguments.of(
            "dateTime", "\"2020-01-01T10:00:00Z\"", DataTypes.StringType, "2020-01-01T10:00:00Z"),
        Arguments.of(
            "instant",
            "\"2020-01-01T10:00:00.000Z\"",
            DataTypes.StringType,
            "2020-01-01T10:00:00.000Z"),
        Arguments.of("time", "\"10:00:00\"", DataTypes.StringType, "10:00:00"),
        Arguments.of("string", "\"a string\"", DataTypes.StringType, "a string"),
        Arguments.of("code", "\"final\"", DataTypes.StringType, "final"),
        Arguments.of("uri", "\"urn:example\"", DataTypes.StringType, "urn:example"),
        Arguments.of("url", "\"http://example.org\"", DataTypes.StringType, "http://example.org"),
        Arguments.of(
            "canonical", "\"http://example.org|1\"", DataTypes.StringType, "http://example.org|1"),
        Arguments.of("oid", "\"urn:oid:1.2.3\"", DataTypes.StringType, "urn:oid:1.2.3"),
        Arguments.of(
            "uuid",
            "\"urn:uuid:c757873d-ec9a-4326-a141-556f43239520\"",
            DataTypes.StringType,
            "urn:uuid:c757873d-ec9a-4326-a141-556f43239520"),
        Arguments.of("id", "\"a1\"", DataTypes.StringType, "a1"),
        Arguments.of("markdown", "\"*emphasis*\"", DataTypes.StringType, "*emphasis*"),
        Arguments.of(
            "base64Binary",
            "\"aGVsbG8=\"",
            DataTypes.BinaryType,
            "hello".getBytes(StandardCharsets.US_ASCII)),
        Arguments.of("xhtml", "\"<div>a</div>\"", DataTypes.StringType, "<div>a</div>"));
  }

  @ParameterizedTest
  @MethodSource("primitives")
  void storesASingularValue(
      @Nonnull final String code,
      @Nonnull final String json,
      @Nonnull final DataType stored,
      @Nonnull final Object expected,
      @TempDir @Nonnull final Path directory) {
    final Dataset<Row> converted =
        convert(directory, code, "{\"value\":" + json + "}", functions.col("value"), false);

    assertEquals(stored, converted.schema().apply("value").dataType());
    assertValue(expected, converted.first().get(0));
  }

  @ParameterizedTest
  @MethodSource("primitives")
  void storesARepeatingValue(
      @Nonnull final String code,
      @Nonnull final String json,
      @Nonnull final DataType stored,
      @Nonnull final Object expected,
      @TempDir @Nonnull final Path directory) {
    final Dataset<Row> converted =
        convert(
            directory,
            code,
            "{\"value\":[" + json + "," + json + "]}",
            functions.col("value"),
            true);

    assertEquals(
        DataTypes.createArrayType(stored, true), converted.schema().apply("value").dataType());
    final List<Object> values = converted.first().getList(0);
    assertEquals(2, values.size());
    assertValue(expected, values.get(0));
    assertValue(expected, values.get(1));
  }

  @Test
  void coversEveryPrimitiveTheDefinitionsCanReport() {
    final List<String> missing =
        Stream.of(FHIRDefinedType.values())
            .filter(type -> type != FHIRDefinedType.NULL)
            .filter(PrimitiveTypes::isPrimitive)
            .filter(type -> PrimitiveConverters.forType(type).isEmpty())
            .map(FHIRDefinedType::toCode)
            .toList();

    assertEquals(List.of(), missing, "every primitive type needs a converter of its own");
  }

  @Test
  void storesWhatTheStorageMappingDeclares() {
    Stream.of(FHIRDefinedType.values())
        .filter(type -> type != FHIRDefinedType.NULL)
        .filter(PrimitiveTypes::isPrimitive)
        .forEach(
            type ->
                assertEquals(
                    PrimitiveTypes.storageTypeOf(type).orElseThrow(),
                    PrimitiveConverters.forType(type).orElseThrow().getStored(),
                    type.toCode()));
  }

  @Test
  void storesADecimalInferredAsALong(@TempDir @Nonnull final Path directory) {
    // A file whose decimals are all integral infers a long, and "value": 100 is conformant FHIR.
    final Dataset<Row> converted =
        convert(directory, "decimal", "{\"value\":100}", functions.col("value"), false);

    assertEquals("100", converted.first().getString(0));
  }

  @Test
  void storesADecimalInferredAsADecimal(@TempDir @Nonnull final Path directory) {
    // An integral value beyond the range of a long, in a file whose decimals are otherwise all
    // integral, infers a decimal of scale zero rather than a long or a double.
    final Dataset<Row> converted =
        convert(
            directory,
            "decimal",
            "{\"value\":12345678901234567890}",
            functions.col("value"),
            false);

    assertEquals("12345678901234567890", converted.first().getString(0));
  }

  // Decimals (T050).

  @ParameterizedTest
  @ValueSource(
      strings = {
        "1.50",
        "-1.50",
        "1e2",
        "1.0e-7",
        "0.000000001",
        "1234567890123456789012345678901234567890.5"
      })
  void storesADecimalAsTextNumericallyEqualToItsSource(
      @Nonnull final String source, @TempDir @Nonnull final Path directory) {
    final Dataset<Row> converted =
        convert(directory, "decimal", "{\"value\":" + source + "}", functions.col("value"), false);

    assertEquals(
        Double.parseDouble(source),
        Double.parseDouble(converted.first().getString(0)),
        "the stored text is that of a double, not the source's lexical form");
  }

  // base64Binary.

  @Test
  void writesBase64OnOneLine(@TempDir @Nonnull final Path directory) {
    // Encoding breaks its output into lines of 76 characters by default, and a value longer than
    // that would otherwise come back with line breaks the source never carried.
    final byte[] bytes = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".repeat(4).getBytes(StandardCharsets.US_ASCII);
    final String encoded = Base64.getEncoder().encodeToString(bytes);
    final PrimitiveConverter converter = PrimitiveConverters.forCode("base64Binary").orElseThrow();
    final Dataset<Row> converted =
        convert(
            directory,
            "base64Binary",
            "{\"value\":\"" + encoded + "\"}",
            functions.col("value"),
            false);

    final String written =
        converted.select(converter.egress(functions.col("value"))).first().getString(0);

    assertFalse(written.contains("\n") || written.contains("\r"), written);
    assertArrayEquals(Base64.getDecoder().decode(written), converted.first().<byte[]>getAs(0));
  }

  @Test
  void decodesBase64CarryingWhitespace(@TempDir @Nonnull final Path directory) {
    // FHIR permits whitespace between the groups of a base64 value, so a wrapped value is
    // conformant; it decodes to the same bytes and is written back without the whitespace.
    final Dataset<Row> converted =
        convert(
            directory,
            "base64Binary",
            "{\"value\":\"aGVs\\nbG8g d29y bGQ=\"}",
            functions.col("value"),
            false);

    assertArrayEquals(
        "hello world".getBytes(StandardCharsets.US_ASCII), converted.first().<byte[]>getAs(0));
  }

  @Nonnull
  private static Dataset<Row> convert(
      @Nonnull final Path directory,
      @Nonnull final String code,
      @Nonnull final String document,
      @Nonnull final Column source,
      final boolean repeating) {
    final PrimitiveConverter converter = PrimitiveConverters.forCode(code).orElseThrow();
    final Dataset<Row> read =
        TransformFixtures.spark().read().json(TransformFixtures.corpus(directory, document));
    final Column stored =
        repeating ? functions.transform(source, converter::ingest) : converter.ingest(source);
    return read.select(stored.alias("value"));
  }

  private static void assertValue(@Nonnull final Object expected, @Nonnull final Object actual) {
    if (expected instanceof final byte[] bytes) {
      assertArrayEquals(bytes, (byte[]) actual);
    } else {
      assertEquals(expected, actual);
    }
  }
}
