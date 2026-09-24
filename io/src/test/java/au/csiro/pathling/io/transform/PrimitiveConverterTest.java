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

package au.csiro.pathling.io.transform;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.spark.sql.types.ArrayType;
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
 * Tests the converter table: what each FHIR primitive type accepts from JSON inference, what it
 * stores, and what it returns to JSON (decision 68).
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

  // The acceptance rule.

  /**
   * The column types each numeric primitive accepts beyond what inference gives, and the ones it
   * refuses (decision 70). Every integral type widens losslessly to an integer and to a decimal; a
   * float has already rounded, and a Spark date or timestamp cannot hold every FHIR one.
   */
  @Nonnull
  static Stream<Arguments> widenings() {
    final List<DataType> integral =
        List.of(DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType, DataTypes.LongType);
    final Stream<Arguments> accepted =
        Stream.of("integer", "unsignedInt", "positiveInt", "integer64", "decimal")
            .flatMap(code -> integral.stream().map(type -> Arguments.of(code, type, true)));
    final Stream<Arguments> other =
        Stream.of(
            Arguments.of("decimal", DataTypes.DoubleType, true),
            Arguments.of("decimal", DataTypes.createDecimalType(38, 0), true),
            Arguments.of("decimal", DataTypes.createDecimalType(10, 2), true),
            Arguments.of("decimal", DataTypes.FloatType, false),
            Arguments.of("integer", DataTypes.DoubleType, false),
            Arguments.of("integer64", DataTypes.FloatType, false),
            Arguments.of("date", DataTypes.DateType, false),
            Arguments.of("dateTime", DataTypes.TimestampType, false),
            Arguments.of("boolean", DataTypes.IntegerType, false));
    return Stream.concat(accepted, other);
  }

  @ParameterizedTest
  @MethodSource("widenings")
  void acceptsTheLosslessWideningsOfAJsonType(
      @Nonnull final String code, @Nonnull final DataType type, final boolean accepted) {
    assertEquals(
        accepted,
        PrimitiveConverters.forCode(code).orElseThrow().accepts(type),
        code + " accepting " + type);
  }

  @Test
  void acceptsADecimalInferredAsALong(@TempDir @Nonnull final Path directory) {
    // A file whose decimals are all integral infers a long, and "value": 100 is conformant FHIR.
    final Dataset<Row> converted =
        convert(directory, "decimal", "{\"value\":100}", functions.col("value"), false);

    assertEquals("100", converted.first().getString(0));
  }

  @Test
  void acceptsADecimalInferredAsADecimal(@TempDir @Nonnull final Path directory) {
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

  @Test
  void nullsAnIntegerBeyondTheRangeOfAnInteger(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> converted =
        convert(directory, "integer", "{\"value\":3000000000}", functions.col("value"), false);

    assertNull(converted.first().get(0), "the value is not wrapped or truncated");
  }

  @Test
  void nullsAnIntegerColumnRetypedByAFraction(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed =
        transform(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"multipleBirthInteger\":2}",
            "{\"resourceType\":\"Patient\",\"id\":\"2\",\"multipleBirthInteger\":1.5}");

    assertEquals(
        DataTypes.IntegerType, transformed.schema().apply("multipleBirthInteger").dataType());
    assertTrue(
        transformed.select("multipleBirthInteger").collectAsList().stream()
            .allMatch(row -> row.isNullAt(0)),
        "one fraction voids the element for the whole file, rather than 1.5 becoming 1");
    assertEquals(
        List.of("Patient.multipleBirthInteger"),
        encodingMismatches(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"multipleBirthInteger\":2}",
            "{\"resourceType\":\"Patient\",\"id\":\"2\",\"multipleBirthInteger\":1.5}"));
  }

  @Test
  void nullsTextSuppliedForABoolean(@TempDir @Nonnull final Path directory) {
    final String document = "{\"resourceType\":\"Patient\",\"id\":\"1\",\"active\":\"yes\"}";
    final Dataset<Row> transformed = transform(directory, document);

    assertEquals(DataTypes.BooleanType, transformed.schema().apply("active").dataType());
    assertNull(transformed.first().getAs("active"), "\"yes\" is not cast to true");
    assertEquals(List.of("Patient.active"), encodingMismatches(directory, document));
  }

  @Test
  void nullsANumberSuppliedForText(@TempDir @Nonnull final Path directory) {
    final String document = "{\"resourceType\":\"Patient\",\"id\":\"1\",\"gender\":1}";
    final Dataset<Row> transformed = transform(directory, document);

    assertNull(transformed.first().getAs("gender"), "a number is not rendered as its text");
    assertEquals(List.of("Patient.gender"), encodingMismatches(directory, document));
  }

  @Test
  void reportsNothingForConformantEncodings(@TempDir @Nonnull final Path directory) {
    assertEquals(
        List.of(),
        encodingMismatches(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"active\":true,"
                + "\"multipleBirthInteger\":2,\"birthDate\":\"1980-01-01\","
                + "\"photo\":[{\"data\":\"aGVsbG8=\"}]}"));
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

  @Test
  void nullsTextThatIsNotBase64(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> converted =
        convert(
            directory,
            "base64Binary",
            "{\"value\":\"not base64!\"}",
            functions.col("value"),
            false);

    assertNull(converted.first().get(0), "malformed content becomes null rather than raising");
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
    final DataType inferred = read.schema().apply("value").dataType();
    final DataType value =
        inferred instanceof final ArrayType array ? array.elementType() : inferred;
    assertTrue(converter.accepts(value), code + " should accept " + value);
    final Column stored =
        repeating ? functions.transform(source, converter::ingest) : converter.ingest(source);
    return read.select(stored.alias("value"));
  }

  @Nonnull
  private static Dataset<Row> transform(
      @Nonnull final Path directory, @Nonnull final String... documents) {
    return TransformFixtures.reader()
        .read("Patient", TransformFixtures.corpus(directory, documents));
  }

  @Nonnull
  private static List<String> encodingMismatches(
      @Nonnull final Path directory, @Nonnull final String... documents) {
    final ResourceTransformer transformer = TransformFixtures.transformer();
    return transformer
        .findings(
            "Patient",
            TransformFixtures.inferred(TransformFixtures.corpus(directory, documents)).schema())
        .stream()
        .filter(NonConformantContent::isEncodingMismatch)
        .map(NonConformantContent::getPath)
        .toList();
  }

  private static void assertValue(@Nonnull final Object expected, @Nonnull final Object actual) {
    if (expected instanceof final byte[] bytes) {
      assertArrayEquals(bytes, (byte[]) actual);
    } else {
      assertEquals(expected, actual);
    }
  }
}
