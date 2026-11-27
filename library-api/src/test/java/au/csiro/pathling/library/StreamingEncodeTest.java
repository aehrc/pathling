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

package au.csiro.pathling.library;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.library.io.SchemaEquality;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests encoding a stream with a schema defined up front: the encode returns a stream with the
 * schema, a stream without one is refused, a batch encode with a schema gives the schema's output,
 * and a stream encoded with a schema and written through a Pathling sink inside {@code
 * foreachBatch} is read back through Pathling (T029, FR-002, FR-012, FR-013, US2).
 *
 * <p>The defined schema has {@code gender}, {@code birthDate} and {@code name.family}. The first
 * Patient carries {@code active} and {@code name.given}, which the schema lacks, and lacks {@code
 * birthDate}. The second lacks {@code gender} and {@code name}.
 */
class StreamingEncodeTest {

  /** The resource type every test encodes. */
  @Nonnull private static final String PATIENT = "Patient";

  /** The message of a streaming encode refused for want of a schema (research R12). */
  @Nonnull
  private static final String REFUSAL =
      "Encoding a streaming dataset needs a schema: resources are otherwise stored with a schema"
          + " inferred from the data, which a stream cannot provide. Pass a schema, for example"
          + " one taken from an existing table's schemas(), or encode each batch within"
          + " foreachBatch instead.";

  /** Encoding FHIR JSON documents. */
  @Nonnull private static final String ENCODE_JSON = "encode JSON";

  /** Encoding FHIR XML documents. */
  @Nonnull private static final String ENCODE_XML = "encode XML";

  /** Encoding the resources of FHIR JSON bundles. */
  @Nonnull private static final String BUNDLE_JSON = "encodeBundle JSON";

  /** Encoding the resources of FHIR XML bundles. */
  @Nonnull private static final String BUNDLE_XML = "encodeBundle XML";

  @Nonnull
  private static final List<String> PATIENT_JSON =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"active\":true,\"gender\":\"female\","
              + "\"name\":[{\"family\":\"Ash\",\"given\":[\"Ann\"]}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"birthDate\":\"1990-01-01\"}");

  @Nonnull
  private static final List<String> PATIENT_XML =
      List.of(
          "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"p1\"/><active value=\"true\"/>"
              + "<name><family value=\"Ash\"/><given value=\"Ann\"/></name>"
              + "<gender value=\"female\"/></Patient>",
          "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"p2\"/>"
              + "<birthDate value=\"1990-01-01\"/></Patient>");

  /** A Patient carrying every element of the defined schema, from which it is read. */
  @Nonnull
  private static final String SCHEMA_DONOR_JSON =
      "{\"resourceType\":\"Patient\",\"id\":\"d\",\"gender\":\"male\","
          + "\"birthDate\":\"2000-01-01\",\"name\":[{\"family\":\"Smith\"}]}";

  private static SparkSession spark;
  private static PathlingContext context;

  /** The defined Patient schema, without annotations. */
  private static StructType schema;

  @TempDir Path directory;

  /** Set up Spark, the Pathling context and the defined schema. */
  @BeforeAll
  static void setUp() {
    spark = TestHelpers.spark();
    context = PathlingContext.create(spark);
    schema =
        FhirReader.of(
                spark,
                FhirDefinitionContext.of(context.getFhirContext()),
                context.getSchemaConfiguration())
            .json()
            .read(PATIENT, spark.createDataset(List.of(SCHEMA_DONOR_JSON), Encoders.STRING()))
            .schema();
  }

  /** Stop Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  /**
   * An encode of streaming FHIR JSON or XML, or of streaming bundles, with a schema returns a
   * streaming dataset with the schema, whose rows are those a batch encode of the same text with
   * the same schema gives (FR-012, US2 scenarios 1 and 2).
   */
  @ParameterizedTest
  @ValueSource(strings = {ENCODE_JSON, ENCODE_XML, BUNDLE_JSON, BUNDLE_XML})
  void streamEncodedWithSchemaIsAStreamWithTheSchema(@Nonnull final String encoding)
      throws IOException, TimeoutException, StreamingQueryException {
    final Dataset<Row> stream = streamOf(lines(encoding));
    assertTrue(stream.isStreaming());

    final Dataset<Row> encoded = encodeWithSchema(encoding, stream);

    assertTrue(encoded.isStreaming(), encoding + " should return a streaming dataset");
    assertTrue(
        SchemaEquality.same(schema, encoded.schema()),
        () ->
            encoding
                + ": expected "
                + schema.treeString()
                + " but got "
                + encoded.schema().treeString());
    final Dataset<Row> batch = encodeWithSchema(encoding, batchOf(lines(encoding)));
    assertEquals(sortedJson(batch), sortedJson(drain(encoded)), encoding);
  }

  /**
   * A streaming encode without a schema is refused with {@code UnsupportedOperationException}, as
   * it is today, and a message telling the user to pass a schema or encode each batch within {@code
   * foreachBatch} (FR-013, research R12, US2 scenario 3). A null schema is no schema.
   */
  @Test
  void streamEncodedWithoutSchemaIsRefused() throws IOException {
    final Dataset<Row> json = streamOf(PATIENT_JSON);
    final Dataset<Row> bundles = streamOf(List.of(jsonBundle()));

    final List<Executable> encodes =
        List.of(
            () -> context.encode(json, PATIENT),
            () -> context.encode(json, PATIENT, PathlingContext.FHIR_JSON, null),
            () -> context.encodeWithSchema(json, PATIENT, PathlingContext.FHIR_JSON, null),
            () -> context.encodeBundle(bundles, PATIENT),
            () -> context.encodeBundle(bundles, PATIENT, PathlingContext.FHIR_JSON, null),
            () ->
                context.encodeBundleWithSchema(bundles, PATIENT, PathlingContext.FHIR_JSON, null));

    assertAll(
        encodes.stream()
            .map(
                encode ->
                    (Executable)
                        () ->
                            assertEquals(
                                REFUSAL,
                                assertThrows(UnsupportedOperationException.class, encode)
                                    .getMessage())));
  }

  /**
   * A batch encode with a schema gives the schema's output: the schema, with the values the
   * documents hold for its fields, and without the content it has no field for (FR-002).
   */
  @ParameterizedTest
  @ValueSource(strings = {ENCODE_JSON, ENCODE_XML, BUNDLE_JSON, BUNDLE_XML})
  void batchEncodedWithSchemaGivesTheSchemasOutput(@Nonnull final String encoding) {
    final Dataset<Row> encoded = encodeWithSchema(encoding, batchOf(lines(encoding)));

    assertFalse(encoded.isStreaming());
    assertTrue(
        SchemaEquality.same(schema, encoded.schema()),
        () ->
            encoding
                + ": expected "
                + schema.treeString()
                + " but got "
                + encoded.schema().treeString());
    assertEquals(
        List.of("p1,female,null,[Ash]", "p2,null,1990-01-01,null"),
        encoded
            .selectExpr(
                "concat_ws(',', id, coalesce(gender, 'null'), coalesce(birthDate, 'null'),"
                    + " coalesce(CAST(name.family AS STRING), 'null'))")
            .as(Encoders.STRING())
            .collectAsList()
            .stream()
            .sorted()
            .toList(),
        encoding);
  }

  /**
   * A stream of NDJSON lines encoded with a schema, and written in more than one micro-batch
   * through a Pathling sink inside {@code foreachBatch}, to Delta with a merge and to Parquet with
   * an append, is read back through Pathling with the schema and the rows of a batch encode of the
   * same lines (US2 independent test, research R8 and R9).
   */
  @Test
  void streamWrittenThroughPathlingSinkIsReadBack()
      throws IOException, TimeoutException, StreamingQueryException {
    // Each document is in a file of its own, and each micro-batch takes one file, so the sinks
    // write more than one batch.
    final Path input = Files.createDirectory(directory.resolve("input"));
    for (int i = 0; i < PATIENT_JSON.size(); i++) {
      Files.writeString(input.resolve("patients-" + i + ".ndjson"), PATIENT_JSON.get(i));
    }
    final Dataset<Row> stream =
        spark.readStream().option("maxFilesPerTrigger", 1).text(input.toString());
    final Dataset<Row> encoded =
        context.encodeWithSchema(stream, PATIENT, PathlingContext.FHIR_JSON, schema);
    final String delta = directory.resolve("delta").toString();
    final String parquet = directory.resolve("parquet").toString();

    final StreamingQuery query =
        encoded
            .writeStream()
            .option("checkpointLocation", directory.resolve("checkpoint").toString())
            .trigger(Trigger.AvailableNow())
            .foreachBatch(
                (VoidFunction2<Dataset<Row>, Long>)
                    (batch, id) -> {
                      context
                          .read()
                          .datasets()
                          .dataset(PATIENT, batch)
                          .write()
                          .saveMode("merge")
                          .delta(delta);
                      context
                          .read()
                          .datasets()
                          .dataset(PATIENT, batch)
                          .write()
                          .saveMode("append")
                          .parquet(parquet);
                    })
            .start();
    query.awaitTermination();

    final List<String> expected =
        sortedJson(
            context.encodeWithSchema(
                batchOf(PATIENT_JSON), PATIENT, PathlingContext.FHIR_JSON, schema));
    final Dataset<Row> fromDelta = context.read().delta(delta).read(PATIENT);
    final Dataset<Row> fromParquet = context.read().parquet(parquet).read(PATIENT);
    assertAll(
        () -> assertTrue(SchemaEquality.same(schema, fromDelta.schema()), "Delta schema"),
        () -> assertTrue(SchemaEquality.same(schema, fromParquet.schema()), "Parquet schema"),
        () -> assertEquals(expected, sortedJson(fromDelta), "Delta rows"),
        () -> assertEquals(expected, sortedJson(fromParquet), "Parquet rows"));
  }

  /** Encodes documents or bundles with the defined schema, as the encoding names. */
  @Nonnull
  private static Dataset<Row> encodeWithSchema(
      @Nonnull final String encoding, @Nonnull final Dataset<Row> text) {
    return switch (encoding) {
      case ENCODE_JSON ->
          context.encodeWithSchema(text, PATIENT, PathlingContext.FHIR_JSON, schema);
      case ENCODE_XML -> context.encodeWithSchema(text, PATIENT, PathlingContext.FHIR_XML, schema);
      case BUNDLE_JSON ->
          context.encodeBundleWithSchema(text, PATIENT, PathlingContext.FHIR_JSON, schema);
      case BUNDLE_XML ->
          context.encodeBundleWithSchema(text, PATIENT, PathlingContext.FHIR_XML, schema);
      default -> throw new IllegalArgumentException("Unknown encoding: " + encoding);
    };
  }

  /** Returns the text an encoding reads, one document or bundle per line. */
  @Nonnull
  private static List<String> lines(@Nonnull final String encoding) {
    return switch (encoding) {
      case ENCODE_JSON -> PATIENT_JSON;
      case ENCODE_XML -> PATIENT_XML;
      case BUNDLE_JSON -> List.of(jsonBundle());
      case BUNDLE_XML -> List.of(xmlBundle());
      default -> throw new IllegalArgumentException("Unknown encoding: " + encoding);
    };
  }

  @Nonnull
  private static String jsonBundle() {
    return "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
        + String.join(",", PATIENT_JSON.stream().map(r -> "{\"resource\":" + r + "}").toList())
        + "]}";
  }

  @Nonnull
  private static String xmlBundle() {
    return "<Bundle xmlns=\"http://hl7.org/fhir\"><type value=\"collection\"/>"
        + String.join(
            "",
            PATIENT_XML.stream().map(r -> "<entry><resource>" + r + "</resource></entry>").toList())
        + "</Bundle>";
  }

  /** Returns lines of text as a batch dataset with a single string column. */
  @Nonnull
  private static Dataset<Row> batchOf(@Nonnull final List<String> lines) {
    return spark.createDataset(lines, Encoders.STRING()).toDF();
  }

  /** Returns lines of text as a streaming dataset, read from a file in a directory of its own. */
  @Nonnull
  private Dataset<Row> streamOf(@Nonnull final List<String> lines) throws IOException {
    final Path input = Files.createDirectory(directory.resolve("stream-" + UUID.randomUUID()));
    Files.write(input.resolve("input.txt"), lines);
    return spark.readStream().text(input.toString());
  }

  /** Runs a stream to the end of its input into a memory sink and returns what it wrote. */
  @Nonnull
  private static Dataset<Row> drain(@Nonnull final Dataset<Row> stream)
      throws TimeoutException, StreamingQueryException {
    final String name = "drained_" + UUID.randomUUID().toString().replace("-", "");
    stream
        .writeStream()
        .format("memory")
        .queryName(name)
        .trigger(Trigger.AvailableNow())
        .start()
        .awaitTermination();
    return spark.table(name);
  }

  /** Returns the rows of a dataset as JSON, sorted so that row order does not matter. */
  @Nonnull
  private static List<String> sortedJson(@Nonnull final Dataset<Row> rows) {
    return rows.toJSON().collectAsList().stream().sorted().toList();
  }
}
