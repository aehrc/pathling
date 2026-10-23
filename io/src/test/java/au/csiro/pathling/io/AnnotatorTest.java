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

import static au.csiro.pathling.io.TransformFixtures.DEFINITIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.schema.annotation.AnnotationKind;
import au.csiro.pathling.schema.annotation.AnnotationSwitch;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests the annotation step with stub kinds, one for each switch, since no kind is built in yet
 * (T055, decision 135). Each stub marks the value it is computed from, so that a computed
 * annotation is told from a stored one.
 */
class AnnotatorTest {

  private static final String NUMERIC = LayoutFields.NUMERIC_ANNOTATION;
  private static final String START = LayoutFields.START_ANNOTATION;
  private static final String END = LayoutFields.END_ANNOTATION;
  private static final String CANONICAL = LayoutFields.CANONICAL_ANNOTATION;
  private static final String CANONICAL_EXACT = LayoutFields.CANONICAL_EXACT_ANNOTATION;

  private static final List<AnnotationKind> STUBS =
      List.of(
          stub(
              AnnotationSwitch.NUMERIC,
              List.of(NUMERIC),
              value -> functions.struct(mark(value, "#").alias(NUMERIC))),
          stub(
              AnnotationSwitch.RANGE,
              List.of(START, END),
              value ->
                  functions.struct(mark(value, "<").alias(START), mark(value, ">").alias(END))),
          stub(
              AnnotationSwitch.CANONICAL,
              List.of(CANONICAL, CANONICAL_EXACT),
              quantity ->
                  functions.struct(
                      mark(quantity.getField("unit"), "=").alias(CANONICAL),
                      mark(quantity.getField("unit"), "==").alias(CANONICAL_EXACT))));

  private static final String OBSERVATION =
      "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
          + "\"code\":{\"text\":\"x\"},\"effectiveDateTime\":\"2020-01\","
          + "\"valueQuantity\":{\"value\":1.5,\"unit\":\"kg\"}}";

  @TempDir Path tempDir;

  @Nonnull
  private static AnnotationKind stub(
      @Nonnull final AnnotationSwitch annotationSwitch,
      @Nonnull final List<String> fields,
      @Nonnull final UnaryOperator<Column> compute) {
    return new AnnotationKind(annotationSwitch, fields) {
      @Override
      @Nonnull
      public Column compute(@Nonnull final Column value) {
        return compute.apply(value);
      }
    };
  }

  @Nonnull
  private static Column mark(@Nonnull final Column value, @Nonnull final String mark) {
    return functions.concat(value.cast("string"), functions.lit(mark));
  }

  @Nonnull
  private static Dataset<Row> read(@Nonnull final String resourceType, @Nonnull final String json) {
    return TransformFixtures.reader()
        .read(
            resourceType,
            TransformFixtures.spark().createDataset(List.of(json), Encoders.STRING()));
  }

  @Nonnull
  private static Dataset<Row> annotate(
      @Nonnull final String resourceType,
      @Nonnull final Dataset<Row> resources,
      @Nonnull final SchemaConfiguration configuration) {
    return Annotator.of(DEFINITIONS, configuration, STUBS).annotate(resourceType, resources);
  }

  @Nonnull
  private static Object value(@Nonnull final Dataset<Row> resources, @Nonnull final String path) {
    return resources.selectExpr(path).first().get(0);
  }

  @Nonnull
  private static List<String> fields(
      @Nonnull final Dataset<Row> resources, @Nonnull final String path) {
    return Arrays.asList(
        ((StructType) resources.selectExpr(path).schema().fields()[0].dataType()).fieldNames());
  }

  @Nonnull
  static Stream<Arguments> switches() {
    return Stream.of(
        Arguments.of("all on", SchemaConfiguration.builder().build(), true, true, true),
        Arguments.of(
            "numeric off",
            SchemaConfiguration.builder().enableNumericAnnotation(false).build(),
            false,
            true,
            true),
        Arguments.of(
            "range off",
            SchemaConfiguration.builder().enableRangeAnnotation(false).build(),
            true,
            false,
            true),
        Arguments.of(
            "canonical off",
            SchemaConfiguration.builder().enableCanonicalAnnotation(false).build(),
            true,
            true,
            false));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("switches")
  void storesTheEnabledKindsAndSkipsADisabledOneAlone(
      @Nonnull final String name,
      @Nonnull final SchemaConfiguration configuration,
      final boolean numeric,
      final boolean range,
      final boolean canonical) {
    final Dataset<Row> annotated =
        annotate("Observation", read("Observation", OBSERVATION), configuration);

    assertEquals(numeric, fields(annotated, "valueQuantity").contains("_value"));
    if (numeric) {
      assertEquals(List.of(NUMERIC), fields(annotated, "valueQuantity._value"));
      assertEquals("1.5#", value(annotated, "valueQuantity._value." + NUMERIC));
    }
    assertEquals(range, Arrays.asList(annotated.columns()).contains("_effectiveDateTime"));
    if (range) {
      assertEquals(List.of(START, END), fields(annotated, "_effectiveDateTime"));
      assertEquals("2020-01<", value(annotated, "_effectiveDateTime." + START));
      assertEquals("2020-01>", value(annotated, "_effectiveDateTime." + END));
    }
    assertEquals(canonical, fields(annotated, "valueQuantity").contains(CANONICAL));
    if (canonical) {
      final List<String> quantity = fields(annotated, "valueQuantity");
      assertEquals(
          List.of(CANONICAL, CANONICAL_EXACT),
          quantity.subList(quantity.size() - 2, quantity.size()));
      assertEquals("kg=", value(annotated, "valueQuantity." + CANONICAL));
      assertEquals("kg==", value(annotated, "valueQuantity." + CANONICAL_EXACT));
    }
  }

  @Test
  void placesTheMetadataGroupItAddsImmediatelyAfterItsPrimitive() {
    final List<String> columns =
        Arrays.asList(
            annotate(
                    "Observation",
                    read("Observation", OBSERVATION),
                    SchemaConfiguration.builder().build())
                .columns());
    assertEquals(columns.indexOf("effectiveDateTime") + 1, columns.indexOf("_effectiveDateTime"));
  }

  @Test
  void annotatesEachValueOfARepeatingPrimitiveByPosition() {
    final Dataset<Row> annotated =
        annotate(
            "Observation",
            read(
                "Observation",
                "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
                    + "\"code\":{\"text\":\"x\"},"
                    + "\"effectiveTiming\":{\"event\":[\"2020\",\"2021\"]}}"),
            SchemaConfiguration.builder().build());
    assertEquals(
        List.of("2020<", "2021<"),
        annotated.selectExpr("effectiveTiming._event." + START).first().<String>getList(0));
  }

  @Test
  void readsAPresentAnnotationAndComputesAnAbsentOneWhenFilling() {
    final Dataset<Row> stored = storedRange(read("Observation", OBSERVATION));

    final Dataset<Row> annotated =
        annotate("Observation", stored, SchemaConfiguration.builder().build());

    assertEquals("stored", value(annotated, "_effectiveDateTime." + START));
    assertEquals("2020-01>", value(annotated, "_effectiveDateTime." + END));
  }

  @Test
  void replacesAPresentAnnotationWhenRecomputing() {
    final Dataset<Row> annotated =
        annotate(
            "Observation",
            storedRange(read("Observation", OBSERVATION)),
            SchemaConfiguration.builder().recomputeAnnotations(true).build());

    assertEquals("2020-01<", value(annotated, "_effectiveDateTime." + START));
  }

  @Test
  void dropsTheAnnotationOfADisabledKindAndKeepsAnUnknownOneByDefault() {
    final Dataset<Row> annotated =
        annotate(
            "Observation",
            withUnknown(storedRange(read("Observation", OBSERVATION))),
            SchemaConfiguration.builder().enableRangeAnnotation(false).build());

    final List<String> columns = Arrays.asList(annotated.columns());
    assertFalse(columns.contains("_effectiveDateTime"));
    assertEquals("__unknown", columns.get(columns.size() - 1));
    final List<String> quantity = fields(annotated, "valueQuantity");
    assertEquals(
        List.of(CANONICAL, CANONICAL_EXACT, "__unknown"),
        quantity.subList(quantity.size() - 3, quantity.size()));
    assertEquals("u", value(annotated, "valueQuantity.__unknown"));
  }

  @Test
  void keepsTheAnnotationOfADisabledKindAndDropsAnUnknownOneWhenConfigured() {
    final Dataset<Row> annotated =
        annotate(
            "Observation",
            withUnknown(storedRange(read("Observation", OBSERVATION))),
            SchemaConfiguration.builder()
                .enableRangeAnnotation(false)
                .keepUnrequestedAnnotations(true)
                .dropUnknownAnnotations(true)
                .build());

    assertEquals(List.of(START), fields(annotated, "_effectiveDateTime"));
    assertEquals("stored", value(annotated, "_effectiveDateTime." + START));
    assertFalse(Arrays.asList(annotated.columns()).contains("__unknown"));
    assertFalse(fields(annotated, "valueQuantity").contains("__unknown"));
  }

  @Test
  void keepsAStoredAnnotationOfAnAbsentPrimitiveAndAddsNoneWhenFilling() {
    final Dataset<Row> annotated =
        annotate(
            "Observation",
            storedRange(read("Observation", OBSERVATION)).drop("effectiveDateTime"),
            SchemaConfiguration.builder().build());

    assertEquals(List.of(START), fields(annotated, "_effectiveDateTime"));
    assertEquals("stored", value(annotated, "_effectiveDateTime." + START));
  }

  @Test
  void computesAStoredAnnotationOfAnAbsentPrimitiveAsForANullWhenRecomputing() {
    final Dataset<Row> annotated =
        annotate(
            "Observation",
            storedRange(read("Observation", OBSERVATION)).drop("effectiveDateTime"),
            SchemaConfiguration.builder().recomputeAnnotations(true).build());

    assertEquals(List.of(START), fields(annotated, "_effectiveDateTime"));
    assertNull(annotated.selectExpr("_effectiveDateTime." + START).first().get(0));
  }

  @Test
  void storesTheMetadataGroupOfAPrimitiveForASourceWithoutOne() {
    final Dataset<Row> source =
        read(
            "Patient", "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"birthDate\":\"1974-12-25\"}");
    assertFalse(Arrays.asList(source.columns()).contains("_birthDate"));

    final Dataset<Row> annotated =
        annotate("Patient", source, SchemaConfiguration.builder().build());

    assertEquals("1974-12-25<", value(annotated, "_birthDate." + START));
  }

  @Test
  void storesTheMetadataGroupOfAPrimitiveForStoredParquetWithoutOne() {
    final String path = tempDir.resolve("Patient.parquet").toString();
    read("Patient", "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"birthDate\":\"1974-12-25\"}")
        .write()
        .parquet(path);
    final Dataset<Row> stored = TransformFixtures.spark().read().parquet(path);

    final Dataset<Row> annotated =
        annotate("Patient", stored, SchemaConfiguration.builder().build());

    assertEquals("1974-12-25>", value(annotated, "_birthDate." + END));
  }

  @Test
  void passesAMetadataGroupOfTheWrongShapeThroughUnchanged() {
    final Dataset<Row> malformed =
        read(
                "Observation",
                "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
                    + "\"code\":{\"text\":\"x\"},"
                    + "\"effectiveTiming\":{\"event\":[\"2020\"]}}")
            .withColumn("_status", functions.lit("s"))
            .withColumn(
                "effectiveTiming",
                functions
                    .col("effectiveTiming")
                    .withField("_event", functions.struct(functions.lit("e").alias("id"))));

    final Dataset<Row> annotated =
        annotate("Observation", malformed, SchemaConfiguration.builder().build());

    assertEquals("s", value(annotated, "_status"));
    assertEquals(List.of("id"), fields(annotated, "effectiveTiming._event"));
  }

  @Test
  void leavesResourcesItHasAnnotatedAsTheyAreWhenFilling() {
    final SchemaConfiguration fill = SchemaConfiguration.builder().build();
    for (final String json :
        List.of(
            OBSERVATION,
            "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
                + "\"code\":{\"text\":\"x\"},"
                + "\"effectiveTiming\":{\"event\":[\"2020\",\"2021\"]}}")) {
      final Dataset<Row> annotated = annotate("Observation", read("Observation", json), fill);

      assertSame(annotated, annotate("Observation", annotated, fill));
    }
  }

  @Test
  void leavesResourcesAsTheyAreWithTheBuiltInKinds() {
    final Dataset<Row> resources = read("Observation", OBSERVATION);

    assertSame(
        resources,
        Annotator.of(DEFINITIONS, SchemaConfiguration.builder().build())
            .annotate("Observation", resources));
  }

  /** Adds a stored range start beside the effective date, as an earlier annotation would have. */
  @Nonnull
  private static Dataset<Row> storedRange(@Nonnull final Dataset<Row> resources) {
    return resources.withColumn(
        "_effectiveDateTime", functions.struct(functions.lit("stored").alias(START)));
  }

  /** Adds a field marked as an annotation that Pathling does not define, at the top and nested. */
  @Nonnull
  private static Dataset<Row> withUnknown(@Nonnull final Dataset<Row> resources) {
    return resources
        .withColumn("__unknown", functions.lit("u"))
        .withColumn(
            "valueQuantity",
            functions.col("valueQuantity").withField("__unknown", functions.lit("u")));
  }
}
