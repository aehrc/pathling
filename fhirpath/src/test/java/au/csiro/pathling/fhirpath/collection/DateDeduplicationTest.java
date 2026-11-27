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

package au.csiro.pathling.fhirpath.collection;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.transform;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.Annotator;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.search.SearchColumnBuilder;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that dates are deduplicated by a key computed once for each date, with the results of
 * comparing them pairwise (decision 151): a date without a time part by its text, and any other
 * date by its range, over data written with and without the range annotations, and with another
 * producer's annotation beside the stored ones; and that a date leaves the engine as its text.
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class DateDeduplicationTest {

  private static final Parser PARSER = new Parser();

  /** A Patient whose names have the given period starts, in order. */
  @Nonnull
  private static String patient(@Nonnull final String id, @Nonnull final String... starts) {
    final StringBuilder names = new StringBuilder();
    for (final String start : starts) {
      if (!names.isEmpty()) {
        names.append(',');
      }
      names.append(
          start == null
              ? "{\"family\":\"N\"}"
              : "{\"family\":\"N\",\"period\":{\"start\":\"" + start + "\"}}");
    }
    return "{\"resourceType\":\"Patient\",\"id\":\"" + id + "\",\"name\":[" + names + "]}";
  }

  private static final List<String> PATIENTS =
      List.of(
          // Equal dates, and the same date at another precision, which is not a duplicate.
          patient("dates", "2020-01-01", "2020", "2020-01-01", null),
          // One instant in two time zones, and without a time zone, which is taken as UTC.
          patient(
              "zones", "2020-01-01T10:00:00+10:00", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00"),
          // Seconds and milliseconds are one precision.
          patient("seconds", "2020-01-01T10:00:00Z", "2020-01-01T10:00:00.000Z"),
          // Minutes and seconds are not, so the two cannot be compared and both are kept.
          patient("minutes", "2020-01-01T10:00Z", "2020-01-01T10:00:00Z"),
          patient("single", "2020-01-01"),
          patient("none"));

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Dataset<Row> read;

  private Dataset<Row> annotated;

  /** The annotated data with another producer's annotation beside each stored bound. */
  private Dataset<Row> foreign;

  @BeforeAll
  void setUp() {
    final Dataset<Row> resources =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Patient", PATIENTS);
    read = PrunedSchemaReader.write(resources, tempDir.resolve("read").toString()).read();
    annotated =
        PrunedSchemaReader.write(
                Annotator.of(
                        FhirDefinitionContext.of(fhirEncoders.getContext()),
                        SchemaConfiguration.builder().build())
                    .annotate("Patient", resources),
                tempDir.resolve("annotated").toString())
            .read();
    foreign =
        annotated.withColumn(
            "name",
            transform(
                col("name"), (name, index) -> name.withField("period._start.__acme_hash", index)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void datesAreDeduplicatedByTheirKey(
      @Nonnull final String expression, @Nonnull final List<String> expected) {
    assertThat(annotated.schema().apply("name").dataType().simpleString()).contains("__start");
    assertThat(foreign.schema().apply("name").dataType().simpleString()).contains("__acme_hash");
    assertThat(rows(read, expression)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(rows(annotated, expression)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(rows(foreign, expression)).containsExactlyInAnyOrderElementsOf(expected);
  }

  /**
   * A date leaves the engine as its value only, its text: its stored annotation fields are never
   * output (decision 142, D10).
   */
  @Test
  void aDateLeavesTheEngineAsItsTextOnly() {
    final SearchColumnBuilder builder =
        SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext());
    final Dataset<Row> repeating =
        annotated.select(
            builder.fromExpression(ResourceType.PATIENT, "name.period.start").alias("x"));
    assertThat(repeating.schema().apply("x").dataType().simpleString()).isEqualTo("array<string>");
    final Dataset<Row> singular =
        annotated
            .filter(col("id").equalTo("single"))
            .select(
                builder
                    .fromExpression(ResourceType.PATIENT, "name.period.start.first()")
                    .alias("x"));
    assertThat(singular.schema().apply("x").dataType().simpleString()).isEqualTo("string");
    assertThat(singular.collectAsList().get(0).getString(0)).isEqualTo("2020-01-01");
  }

  @Nonnull
  Stream<Arguments> cases() {
    return Stream.of(
        Arguments.of(
            "name.period.start.distinct()",
            List.of(
                "dates=[2020-01-01, 2020]",
                "zones=[2020-01-01T10:00:00+10:00]",
                "seconds=[2020-01-01T10:00:00Z]",
                "minutes=[2020-01-01T10:00Z, 2020-01-01T10:00:00Z]",
                "single=[2020-01-01]",
                "none=[]")),
        Arguments.of(
            "name.period.start.distinct().count()",
            List.of("dates=2", "zones=1", "seconds=1", "minutes=2", "single=1", "none=0")),
        Arguments.of(
            "(name.period.start | @2020-01-01).count()",
            List.of("dates=2", "zones=2", "seconds=2", "minutes=3", "single=1", "none=1")),
        Arguments.of(
            "(name.period.start | @2020-01-01T00:00:00Z).count()",
            List.of("dates=3", "zones=1", "seconds=2", "minutes=3", "single=2", "none=1")),
        // Equality reads the bounds through the accessors, whatever annotation fields are stored.
        Arguments.of(
            "name[0].period.start = name[2].period.start",
            List.of(
                "dates=true",
                "zones=true",
                "seconds=null",
                "minutes=null",
                "single=null",
                "none=null")),
        Arguments.of(
            "name.period.start.isDistinct()",
            List.of(
                "dates=false",
                "zones=false",
                "seconds=false",
                "minutes=true",
                "single=true",
                "none=true")));
  }

  /** The value of an expression for each Patient, as {@code id=value}. */
  @Nonnull
  private List<String> rows(@Nonnull final Dataset<Row> data, @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirEncoders.getContext())
            .withDataset(data)
            .build();
    return evaluator.evaluate(PARSER.parse(expression)).toIdValueDataset().collectAsList().stream()
        .map(
            row ->
                row.getString(0)
                    + "="
                    + (row.get(1) instanceof final scala.collection.Seq<?> seq
                        ? String.valueOf(
                            scala.jdk.javaapi.CollectionConverters.asJava(seq).stream()
                                .map(
                                    each ->
                                        each instanceof final Row date ? date.getString(0) : each)
                                .toList())
                        : String.valueOf(row.get(1))))
        .toList();
  }
}
