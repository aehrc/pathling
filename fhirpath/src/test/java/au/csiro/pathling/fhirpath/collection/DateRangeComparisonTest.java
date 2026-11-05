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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.FileSourceScanExec;
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
import scala.jdk.javaapi.CollectionConverters;

/**
 * Tests that date comparison, equality and date search give the same results where the range
 * annotations are stored and taken as a fast path, where the data was written without them, and
 * where they are disabled (FR-004, FR-022, FR-023), and that the range of a date is computed from
 * one parse of it where the schema does not carry it.
 *
 * <p>The fixtures cover each precision from a year to a fraction of a second, a time with a
 * timezone, a time without one, which is taken as UTC, a date before the Gregorian cutover, a
 * dateTime, an instant, a date under a repeating parent, a repeating date ({@code Timing.event})
 * and a Period.
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class DateRangeComparisonTest {

  private static final Parser PARSER = new Parser();

  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"birthDate\":\"1980-06-15\","
              + "\"deceasedDateTime\":\"2020-01-02T03:04:05+10:00\","
              + "\"meta\":{\"lastUpdated\":\"2024-05-06T07:08:09.123Z\"},"
              + "\"name\":[{\"family\":\"A\",\"period\":{\"start\":\"2001\"}},"
              + "{\"family\":\"B\"},"
              + "{\"family\":\"C\",\"period\":{\"start\":\"2002-03-04\"}}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"birthDate\":\"1980\","
              + "\"deceasedDateTime\":\"2020-01-01T17:04:05Z\","
              + "\"meta\":{\"lastUpdated\":\"2024-05-06T07:08:09Z\"}}",
          "{\"resourceType\":\"Patient\",\"id\":\"p3\",\"birthDate\":\"1500-02-03\","
              + "\"deceasedDateTime\":\"2020-01-02T03:04:05\","
              + "\"name\":[{\"family\":\"D\",\"period\":{\"start\":\"2003-01\"}}]}");

  private static final String MEDICATION_REQUEST =
      "{\"resourceType\":\"MedicationRequest\",\"id\":\"%s\",\"status\":\"active\","
          + "\"intent\":\"order\",\"subject\":{\"reference\":\"Patient/p\"},"
          + "\"medicationCodeableConcept\":{\"text\":\"x\"}%s}";

  private static final List<String> MEDICATION_REQUESTS =
      List.of(
          String.format(
              MEDICATION_REQUEST,
              "m1",
              ",\"dosageInstruction\":[{\"timing\":{\"event\":[\"2020\",\"2021-02\"]}}]"),
          String.format(
              MEDICATION_REQUEST,
              "m2",
              ",\"dosageInstruction\":[{\"timing\":{\"event\":[\"2019-12-31\"]}}]"),
          String.format(MEDICATION_REQUEST, "m3", ""));

  private static final List<String> ENCOUNTERS =
      List.of(
          "{\"resourceType\":\"Encounter\",\"id\":\"e1\",\"status\":\"finished\","
              + "\"class\":{\"code\":\"AMB\"},"
              + "\"period\":{\"start\":\"2020-01\",\"end\":\"2020-03-15\"}}",
          "{\"resourceType\":\"Encounter\",\"id\":\"e2\",\"status\":\"finished\","
              + "\"class\":{\"code\":\"AMB\"},\"period\":{\"start\":\"2021-06-01\"}}",
          "{\"resourceType\":\"Encounter\",\"id\":\"e3\",\"status\":\"finished\","
              + "\"class\":{\"code\":\"AMB\"}}");

  private static final Map<String, List<String>> FIXTURES =
      Map.of(
          "Patient", PATIENTS, "MedicationRequest", MEDICATION_REQUESTS, "Encounter", ENCOUNTERS);

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Map<String, Dataset<Row>> read;

  private Map<String, Dataset<Row>> annotated;

  private Map<String, Dataset<Row>> disabled;

  @BeforeAll
  void setUp() {
    read = stored("read", Optional.empty());
    annotated = stored("annotated", Optional.of(SchemaConfiguration.builder().build()));
    disabled =
        stored(
            "disabled",
            Optional.of(SchemaConfiguration.builder().enableRangeAnnotation(false).build()));
  }

  @Nonnull
  Stream<Arguments> cases() {
    return Stream.of(
        // A date at the root, against literals of the same, a coarser and a finer precision. Where
        // the ranges overlap without being equal, the comparison is empty.
        arguments("Patient", "birthDate > @1980-06-01", List.of("p1=true", "p2=null", "p3=false")),
        arguments("Patient", "birthDate < @1980", List.of("p1=null", "p2=false", "p3=true")),
        arguments("Patient", "birthDate >= @1980", List.of("p1=null", "p2=true", "p3=false")),
        arguments("Patient", "@1980-06-01 < birthDate", List.of("p1=true", "p2=null", "p3=false")),
        // A date before the Gregorian cutover.
        arguments(
            "Patient", "birthDate <= @1500-02-03", List.of("p1=false", "p2=false", "p3=true")),
        arguments("Patient", "birthDate < @1582-10-15", List.of("p1=false", "p2=false", "p3=true")),
        // A dateTime with a timezone, with UTC, and without a timezone, which is taken as UTC.
        arguments(
            "Patient",
            "deceased.ofType(dateTime) > @2020-01-01T17:04:05Z",
            List.of("p1=false", "p2=false", "p3=true")),
        arguments(
            "Patient",
            "deceased.ofType(dateTime) < @2020-01-02T03:04:05+00:00",
            List.of("p1=true", "p2=true", "p3=false")),
        arguments(
            "Patient",
            "deceased.ofType(dateTime) >= @2020-01-02T03:04",
            List.of("p1=false", "p2=false", "p3=null")),
        // An instant, stated to the second and to a fraction of one.
        arguments(
            "Patient",
            "meta.lastUpdated > @2024-05-06T07:08:09Z",
            List.of("p1=true", "p2=false", "p3=null")),
        arguments(
            "Patient",
            "meta.lastUpdated <= @2024-05-06T07:08:09.123Z",
            List.of("p1=true", "p2=true", "p3=null")),
        // Two stored dates.
        arguments(
            "Patient",
            "birthDate < deceased.ofType(dateTime)",
            List.of("p1=true", "p2=true", "p3=true")),
        // A date under a repeating parent.
        arguments(
            "Patient",
            "name.where(period.start > @2001-06).count()",
            List.of("p1=1", "p2=0", "p3=1")),
        arguments(
            "Patient",
            "name.where(period.start <= @2001).family.first()",
            List.of("p1=A", "p2=null", "p3=null")),
        // Equality, of one date and of several, where equal ranges are equal and overlapping ones
        // are empty.
        arguments("Patient", "birthDate = @1980", List.of("p1=null", "p2=true", "p3=false")),
        arguments("Patient", "birthDate != @1980-06-15", List.of("p1=false", "p2=null", "p3=true")),
        arguments(
            "Patient",
            "meta.lastUpdated = @2024-05-06T07:08:09.123Z",
            List.of("p1=true", "p2=false", "p3=null")),
        arguments(
            "Patient",
            "name.period.start = name.period.start",
            List.of("p1=true", "p2=null", "p3=true")),
        // A repeating date, through where, select, an index and first, and compared as a whole.
        arguments(
            "MedicationRequest",
            "dosageInstruction.timing.event.where($this > @2020-06).count()",
            List.of("m1=1", "m2=0", "m3=0")),
        arguments(
            "MedicationRequest",
            "dosageInstruction.timing.event.select($this >= @2020).allTrue()",
            List.of("m1=true", "m2=false", "m3=true")),
        arguments(
            "MedicationRequest",
            "dosageInstruction.timing.event[1] = @2021-02",
            List.of("m1=true", "m2=null", "m3=null")),
        arguments(
            "MedicationRequest",
            "dosageInstruction.timing.event != (@2020 | @2021-02)",
            List.of("m1=false", "m2=true", "m3=null")),
        arguments(
            "MedicationRequest",
            "dosageInstruction.timing.event.first() < @2020-06",
            List.of("m1=null", "m2=true", "m3=null")),
        arguments(
            "MedicationRequest",
            "dosageInstruction.timing.event = (@2020 | @2021-02)",
            List.of("m1=true", "m2=false", "m3=null")),
        // A Period's dates.
        arguments(
            "Encounter", "period.start < @2020-02", List.of("e1=true", "e2=false", "e3=null")),
        arguments(
            "Encounter", "period.end = @2020-03-15", List.of("e1=true", "e2=null", "e3=null")));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("cases")
  void dateComparison(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(read.get(resourceType), resourceType, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("cases")
  void dateComparisonOverAnnotatedDates(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(annotated.get(resourceType), resourceType, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("cases")
  void dateComparisonWithTheAnnotationDisabled(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(disabled.get(resourceType), resourceType, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> searchCases() {
    return Stream.of(
        // A date at the root.
        arguments("Patient", "birthdate=ge1980", List.of("p1", "p2")),
        arguments("Patient", "birthdate=1980", List.of("p1", "p2")),
        arguments("Patient", "birthdate=lt1600", List.of("p3")),
        // A repeating date.
        arguments("MedicationRequest", "date=ge2021", List.of("m1")),
        arguments("MedicationRequest", "date=2020-06", List.of("m1")),
        arguments("MedicationRequest", "date=lt2020", List.of("m2")),
        // A Period, one of whose bounds is open.
        arguments("Encounter", "date=2020-02", List.of("e1")),
        arguments("Encounter", "date=ge2021", List.of("e2")),
        arguments("Encounter", "date=gt2020-12", List.of("e2")));
  }

  /**
   * Date search gives the same resources over each path, and takes the stored range where the
   * schema carries it, parsing no date, and computes it otherwise.
   */
  @ParameterizedTest(name = "{0}?{1}")
  @MethodSource("searchCases")
  void dateSearch(
      @Nonnull final String resourceType,
      @Nonnull final String query,
      @Nonnull final List<String> expected) {
    final Function<Dataset<Row>, Dataset<Row>> search =
        dataset ->
            dataset.filter(
                SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
                    .fromQueryString(ResourceType.fromCode(resourceType), query));
    for (final Map<String, Dataset<Row>> path : List.of(read, annotated, disabled)) {
      assertThat(
              search.apply(path.get(resourceType)).select("id").collectAsList().stream()
                  .map(row -> row.getString(0))
                  .toList())
          .containsExactlyInAnyOrderElementsOf(expected);
    }
    assertThat(parses(search.apply(annotated.get(resourceType)))).isZero();
    assertThat(parses(search.apply(disabled.get(resourceType)))).isPositive();
  }

  @Nonnull
  Stream<Arguments> fastPathCases() {
    return Stream.of(
        arguments("Patient", "birthDate > @1980", "_birthDate"),
        arguments("Patient", "birthDate = @1980", "_birthDate"),
        arguments("Patient", "name.where(period.start > @2001-06).count()", "_start"),
        arguments(
            "MedicationRequest",
            "dosageInstruction.timing.event.where($this > @2020-06).count()",
            "_event"),
        arguments(
            "MedicationRequest", "dosageInstruction.timing.event = (@2020 | @2021-02)", "_event"),
        arguments(
            "MedicationRequest", "dosageInstruction.timing.event != (@2020 | @2021-02)", "_event"),
        arguments("Patient", "birthDate != @1980", "_birthDate"),
        arguments(
            "MedicationRequest",
            "dosageInstruction.timing.event.select($this >= @2020).allTrue()",
            "_event"));
  }

  /**
   * Comparison and equality take the stored range where the schema carries it, reading the two
   * annotations from the metadata group, and compute it from the stored date otherwise, so that the
   * annotated plan parses less than the computed one. Which of the two is decided once, from the
   * schema (FR-023).
   */
  @ParameterizedTest(name = "{1}")
  @MethodSource("fastPathCases")
  void comparisonTakesTheStoredRangeWhereTheSchemaCarriesIt(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final String group) {
    final Dataset<Row> fast = select(annotated.get(resourceType), resourceType, expression);
    assertThat(readSchema(fast)).contains(group + ":");

    for (final Map<String, Dataset<Row>> unannotated : List.of(read, disabled)) {
      final Dataset<Row> computed = select(unannotated.get(resourceType), resourceType, expression);
      assertThat(parses(fast)).isLessThan(parses(computed));
      assertThat(readSchema(computed)).doesNotContain(group + ":");
    }
  }

  /**
   * The range of a date that is not stored is computed from one parse of it for both its bounds,
   * and the range of the literal it is compared with is computed as the query is built, so that the
   * plan parses the stored date once and no literal.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("onceCases")
  void theComputedRangeTakesOneParse(@Nonnull final String expression) {
    final Dataset<Row> computed = select(disabled.get("Patient"), "Patient", expression);
    assertThat(parses(computed)).isOne();
    assertThat(literalParses(computed)).isZero();
  }

  @Nonnull
  Stream<String> onceCases() {
    return Stream.of("birthDate > @1980", "@1980 <= birthDate");
  }

  /** Counts the parses of dates in the optimised plan of a query. */
  private static long parses(@Nonnull final Dataset<Row> query) {
    return count(query, "date_range\\(");
  }

  /** Counts the parses of literals in the optimised plan of a query. */
  private static long literalParses(@Nonnull final Dataset<Row> query) {
    return count(query, "date_range\\(\\d");
  }

  private static long count(@Nonnull final Dataset<Row> query, @Nonnull final String pattern) {
    final Matcher matcher =
        Pattern.compile(pattern).matcher(query.queryExecution().optimizedPlan().toString());
    long count = 0;
    while (matcher.find()) {
      count++;
    }
    return count;
  }

  /** Returns the schema the scan of a query reads, which shows what the read prunes to. */
  @Nonnull
  private static String readSchema(@Nonnull final Dataset<Row> query) {
    return CollectionConverters.asJava(query.queryExecution().executedPlan().collectLeaves())
        .stream()
        .filter(FileSourceScanExec.class::isInstance)
        .map(scan -> ((FileSourceScanExec) scan).requiredSchema().simpleString())
        .findFirst()
        .orElseThrow();
  }

  /**
   * Builds the fixtures, as read or annotated as encoding stores them under a configuration, and
   * writes them to Parquet and reads them back, so that the engine reads stored files.
   */
  @Nonnull
  private Map<String, Dataset<Row>> stored(
      @Nonnull final String name, @Nonnull final Optional<SchemaConfiguration> annotation) {
    return FIXTURES.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                fixture -> {
                  final String resourceType = fixture.getKey();
                  final Dataset<Row> resources =
                      PofDatasets.fromJson(
                          spark, fhirEncoders.getContext(), resourceType, fixture.getValue());
                  final Dataset<Row> dataset =
                      annotation
                          .map(
                              configuration ->
                                  Annotator.of(
                                          FhirDefinitionContext.of(fhirEncoders.getContext()),
                                          configuration)
                                      .annotate(resourceType, resources))
                          .orElse(resources);
                  return PrunedSchemaReader.write(
                          dataset, tempDir.resolve(name + "-" + resourceType).toString())
                      .read();
                }));
  }

  /** Selects the column an expression builds over each resource in the dataset. */
  @Nonnull
  private Dataset<Row> select(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String resourceType,
      @Nonnull final String expression) {
    final Column column =
        SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
            .fromExpression(ResourceType.fromCode(resourceType), expression);
    return dataset.select(column.alias("x"));
  }

  /**
   * Evaluates an expression over each resource in the dataset, returning one {@code id=value} entry
   * per resource.
   */
  @Nonnull
  private List<String> evaluate(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String resourceType,
      @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(
                ResourceType.fromCode(resourceType), fhirEncoders.getContext())
            .withDataset(dataset)
            .build();
    return evaluator
        .evaluate(PARSER.parse(expression))
        .toCanonical()
        .toIdValueDataset()
        .collectAsList()
        .stream()
        .map(row -> row.getString(0) + "=" + Objects.toString(row.get(1)))
        .toList();
  }

  /**
   * A root date that is only output, not compared, neither parses its text nor reads its range
   * annotations, because the optimiser drops the unused range of the System date. A date under a
   * repeating parent, or a repeating one, is converted inside a lambda, where the range is not
   * dropped.
   */
  @Test
  void anOutputRootDateNeitherParsesNorReadsItsRange() {
    assertThat(parses(select(disabled.get("Patient"), "Patient", "birthDate"))).isZero();
    assertThat(readSchema(select(annotated.get("Patient"), "Patient", "birthDate")))
        .doesNotContain("_birthDate");
  }
}
