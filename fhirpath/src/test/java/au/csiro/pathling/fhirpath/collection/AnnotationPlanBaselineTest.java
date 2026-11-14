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
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
import org.springframework.beans.factory.annotation.Autowired;
import scala.jdk.javaapi.CollectionConverters;

/**
 * The baseline plan tests of the annotations design (`design/annotations-in-the-engine.md` §7,
 * tickets 42 and 43).
 *
 * <p>Each test asserts the plan, or the result shape, that the engine builds as tickets 11 to 14
 * delivered it. They were written against that code and committed before tickets 42 and 43 changed
 * anything, so that each later change to what those tickets delivered shows as a change to one of
 * these tests. A change that alters an assertion updates it and says why, in the assertion's
 * comment.
 *
 * <ul>
 *   <li>An output-only date under a repeating parent, and the same after {@code where()}: whether
 *       the optimised plan computes the date's range, and whether the scan reads the stored range.
 *   <li>A merge of different shapes: an annotated date with an unannotated literal, and a stored
 *       quantity with a literal (`design/annotations-by-example.md` §5.1).
 *   <li>{@code birthdate=ge2000} and {@code where(birthDate >= @2000)} over annotated Parquet: the
 *       filters pushed to the scan.
 * </ul>
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class AnnotationPlanBaselineTest {

  private static final Parser PARSER = new Parser();

  /** Any computation of a date's range, whether as one structure or as one bound at a time. */
  private static final Pattern RANGE_COMPUTATION = Pattern.compile("date_(range|start|end)\\(");

  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"birthDate\":\"1970-03\","
              + "\"name\":[{\"family\":\"A\",\"period\":{\"start\":\"2001\"}},"
              + "{\"family\":\"B\",\"period\":{\"start\":\"2002-03-04\"}}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"birthDate\":\"2005-06-07\"}");

  private static final List<String> OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
              + "\"code\":{\"text\":\"weight\"},"
              + "\"valueQuantity\":{\"value\":72.50,\"unit\":\"kg\","
              + "\"system\":\"http://unitsofmeasure.org\",\"code\":\"kg\"}}");

  private static final Map<String, List<String>> FIXTURES =
      Map.of("Patient", PATIENTS, "Observation", OBSERVATIONS);

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  /** The fixtures as read, without annotations. */
  private Map<String, Dataset<Row>> read;

  /** The fixtures as annotated with every annotation on. */
  private Map<String, Dataset<Row>> annotated;

  @BeforeAll
  void setUp() {
    read = stored("read", Optional.empty());
    annotated = stored("annotated", Optional.of(SchemaConfiguration.builder().build()));
  }

  // An output-only date under a repeating parent.

  /**
   * Baseline (tickets 11 to 14): a date under a repeating parent that is only output is converted
   * to a System date inside the parent's lambda, where the optimiser does not drop the unused
   * range, so over unannotated data the plan computes the range of every date.
   *
   * <p>Changed by ticket 43: nothing is computed as a date is traversed, and a bound is computed
   * only where an operation reads it (design §4.3), so a date that is only output computes no range
   * (finding 2). The values are unchanged.
   */
  @Test
  void anOutputDateUnderARepeatingParentComputesNoRangeOverUnannotatedData() {
    final Dataset<Row> query = select(read, "Patient", "name.period.start");
    assertThat(computesRange(query)).isFalse();
    assertThat(values(query)).containsExactlyInAnyOrder("[2001, 2002-03-04]", "null");
  }

  /**
   * Baseline (tickets 11 to 14): over annotated data the same date computes nothing, and takes its
   * range from the stored annotations, which the scan therefore reads although nothing uses them.
   */
  @Test
  void anOutputDateUnderARepeatingParentReadsItsStoredRangeOverAnnotatedData() {
    final Dataset<Row> query = select(annotated, "Patient", "name.period.start");
    assertThat(computesRange(query)).isFalse();
    assertThat(readSchema(query)).contains("_start:struct<__start:timestamp,__end:timestamp>");
    assertThat(values(query)).containsExactlyInAnyOrder("[2001, 2002-03-04]", "null");
  }

  /**
   * Baseline (tickets 11 to 14): after {@code where()}, as without it, the range is computed.
   *
   * <p>Changed by ticket 43: as without {@code where()}, no range is computed for a date that is
   * only output. The values are unchanged.
   */
  @Test
  void anOutputDateUnderARepeatingParentAfterWhereComputesNoRangeOverUnannotatedData() {
    final Dataset<Row> query = select(read, "Patient", "name.where(family = 'B').period.start");
    assertThat(computesRange(query)).isFalse();
    assertThat(values(query)).containsExactlyInAnyOrder("[2002-03-04]", "null");
  }

  /**
   * Baseline (tickets 11 to 14): after {@code where()} over annotated data, nothing is computed and
   * the stored range is read.
   */
  @Test
  void anOutputDateUnderARepeatingParentAfterWhereReadsItsStoredRangeOverAnnotatedData() {
    final Dataset<Row> query =
        select(annotated, "Patient", "name.where(family = 'B').period.start");
    assertThat(computesRange(query)).isFalse();
    assertThat(readSchema(query)).contains("_start:struct<__start:timestamp,__end:timestamp>");
    assertThat(values(query)).containsExactlyInAnyOrder("[2002-03-04]", "null");
  }

  // A merge of different shapes.

  /**
   * Baseline (tickets 11 to 14): a stored date merged with a literal is a System date on both
   * sides, a structure of its text and its range, which is the stored one or else computed, and the
   * literal's constant. The shape does not depend on the annotations.
   *
   * <p>Changed by ticket 42: the System date carries its bounds as flat fields beside its text,
   * {@code __start} and {@code __end}, rather than grouped in {@code range} (decision 152), because
   * each bound is now computed on its own over a memoised parse rather than as one structure
   * (decision 145). The values are unchanged.
   *
   * <p>Changed by ticket 43: a merge keeps only the annotation fields every side has (decision 142,
   * D9; `design/annotations-by-example.md` §5.1). Over annotated data the stored date and the
   * literal both carry their bounds, and both are kept. Over unannotated data the stored date
   * carries none, so the literal's constants are dropped too, and the bounds are computed where an
   * operation reads them. The texts are unchanged.
   */
  @Test
  void aStoredDateMergedWithALiteralKeepsTheBoundsBothSidesHave() {
    final Dataset<Row> annotatedResult = evaluate(annotated, "Patient", "birthDate | @2000");
    assertThat(annotatedResult.schema().apply("value").dataType().simpleString())
        .isEqualTo("array<struct<value:string,__start:timestamp,__end:timestamp>>");
    assertThat(rows(annotatedResult))
        .containsExactlyInAnyOrder(
            "p1=[[1970-03,1970-03-01 00:00:00.0,1970-03-31 23:59:59.999999],"
                + " [2000,2000-01-01 00:00:00.0,2000-12-31 23:59:59.999999]]",
            "p2=[[2005-06-07,2005-06-07 00:00:00.0,2005-06-07 23:59:59.999999],"
                + " [2000,2000-01-01 00:00:00.0,2000-12-31 23:59:59.999999]]");
    final Dataset<Row> readResult = evaluate(read, "Patient", "birthDate | @2000");
    assertThat(readResult.schema().apply("value").dataType().simpleString())
        .isEqualTo("array<struct<value:string>>");
    assertThat(rows(readResult))
        .containsExactlyInAnyOrder("p1=[[1970-03], [2000]]", "p2=[[2005-06-07], [2000]]");
  }

  /**
   * Baseline (tickets 11 to 14): a stored quantity merged with a literal is a System quantity on
   * both sides, in its one fixed shape, with the stored quantity's text, its numeric value and its
   * canonical form, stored or else computed, and the literal's constants.
   *
   * <p>Changed by ticket 42: the canonical form is carried as two flat annotations, {@code
   * __canonical_code} and {@code __canonical_value_exact}, rather than grouped in {@code
   * __canonical_exact}, as ingest now stores it (decision 149). The values are unchanged.
   *
   * <p>Changed by ticket 50: a System quantity is {@code {value, unit, code, system}}, with its
   * value the engine's System.Decimal, and carries its canonical annotations only where they are
   * available (decision 150), and a merge keeps only the annotations every side has (decision 142,
   * D9; `design/annotations-by-example.md` §5.1). Over unannotated data the stored quantity has
   * none, so the literal's constants are dropped too, and the canonical forms are computed where a
   * comparison reads them. Over annotated data both sides keep them.
   */
  @Test
  void aStoredQuantityMergedWithALiteralKeepsTheCanonicalFormsBothSidesHave() {
    final Dataset<Row> readResult =
        evaluate(read, "Observation", "value.ofType(Quantity) | 5 'kg'");
    assertThat(readResult.schema().apply("value").dataType().simpleString())
        .isEqualTo("array<struct<value:decimal(38,6),unit:string,code:string,system:string>>");
    assertThat(rows(readResult))
        .containsExactly(
            "o1=[[72.500000,kg,kg,http://unitsofmeasure.org],"
                + " [5.000000,kg,kg,http://unitsofmeasure.org]]");
    final Dataset<Row> annotatedResult =
        evaluate(annotated, "Observation", "value.ofType(Quantity) | 5 'kg'");
    assertThat(annotatedResult.schema().apply("value").dataType().simpleString())
        .isEqualTo(
            "array<struct<value:decimal(38,6),unit:string,code:string,system:string,"
                + "__canonical_code:string,"
                + "__canonical_value_exact:struct<value:decimal(38,0),scale:int>>>");
    assertThat(rows(annotatedResult))
        .containsExactly(
            "o1=[[72.500000,kg,kg,http://unitsofmeasure.org,g,[72500,0]],"
                + " [5.000000,kg,kg,http://unitsofmeasure.org,g,[5000,0]]]");
  }

  // Pushdown of a date comparison over annotated Parquet.

  /**
   * Baseline (tickets 11 to 14): date search over annotated Parquet compares the stored range, read
   * through the structure the System date builds, so no filter on either bound reaches the scan.
   *
   * <p>Changed by ticket 43: the bounds are read through the date's accessors, which read the
   * stored annotations plainly, and {@code ge} adds a plain conjunct on the upper bound (decision
   * 144), so filters on both bounds reach the scan. The matches are unchanged.
   */
  @Test
  void aDateSearchPushesAFilterOnTheStoredRange() {
    final Dataset<Row> query =
        annotated
            .get("Patient")
            .filter(
                SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
                    .fromQueryString(ResourceType.PATIENT, "birthdate=ge2000"));
    assertThat(pushedFilters(query))
        .contains("GreaterThanOrEqual(_birthDate.__end,")
        .contains("GreaterThanOrEqual(_birthDate.__start,");
    assertThat(ids(query)).containsExactly("p2");
  }

  /**
   * Baseline (tickets 11 to 14): a date comparison in {@code where()} over annotated Parquet binds
   * each range once and compares it under a condition, so no filter on either bound reaches the
   * scan.
   *
   * <p>Changed by ticket 43: the comparison is emitted beside a plain conjunct on the stored upper
   * bound (decision 144), which reaches the scan, while the condition stays a residual filter. The
   * matches are unchanged.
   */
  @Test
  void aDateComparisonPushesAFilterOnTheStoredUpperBound() {
    final Dataset<Row> query =
        annotated
            .get("Patient")
            .filter(
                SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
                    .fromExpression(ResourceType.PATIENT, "birthDate >= @2000"));
    assertThat(pushedFilters(query)).contains("GreaterThanOrEqual(_birthDate.__end,");
    assertThat(ids(query)).containsExactly("p2");
  }

  /** Selects the column an expression builds over each resource, as a caller is given it. */
  @Nonnull
  private Dataset<Row> select(
      @Nonnull final Map<String, Dataset<Row>> data,
      @Nonnull final String resourceType,
      @Nonnull final String expression) {
    final Column column =
        SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
            .fromExpression(ResourceType.fromCode(resourceType), expression);
    return data.get(resourceType).select(column.alias("x"));
  }

  /** Evaluates an expression to the engine's own form of its result, by resource id. */
  @Nonnull
  private Dataset<Row> evaluate(
      @Nonnull final Map<String, Dataset<Row>> data,
      @Nonnull final String resourceType,
      @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(
                ResourceType.fromCode(resourceType), fhirEncoders.getContext())
            .withDataset(data.get(resourceType))
            .build();
    return evaluator.evaluate(PARSER.parse(expression)).toIdValueDataset();
  }

  /** Whether the optimised plan of a query computes the range of a date. */
  private static boolean computesRange(@Nonnull final Dataset<Row> query) {
    return RANGE_COMPUTATION.matcher(query.queryExecution().optimizedPlan().toString()).find();
  }

  /** The single column of a query, one string per row. */
  @Nonnull
  private static List<String> values(@Nonnull final Dataset<Row> query) {
    return query.collectAsList().stream()
        .map(row -> row.isNullAt(0) ? "null" : String.valueOf(row.getList(0)))
        .toList();
  }

  /** The rows of an id and value dataset, as {@code id=value}. */
  @Nonnull
  private static List<String> rows(@Nonnull final Dataset<Row> result) {
    return result.collectAsList().stream()
        .map(row -> row.getString(0) + "=" + row.getList(1))
        .toList();
  }

  /** The ids of the resources a filtered query keeps. */
  @Nonnull
  private static List<String> ids(@Nonnull final Dataset<Row> query) {
    return query.select("id").collectAsList().stream().map(row -> row.getString(0)).toList();
  }

  /** The scan of a query, which shows what the read prunes to and what is pushed down to it. */
  @Nonnull
  private static FileSourceScanExec scan(@Nonnull final Dataset<Row> query) {
    return CollectionConverters.asJava(query.queryExecution().executedPlan().collectLeaves())
        .stream()
        .filter(FileSourceScanExec.class::isInstance)
        .map(FileSourceScanExec.class::cast)
        .findFirst()
        .orElseThrow();
  }

  /** The schema the scan of a query reads. */
  @Nonnull
  private static String readSchema(@Nonnull final Dataset<Row> query) {
    return scan(query).requiredSchema().simpleString();
  }

  /** The filters pushed to the scan of a query, as the plan describes them. */
  @Nonnull
  private static String pushedFilters(@Nonnull final Dataset<Row> query) {
    return scan(query).metadata().get("PushedFilters").getOrElse(() -> "");
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
}
