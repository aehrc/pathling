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
import au.csiro.pathling.encoders.ColumnFunctions;
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
import au.csiro.pathling.utilities.SystemDecimal;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Tests decimal comparison, arithmetic and ordering over decimals stored as text in the Parquet on
 * FHIR layout, which give the results the previous layout's {@code DECIMAL(32,6)} columns gave
 * (T086, FR-035), and give the same results where the numeric annotation is stored beside the text
 * and taken as a fast path (T086a, T095a), and where it is disabled and the text is decoded.
 *
 * <p>Each fixture is given as FHIR JSON. The fixtures are aimed at what is particular to text:
 * decimals whose stored text is set by a double round trip rather than by the source, such as
 * {@code 1.50} stored as {@code 1.5} and {@code 1e-7} as {@code 1.0E-7}; a field whose values are
 * all integral, which is stored without a fractional part; an integral value beyond the range of a
 * long; more than six fractional digits; and a pair, {@code 9.5} and {@code 10.0}, that orders one
 * way as text and the other as numbers. The schema is also pruned to the elements the fixtures
 * populate.
 *
 * <p>The query-time value is {@code DECIMAL(32,6)} and is rendered with its trailing zeros
 * stripped, so the scale of the source is not observable through FHIRPath. It is asserted on the
 * text that the traversal expression yields before the engine decodes it, which is the double round
 * trip's.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class DecimalCollectionTest {

  private static final Parser PARSER = new Parser();

  /** A type other than System.Decimal's, at which a numeric annotation may have been stored. */
  private static final DecimalType WIDER = DataTypes.createDecimalType(38, 8);

  private static final List<String> LOCATIONS =
      List.of(
          "{\"resourceType\":\"Location\",\"id\":\"l1\","
              + "\"position\":{\"latitude\":1.50,\"longitude\":-2.25,\"altitude\":100}}",
          "{\"resourceType\":\"Location\",\"id\":\"l2\","
              + "\"position\":{\"latitude\":9.5,\"longitude\":10.0,\"altitude\":7}}",
          "{\"resourceType\":\"Location\",\"id\":\"l3\","
              + "\"position\":{\"latitude\":0.1234567,\"longitude\":1e-7,\"altitude\":0}}");

  private static final List<String> RISK_ASSESSMENTS =
      List.of(
          "{\"resourceType\":\"RiskAssessment\",\"id\":\"r1\",\"status\":\"final\","
              + "\"subject\":{\"reference\":\"Patient/p\"},"
              + "\"prediction\":[{\"probabilityDecimal\":0.25},{\"probabilityDecimal\":0.75}]}",
          "{\"resourceType\":\"RiskAssessment\",\"id\":\"r2\",\"status\":\"final\","
              + "\"subject\":{\"reference\":\"Patient/p\"},"
              + "\"prediction\":[{\"probabilityDecimal\":0.5}]}");

  /**
   * A decimal beyond the range of {@code DECIMAL(32,6)} under a repeating parent, kept apart from
   * the fixtures above so that their expected values stay as T086 pins them.
   */
  private static final List<String> OUT_OF_RANGE =
      List.of(
          "{\"resourceType\":\"RiskAssessment\",\"id\":\"r1\",\"status\":\"final\","
              + "\"subject\":{\"reference\":\"Patient/p\"},"
              + "\"prediction\":[{\"probabilityDecimal\":0.5},{\"probabilityDecimal\":1e27}]}");

  private static final List<String> CHARGE_ITEMS =
      List.of(
          "{\"resourceType\":\"ChargeItem\",\"id\":\"c1\",\"status\":\"billable\","
              + "\"code\":{\"text\":\"x\"},\"subject\":{\"reference\":\"Patient/p\"},"
              + "\"factorOverride\":12345678901234567890}",
          "{\"resourceType\":\"ChargeItem\",\"id\":\"c2\",\"status\":\"billable\","
              + "\"code\":{\"text\":\"x\"},\"subject\":{\"reference\":\"Patient/p\"},"
              + "\"factorOverride\":3}");

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Map<String, Dataset<Row>> datasets;

  private Map<String, Dataset<Row>> annotated;

  private Map<String, Dataset<Row>> disabled;

  @BeforeAll
  void setUp() {
    datasets = datasets("read", Optional.empty());
    annotated = datasets("annotated", Optional.of(SchemaConfiguration.builder().build()));
    disabled =
        datasets(
            "disabled",
            Optional.of(SchemaConfiguration.builder().enableNumericAnnotation(false).build()));
  }

  @Nonnull
  Stream<Arguments> cases() {
    return Stream.of(
        // Decimals under a singular parent.
        arguments(
            "Location", "position.latitude", List.of("l1=1.500000", "l2=9.500000", "l3=0.123457")),
        arguments(
            "Location",
            "position.longitude",
            List.of("l1=-2.250000", "l2=10.000000", "l3=0.000000")),
        arguments(
            "Location",
            "position.altitude",
            List.of("l1=100.000000", "l2=7.000000", "l3=0.000000")),
        // Comparison, against literals and integers, and between two decimals.
        arguments(
            "Location", "position.latitude = 1.5", List.of("l1=true", "l2=false", "l3=false")),
        arguments(
            "Location", "position.latitude != 1.5", List.of("l1=false", "l2=true", "l3=true")),
        arguments(
            "Location", "position.altitude = 100", List.of("l1=true", "l2=false", "l3=false")),
        arguments("Location", "position.longitude > 0", List.of("l1=false", "l2=true", "l3=false")),
        // Ordering, where text and numbers disagree for 9.5 and 10.0.
        arguments("Location", "position.latitude > 2", List.of("l1=false", "l2=true", "l3=false")),
        arguments(
            "Location", "position.latitude <= 1.5", List.of("l1=true", "l2=false", "l3=true")),
        arguments(
            "Location", "position.latitude >= 9.5", List.of("l1=false", "l2=true", "l3=false")),
        arguments(
            "Location",
            "position.latitude < position.longitude",
            List.of("l1=false", "l2=true", "l3=false")),
        // Arithmetic, against literals, integers and another decimal.
        arguments(
            "Location",
            "position.latitude + 1",
            List.of("l1=2.500000", "l2=10.500000", "l3=1.123457")),
        arguments(
            "Location",
            "position.latitude - position.longitude",
            List.of("l1=3.750000", "l2=-0.500000", "l3=0.123457")),
        arguments(
            "Location",
            "position.latitude * position.longitude",
            List.of("l1=-3.375000", "l2=95.000000", "l3=0.000000")),
        arguments(
            "Location",
            "position.latitude / 3",
            List.of("l1=0.500000", "l2=3.166667", "l3=0.041152")),
        // Rendering as a string.
        arguments(
            "Location", "position.latitude.toString()", List.of("l1=1.5", "l2=9.5", "l3=0.123457")),
        // Decimals under a repeating parent, reached through a choice.
        arguments(
            "RiskAssessment",
            "prediction.probability.ofType(decimal).first()",
            List.of("r1=0.250000", "r2=0.500000")),
        arguments(
            "RiskAssessment",
            "prediction.probability.ofType(decimal).where($this > 0.3).count()",
            List.of("r1=1", "r2=1")),
        arguments(
            "RiskAssessment",
            "prediction.probability.ofType(decimal)[1] * 2",
            List.of("r1=1.500000", "r2=null")),
        arguments(
            "RiskAssessment",
            "prediction.probability.ofType(decimal) contains 0.75",
            List.of("r1=true", "r2=false")),
        // A decimal at the resource root, beyond the range of a long.
        arguments(
            "ChargeItem",
            "factorOverride",
            List.of("c1=12345678901234567890.000000", "c2=3.000000")),
        arguments("ChargeItem", "factorOverride > 100", List.of("c1=true", "c2=false")),
        arguments(
            "ChargeItem",
            "factorOverride * 2",
            List.of("c1=24691357802469135780.000000", "c2=6.000000")),
        arguments(
            "ChargeItem", "factorOverride.toString()", List.of("c1=12345678901234567890", "c2=3")));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("cases")
  void decimalOperations(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(datasets.get(resourceType), resourceType, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("cases")
  void decimalOperationsOverAnnotatedDecimals(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(annotated.get(resourceType), resourceType, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> widerAnnotationCases() {
    return Stream.of(
        arguments(
            "Location", "position.latitude", List.of("l1=1.500000", "l2=9.500000", "l3=0.123457")),
        arguments(
            "RiskAssessment",
            "prediction.probability.ofType(decimal).first()",
            List.of("r1=0.250000", "r2=0.500000")),
        arguments(
            "ChargeItem",
            "factorOverride",
            List.of("c1=12345678901234567890.000000", "c2=3.000000")));
  }

  /**
   * A numeric annotation stored at a type other than the engine's System.Decimal is decoded to it,
   * so the decimal has the type and value it would have had from the text.
   */
  @ParameterizedTest(name = "{1}")
  @MethodSource("widerAnnotationCases")
  void anAnnotationStoredAtAnotherTypeIsDecodedToSystemDecimal(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    final Dataset<Row> wider = withWiderAnnotation(resourceType, annotated.get(resourceType));
    assertThat(wider.schema().simpleString()).contains("__numeric:" + WIDER.simpleString());

    assertThat(select(wider, resourceType, expression).schema().apply("x").dataType())
        .isIn(SystemDecimal.TYPE, DataTypes.createArrayType(SystemDecimal.TYPE));
    assertThat(evaluate(wider, resourceType, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  /** Stores each numeric annotation of a fixture at {@link #WIDER} rather than as encoded. */
  @Nonnull
  private Dataset<Row> withWiderAnnotation(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> resources) {
    final Dataset<Row> wider =
        switch (resourceType) {
          case "Location" ->
              resources.withColumn(
                  "position",
                  functions
                      .col("position")
                      .withField(
                          "_latitude.__numeric",
                          functions.col("position._latitude.__numeric").cast(WIDER)));
          case "RiskAssessment" ->
              resources.withColumn(
                  "prediction",
                  functions.transform(
                      functions.col("prediction"),
                      p ->
                          p.withField(
                              "_probabilityDecimal.__numeric",
                              p.getField("_probabilityDecimal")
                                  .getField("__numeric")
                                  .cast(WIDER))));
          default ->
              resources.withColumn(
                  "_factorOverride",
                  functions
                      .col("_factorOverride")
                      .withField(
                          "__numeric", functions.col("_factorOverride.__numeric").cast(WIDER)));
        };
    final String directory = tempDir.resolve("wider-" + resourceType).toString();
    wider.write().parquet(directory);
    return spark.read().parquet(directory);
  }

  /**
   * A text that does not decode gives a null item, whether the annotation is stored or the text is
   * decoded, as decoding the text alone gave before the annotation existed.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("outOfRangeCases")
  void aDecimalBeyondTheRangeGivesTheSameItemsOnEveryPath(
      @Nonnull final String expression, @Nonnull final List<String> expected) {
    final Map<String, Optional<SchemaConfiguration>> paths =
        Map.of(
            "read",
            Optional.empty(),
            "annotated",
            Optional.of(SchemaConfiguration.builder().build()),
            "disabled",
            Optional.of(SchemaConfiguration.builder().enableNumericAnnotation(false).build()));
    paths.forEach(
        (name, annotation) ->
            assertThat(
                    evaluate(
                        stored("out-of-range-" + name, "RiskAssessment", OUT_OF_RANGE, annotation),
                        "RiskAssessment",
                        expression))
                .as(name)
                .containsExactlyInAnyOrderElementsOf(expected));
  }

  @Nonnull
  Stream<Arguments> outOfRangeCases() {
    return Stream.of(
        arguments("prediction.probability.ofType(decimal).count()", List.of("r1=2")),
        arguments("prediction.probability.ofType(decimal)[1]", List.of("r1=null")),
        arguments("prediction.probability.ofType(decimal).first()", List.of("r1=0.500000")));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("cases")
  void decimalOperationsWithTheAnnotationDisabled(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(disabled.get(resourceType), resourceType, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> decodingCases() {
    return Stream.of(
        arguments("Location", "position.latitude", "latitude"),
        arguments("RiskAssessment", "prediction.probability.ofType(decimal)", "probabilityDecimal"),
        arguments("ChargeItem", "factorOverride", "factorOverride"));
  }

  /**
   * Decoding takes the stored annotation where the schema carries it, and reads only the annotation
   * from the metadata group. Where the schema does not carry it, because the data was not annotated
   * or was annotated with the numeric annotation disabled, the text is decoded. Which of the two is
   * decided once, from the schema (FR-023).
   */
  @ParameterizedTest(name = "{1}")
  @MethodSource("decodingCases")
  void decodingTakesTheAnnotationWhereTheSchemaCarriesIt(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final String element) {
    // The text is decoded by a cast, which names the element, followed by the id Spark gives it.
    final String decodeText = element + "(#\\d+)? as decimal\\(32,6\\)";

    final Dataset<Row> fast = select(annotated.get(resourceType), resourceType, expression);
    assertThat(fast.queryExecution().optimizedPlan().expressions().mkString(","))
        .contains("__numeric")
        .doesNotContainPattern(decodeText);
    assertThat(readSchema(fast)).contains("_" + element + ":struct<__numeric:decimal(32,6)>");

    for (final Map<String, Dataset<Row>> unannotated : List.of(datasets, disabled)) {
      final Dataset<Row> lexical = select(unannotated.get(resourceType), resourceType, expression);
      // The text is decoded once: reading the computed annotation as System.Decimal, its own type,
      // leaves it as it is.
      assertThat(lexical.queryExecution().optimizedPlan().expressions().mkString(","))
          .containsPattern(decodeText)
          .doesNotContain("try_cast(try_cast(");
      assertThat(readSchema(lexical)).doesNotContain("_" + element);
    }
  }

  @Nonnull
  Stream<Arguments> storedText() {
    final Column latitude =
        ColumnFunctions.resolveOrNull(functions.col("position"), "latitude", DataTypes.StringType);
    final Column longitude =
        ColumnFunctions.resolveOrNull(functions.col("position"), "longitude", DataTypes.StringType);
    final Column altitude =
        ColumnFunctions.resolveOrNull(functions.col("position"), "altitude", DataTypes.StringType);
    final Column factorOverride =
        ColumnFunctions.columnOrNull("factorOverride", DataTypes.StringType);
    // The layout stores what a double round trip gives, or the integer where every value of the
    // field is integral.
    return Stream.of(
        arguments("Location", latitude, List.of("l1=1.5", "l2=9.5", "l3=0.1234567")),
        arguments("Location", longitude, List.of("l1=-2.25", "l2=10.0", "l3=1.0E-7")),
        arguments("Location", altitude, List.of("l1=100", "l2=7", "l3=0")),
        arguments("ChargeItem", factorOverride, List.of("c1=12345678901234567890", "c2=3")));
  }

  @ParameterizedTest(name = "{0} decimal text, case {index}")
  @MethodSource("storedText")
  void traversalYieldsTheStoredText(
      @Nonnull final String resourceType,
      @Nonnull final Column traversal,
      @Nonnull final List<String> expected) {
    final Dataset<Row> text =
        datasets.get(resourceType).select(functions.col("id"), traversal.alias("text"));
    assertThat(text.schema().apply("text").dataType()).isEqualTo(DataTypes.StringType);
    assertThat(
            text.collectAsList().stream().map(row -> row.getString(0) + "=" + row.get(1)).toList())
        .containsExactlyInAnyOrderElementsOf(expected);
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
   * Builds each fixture, as read or annotated as encoding stores it under a configuration, and
   * writes it to Parquet and reads it back, so that the engine reads stored files.
   */
  @Nonnull
  private Map<String, Dataset<Row>> datasets(
      @Nonnull final String name, @Nonnull final Optional<SchemaConfiguration> annotation) {
    return Map.of(
        "Location",
        stored(name, "Location", LOCATIONS, annotation),
        "RiskAssessment",
        stored(name, "RiskAssessment", RISK_ASSESSMENTS, annotation),
        "ChargeItem",
        stored(name, "ChargeItem", CHARGE_ITEMS, annotation));
  }

  @Nonnull
  private Dataset<Row> stored(
      @Nonnull final String name,
      @Nonnull final String resourceType,
      @Nonnull final List<String> json,
      @Nonnull final Optional<SchemaConfiguration> annotation) {
    final Dataset<Row> read =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), resourceType, json);
    final Dataset<Row> dataset =
        annotation
            .map(
                configuration ->
                    Annotator.of(FhirDefinitionContext.of(fhirEncoders.getContext()), configuration)
                        .annotate(resourceType, read))
            .orElse(read);
    return PrunedSchemaReader.write(dataset, tempDir.resolve(name + "-" + resourceType).toString())
        .read();
  }

  /** Selects the column an expression builds over each resource in the dataset. */
  @Nonnull
  private Dataset<Row> select(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String resourceType,
      @Nonnull final String expression) {
    return dataset.select(
        SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
            .fromExpression(ResourceType.fromCode(resourceType), expression)
            .alias("x"));
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
}
