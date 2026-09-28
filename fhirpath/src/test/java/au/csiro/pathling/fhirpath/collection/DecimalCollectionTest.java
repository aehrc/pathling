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

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that decimal comparison, arithmetic and ordering give the same results over decimals stored
 * as text in the new layout as over the previous layout's {@code DECIMAL(32,6)} columns (T086,
 * FR-035).
 *
 * <p>Each fixture is given once as FHIR JSON and read in both layouts, so every case runs over each
 * layout with the same expected answer. The fixtures are aimed at what emulating the new layout on
 * the previous one cannot produce: decimals whose stored text is set by a double round trip rather
 * than by the source, such as {@code 1.50} stored as {@code 1.5} and {@code 1e-7} as {@code
 * 1.0E-7}; a field whose values are all integral, which is stored without a fractional part; an
 * integral value beyond the range of a long; more than six fractional digits; and a pair, {@code
 * 9.5} and {@code 10.0}, that orders one way as text and the other as numbers. The new layout's
 * schema is also pruned to the elements the fixtures populate.
 *
 * <p>The query-time value is {@code DECIMAL(32,6)} on both layouts and is rendered with its
 * trailing zeros stripped, so the scale of the source is not observable through FHIRPath on either.
 * It is asserted on the text that the traversal expression yields before the engine decodes it: the
 * source scale over the previous layout, and the double round trip's over the new layout.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class DecimalCollectionTest {

  private static final Parser PARSER = new Parser();

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

  private Map<String, Map<String, Dataset<Row>>> datasets;

  @BeforeAll
  void setUp() {
    datasets =
        Map.of("previous", datasetsFor(TestLayout.PREVIOUS), "pof", datasetsFor(TestLayout.POF));
  }

  @Nonnull
  Stream<Arguments> cases() {
    return Stream.of(
            // Decimals under a singular parent.
            arguments(
                "Location",
                "position.latitude",
                List.of("l1=1.500000", "l2=9.500000", "l3=0.123457")),
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
            arguments(
                "Location", "position.longitude > 0", List.of("l1=false", "l2=true", "l3=false")),
            // Ordering, where text and numbers disagree for 9.5 and 10.0.
            arguments(
                "Location", "position.latitude > 2", List.of("l1=false", "l2=true", "l3=false")),
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
                "Location",
                "position.latitude.toString()",
                List.of("l1=1.5", "l2=9.5", "l3=0.123457")),
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
                "ChargeItem",
                "factorOverride.toString()",
                List.of("c1=12345678901234567890", "c2=3")))
        .flatMap(
            c ->
                Stream.of("previous", "pof")
                    .map(layout -> arguments(layout, c.get()[0], c.get()[1], c.get()[2])));
  }

  @ParameterizedTest(name = "{2} over the {0} layout")
  @MethodSource("cases")
  void decimalOperationsMatchOnBothLayouts(
      @Nonnull final String layout,
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(datasets.get(layout).get(resourceType), resourceType, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
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
        ColumnFunctions.decimalColumnOrNull("factorOverride", DataTypes.StringType);
    return Stream.of(
        // The previous layout keeps the source scale, capped at six.
        arguments("previous", "Location", latitude, List.of("l1=1.50", "l2=9.5", "l3=0.123457")),
        arguments("previous", "Location", longitude, List.of("l1=-2.25", "l2=10.0", "l3=0.000000")),
        arguments("previous", "Location", altitude, List.of("l1=100", "l2=7", "l3=0")),
        arguments(
            "previous", "ChargeItem", factorOverride, List.of("c1=12345678901234567890", "c2=3")),
        // The new layout stores what a double round trip gives, or the integer where every value
        // of the field is integral.
        arguments("pof", "Location", latitude, List.of("l1=1.5", "l2=9.5", "l3=0.1234567")),
        arguments("pof", "Location", longitude, List.of("l1=-2.25", "l2=10.0", "l3=1.0E-7")),
        arguments("pof", "Location", altitude, List.of("l1=100", "l2=7", "l3=0")),
        arguments("pof", "ChargeItem", factorOverride, List.of("c1=12345678901234567890", "c2=3")));
  }

  @ParameterizedTest(name = "{1} decimal text over the {0} layout, case {index}")
  @MethodSource("storedText")
  void traversalYieldsTheLayoutsText(
      @Nonnull final String layout,
      @Nonnull final String resourceType,
      @Nonnull final Column traversal,
      @Nonnull final List<String> expected) {
    final Dataset<Row> text =
        datasets.get(layout).get(resourceType).select(functions.col("id"), traversal.alias("text"));
    assertThat(text.schema().apply("text").dataType()).isEqualTo(DataTypes.StringType);
    assertThat(
            text.collectAsList().stream().map(row -> row.getString(0) + "=" + row.get(1)).toList())
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  /**
   * Builds each fixture in a layout, and writes it to Parquet and reads it back, so that the engine
   * reads stored files. A dataset built in memory can hand a decimal to the query with more
   * fractional digits than its type holds, which no stored file does.
   */
  @Nonnull
  private Map<String, Dataset<Row>> datasetsFor(@Nonnull final TestLayout layout) {
    return Map.of(
        "Location",
        stored(layout, "Location", LOCATIONS),
        "RiskAssessment",
        stored(layout, "RiskAssessment", RISK_ASSESSMENTS),
        "ChargeItem",
        stored(layout, "ChargeItem", CHARGE_ITEMS));
  }

  @Nonnull
  private Dataset<Row> stored(
      @Nonnull final TestLayout layout,
      @Nonnull final String resourceType,
      @Nonnull final List<String> json) {
    final Dataset<Row> dataset =
        LayoutDatasets.fromJson(spark, fhirEncoders, layout, resourceType, json);
    return PrunedSchemaReader.write(
            dataset, tempDir.resolve(layout + "-" + resourceType).toString())
        .read();
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
