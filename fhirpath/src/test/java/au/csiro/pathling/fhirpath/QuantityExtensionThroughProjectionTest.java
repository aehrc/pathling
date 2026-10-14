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

package au.csiro.pathling.fhirpath;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that {@code distinct()} and {@code select(...)} keep stored quantities, and so their
 * extensions, as the indexer and {@code where(...)} do (T100l).
 *
 * <p>A quantity is held in the stored shape and decoded only where it is computed with (decision
 * 82). {@code distinct()} compares quantities by their decoded, unit-aware values, and keeps the
 * first stored element of each set of equal ones, so the extension of the result is that of the
 * first occurrence. {@code select(...)} returns what its projection returns. Every case runs on
 * both layouts in the same run.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class QuantityExtensionThroughProjectionTest {

  private static final Parser PARSER = new Parser();

  /** Component quantities, each with a distinct extension, the first two equal in value. */
  private static final String EXTENDED =
      "{\"resourceType\":\"Observation\",\"id\":\"q1\",\"status\":\"final\","
          + "\"code\":{\"text\":\"x\"},\"component\":["
          + component("urn:kg", "1", "kg")
          + ","
          + component("urn:g", "1000", "g")
          + ","
          + component("urn:mg", "5", "mg")
          + "]}";

  /** An observation whose component quantity carries no extension. */
  private static final String BARE =
      "{\"resourceType\":\"Observation\",\"id\":\"q2\",\"status\":\"final\","
          + "\"code\":{\"text\":\"x\"},"
          + "\"component\":[{\"code\":{\"text\":\"a\"},\"valueQuantity\":{\"value\":5}}]}";

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  private Map<String, Dataset<Row>> datasets;

  @BeforeAll
  void setUp() {
    datasets =
        Map.of(
            "pof",
            LayoutDatasets.fromJson(
                spark, fhirEncoders, TestLayout.POF, "Observation", List.of(EXTENDED, BARE)),
            "previous",
            LayoutDatasets.fromJson(
                spark, fhirEncoders, TestLayout.PREVIOUS, "Observation", List.of(EXTENDED, BARE)));
  }

  @Nonnull
  private static String component(
      @Nonnull final String url, @Nonnull final String value, @Nonnull final String code) {
    return "{\"code\":{\"text\":\"c\"},\"valueQuantity\":{\"extension\":[{\"url\":\""
        + url
        + "\",\"valueString\":\"v\"}],\"value\":"
        + value
        + ",\"system\":\"http://unitsofmeasure.org\",\"code\":\""
        + code
        + "\"}}";
  }

  @Nonnull
  static Stream<Arguments> expressions() {
    final String quantities = "component.value.ofType(Quantity)";
    final String distinct = quantities + ".distinct()";
    final String selected = "component.select(value.ofType(Quantity))";
    return Stream.of(
        // The control: the quantities themselves keep their extensions.
        arguments(quantities + ".extension.url.count()", "q1=3", "q2=0"),
        // Of two equal quantities, distinct keeps the first, with its extension.
        arguments(distinct + ".count()", "q1=2", "q2=1"),
        arguments(distinct + ".extension.url.count()", "q1=2", "q2=0"),
        arguments(distinct + ".extension.where(url = 'urn:kg').exists()", "q1=true", "q2=false"),
        arguments(distinct + ".extension.where(url = 'urn:g').exists()", "q1=false", "q2=false"),
        arguments(distinct + ".extension.where(url = 'urn:mg').exists()", "q1=true", "q2=false"),
        arguments(distinct + ".code.count()", "q1=2", "q2=0"),
        arguments(distinct + ".where(extension.url = 'urn:mg').code.first()", "q1=mg", "q2=null"),
        // A projection returns its result as it is.
        arguments(selected + ".extension.url.count()", "q1=3", "q2=0"),
        arguments(selected + ".extension.where(url = 'urn:g').exists()", "q1=true", "q2=false"),
        arguments(selected + ".where(extension.url = 'urn:g').code.first()", "q1=g", "q2=null"),
        arguments("(" + selected + ".first() = 1 'kg')", "q1=true", "q2=null"));
  }

  @Nonnull
  Stream<Arguments> cases() {
    return expressions()
        .flatMap(
            expression ->
                datasets.keySet().stream()
                    .map(
                        dataset ->
                            arguments(
                                dataset,
                                expression.get()[0],
                                expression.get()[1],
                                expression.get()[2])));
  }

  @ParameterizedTest(name = "{1} over {0}")
  @MethodSource("cases")
  void quantityKeepsItsExtensions(
      @Nonnull final String dataset,
      @Nonnull final String expression,
      @Nonnull final String expected1,
      @Nonnull final String expected2) {
    assertThat(evaluate(datasets.get(dataset), expression))
        .as("%s over %s", expression, dataset)
        .containsExactlyInAnyOrder(expected1, expected2);
  }

  /**
   * Evaluates an expression over each observation in the dataset, returning one {@code id=value}
   * entry per observation.
   */
  @Nonnull
  private List<String> evaluate(
      @Nonnull final Dataset<Row> dataset, @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(ResourceType.OBSERVATION, fhirEncoders.getContext())
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
