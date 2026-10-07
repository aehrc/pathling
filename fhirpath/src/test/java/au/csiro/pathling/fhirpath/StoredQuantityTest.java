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
 * Tests that a quantity reached by traversal is held in the stored shape, and is decoded only where
 * it is computed with.
 *
 * <p>A stored quantity keeps its extensions through the operations that only select or combine
 * stored quantities. Where it meets a System quantity, such as a literal, it is taken as the System
 * quantity it decodes to, which has no extensions. Every case runs on both layouts in the same run.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class StoredQuantityTest {

  private static final Parser PARSER = new Parser();

  /** An observation whose quantity and component quantities each carry a distinct extension. */
  private static final String EXTENDED =
      "{\"resourceType\":\"Observation\",\"id\":\"q1\",\"status\":\"final\","
          + "\"code\":{\"text\":\"x\"},\"valueQuantity\":{"
          + extension("urn:quantity")
          + ",\"value\":1.5,\"unit\":\"g\",\"system\":\"http://unitsofmeasure.org\","
          + "\"code\":\"g\"},\"component\":["
          + "{\"code\":{\"text\":\"a\"},\"valueQuantity\":{"
          + extension("urn:first")
          + ",\"value\":2,\"system\":\"http://unitsofmeasure.org\",\"code\":\"kg\"}},"
          + "{\"code\":{\"text\":\"b\"},\"valueQuantity\":{"
          + extension("urn:second")
          + ",\"value\":3,\"system\":\"http://unitsofmeasure.org\",\"code\":\"mg\"}}]}";

  /** An observation whose quantities carry no extension. */
  private static final String BARE =
      "{\"resourceType\":\"Observation\",\"id\":\"q2\",\"status\":\"final\","
          + "\"code\":{\"text\":\"x\"},\"valueQuantity\":{\"value\":4,\"unit\":\"g\","
          + "\"system\":\"http://unitsofmeasure.org\",\"code\":\"g\"},"
          + "\"component\":[{\"code\":{\"text\":\"a\"},\"valueQuantity\":{\"value\":5,"
          + "\"system\":\"http://unitsofmeasure.org\",\"code\":\"g\"}}]}";

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
  private static String extension(@Nonnull final String url) {
    return "\"extension\":[{\"url\":\"" + url + "\",\"valueString\":\"v\"}]";
  }

  @Nonnull
  static Stream<Arguments> expressions() {
    final String value = "value.ofType(Quantity)";
    final String components = "component.value.ofType(Quantity)";
    return Stream.of(
        // A union of stored quantities keeps the extensions of each of them.
        arguments(
            "(" + value + " | " + components + ").extension.where(url = 'urn:quantity').exists()",
            "q1=true",
            "q2=false"),
        arguments(
            "(" + value + " | " + components + ").extension.where(url = 'urn:second').exists()",
            "q1=true",
            "q2=false"),
        arguments("(" + value + " | " + components + ").count()", "q1=3", "q2=2"),
        // Combining keeps the extensions too, and does not deduplicate.
        arguments(
            value + ".combine(" + value + ").extension.where(url = 'urn:quantity').count()",
            "q1=2",
            "q2=0"),
        // A union with a System quantity takes the stored quantity as a System quantity, which has
        // no extensions, and keeps its value.
        arguments("(" + value + " | 7 'g').extension.exists()", "q1=false", "q2=false"),
        arguments("(" + value + " | 7 'g').count()", "q1=2", "q2=2"),
        arguments("(7 'g' | " + value + ").count()", "q1=2", "q2=2"),
        // A union deduplicates by the canonical value, whichever the form of the operands.
        arguments("(" + value + " | 1500 'mg').count()", "q1=1", "q2=2"),
        // Equality and comparison decode the stored quantity.
        arguments(value + " = 1500 'mg'", "q1=true", "q2=false"),
        arguments(value + " = " + value, "q1=true", "q2=true"),
        arguments(components + "[0] > " + components + "[1]", "q1=true", "q2=null"),
        arguments(value + " < 1 'kg'", "q1=true", "q2=true"),
        // A System quantity on the left decodes the stored quantity on the right just the same.
        arguments("1500 'mg' = " + value, "q1=true", "q2=false"),
        arguments("2 'g' < " + value, "q1=false", "q2=true"),
        arguments("1 'kg' > " + value, "q1=true", "q2=true"),
        // A stored quantity is deduplicated by its canonical value.
        arguments("(" + components + " | " + components + ").count()", "q1=2", "q2=1"),
        // Operations that compute with the quantity decode it.
        arguments(value + ".toString()", "q1=1.5 'g'", "q2=4 'g'"),
        arguments(value + ".toQuantity('mg').value", "q1=1500.000000", "q2=4000.000000"),
        arguments(value + ".toQuantity().extension.exists()", "q1=false", "q2=false"),
        arguments(value + ".extension.exists()", "q1=true", "q2=false"));
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
  void storedQuantityIsDecodedOnlyWhereItIsComputedWith(
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
