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
 * Tests the indexer over quantities, which are held in the stored shape for their extensions and
 * decoded only where they are computed with (T097, decision 80).
 *
 * <p>Indexing only selects among the elements, so it keeps the stored element. An extension of the
 * indexed quantity is therefore found on either layout, and it is the extension of the element at
 * that index. Every case runs on both layouts in the same run.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class IndexedQuantityExtensionTest {

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
          + "\"code\":{\"text\":\"x\"},\"valueQuantity\":{\"value\":4,\"unit\":\"g\"},"
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
  private static String extension(@Nonnull final String url) {
    return "\"extension\":[{\"url\":\"" + url + "\",\"valueString\":\"v\"}]";
  }

  @Nonnull
  static Stream<Arguments> indexings() {
    final String components = "component.value.ofType(Quantity)";
    return Stream.of(
        // The extension of the element at the index, and not of any other element.
        arguments(
            components + "[0].extension.where(url = 'urn:first').exists()", "q1=true", "q2=false"),
        arguments(
            components + "[0].extension.where(url = 'urn:second').exists()",
            "q1=false",
            "q2=false"),
        arguments(
            components + "[1].extension.where(url = 'urn:second').exists()", "q1=true", "q2=false"),
        arguments(
            components + "[1].extension.where(url = 'urn:first').exists()", "q1=false", "q2=false"),
        arguments(components + "[2].extension.exists()", "q1=false", "q2=false"),
        // A singular quantity is its own first element.
        arguments(
            "value.ofType(Quantity)[0].extension.where(url = 'urn:quantity').exists()",
            "q1=true",
            "q2=false"),
        // The decoded element stays aligned with the stored one.
        arguments(components + "[1].code", "q1=mg", "q2=null"),
        arguments(components + "[0].code", "q1=kg", "q2=null"),
        arguments("(" + components + "[1] = 3 'mg')", "q1=true", "q2=null"));
  }

  @Nonnull
  Stream<Arguments> cases() {
    return indexings()
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
  void indexedQuantityKeepsItsExtensions(
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
