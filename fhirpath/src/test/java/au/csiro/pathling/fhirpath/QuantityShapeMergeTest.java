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
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.layout.AnnotatedDatasets;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
 * Tests that stored quantities whose structures differ in shape are compared by their canonical
 * form, whether or not they carry their canonical annotations.
 *
 * <p>Parquet prunes the fields no row populates, so the quantity of an observation and the
 * quantities of its components are routinely stored in different shapes. Comparing them merges the
 * two shapes, and the merge must keep the exact canonical value in the order of its type, which is
 * read by position.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class QuantityShapeMergeTest {

  private static final Parser PARSER = new Parser();

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Map<String, Dataset<Row>> datasets;

  /**
   * An observation whose quantity equals its component's quantity in canonical form. Where the
   * quantity has a comparator, only the quantity carries that field, so the two are stored in
   * different shapes, and a quantity with a comparator is a bound that takes part in no comparison.
   */
  @Nonnull
  private static String observation(@Nonnull final String id, final boolean comparator) {
    return "{\"resourceType\":\"Observation\",\"id\":\""
        + id
        + "\",\"status\":\"final\",\"code\":{\"text\":\"c\"},\"valueQuantity\":{\"value\":1,"
        + (comparator ? "\"comparator\":\"<\"," : "")
        + "\"unit\":\"g\",\"system\":\"http://unitsofmeasure.org\",\"code\":\"g\"},"
        + "\"component\":[{\"code\":{\"text\":\"a\"},\"valueQuantity\":{\"value\":1000,"
        + "\"unit\":\"mg\",\"system\":\"http://unitsofmeasure.org\",\"code\":\"mg\"}}]}";
  }

  @BeforeAll
  void setUp() {
    datasets =
        Map.of(
            "read",
            stored("read", Optional.empty()),
            "annotated",
            stored("annotated", Optional.of(SchemaConfiguration.builder().build())));
  }

  @Nonnull
  Stream<Arguments> cases() {
    final String value = "value.ofType(Quantity)";
    final String components = "component.value.ofType(Quantity)";
    final String union = "(" + components + " | " + value + ")";
    return Stream.of(
            arguments(value + " = " + components, List.of("x=true", "y=null")),
            arguments(union + ".count()", List.of("x=1", "y=2")),
            arguments(union + ".distinct().count()", List.of("x=1", "y=2")))
        .flatMap(
            expression ->
                datasets.keySet().stream()
                    .map(dataset -> arguments(dataset, expression.get()[0], expression.get()[1])));
  }

  @ParameterizedTest(name = "{1} over {0}")
  @MethodSource("cases")
  void quantitiesOfDifferingShapeAreComparedByTheirCanonicalForm(
      @Nonnull final String dataset,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(datasets.get(dataset), expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  private Dataset<Row> stored(
      @Nonnull final String name, @Nonnull final Optional<SchemaConfiguration> annotation) {
    return AnnotatedDatasets.stored(
        fhirEncoders.getContext(),
        "Observation",
        PofDatasets.fromJson(
            spark,
            fhirEncoders.getContext(),
            "Observation",
            List.of(observation("x", false), observation("y", true))),
        annotation,
        tempDir.resolve(name).toString());
  }

  /** Evaluates an expression over each observation, as one {@code id=value} entry for each. */
  @Nonnull
  private List<String> evaluate(
      @Nonnull final Dataset<Row> dataset, @Nonnull final String expression) {
    return DatasetEvaluatorBuilder.create(ResourceType.OBSERVATION, fhirEncoders.getContext())
        .withDataset(dataset)
        .build()
        .evaluate(PARSER.parse(expression))
        .toCanonical()
        .toIdValueDataset()
        .collectAsList()
        .stream()
        .map(row -> row.getString(0) + "=" + Objects.toString(row.get(1)))
        .toList();
  }
}
