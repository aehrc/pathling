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
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.transform;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.helpers.PrimitiveValues;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.util.List;
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
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that {@code |} compares a complex value by a key that leaves out another producer's
 * annotation fields, at every depth, and keeps the first of each set of equal values (decision
 * 151), and that {@code =} and {@code distinct()} stay rejected on such values, as on main.
 *
 * <p>The two names of the Patient are equal but for a foreign annotation, {@code __acme_hash}, on
 * each name and on each name's period, whose values differ, so that a comparison that took the
 * field into account would tell them apart. The same expressions over the data without the field
 * are the control.
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class ComparisonKeyTest {

  private static final Parser PARSER = new Parser();

  private static final String PATIENT =
      "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"name\":["
          + "{\"family\":\"Smith\",\"given\":[\"Anne\"],\"period\":{\"start\":\"2001\"}},"
          + "{\"family\":\"Smith\",\"given\":[\"Anne\"],\"period\":{\"start\":\"2001\"}},"
          + "{\"family\":\"Jones\",\"period\":{\"start\":\"2001\"}}]}";

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  private Dataset<Row> plain;

  private Dataset<Row> foreign;

  @BeforeAll
  void setUp() {
    plain =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Patient", List.of(PATIENT)).cache();
    // Another producer's annotation, different on every name and on every name's period.
    foreign =
        plain.withColumn(
            "name",
            transform(
                col("name"),
                (name, index) ->
                    name.withField("__acme_hash", index)
                        .withField("period.__acme_hash", index.plus(lit(10)))));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void foreignAnnotationsTakeNoPartInAComparison(
      @Nonnull final String expression, @Nonnull final String expected) {
    assertThat(foreign.schema().apply("name").dataType().simpleString()).contains("__acme_hash");
    assertThat(evaluate(plain, expression)).isEqualTo(expected);
    assertThat(evaluate(foreign, expression)).isEqualTo(expected);
  }

  @Nonnull
  Stream<Arguments> cases() {
    // Equality and distinct() are not supported on complex types other than Quantity and Coding,
    // so the union's deduplication is the comparison exercised here.
    return Stream.of(
        Arguments.of("(name[0] | name[1]).count()", "[1]"),
        Arguments.of("(name[0] | name[2]).count()", "[2]"),
        Arguments.of("(name | name).count()", "[2]"),
        Arguments.of("(name.period | name.period).count()", "[1]"),
        Arguments.of("(name | name).family", "[ArraySeq(Smith, Jones)]"));
  }

  /**
   * Equality on a complex type other than Quantity and Coding stays rejected, as on main, whether
   * or not the values carry another producer's annotations; it is for the equality review of ticket
   * 26 (decision 151).
   */
  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"name[0] = name[1]", "name[0] != name[1]", "name.distinct()"})
  void equalityOnAComplexTypeIsStillRejected(@Nonnull final String expression) {
    for (final Dataset<Row> data : List.of(plain, foreign)) {
      assertThatThrownBy(() -> evaluate(data, expression))
          .isInstanceOf(UnsupportedFhirPathFeatureError.class)
          .hasMessage("Unsupported equality for complex types");
    }
  }

  @Nonnull
  private String evaluate(@Nonnull final Dataset<Row> data, @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirEncoders.getContext())
            .withDataset(data)
            .build();
    return String.valueOf(
        PrimitiveValues.of(evaluator.evaluate(PARSER.parse(expression)))
            .toIdValueDataset()
            .collectAsList()
            .stream()
            .map(row -> row.get(1))
            .map(String::valueOf)
            .toList());
  }
}
