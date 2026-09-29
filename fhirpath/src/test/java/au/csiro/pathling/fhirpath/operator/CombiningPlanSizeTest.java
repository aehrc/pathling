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

package au.csiro.pathling.fhirpath.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.CollectionDataset;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
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
 * Tests that a chain of unions or combinations of stored structures grows the analysed plan
 * linearly in the length of the chain (FR-056).
 *
 * <p>Each operand of a combination of structures is projected into the merged structure of all of
 * them, and the projection of one operand depends on the types of the others. Were each operand's
 * projection to carry every operand, a chain would carry the whole of its left side twice at every
 * level, and the analysed plan would double with every operand added. The test compares the size of
 * the analysed plan of a chain with that of a chain half as long, which a linear plan keeps close
 * to twice and a doubling plan multiplies many times over.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class CombiningPlanSizeTest {

  private static final Parser PARSER = new Parser();

  // The names of a Patient have given names, and those of its contacts have text, so that the two
  // HumanName shapes of the new layout differ.
  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\","
              + "\"name\":[{\"family\":\"F1\",\"given\":[\"G1\"]}],"
              + "\"contact\":[{\"name\":{\"text\":\"Contact One\",\"family\":\"C1\"}}]}");

  private static final int SHORT_CHAIN = 4;

  private static final int LONG_CHAIN = 8;

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Map<String, Dataset<Row>> patients;

  @BeforeAll
  void setUp() {
    patients =
        Map.of(
            "previous",
            stored(TestLayout.PREVIOUS, "previous"),
            "pof",
            stored(TestLayout.POF, "pof"));
  }

  @Nonnull
  Stream<Arguments> chains() {
    return Stream.of("previous", "pof")
        .flatMap(layout -> Stream.of(arguments(layout, "union"), arguments(layout, "combine")));
  }

  @ParameterizedTest(name = "{1} over the {0} layout")
  @MethodSource("chains")
  void chainedCombinationGrowsThePlanLinearly(
      @Nonnull final String layout, @Nonnull final String form) {
    final Dataset<Row> dataset = patients.get(layout);
    final long shortSize = analysedSize(dataset, chain(form, SHORT_CHAIN));
    final long longSize = analysedSize(dataset, chain(form, LONG_CHAIN));

    // A chain twice as long has a plan about twice as large when the plan grows linearly. A plan
    // that doubles at every level is sixteen times as large.
    assertThat(longSize)
        .as("analysed plan nodes for chains of %d and %d", SHORT_CHAIN, LONG_CHAIN)
        .isLessThanOrEqualTo(3 * shortSize);
  }

  @ParameterizedTest(name = "{1} over the {0} layout")
  @MethodSource("chains")
  void chainedCombinationKeepsItsValues(@Nonnull final String layout, @Nonnull final String form) {
    // The union of the alternating names is the two distinct names, and their combination keeps
    // every one of the nine operands.
    final String expected = "union".equals(form) ? "p1=F1,C1" : "p1=F1,C1,F1,C1,F1,C1,F1,C1,F1";
    assertThat(evaluate(patients.get(layout), "(" + chain(form, LONG_CHAIN) + ").family.join(',')"))
        .containsExactly(expected);
  }

  /**
   * Returns a chain of the given number of combinations of the names of a Patient and those of its
   * contacts, alternately.
   */
  @Nonnull
  private static String chain(@Nonnull final String form, final int length) {
    if ("union".equals(form)) {
      return IntStream.rangeClosed(0, length)
          .mapToObj(CombiningPlanSizeTest::operand)
          .collect(Collectors.joining(" | "));
    }
    return operand(0)
        + IntStream.rangeClosed(1, length)
            .mapToObj(index -> ".combine(" + operand(index) + ")")
            .collect(Collectors.joining());
  }

  @Nonnull
  private static String operand(final int index) {
    return index % 2 == 0 ? "name" : "contact.name";
  }

  /** Returns the number of expression nodes in the analysed plan of an expression's value. */
  private long analysedSize(@Nonnull final Dataset<Row> dataset, @Nonnull final String expression) {
    final LogicalPlan analysed =
        evaluateToDataset(dataset, "(" + expression + ").family")
            .toIdValueDataset()
            .queryExecution()
            .analyzed();
    final scala.collection.Iterator<Expression> expressions = analysed.expressions().iterator();
    long size = 0;
    while (expressions.hasNext()) {
      size += size(expressions.next());
    }
    return size;
  }

  private static long size(@Nonnull final Expression expression) {
    long size = 1;
    final scala.collection.Iterator<Expression> children = expression.children().iterator();
    while (children.hasNext()) {
      size += size(children.next());
    }
    return size;
  }

  /**
   * Builds the fixtures in a layout, and writes them to Parquet and reads them back, so that the
   * engine reads stored files.
   */
  @Nonnull
  private Dataset<Row> stored(@Nonnull final TestLayout layout, @Nonnull final String name) {
    final Dataset<Row> dataset =
        LayoutDatasets.fromJson(spark, fhirEncoders, layout, "Patient", PATIENTS);
    return PrunedSchemaReader.write(dataset, tempDir.resolve(name).toString()).read();
  }

  @Nonnull
  private CollectionDataset evaluateToDataset(
      @Nonnull final Dataset<Row> dataset, @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(ResourceType.PATIENT, fhirEncoders.getContext())
            .withDataset(dataset)
            .build();
    return evaluator.evaluate(PARSER.parse(expression));
  }

  @Nonnull
  private List<String> evaluate(
      @Nonnull final Dataset<Row> dataset, @Nonnull final String expression) {
    return evaluateToDataset(dataset, expression)
        .toCanonical()
        .toIdValueDataset()
        .collectAsList()
        .stream()
        .map(row -> row.getString(0) + "=" + Objects.toString(row.get(1)))
        .toList();
  }
}
