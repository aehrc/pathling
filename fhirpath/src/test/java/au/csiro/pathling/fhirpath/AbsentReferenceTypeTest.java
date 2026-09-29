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
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests {@code resolve()} over references whose {@code type} the input schema does not carry
 * (FR-025).
 *
 * <p>The type of a resolved reference is taken from its {@code type} where it has one, and parsed
 * from its {@code reference} where it does not. The new layout omits {@code type} where no row
 * populates it, which is the ordinary shape of references, so the type must be read as a typed null
 * there. The previous layout carries every element, so there {@code type} is removed from the
 * schema, as it is from a pruned table. Every case runs on both layouts in the same run.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class AbsentReferenceTypeTest {

  private static final Parser PARSER = new Parser();

  /** An observation whose references have no type. */
  private static final String UNTYPED =
      "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
          + "\"code\":{\"text\":\"x\"},\"subject\":{\"reference\":\"Patient/1\"},"
          + "\"performer\":[{\"reference\":\"Practitioner/1\"},"
          + "{\"reference\":\"Organization/2\"}]}";

  /** An observation whose references have a type. */
  private static final String TYPED =
      "{\"resourceType\":\"Observation\",\"id\":\"o2\",\"status\":\"final\","
          + "\"code\":{\"text\":\"x\"},"
          + "\"subject\":{\"reference\":\"Patient/1\",\"type\":\"Patient\"},"
          + "\"performer\":[{\"reference\":\"Practitioner/1\",\"type\":\"Practitioner\"}]}";

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Map<String, Dataset<Row>> untyped;

  private Map<String, Dataset<Row>> mixed;

  @BeforeAll
  void setUp() {
    final PrunedSchemaReader previousUntyped =
        write(TestLayout.PREVIOUS, "previous-untyped", UNTYPED);
    final PrunedSchemaReader previousMixed =
        write(TestLayout.PREVIOUS, "previous-mixed", UNTYPED, TYPED);
    // No reference in these datasets has a type.
    untyped =
        Map.of(
            "pof",
            write(TestLayout.POF, "pof-untyped", UNTYPED).read(),
            "previous, full schema",
            previousUntyped.read(),
            "previous, without performer.type and subject.type",
            previousUntyped.readWithout("performer.type", "subject.type"));
    // The positive controls: typed references beside untyped ones.
    mixed =
        Map.of(
            "pof",
            write(TestLayout.POF, "pof-mixed", UNTYPED, TYPED).read(),
            "previous, full schema",
            previousMixed.read());
  }

  @Nonnull
  static Stream<Arguments> resolutions() {
    return Stream.of(
        arguments("performer.resolve().count()", "o1=2", "o2=1"),
        arguments("performer.resolve().ofType(Practitioner).count()", "o1=1", "o2=1"),
        arguments("performer.resolve().ofType(Organization).count()", "o1=1", "o2=0"),
        arguments("performer.where(resolve() is Practitioner).count()", "o1=1", "o2=1"),
        arguments("performer.first().resolve().ofType(Practitioner).count()", "o1=1", "o2=1"),
        arguments("subject.resolve().ofType(Patient).count()", "o1=1", "o2=1"));
  }

  @Nonnull
  Stream<Arguments> untypedCases() {
    return resolutions()
        .flatMap(
            expression ->
                untyped.keySet().stream()
                    .map(dataset -> arguments(dataset, expression.get()[0], expression.get()[1])));
  }

  @ParameterizedTest(name = "{1} over {0}")
  @MethodSource("untypedCases")
  void resolveParsesTypeWhereTypeIsAbsent(
      @Nonnull final String dataset,
      @Nonnull final String expression,
      @Nonnull final String expected) {
    assertThat(evaluate(untyped.get(dataset), expression))
        .as("%s over %s", expression, dataset)
        .containsExactly(expected);
  }

  @Nonnull
  Stream<Arguments> mixedCases() {
    return resolutions()
        .flatMap(
            expression ->
                mixed.keySet().stream()
                    .map(
                        dataset ->
                            arguments(
                                dataset,
                                expression.get()[0],
                                expression.get()[1],
                                expression.get()[2])));
  }

  @ParameterizedTest(name = "{1} over {0}")
  @MethodSource("mixedCases")
  void resolveBesideTypedReferences(
      @Nonnull final String dataset,
      @Nonnull final String expression,
      @Nonnull final String expected1,
      @Nonnull final String expected2) {
    assertThat(evaluate(mixed.get(dataset), expression))
        .as("%s over %s", expression, dataset)
        .containsExactlyInAnyOrder(expected1, expected2);
  }

  @Nonnull
  private PrunedSchemaReader write(
      @Nonnull final TestLayout layout, @Nonnull final String name, @Nonnull final String... json) {
    final Dataset<Row> dataset =
        LayoutDatasets.fromJson(spark, fhirEncoders, layout, "Observation", List.of(json));
    return PrunedSchemaReader.write(dataset, tempDir.resolve(name).toString());
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
