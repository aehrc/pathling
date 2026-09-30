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
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests the terminology functions over a CodeableConcept whose {@code coding} the input schema does
 * not carry, or whose CodeableConcept it does not carry at all (FR-025).
 *
 * <p>The terminology functions read the codings of a CodeableConcept as one array per concept, so
 * that the concept is judged as a whole. Where the schema lacks the codings, the concept holds
 * none, and each function yields empty. The new layout omits an element that no row populates, so a
 * category holding only text, or no category at all, is ordinary data there. The previous layout
 * carries every element, so there the element is removed from the schema, as it is from a pruned
 * table. Every case runs on both layouts in the same run.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class AbsentCodingTerminologyTest {

  private static final Parser PARSER = new Parser();

  private static final String VALUE_SET = "http://example.org/ValueSet/vs";

  private static final String SNOMED = "http://snomed.info/sct";

  /** A condition whose code holds only text, and which has no category. */
  private static final String TEXT_ONLY_CODE =
      "{\"resourceType\":\"Condition\",\"id\":\"c1\",\"subject\":{\"reference\":\"Patient/1\"},"
          + "\"code\":{\"text\":\"text\"}}";

  /** A condition with a coded code, and no category. */
  private static final String CODED =
      "{\"resourceType\":\"Condition\",\"id\":\"c2\",\"subject\":{\"reference\":\"Patient/1\"},"
          + "\"code\":{\"coding\":[{\"system\":\""
          + SNOMED
          + "\",\"code\":\"123\"}]}}";

  /** A condition with neither a code nor a category. */
  private static final String NO_CODE =
      "{\"resourceType\":\"Condition\",\"id\":\"c3\",\"subject\":{\"reference\":\"Patient/1\"}}";

  /** A condition with no code, and a category holding only text. */
  private static final String TEXT_ONLY_CATEGORY =
      "{\"resourceType\":\"Condition\",\"id\":\"c4\",\"subject\":{\"reference\":\"Patient/1\"},"
          + "\"category\":[{\"text\":\"category\"}]}";

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Autowired TerminologyService terminologyService;

  @TempDir static Path tempDir;

  private Map<String, Dataset<Row>> uncoded;

  private Map<String, Dataset<Row>> coded;

  @BeforeAll
  void setUp() {
    final PrunedSchemaReader previousUncoded =
        write(TestLayout.PREVIOUS, "previous-uncoded", TEXT_ONLY_CODE, NO_CODE, TEXT_ONLY_CATEGORY);
    final PrunedSchemaReader previousCoded =
        write(TestLayout.PREVIOUS, "previous-coded", CODED, TEXT_ONLY_CODE);
    // Every dataset here holds no coding in any code or category.
    uncoded =
        Map.of(
            "pof, code with only text, category with only text",
            write(TestLayout.POF, "pof-text", TEXT_ONLY_CODE, NO_CODE, TEXT_ONLY_CATEGORY).read(),
            "pof, no code, category with only text",
            write(TestLayout.POF, "pof-no-code", TEXT_ONLY_CATEGORY).read(),
            "pof, neither code nor category",
            write(TestLayout.POF, "pof-neither", NO_CODE).read(),
            "previous, full schema",
            previousUncoded.read(),
            "previous, without code.coding and category.coding",
            previousUncoded.readWithout("code.coding", "category.coding"),
            "previous, without code and category",
            previousUncoded.readWithout("code", "category"));
    // Every dataset here holds a coded code, and no category.
    coded =
        Map.of(
            "pof",
            write(TestLayout.POF, "pof-coded", CODED, TEXT_ONLY_CODE).read(),
            "previous, full schema",
            previousCoded.read(),
            "previous, without category",
            previousCoded.readWithout("category"));
  }

  @BeforeEach
  void setUpTerminology() {
    SharedMocks.resetAll();
    TerminologyServiceHelpers.setupValidate(terminologyService)
        .withValueSet(VALUE_SET, new Coding(SNOMED, "123", null));
    TerminologyServiceHelpers.setupSubsumes(terminologyService);
  }

  @Nonnull
  Stream<Arguments> uncodedCases() {
    return Stream.of(
            arguments("code.memberOf('" + VALUE_SET + "').empty()", "true"),
            arguments("code.subsumes(" + SNOMED + "|123).empty()", "true"),
            arguments("code.subsumedBy(" + SNOMED + "|123).empty()", "true"),
            arguments("code.where(memberOf('" + VALUE_SET + "')).exists()", "false"),
            arguments("code.select(memberOf('" + VALUE_SET + "')).empty()", "true"),
            // A repeating concept is compared by its value rather than by emptiness, because the
            // previous layout gives a category without codings a null result that counts as an
            // item.
            arguments("category.memberOf('" + VALUE_SET + "')", "null"),
            arguments("category.where(memberOf('" + VALUE_SET + "')).exists()", "false"),
            arguments("category.select(subsumes(" + SNOMED + "|123))", "null"))
        .flatMap(
            expression ->
                uncoded.keySet().stream()
                    .map(dataset -> arguments(dataset, expression.get()[0], expression.get()[1])));
  }

  @ParameterizedTest(name = "{1} over {0}")
  @MethodSource("uncodedCases")
  void terminologyFunctionOverAbsentCodingsIsEmpty(
      @Nonnull final String dataset,
      @Nonnull final String expression,
      @Nonnull final String expected) {
    final Dataset<Row> input = uncoded.get(dataset);
    final List<String> ids = input.select("id").as(Encoders.STRING()).collectAsList();
    assertThat(evaluate(input, expression))
        .as("%s over %s", expression, dataset)
        .containsExactlyInAnyOrderElementsOf(ids.stream().map(id -> id + "=" + expected).toList());
  }

  @Nonnull
  Stream<Arguments> codedCases() {
    return Stream.of(
            // A category that no row populates, beside a populated code: the ordinary shape of
            // new-layout data.
            arguments("category.memberOf('" + VALUE_SET + "')", "c1=null", "c2=null"),
            arguments(
                "category.where(memberOf('" + VALUE_SET + "')).exists()", "c1=false", "c2=false"),
            // The positive controls: the coded code is judged as a concept.
            arguments("code.memberOf('" + VALUE_SET + "')", "c1=null", "c2=true"),
            arguments("code.subsumes(" + SNOMED + "|123)", "c1=null", "c2=true"),
            arguments("code.where(memberOf('" + VALUE_SET + "')).exists()", "c1=false", "c2=true"))
        .flatMap(
            expression ->
                coded.keySet().stream()
                    .map(
                        dataset ->
                            arguments(
                                dataset,
                                expression.get()[0],
                                expression.get()[1],
                                expression.get()[2])));
  }

  @ParameterizedTest(name = "{1} over {0}")
  @MethodSource("codedCases")
  void terminologyFunctionBesideAbsentCategory(
      @Nonnull final String dataset,
      @Nonnull final String expression,
      @Nonnull final String expected1,
      @Nonnull final String expected2) {
    assertThat(evaluate(coded.get(dataset), expression))
        .as("%s over %s", expression, dataset)
        .containsExactlyInAnyOrder(expected1, expected2);
  }

  @Nonnull
  private PrunedSchemaReader write(
      @Nonnull final TestLayout layout, @Nonnull final String name, @Nonnull final String... json) {
    final Dataset<Row> dataset =
        LayoutDatasets.fromJson(spark, fhirEncoders, layout, "Condition", List.of(json));
    return PrunedSchemaReader.write(dataset, tempDir.resolve(name).toString());
  }

  /**
   * Evaluates an expression over each condition in the dataset, returning one {@code id=value}
   * entry per condition.
   */
  @Nonnull
  private List<String> evaluate(
      @Nonnull final Dataset<Row> dataset, @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(ResourceType.CONDITION, fhirEncoders.getContext())
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
