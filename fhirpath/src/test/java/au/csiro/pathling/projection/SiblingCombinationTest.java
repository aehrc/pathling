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

package au.csiro.pathling.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.DatasetDataSource;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Questionnaire;
import org.hl7.fhir.r4.model.Questionnaire.QuestionnaireItemType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that sibling column combination tolerates a complex element absent from the input schema,
 * written ahead of T113. Under FR-055 an absent complex element is the bottom type, and both the
 * recursive selection path, which computes an expected element type, and the combination of sibling
 * selections meet it where they previously met a statically empty collection.
 *
 * <p>The views take the shape of the two {@code deep_nesting.json} cases excluded under #2625, but
 * over a schema from which the element is genuinely absent rather than past the encoder's nesting
 * bound. That bound already omits {@code item.item} from the full schema, so the nested cases use
 * {@code item.enableWhen}, which the full schema carries. The removed elements are never populated
 * in the source, so each view is run twice: over the full schema as the control, which passes
 * before T110, and over the pruned schema, which is tagged {@code pending-T110} until T110 and T113
 * land.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class SiblingCombinationTest {

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Autowired Gson gson;

  @TempDir static Path tempDir;

  /** Questionnaires whose items carry no conditions. */
  private PrunedSchemaReader questionnaires;

  /** Questionnaires with no items at all. */
  private PrunedSchemaReader itemless;

  @BeforeAll
  void setUp() {
    final Questionnaire withItem = questionnaire("q1");
    withItem.addItem().setLinkId("1").setText("One").setType(QuestionnaireItemType.GROUP);
    questionnaires =
        PrunedSchemaReader.write(
            encode(withItem, questionnaire("q2")), tempDir.resolve("nested").toString());
    itemless =
        PrunedSchemaReader.write(
            encode(questionnaire("q3"), questionnaire("q4")), tempDir.resolve("root").toString());
  }

  @Nonnull
  Stream<Arguments> nestedAbsence() {
    return Stream.of(
        // The first #2625 case: a forEach into the absent nested element, beside a sibling column.
        arguments(
            "forEach into an absent nested element",
            """
            {
              "forEach": "item.enableWhen",
              "column": [ { "name": "question", "path": "question" } ]
            }
            """,
            List.of()),
        // The second #2625 case: sibling selections inside the forEach, combined by struct
        // product.
        arguments(
            "forEach into an absent nested element with sibling selections",
            """
            {
              "forEach": "item.enableWhen",
              "select": [
                { "column": [ { "name": "question", "path": "question" } ] },
                { "column": [ { "name": "operator", "path": "operator" } ] }
              ]
            }
            """,
            List.of()),
        // A forEachOrNull keeps a row per resource, so the sibling combination has rows to
        // combine.
        arguments(
            "forEachOrNull into an absent nested element with sibling selections",
            """
            {
              "forEachOrNull": "item.enableWhen",
              "select": [
                { "column": [ { "name": "question", "path": "question" } ] },
                { "column": [ { "name": "operator", "path": "operator" } ] }
              ]
            }
            """,
            List.of("q1|null|null", "q2|null|null")),
        // The recursive selection path, with a column that traverses the absent element at each
        // level.
        arguments(
            "repeat with a column over an absent nested element",
            """
            {
              "repeat": ["item"],
              "column": [
                { "name": "linkId", "path": "linkId" },
                { "name": "question", "path": "enableWhen.question.first()" }
              ]
            }
            """,
            List.of("q1|1|null")),
        // The recursive selection path, with sibling selections one of which unnests the absent
        // element.
        arguments(
            "repeat with sibling selections over an absent nested element",
            """
            {
              "repeat": ["item"],
              "select": [
                { "column": [ { "name": "linkId", "path": "linkId" } ] },
                {
                  "forEachOrNull": "enableWhen",
                  "column": [ { "name": "question", "path": "question" } ]
                }
              ]
            }
            """,
            List.of("q1|1|null")));
  }

  @Nonnull
  Stream<Arguments> rootAbsence() {
    return Stream.of(
        arguments(
            "forEachOrNull into an absent top-level element with sibling selections",
            """
            {
              "forEachOrNull": "item",
              "select": [
                { "column": [ { "name": "linkId", "path": "linkId" } ] },
                { "column": [ { "name": "text", "path": "text" } ] }
              ]
            }
            """,
            List.of("q3|null|null", "q4|null|null")),
        // The recursive selection path, whose starting element is itself absent. This is where its
        // expected element type meets the bottom type rather than a statically empty collection.
        arguments(
            "repeat over an absent top-level element",
            """
            {
              "repeat": ["item"],
              "column": [
                { "name": "linkId", "path": "linkId" },
                { "name": "text", "path": "text" }
              ]
            }
            """,
            List.of()),
        arguments(
            "repeat over an absent top-level element beside a forEachOrNull",
            """
            {
              "select": [
                {
                  "repeat": ["item"],
                  "column": [ { "name": "linkId", "path": "linkId" } ]
                },
                {
                  "forEachOrNull": "item",
                  "column": [ { "name": "text", "path": "text" } ]
                }
              ]
            }
            """,
            List.of()));
  }

  @ParameterizedTest(name = "{0}, over the full schema")
  @MethodSource("nestedAbsence")
  void nestedUnpopulatedElementOverFullSchema(
      @Nonnull final String description,
      @Nonnull final String selection,
      @Nonnull final List<String> expected) {
    assertThat(run(questionnaires.read(), selection)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Tag("pending-T110")
  @ParameterizedTest(name = "{0}, with item.enableWhen absent from the schema")
  @MethodSource("nestedAbsence")
  void siblingCombinationToleratesAbsentNestedElement(
      @Nonnull final String description,
      @Nonnull final String selection,
      @Nonnull final List<String> expected) {
    assertThat(run(questionnaires.readWithout("item.enableWhen"), selection))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @ParameterizedTest(name = "{0}, over the full schema")
  @MethodSource("rootAbsence")
  void rootUnpopulatedElementOverFullSchema(
      @Nonnull final String description,
      @Nonnull final String selection,
      @Nonnull final List<String> expected) {
    assertThat(run(itemless.read(), selection)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Tag("pending-T110")
  @ParameterizedTest(name = "{0}, with item absent from the schema")
  @MethodSource("rootAbsence")
  void siblingCombinationToleratesAbsentTopLevelElement(
      @Nonnull final String description,
      @Nonnull final String selection,
      @Nonnull final List<String> expected) {
    assertThat(run(itemless.readWithout("item"), selection))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  /**
   * Runs a view over the Questionnaire dataset, with an {@code id} column beside the given
   * selection, and returns each row with its values joined by {@code |}.
   */
  @Nonnull
  private List<String> run(
      @Nonnull final Dataset<Row> questionnaires, @Nonnull final String selection) {
    final String json =
        """
        {
          "resource": "Questionnaire",
          "select": [
            { "column": [ { "name": "id", "path": "id" } ] },
            %s
          ]
        }
        """
            .formatted(selection);
    final FhirViewExecutor executor =
        new FhirViewExecutor(
            fhirEncoders.getContext(),
            new DatasetDataSource(Map.of("Questionnaire", questionnaires)));
    return executor.buildQuery(gson.fromJson(json, FhirView.class)).collectAsList().stream()
        .map(
            row ->
                IntStream.range(0, row.length())
                    .mapToObj(i -> Objects.toString(row.get(i)))
                    .collect(Collectors.joining("|")))
        .toList();
  }

  @Nonnull
  private Dataset<Row> encode(@Nonnull final Questionnaire... resources) {
    return spark.createDataset(Arrays.asList(resources), fhirEncoders.of("Questionnaire")).toDF();
  }

  @Nonnull
  private static Questionnaire questionnaire(@Nonnull final String id) {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId(id);
    questionnaire.setStatus(PublicationStatus.ACTIVE);
    return questionnaire;
  }
}
