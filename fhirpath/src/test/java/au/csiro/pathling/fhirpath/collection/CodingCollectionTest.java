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

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
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
 * Tests that Coding equality and the terminology functions read a Coding by field name, so that
 * they work over a Coding narrower than the canonical structure (T083–T085, FR-031, FR-032).
 *
 * <p>Each fixture is given once as FHIR JSON and read in both layouts, so every case runs over each
 * layout with the same expected answer, and the expected answers are the ones the previous layout
 * gives before the port. The new layout's schema is pruned to the elements the fixtures populate.
 * No fixture populates a Coding's {@code version} or {@code userSelected}, so the new layout's
 * Codings carry neither.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class CodingCollectionTest {

  private static final Parser PARSER = new Parser();

  private static final String SYSTEM_S = "http://example.org/s";

  private static final String SYSTEM_T = "http://example.org/t";

  private static final String VALUE_SET = "uuid:vs";

  private static final List<String> OBSERVATIONS =
      List.of(
          observation("o1", coding(SYSTEM_S, "A", "Alpha")),
          observation("o2", coding(SYSTEM_S, "B", null) + "," + coding(SYSTEM_T, "C", "Gamma")),
          "{\"resourceType\":\"Observation\",\"id\":\"o3\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"}}");

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Autowired TerminologyService terminologyService;

  @TempDir static Path tempDir;

  private Map<String, Dataset<Row>> datasets;

  @BeforeAll
  void setUp() {
    datasets = Map.of("previous", stored(TestLayout.PREVIOUS), "pof", stored(TestLayout.POF));
  }

  @BeforeEach
  void setUpTerminology() {
    TerminologyServiceHelpers.setupSubsumes(terminologyService)
        .withSubsumes(new Coding(SYSTEM_S, "A", null), new Coding(SYSTEM_S, "B", null));
    TerminologyServiceHelpers.setupValidate(terminologyService)
        .withValueSet(VALUE_SET, new Coding(SYSTEM_S, "B", null));
  }

  @Nonnull
  Stream<Arguments> cases() {
    return Stream.of(
            // Equality compares every Coding property, and an absent property is an empty one.
            arguments(
                "code.coding.first() = " + SYSTEM_S + "|A||Alpha",
                List.of("o1=true", "o2=false", "o3=null")),
            arguments(
                "code.coding.first() = " + SYSTEM_S + "|B",
                List.of("o1=false", "o2=true", "o3=null")),
            arguments(
                "code.coding contains " + SYSTEM_T + "|C||Gamma",
                List.of("o1=false", "o2=true", "o3=false")),
            arguments(
                "code.coding.where($this = " + SYSTEM_S + "|B).count()",
                List.of("o1=0", "o2=1", "o3=0")),
            // The terminology functions, over a singular and a repeating Coding.
            arguments(
                "code.coding.first().subsumes(" + SYSTEM_S + "|B)",
                List.of("o1=true", "o2=true", "o3=null")),
            arguments(
                "code.coding.where(subsumedBy(" + SYSTEM_S + "|A)).count()",
                List.of("o1=1", "o2=1", "o3=0")),
            arguments(
                "code.coding.where(memberOf('" + VALUE_SET + "')).count()",
                List.of("o1=0", "o2=1", "o3=0")),
            arguments(
                "code.memberOf('" + VALUE_SET + "')", List.of("o1=false", "o2=true", "o3=null")))
        .flatMap(
            c ->
                Stream.of("previous", "pof")
                    .map(layout -> arguments(layout, c.get()[0], c.get()[1])));
  }

  @ParameterizedTest(name = "{1} over the {0} layout")
  @MethodSource("cases")
  void codingOperationsMatchOnBothLayouts(
      @Nonnull final String layout,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(datasets.get(layout), expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  /**
   * Builds the fixtures in a layout, and writes them to Parquet and reads them back, so that the
   * engine reads stored files.
   */
  @Nonnull
  private Dataset<Row> stored(@Nonnull final TestLayout layout) {
    final Dataset<Row> dataset =
        LayoutDatasets.fromJson(spark, fhirEncoders, layout, "Observation", OBSERVATIONS);
    return PrunedSchemaReader.write(dataset, tempDir.resolve(layout + "-Observation").toString())
        .read();
  }

  /**
   * Evaluates an expression over each resource in the dataset, returning one {@code id=value} entry
   * per resource.
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

  @Nonnull
  private static String coding(
      @Nonnull final String system, @Nonnull final String code, @Nullable final String display) {
    return "{\"system\":\""
        + system
        + "\",\"code\":\""
        + code
        + "\""
        + (display == null ? "" : ",\"display\":\"" + display + "\"")
        + "}";
  }

  @Nonnull
  private static String observation(@Nonnull final String id, @Nonnull final String codings) {
    return "{\"resourceType\":\"Observation\",\"id\":\""
        + id
        + "\",\"status\":\"final\""
        + ",\"code\":{\"coding\":["
        + codings
        + "]}}";
  }
}
