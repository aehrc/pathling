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

import static au.csiro.pathling.views.FhirView.columns;
import static org.apache.spark.sql.functions.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.Annotator;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.search.SearchColumnBuilder;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.PofDatasets;
import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests each annotated type at each level the engine reaches it, over annotated and unannotated
 * data (ticket 51; design §8): a date and a quantity at the root, under a repeating parent and
 * inside an extension, a decimal at the root and under a repeating parent, and a repeating
 * primitive under a repeating parent.
 *
 * <p>At each level an operation that reads an annotation gives the same results either way, reads
 * the stored annotation where it is present, so that the plan over annotated data computes none,
 * and computes it over unannotated data. Where only the value is output, nothing is computed over
 * either.
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class AnnotationLevelsTest {

  private static final Parser PARSER = new Parser();

  private static final String DATE_URL = "http://example.org/date";

  private static final String QUANTITY_URL = "http://example.org/quantity";

  /**
   * Any computation of an annotation: a date's bounds, a quantity's canonical form, or a decimal's
   * number decoded from its text.
   */
  private static final Pattern COMPUTATION =
      Pattern.compile(
          "date_(start|end)(_or_null)?\\(|quantity_canonical_(code|value_exact)\\(|try_cast\\(");

  private static final List<String> OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},"
              + "\"extension\":[{\"url\":\""
              + DATE_URL
              + "\",\"valueDate\":\"2020-05-06\"},{\"url\":\""
              + QUANTITY_URL
              + "\",\"valueQuantity\":"
              + quantity("3", "kg")
              + "}],"
              + "\"effectiveDateTime\":\"2020-03\",\"valueQuantity\":"
              + quantity("10", "g")
              + ",\"component\":[{\"code\":{\"text\":\"a\"},\"valueQuantity\":"
              + quantity("9", "g")
              + "},{\"code\":{\"text\":\"b\"},\"valueDateTime\":\"2018-01-01T10:00:00Z\"},"
              + "{\"code\":{\"text\":\"c\"},\"valueQuantity\":"
              + quantity("2000", "mg")
              + "}]}",
          "{\"resourceType\":\"Observation\",\"id\":\"o2\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},"
              + "\"extension\":[{\"url\":\""
              + DATE_URL
              + "\",\"valueDate\":\"2019\"},{\"url\":\""
              + QUANTITY_URL
              + "\",\"valueQuantity\":"
              + quantity("500", "g")
              + "}],"
              + "\"effectiveDateTime\":\"2018-01-01\",\"valueQuantity\":"
              + quantity("1", "g")
              + ",\"component\":[{\"code\":{\"text\":\"b\"},"
              + "\"valueDateTime\":\"2021-02-03\"}]}");

  private static final List<String> MEDICATION_REQUESTS =
      List.of(
          "{\"resourceType\":\"MedicationRequest\",\"id\":\"m1\",\"status\":\"active\","
              + "\"intent\":\"order\",\"subject\":{\"reference\":\"Patient/p1\"},"
              + "\"dosageInstruction\":[{\"timing\":{\"event\":[\"2020-01-01\",\"2020-02\"]}},"
              + "{\"timing\":{\"event\":[\"2017\"]}}]}",
          "{\"resourceType\":\"MedicationRequest\",\"id\":\"m2\",\"status\":\"active\","
              + "\"intent\":\"order\",\"subject\":{\"reference\":\"Patient/p1\"},"
              + "\"dosageInstruction\":[{\"timing\":{\"event\":[\"2016-05-05T10:00:00Z\"]}}]}");

  private static final Map<String, List<String>> FIXTURES =
      Map.of("Observation", OBSERVATIONS, "MedicationRequest", MEDICATION_REQUESTS);

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Map<String, Dataset<Row>> read;

  private Map<String, Dataset<Row>> annotated;

  @BeforeAll
  void setUp() {
    read = stored("read", null);
    annotated = stored("annotated", SchemaConfiguration.builder().build());
  }

  @Test
  void theAnnotatedDataCarriesAnAnnotationAtEachLevel() {
    final String observations = annotated.get("Observation").schema().simpleString();
    assertThat(observations)
        .contains("_effectiveDateTime:struct<__start:timestamp,__end:timestamp>")
        .contains("_valueDate:struct<__start:timestamp,__end:timestamp>")
        .contains("_valueDateTime:struct<__start:timestamp,__end:timestamp>")
        .contains("__canonical_code")
        .contains("__numeric");
    assertThat(annotated.get("MedicationRequest").schema().simpleString())
        .contains("_event:array<struct<__start:timestamp,__end:timestamp>>");
    assertThat(read.get("Observation").schema().simpleString()).doesNotContain("__");
    assertThat(read.get("MedicationRequest").schema().simpleString()).doesNotContain("__");
  }

  @Nonnull
  Stream<Arguments> comparisons() {
    return Stream.of(
        // A date and a quantity at the root.
        arguments("Observation", "effectiveDateTime > @2019", List.of("o1=true", "o2=false")),
        arguments("Observation", "valueQuantity > 5000 'mg'", List.of("o1=true", "o2=false")),
        // A decimal at the root.
        arguments("Observation", "valueQuantity.value > 5", List.of("o1=true", "o2=false")),
        // A date and a quantity under a repeating parent.
        arguments(
            "Observation",
            "component.value.ofType(dateTime).where($this > @2020).count()",
            List.of("o1=0", "o2=1")),
        arguments(
            "Observation",
            "component.value.ofType(Quantity).where($this > 5 'g').count()",
            List.of("o1=1", "o2=0")),
        // A decimal under a repeating parent.
        arguments(
            "Observation",
            "component.value.ofType(Quantity).value.where($this > 1000).count()",
            List.of("o1=1", "o2=0")),
        // A date and a quantity inside an extension.
        arguments(
            "Observation",
            "extension('" + DATE_URL + "').value.ofType(date).where($this > @2020-01-01).exists()",
            List.of("o1=true", "o2=false")),
        arguments(
            "Observation",
            "extension('"
                + QUANTITY_URL
                + "').value.ofType(Quantity).where($this > 1 'kg').exists()",
            List.of("o1=true", "o2=false")),
        // A repeating primitive under a repeating parent.
        arguments(
            "MedicationRequest",
            "dosageInstruction.timing.event.where($this > @2018).count()",
            List.of("m1=2", "m2=0")));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("comparisons")
  void aComparisonReadsTheStoredAnnotationAndGivesTheSameResultsEitherWay(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    final Dataset<Row> overRead = evaluate(read, resourceType, expression);
    final Dataset<Row> overAnnotated = evaluate(annotated, resourceType, expression);
    assertThat(rows(overRead)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(rows(overAnnotated)).containsExactlyInAnyOrderElementsOf(expected);
    // Over unannotated data the annotation is computed where it is read, and over annotated data
    // the stored one is read, so nothing is computed.
    assertThat(computes(overRead)).isTrue();
    assertThat(computes(overAnnotated)).isFalse();
  }

  @Nonnull
  Stream<Arguments> outputs() {
    return Stream.of(
        arguments("Observation", "effectiveDateTime", List.of("o1=[2020-03]", "o2=[2018-01-01]")),
        arguments(
            "Observation",
            "component.value.ofType(dateTime)",
            List.of("o1=[2018-01-01T10:00:00Z]", "o2=[2021-02-03]")),
        arguments(
            "Observation",
            "extension('" + DATE_URL + "').value.ofType(date)",
            List.of("o1=[2020-05-06]", "o2=[2019]")),
        arguments(
            "MedicationRequest",
            "dosageInstruction.timing.event",
            List.of("m1=[2020-01-01, 2020-02, 2017]", "m2=[2016-05-05T10:00:00Z]")),
        arguments("Observation", "valueQuantity.code", List.of("o1=[g]", "o2=[g]")),
        arguments(
            "Observation", "component.value.ofType(Quantity).code", List.of("o1=[g, mg]", "o2=[]")),
        arguments(
            "Observation",
            "extension('" + QUANTITY_URL + "').value.ofType(Quantity).code",
            List.of("o1=[kg]", "o2=[g]")));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("outputs")
  void aValueThatIsOnlyOutputComputesNothing(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    final Dataset<Row> overRead = view(read, resourceType, expression);
    final Dataset<Row> overAnnotated = view(annotated, resourceType, expression);
    assertThat(viewRows(overRead)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(viewRows(overAnnotated)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(computes(overRead)).isFalse();
    assertThat(computes(overAnnotated)).isFalse();
  }

  @Nonnull
  Stream<Arguments> decimalOutputs() {
    return Stream.of(
        arguments("Observation", "valueQuantity.value", List.of("o1=[10]", "o2=[1]")),
        arguments(
            "Observation",
            "component.value.ofType(Quantity).value",
            List.of("o1=[9, 2000]", "o2=[]")));
  }

  /**
   * A decimal is the one exception: traversal tests whether its number decodes, because a decimal
   * the engine's System.Decimal cannot hold is absent (design §3.1 of `annotations-by-example.md`,
   * ticket 50), and a view outputs its number until ticket 16. Over annotated data the stored
   * number is read, and nothing is computed; over unannotated data the text is decoded.
   */
  @ParameterizedTest(name = "{1}")
  @MethodSource("decimalOutputs")
  void aDecimalThatIsOnlyOutputIsDecodedOnlyOverUnannotatedData(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    final Dataset<Row> overRead = view(read, resourceType, expression);
    final Dataset<Row> overAnnotated = view(annotated, resourceType, expression);
    assertThat(viewRows(overRead)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(viewRows(overAnnotated)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(computes(overRead)).isTrue();
    assertThat(computes(overAnnotated)).isFalse();
  }

  @Nonnull
  Stream<Arguments> quantityOutputs() {
    return Stream.of(
        arguments("valueQuantity", "[10.000000,g,g,http://unitsofmeasure.org]"),
        arguments(
            "component.value.ofType(Quantity)",
            "ArraySeq([9.000000,g,g,http://unitsofmeasure.org], [2000.000000,mg,mg,"
                + "http://unitsofmeasure.org])"),
        arguments(
            "extension('" + QUANTITY_URL + "').value.ofType(Quantity)",
            "ArraySeq([3.000000,kg,kg,http://unitsofmeasure.org])"));
  }

  /**
   * A quantity that is only output, as a caller is given it, computes no canonical form at the
   * root, under a repeating parent or in an extension. Its value is the System.Decimal, so it is
   * decoded from the text over unannotated data, as an output decimal is.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("quantityOutputs")
  void aQuantityThatIsOnlyOutputComputesNoCanonicalForm(
      @Nonnull final String expression, @Nonnull final String expected) {
    for (final Map<String, Dataset<Row>> data : List.of(read, annotated)) {
      final Dataset<Row> output =
          data.get("Observation")
              .filter(col("id").equalTo("o1"))
              .select(
                  SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
                      .fromExpression(ResourceType.OBSERVATION, expression)
                      .alias("x"));
      assertThat(String.valueOf(output.collectAsList().get(0).get(0))).isEqualTo(expected);
      assertThat(output.queryExecution().optimizedPlan().toString())
          .doesNotContain("quantity_canonical_");
    }
  }

  /** Whether the optimised plan of a query computes an annotation. */
  private static boolean computes(@Nonnull final Dataset<Row> query) {
    return COMPUTATION.matcher(query.queryExecution().optimizedPlan().toString()).find();
  }

  @Nonnull
  private Dataset<Row> evaluate(
      @Nonnull final Map<String, Dataset<Row>> data,
      @Nonnull final String resourceType,
      @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(
                ResourceType.fromCode(resourceType), fhirEncoders.getContext())
            .withDataset(data.get(resourceType))
            .build();
    return evaluator.evaluate(PARSER.parse(expression)).toIdValueDataset();
  }

  /** A view of the resource id and the value of an expression, as a collection. */
  @Nonnull
  private Dataset<Row> view(
      @Nonnull final Map<String, Dataset<Row>> data,
      @Nonnull final String resourceType,
      @Nonnull final String expression) {
    final DataSource source =
        new DataSource() {
          @Nonnull
          @Override
          public Dataset<Row> read(@Nullable final String resourceCode) {
            return data.get(resourceCode);
          }

          @Nonnull
          @Override
          public Set<String> getResourceTypes() {
            return data.keySet();
          }
        };
    return new FhirViewExecutor(fhirEncoders.getContext(), source)
        .buildQuery(
            FhirView.ofResource(resourceType)
                .select(
                    columns(
                        Column.single("id", "id"),
                        Column.collectionBuilder("value", expression).build()))
                .build());
  }

  @Nonnull
  private static List<String> rows(@Nonnull final Dataset<Row> result) {
    return result.collectAsList().stream().map(row -> row.getString(0) + "=" + row.get(1)).toList();
  }

  @Nonnull
  private static List<String> viewRows(@Nonnull final Dataset<Row> result) {
    return result.collectAsList().stream()
        .map(row -> row.getString(0) + "=" + row.getList(1))
        .toList();
  }

  /**
   * Builds the fixtures, as read or as annotated under a configuration, and writes them to Parquet
   * and reads them back, so that the engine reads stored files.
   */
  @Nonnull
  private Map<String, Dataset<Row>> stored(
      @Nonnull final String name, @Nullable final SchemaConfiguration annotation) {
    final FhirDefinitionContext definitions = FhirDefinitionContext.of(fhirEncoders.getContext());
    return Map.of(
        "Observation", store(name, "Observation", annotation, definitions),
        "MedicationRequest", store(name, "MedicationRequest", annotation, definitions));
  }

  @Nonnull
  private Dataset<Row> store(
      @Nonnull final String name,
      @Nonnull final String resourceType,
      @Nullable final SchemaConfiguration annotation,
      @Nonnull final FhirDefinitionContext definitions) {
    final Dataset<Row> resources =
        PofDatasets.fromJson(
            spark, fhirEncoders.getContext(), resourceType, FIXTURES.get(resourceType));
    final Dataset<Row> dataset =
        annotation == null
            ? resources
            : Annotator.of(definitions, annotation).annotate(resourceType, resources);
    return PrunedSchemaReader.write(dataset, tempDir.resolve(name + "-" + resourceType).toString())
        .read();
  }

  @Nonnull
  private static String quantity(@Nonnull final String value, @Nonnull final String code) {
    return "{\"value\":"
        + value
        + ",\"unit\":\""
        + code
        + "\",\"system\":\"http://unitsofmeasure.org\",\"code\":\""
        + code
        + "\"}";
  }
}
