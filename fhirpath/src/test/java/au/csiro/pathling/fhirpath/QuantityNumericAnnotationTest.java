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

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.Annotator;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.search.SearchColumnBuilder;
import au.csiro.pathling.search.SearchParameterRegistry;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Enumerations.SearchParamType;
import org.hl7.fhir.r4.model.SearchParameter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that a quantity's value is read from its numeric annotation, {@code _value.__numeric},
 * where the schema carries it, and decoded from its text otherwise, with the same results (ticket
 * 41, FR-059).
 *
 * <p>The value is read where the engine compares quantities by value rather than by canonical form,
 * which is where a quantity has no UCUM unit; where a stored quantity taken as a System one is
 * navigated to its value; and where a search matches a quantity by value.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class QuantityNumericAnnotationTest {

  private static final Parser PARSER = new Parser();

  private static final String UNITS = "http://example.org/units";

  /**
   * Observations whose quantities have a unit outside UCUM, and so no canonical form, with a value
   * of more than six fractional digits, one beyond the range of System.Decimal and one with more
   * integer digits than {@code DECIMAL(32,6)} holds, and one with a UCUM unit.
   */
  private static final List<String> OBSERVATIONS =
      List.of(
          observation("o1", "1.2345678", UNITS, "x", "2", "3"),
          observation("o2", "4", "http://unitsofmeasure.org", "g", "5", "5"),
          observation("o3", "1e33", UNITS, "x", "1e33", "1"),
          observation("o4", "1e30", UNITS, "x", "1e30", "2e30"));

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Map<String, Dataset<Row>> datasets;

  @Nonnull
  private static String observation(
      @Nonnull final String id,
      @Nonnull final String value,
      @Nonnull final String system,
      @Nonnull final String code,
      @Nonnull final String first,
      @Nonnull final String second) {
    return "{\"resourceType\":\"Observation\",\"id\":\""
        + id
        + "\",\"status\":\"final\",\"code\":{\"text\":\"x\"},\"valueQuantity\":"
        + quantity(value, system, code)
        + ",\"component\":[{\"code\":{\"text\":\"a\"},\"valueQuantity\":"
        + quantity(first, system, code)
        + "},{\"code\":{\"text\":\"b\"},\"valueQuantity\":"
        + quantity(second, system, code)
        + "}]}";
  }

  @Nonnull
  private static String quantity(
      @Nonnull final String value, @Nonnull final String system, @Nonnull final String code) {
    return "{\"value\":" + value + ",\"system\":\"" + system + "\",\"code\":\"" + code + "\"}";
  }

  @BeforeAll
  void setUp() {
    datasets =
        Map.of(
            "read",
            stored("read", Optional.empty()),
            "annotated",
            stored("annotated", Optional.of(SchemaConfiguration.builder().build())),
            "disabled",
            stored(
                "disabled",
                Optional.of(SchemaConfiguration.builder().enableNumericAnnotation(false).build())));
  }

  @Nonnull
  Stream<Arguments> expressions() {
    final String value = "value.ofType(Quantity)";
    final String components = "component.value.ofType(Quantity)";
    return Stream.of(
        arguments(value + " = " + value, List.of("o1=true", "o2=true", "o3=null", "o4=true")),
        arguments(
            components + "[0] < " + components + "[1]",
            List.of("o1=true", "o2=false", "o3=null", "o4=true")),
        arguments(
            value + ".combine(7 'g').value.first()",
            List.of(
                "o1=1.234568",
                "o2=4.000000",
                "o3=null",
                "o4=1000000000000000000000000000000.000000")),
        // A quantity the engine builds carries its value as decoding its text gives it.
        arguments(
            "(1.2345675 'g').value",
            List.of("o1=1.234568", "o2=1.234568", "o3=1.234568", "o4=1.234568")),
        arguments(
            "'1.2345675 \\'g\\''.toQuantity().value",
            List.of("o1=1.234568", "o2=1.234568", "o3=1.234568", "o4=1.234568")),
        arguments(
            "(1000000000000000000000000000000 'g').value",
            Stream.of("o1", "o2", "o3", "o4")
                .map(id -> id + "=1000000000000000000000000000000.000000")
                .toList()),
        arguments(
            "(123456789012345678901234567890123 'g').value",
            List.of("o1=null", "o2=null", "o3=null", "o4=null")));
  }

  @Nonnull
  Stream<Arguments> cases() {
    return expressions()
        .flatMap(
            expression ->
                datasets.keySet().stream()
                    .map(dataset -> arguments(dataset, expression.get()[0], expression.get()[1])));
  }

  /** Reading the annotation and decoding the text give the same results. */
  @ParameterizedTest(name = "{1} over {0}")
  @MethodSource("cases")
  void aQuantitysValueIsTheSameOnEveryPath(
      @Nonnull final String dataset,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(datasets.get(dataset), expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> searches() {
    return datasets.keySet().stream()
        .flatMap(
            dataset ->
                Stream.of(
                    arguments(
                        dataset, "value-quantity=gt1.2345|" + UNITS + "|x", List.of("o1", "o4")),
                    arguments(dataset, "value-quantity=1.234568|" + UNITS + "|x", List.of("o1")),
                    arguments(
                        dataset,
                        "value-quantity=lt5000|http://unitsofmeasure.org|mg",
                        List.of("o2"))));
  }

  /** A search matches a quantity by the same value on every path. */
  @ParameterizedTest(name = "{1} over {0}")
  @MethodSource("searches")
  void aSearchMatchesTheSameQuantitiesOnEveryPath(
      @Nonnull final String dataset,
      @Nonnull final String query,
      @Nonnull final List<String> expected) {
    assertThat(
            datasets.get(dataset).filter(search(query)).select("id").collectAsList().stream()
                .map(row -> row.getString(0))
                .toList())
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> plans() {
    final BiFunction<QuantityNumericAnnotationTest, String, Column> expression =
        (test, e) ->
            SearchColumnBuilder.withDefaultRegistry(test.fhirEncoders.getContext())
                .fromExpression(ResourceType.OBSERVATION, e);
    final BiFunction<QuantityNumericAnnotationTest, String, Column> search =
        QuantityNumericAnnotationTest::search;
    return Stream.of(
        arguments("value.ofType(Quantity) = value.ofType(Quantity)", expression),
        arguments("value.ofType(Quantity).combine(7 'g').value.first() > 1", expression),
        arguments("value-quantity=gt1|" + UNITS + "|x", search));
  }

  /**
   * The value is read from the annotation where the schema carries it, and decoded from the text
   * where it does not, decided once from the schema (FR-023).
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("plans")
  void theValueTakesTheAnnotationWhereTheSchemaCarriesIt(
      @Nonnull final String query,
      @Nonnull final BiFunction<QuantityNumericAnnotationTest, String, Column> filter) {
    // The text is decoded by a cast of the value field, followed by the id Spark gives it.
    final String decodeText = "value(#\\d+)? as decimal\\(38,6\\)";

    assertThat(plan(datasets.get("annotated"), filter.apply(this, query)))
        .contains("__numeric")
        .doesNotContainPattern(decodeText);
    for (final String unannotated : List.of("read", "disabled")) {
      assertThat(plan(datasets.get(unannotated), filter.apply(this, query)))
          .containsPattern(decodeText);
    }
  }

  @Nonnull
  private static String plan(@Nonnull final Dataset<Row> dataset, @Nonnull final Column filter) {
    return dataset.filter(filter).queryExecution().optimizedPlan().toString();
  }

  @Nonnull
  private Column search(@Nonnull final String query) {
    final SearchParameter parameter = new SearchParameter();
    parameter.setCode("value-quantity");
    parameter.setType(SearchParamType.QUANTITY);
    parameter.addBase("Observation");
    parameter.setExpression("Observation.value.ofType(Quantity)");
    return SearchColumnBuilder.withRegistry(
            fhirEncoders.getContext(),
            SearchParameterRegistry.fromSearchParameters(List.of(parameter)))
        .fromQueryString(ResourceType.OBSERVATION, query);
  }

  @Nonnull
  private Dataset<Row> stored(
      @Nonnull final String name, @Nonnull final Optional<SchemaConfiguration> annotation) {
    final Dataset<Row> read =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Observation", OBSERVATIONS);
    final Dataset<Row> dataset =
        annotation
            .map(
                configuration ->
                    Annotator.of(FhirDefinitionContext.of(fhirEncoders.getContext()), configuration)
                        .annotate("Observation", read))
            .orElse(read);
    return PrunedSchemaReader.write(dataset, tempDir.resolve(name).toString()).read();
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
