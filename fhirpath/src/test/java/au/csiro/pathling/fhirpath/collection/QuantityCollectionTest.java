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
import au.csiro.pathling.search.SearchColumnBuilder;
import au.csiro.pathling.sql.misc.CanonicalQuantityCode;
import au.csiro.pathling.sql.misc.CanonicalQuantityValue;
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
 * Tests that cross-unit quantity comparison computes canonicalisation when no annotation is
 * present, and that it continues to work over the previous layout's quantities, which carry {@code
 * _value_canonicalized} and {@code _code_canonicalized} (T087, FR-022).
 *
 * <p>Each fixture is given once as FHIR JSON and read in both layouts, so every case runs over each
 * layout with the same expected answer, and the expected answers are the ones the previous layout
 * gives before the port. The new layout carries no canonical form of a quantity until M5, so there
 * the canonical form can only have been computed from the structure. Its schema is also pruned to
 * the elements the fixtures populate, so its quantities have a different shape from the previous
 * layout's and from the engine's own quantity literals.
 *
 * <p>The fixtures cover a quantity at the resource root and under a repeating parent, units related
 * by a multiplicative factor and by an additive offset, a unit outside UCUM, and a value with more
 * than six fractional digits.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class QuantityCollectionTest {

  private static final Parser PARSER = new Parser();

  private static final String UCUM = "http://unitsofmeasure.org";

  private static final List<String> OBSERVATIONS =
      List.of(
          observation("o1", quantity("1.5", "g", UCUM, "g"), ""),
          observation("o2", quantity("1500", "mg", UCUM, "mg"), ""),
          observation(
              "o3",
              quantity("2", "kg", UCUM, "kg"),
              ",\"component\":[{\"code\":{\"text\":\"a\"},\"valueQuantity\":"
                  + quantity("500", "mg", UCUM, "mg")
                  + "},{\"code\":{\"text\":\"b\"},\"valueQuantity\":"
                  + quantity("0.5", "g", UCUM, "g")
                  + "}]"),
          observation("o4", quantity("3", "tab", "http://example.org/units", "tab"), ""),
          observation("o5", quantity("0.0000002", "kg", UCUM, "kg"), ""),
          observation("o6", quantity("37", "Cel", UCUM, "Cel"), ""));

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Map<String, Dataset<Row>> datasets;

  @BeforeAll
  void setUp() {
    datasets = Map.of("previous", stored(TestLayout.PREVIOUS), "pof", stored(TestLayout.POF));
  }

  @Nonnull
  Stream<Arguments> cases() {
    return Stream.of(
            // Equality across units related by a factor, against a literal.
            arguments(
                "value.ofType(Quantity) = 1500 'mg'",
                List.of("o1=true", "o2=true", "o3=false", "o4=null", "o5=false", "o6=null")),
            // Ordering across units, in both directions.
            arguments(
                "value.ofType(Quantity) > 1 'g'",
                List.of("o1=true", "o2=true", "o3=true", "o4=null", "o5=false", "o6=null")),
            arguments(
                "value.ofType(Quantity) < 1 'kg'",
                List.of("o1=true", "o2=true", "o3=false", "o4=null", "o5=true", "o6=null")),
            arguments(
                "1 'kg' >= value.ofType(Quantity)",
                List.of("o1=true", "o2=true", "o3=false", "o4=null", "o5=true", "o6=null")),
            // Units related by an additive offset.
            arguments(
                "value.ofType(Quantity) > 300 'K'",
                List.of("o1=null", "o2=null", "o3=null", "o4=null", "o5=null", "o6=true")),
            // Two stored quantities in different units, under a repeating parent.
            arguments(
                "component.value.ofType(Quantity)[0] = component.value.ofType(Quantity)[1]",
                List.of("o1=null", "o2=null", "o3=true", "o4=null", "o5=null", "o6=null")),
            arguments(
                "component.value.ofType(Quantity).where($this >= 0.5 'g').count()",
                List.of("o1=0", "o2=0", "o3=2", "o4=0", "o5=0", "o6=0")),
            // A unit outside UCUM has no canonical form, and compares on its stored value.
            arguments(
                "value.ofType(Quantity) = value.ofType(Quantity)",
                List.of("o1=true", "o2=true", "o3=true", "o4=true", "o5=true", "o6=true")),
            // The elements of a quantity.
            arguments(
                "value.ofType(Quantity).value",
                List.of(
                    "o1=1.500000",
                    "o2=1500.000000",
                    "o3=2.000000",
                    "o4=3.000000",
                    "o5=0.000000",
                    "o6=37.000000")),
            arguments(
                "value.ofType(Quantity).code",
                List.of("o1=g", "o2=mg", "o3=kg", "o4=tab", "o5=kg", "o6=Cel")),
            arguments(
                "value.ofType(Quantity).unit",
                List.of("o1=g", "o2=mg", "o3=kg", "o4=tab", "o5=kg", "o6=Cel")),
            // Rendering, conversion and combination with a literal.
            arguments(
                "value.ofType(Quantity).toString()",
                List.of(
                    "o1=1.5 'g'",
                    "o2=1500 'mg'",
                    "o3=2 'kg'",
                    "o4=null",
                    "o5=0 'kg'",
                    "o6=37 'Cel'")),
            arguments(
                "value.ofType(Quantity).toQuantity('g').value",
                List.of(
                    "o1=1.500000",
                    "o2=1.500000",
                    "o3=2000.000000",
                    "o4=null",
                    "o5=0.000000",
                    "o6=null")),
            arguments(
                "value.ofType(Quantity).convertsToQuantity('g')",
                List.of("o1=true", "o2=true", "o3=true", "o4=false", "o5=true", "o6=false")),
            arguments(
                "(value.ofType(Quantity) | 1 'g').count()",
                List.of("o1=2", "o2=2", "o3=2", "o4=2", "o5=2", "o6=2")))
        .flatMap(
            c ->
                Stream.of("previous", "pof")
                    .map(layout -> arguments(layout, c.get()[0], c.get()[1])));
  }

  @ParameterizedTest(name = "{1} over the {0} layout")
  @MethodSource("cases")
  void quantityOperationsMatchOnBothLayouts(
      @Nonnull final String layout,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(datasets.get(layout), expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> precisionCases() {
    // The new layout stores the source text, so a value with more than six fractional digits keeps
    // its magnitude through canonicalisation. The previous layout's value column holds six.
    return Stream.of(
        arguments(
            "value.ofType(Quantity) = 0.2 'mg'",
            List.of("o1=false", "o2=false", "o3=false", "o4=null", "o5=true", "o6=null")),
        arguments(
            "value.ofType(Quantity) > 0.1 'mg'",
            List.of("o1=true", "o2=true", "o3=true", "o4=null", "o5=true", "o6=null")));
  }

  @ParameterizedTest(name = "{0} over the new layout")
  @MethodSource("precisionCases")
  void canonicalisationKeepsTheMagnitudeOfTheStoredText(
      @Nonnull final String expression, @Nonnull final List<String> expected) {
    assertThat(evaluate(datasets.get("pof"), expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> fieldPlanCases() {
    // The previous layout's value is rendered to text once, by one CASE WHEN over its scale. The
    // new layout's value is text already.
    return Stream.of(
        arguments("previous", "value.ofType(Quantity).value", 1),
        arguments("pof", "value.ofType(Quantity).value", 0),
        arguments("previous", "component.value.ofType(Quantity).value", 1),
        arguments("pof", "component.value.ofType(Quantity).value", 0));
  }

  /**
   * A field of a decoded quantity is read from the stored quantity, which is normalised once. The
   * decoded structure has the type of a previous-layout quantity, so reading the field from it
   * would normalise it a second time, and compute its canonical form for nothing.
   */
  @ParameterizedTest(name = "{1} over the {0} layout")
  @MethodSource("fieldPlanCases")
  void fieldOfADecodedQuantityIsNormalisedOnce(
      @Nonnull final String layout,
      @Nonnull final String expression,
      final int expectedRenderings) {
    final Dataset<Row> dataset = datasets.get(layout);
    final String plan =
        dataset
            .select(
                SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
                    .fromExpression(ResourceType.OBSERVATION, expression)
                    .alias("x"))
            .queryExecution()
            .optimizedPlan()
            .expressions()
            .mkString(",");
    assertThat(plan)
        .doesNotContain(CanonicalQuantityValue.FUNCTION_NAME)
        .doesNotContain(CanonicalQuantityCode.FUNCTION_NAME);
    assertThat(plan.split("CASE WHEN", -1)).hasSize(expectedRenderings + 1);
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
  private static String quantity(
      @Nonnull final String value,
      @Nonnull final String unit,
      @Nonnull final String system,
      @Nonnull final String code) {
    return "{\"value\":"
        + value
        + ",\"unit\":\""
        + unit
        + "\",\"system\":\""
        + system
        + "\",\"code\":\""
        + code
        + "\"}";
  }

  @Nonnull
  private static String observation(
      @Nonnull final String id, @Nonnull final String valueQuantity, @Nonnull final String rest) {
    return "{\"resourceType\":\"Observation\",\"id\":\""
        + id
        + "\",\"status\":\"final\",\"code\":{\"text\":\"x\"},\"valueQuantity\":"
        + valueQuantity
        + rest
        + "}";
  }
}
