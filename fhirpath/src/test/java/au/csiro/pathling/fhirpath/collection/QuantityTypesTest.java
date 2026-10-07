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
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluator;
import au.csiro.pathling.search.SearchColumnBuilder;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
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
 * Tests that the types profiled on Quantity, such as Duration and Age, are decoded at traversal as
 * a Quantity is, on both layouts (decision 80).
 *
 * <p>The previous layout stores each of them in the structure the engine computes with, with a
 * {@code DECIMAL(32,6)} value, its scale and a canonical form. Before the port, a single-instance
 * evaluation rendered their value as a number, and a projected column had that structure. Both must
 * hold on the previous layout after the port, where the traversal expression normalises the stored
 * element to text, and they hold on the new layout too, because the element is decoded there in the
 * same way.
 *
 * <p>The fixtures cover a Duration at the resource root, an Age that is a choice at the root, a
 * Duration nested in a singular parent, and a Duration nested in a repeating one.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class QuantityTypesTest {

  private static final String UCUM = "http://unitsofmeasure.org";

  private static final Map<String, List<String>> RESOURCES =
      Map.of(
          "Encounter",
          List.of(
              "{\"resourceType\":\"Encounter\",\"id\":\"e1\",\"status\":\"finished\","
                  + "\"class\":{\"code\":\"AMB\"},\"length\":"
                  + quantity("90", "min", "min")
                  + "}"),
          "Condition",
          List.of(
              "{\"resourceType\":\"Condition\",\"id\":\"c1\","
                  + "\"subject\":{\"reference\":\"Patient/p1\"},\"onsetAge\":"
                  + quantity("12", "a", "a")
                  + "}"),
          "MedicationRequest",
          List.of(
              "{\"resourceType\":\"MedicationRequest\",\"id\":\"m1\",\"status\":\"active\","
                  + "\"intent\":\"order\",\"subject\":{\"reference\":\"Patient/p1\"},"
                  + "\"medicationCodeableConcept\":{\"text\":\"x\"},"
                  + "\"dosageInstruction\":[{\"timing\":{\"repeat\":{\"boundsDuration\":"
                  + quantity("7", "days", "d")
                  + "}}}],"
                  + "\"dispenseRequest\":{\"expectedSupplyDuration\":"
                  + quantity("30", "days", "d")
                  + "}}"));

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Map<TestLayout, Map<String, Dataset<Row>>> datasets;

  @BeforeAll
  void setUp() {
    datasets =
        Map.of(
            TestLayout.PREVIOUS, stored(TestLayout.PREVIOUS),
            TestLayout.POF, stored(TestLayout.POF));
  }

  @Nonnull
  Stream<Arguments> wholeValues() {
    return Stream.of(
            arguments(
                "Encounter",
                "length",
                "Duration",
                "{\"value\":90.000000,\"unit\":\"min\",\"system\":\""
                    + UCUM
                    + "\",\"code\":\"min\"}"),
            arguments(
                "Condition",
                "onset.ofType(Age)",
                "Age",
                "{\"value\":12.000000,\"unit\":\"a\",\"system\":\"" + UCUM + "\",\"code\":\"a\"}"),
            arguments(
                "MedicationRequest",
                "dispenseRequest.expectedSupplyDuration",
                "Duration",
                "{\"value\":30.000000,\"unit\":\"days\",\"system\":\""
                    + UCUM
                    + "\",\"code\":\"d\"}"),
            arguments(
                "MedicationRequest",
                "dosageInstruction.timing.repeat.bounds.ofType(Duration)",
                "Duration",
                "{\"value\":7.000000,\"unit\":\"days\",\"system\":\""
                    + UCUM
                    + "\",\"code\":\"d\"}"),
            arguments("Encounter", "length.value", "decimal", "90.000000"))
        .flatMap(c -> bothLayouts(c.get()));
  }

  @ParameterizedTest(name = "{2} over the {0} layout")
  @MethodSource("wholeValues")
  void singleInstanceEvaluationRendersTheDecodedValue(
      @Nonnull final TestLayout layout,
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final String expectedType,
      @Nonnull final String expectedValue) {
    final SingleInstanceEvaluationResult result =
        SingleInstanceEvaluator.evaluate(
            datasets.get(layout).get(resourceType),
            resourceType,
            fhirEncoders.getContext(),
            expression,
            null,
            null);
    assertThat(result.getResults())
        .singleElement()
        .satisfies(
            value -> {
              assertThat(value.getType()).isEqualTo(expectedType);
              assertThat(String.valueOf(value.getValue())).isEqualTo(expectedValue);
            });
  }

  @Nonnull
  Stream<Arguments> projectedColumns() {
    final DataType quantity = QuantityEncoding.dataType();
    return Stream.of(
            // A Duration at the root: 90 min is 5400 s.
            arguments("Encounter", "length", quantity, "5400", "s"),
            // An Age at the root, reached through a choice: 12 a is 378691200 s.
            arguments("Condition", "onset.ofType(Age)", quantity, "378691200", "s"),
            // A Duration nested in a singular parent: 30 d is 2592000 s.
            arguments(
                "MedicationRequest",
                "dispenseRequest.expectedSupplyDuration",
                quantity,
                "2592000",
                "s"),
            // A Duration nested in a repeating parent: 7 d is 604800 s.
            arguments(
                "MedicationRequest",
                "dosageInstruction.timing.repeat.bounds.ofType(Duration)",
                DataTypes.createArrayType(quantity),
                "604800",
                "s"))
        .flatMap(c -> bothLayouts(c.get()));
  }

  @ParameterizedTest(name = "{2} over the {0} layout")
  @MethodSource("projectedColumns")
  void projectedColumnHasTheDecodedStructure(
      @Nonnull final TestLayout layout,
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final DataType expectedType,
      @Nonnull final String expectedCanonicalValue,
      @Nonnull final String expectedCanonicalCode) {
    final Dataset<Row> projected =
        datasets
            .get(layout)
            .get(resourceType)
            .select(
                SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
                    .fromExpression(ResourceType.fromCode(resourceType), expression)
                    .alias("x"));
    assertThat(projected.schema().apply("x").dataType().simpleString())
        .isEqualTo(expectedType.simpleString());

    final Row decoded = firstQuantity(projected.first());
    assertThat(decoded.<Integer>getAs("value_scale")).isZero();
    assertThat(
            decoded
                .<Row>getAs(QuantityEncoding.CANONICALIZED_VALUE_COLUMN)
                .<BigDecimal>getAs("value")
                .toPlainString())
        .isEqualTo(expectedCanonicalValue);
    assertThat(decoded.<String>getAs(QuantityEncoding.CANONICALIZED_CODE_COLUMN))
        .isEqualTo(expectedCanonicalCode);
  }

  @Nonnull
  private static Row firstQuantity(@Nonnull final Row row) {
    final Object value = row.get(0);
    return value instanceof final Row struct ? struct : row.<Row>getList(0).get(0);
  }

  @Nonnull
  private static Stream<Arguments> bothLayouts(@Nonnull final Object[] arguments) {
    return Stream.of(TestLayout.PREVIOUS, TestLayout.POF)
        .map(
            layout -> {
              final Object[] withLayout = new Object[arguments.length + 1];
              withLayout[0] = layout;
              System.arraycopy(arguments, 0, withLayout, 1, arguments.length);
              return arguments(withLayout);
            });
  }

  /**
   * Builds the fixtures in a layout, and writes them to Parquet and reads them back, so that the
   * engine reads stored files.
   */
  @Nonnull
  private Map<String, Dataset<Row>> stored(@Nonnull final TestLayout layout) {
    return Map.of(
        "Encounter", stored(layout, "Encounter"),
        "Condition", stored(layout, "Condition"),
        "MedicationRequest", stored(layout, "MedicationRequest"));
  }

  @Nonnull
  private Dataset<Row> stored(@Nonnull final TestLayout layout, @Nonnull final String type) {
    final Dataset<Row> dataset =
        LayoutDatasets.fromJson(spark, fhirEncoders, layout, type, RESOURCES.get(type));
    return PrunedSchemaReader.write(dataset, tempDir.resolve(layout + "-" + type).toString())
        .read();
  }

  @Nonnull
  private static String quantity(
      @Nonnull final String value, @Nonnull final String unit, @Nonnull final String code) {
    return "{\"value\":"
        + value
        + ",\"unit\":\""
        + unit
        + "\",\"system\":\""
        + UCUM
        + "\",\"code\":\""
        + code
        + "\"}";
  }
}
