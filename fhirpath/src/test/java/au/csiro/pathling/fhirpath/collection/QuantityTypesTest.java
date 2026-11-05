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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluator;
import au.csiro.pathling.search.SearchColumnBuilder;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.helpers.JsonElementRenderer;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
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
 * Tests that the types profiled on Quantity, such as Duration and Age, are handled as a Quantity is
 * (decision 80).
 *
 * <p>A single-instance evaluation renders their value as a number. A projected column is a System
 * quantity, in its fixed shape, which carries no canonical form; the canonical form is computed
 * from it (FR-059). A whole value is rendered as FHIR JSON from the stored element, so its value is
 * a number with the text of the source (T100j).
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

  private Map<String, Dataset<Row>> datasets;

  @BeforeAll
  void setUp() {
    datasets = stored();
  }

  @Nonnull
  Stream<Arguments> wholeValues() {
    return Stream.of(
        arguments(
            "Encounter",
            "length",
            "Duration",
            "{\"value\":90,\"unit\":\"min\",\"system\":\"" + UCUM + "\",\"code\":\"min\"}"),
        arguments(
            "Condition",
            "onset.ofType(Age)",
            "Age",
            "{\"value\":12,\"unit\":\"a\",\"system\":\"" + UCUM + "\",\"code\":\"a\"}"),
        arguments(
            "MedicationRequest",
            "dispenseRequest.expectedSupplyDuration",
            "Duration",
            "{\"value\":30,\"unit\":\"days\",\"system\":\"" + UCUM + "\",\"code\":\"d\"}"),
        arguments(
            "MedicationRequest",
            "dosageInstruction.timing.repeat.bounds.ofType(Duration)",
            "Duration",
            "{\"value\":7,\"unit\":\"days\",\"system\":\"" + UCUM + "\",\"code\":\"d\"}"),
        arguments("Encounter", "length.value", "decimal", "90.000000"));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("wholeValues")
  void singleInstanceEvaluationRendersTheStoredValue(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final String expectedType,
      @Nonnull final String expectedValue) {
    final SingleInstanceEvaluationResult result =
        SingleInstanceEvaluator.evaluate(
            datasets.get(resourceType),
            resourceType,
            fhirEncoders.getContext(),
            expression,
            null,
            null,
            JsonElementRenderer.of(fhirEncoders.getContext()));
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
            "s"));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("projectedColumns")
  void projectedColumnIsASystemQuantity(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final DataType expectedType,
      @Nonnull final String expectedCanonicalValue,
      @Nonnull final String expectedCanonicalCode) {
    final Dataset<Row> projected =
        datasets
            .get(resourceType)
            .select(
                SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
                    .fromExpression(ResourceType.fromCode(resourceType), expression)
                    .alias("x"));
    assertThat(projected.schema().apply("x").dataType().simpleString())
        .isEqualTo(expectedType.simpleString());

    // The System quantity carries its canonical form (FR-059), computed from the stored quantity,
    // which has no annotation.
    final Column quantity = expectedType instanceof ArrayType ? col("x").getItem(0) : col("x");
    final Column canonical = QuantityEncoding.canonical(quantity);
    final Row canonicalRow =
        projected
            .select(canonical.getField("value").getField("value"), canonical.getField("code"))
            .first();
    assertThat(canonicalRow.<BigDecimal>getAs(0).toPlainString()).isEqualTo(expectedCanonicalValue);
    assertThat(canonicalRow.<String>getAs(1)).isEqualTo(expectedCanonicalCode);
  }

  /**
   * Builds the fixtures, and writes them to Parquet and reads them back, so that the engine reads
   * stored files.
   */
  @Nonnull
  private Map<String, Dataset<Row>> stored() {
    return Map.of(
        "Encounter", stored("Encounter"),
        "Condition", stored("Condition"),
        "MedicationRequest", stored("MedicationRequest"));
  }

  @Nonnull
  private Dataset<Row> stored(@Nonnull final String type) {
    final Dataset<Row> dataset =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), type, RESOURCES.get(type));
    return PrunedSchemaReader.write(dataset, tempDir.resolve(type).toString()).read();
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
