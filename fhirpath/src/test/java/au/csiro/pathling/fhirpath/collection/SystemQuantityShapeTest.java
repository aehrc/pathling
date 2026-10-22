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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.layout.PofDatasets;
import au.csiro.pathling.utilities.SparkTypes;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Pins the shape of a {@code System.Quantity} that the engine builds itself (FR-059).
 *
 * <p>Whatever produces it, such a quantity has one fixed, complete shape: the elements of a FHIR
 * Quantity other than {@code extension}, in definition order, with its value as the text of a
 * decimal, as the Parquet on FHIR layout stores one. It carries none of the previous layout's
 * companions: no {@code DECIMAL} value with a {@code value_scale} beside it, no canonical form in
 * {@code _value_canonicalized} and {@code _code_canonicalized}, and no {@code _fid}. The canonical
 * form is computed where a quantity is compared, from its value and code.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class SystemQuantityShapeTest {

  private static final Parser PARSER = new Parser();

  /** The fixed shape of an engine-built System.Quantity, written out field by field. */
  private static final StructType SYSTEM_QUANTITY =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.StringType, true),
            DataTypes.createStructField("value", DataTypes.StringType, true),
            DataTypes.createStructField("comparator", DataTypes.StringType, true),
            DataTypes.createStructField("unit", DataTypes.StringType, true),
            DataTypes.createStructField("system", DataTypes.StringType, true),
            DataTypes.createStructField("code", DataTypes.StringType, true)
          });

  /** The field names by which a structure is recognised as a previous-layout quantity. */
  private static final List<String> LEGACY_FIELDS =
      List.of("value_scale", "_value_canonicalized", "_code_canonicalized", "_fid");

  private static final String OBSERVATION =
      "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
          + "\"code\":{\"text\":\"x\"},\"valueQuantity\":{\"id\":\"q1\",\"value\":1.5,"
          + "\"comparator\":\"<\",\"unit\":\"g\",\"system\":\"http://unitsofmeasure.org\","
          + "\"code\":\"g\",\"extension\":[{\"url\":\"http://example.org/e\","
          + "\"valueString\":\"e\"}]}}";

  @Autowired SparkSession spark;

  @Autowired FhirContext fhirContext;

  private Dataset<Row> dataset;

  @BeforeAll
  void setUp() {
    dataset = PofDatasets.fromJson(spark, fhirContext, "Observation", List.of(OBSERVATION));
  }

  @Nonnull
  Stream<Arguments> producers() {
    return Stream.of(
        arguments("a literal", "1.50 'mg'"),
        arguments("a calendar duration literal", "3 days"),
        arguments("an arithmetic result promoted to a quantity", "(1 + 2).toQuantity()"),
        arguments("a decimal arithmetic result promoted to a quantity", "(1.5 * 2).toQuantity()"),
        arguments("a string converted to a quantity", "'5.5 \\'mg\\''.toQuantity()"),
        arguments("a unit conversion", "(1 'g').toQuantity('mg')"),
        arguments(
            "a stored quantity converted to a unit", "value.ofType(Quantity).toQuantity('mg')"),
        arguments("a union of a literal with a stored quantity", "value.ofType(Quantity) | 1 'g'"),
        arguments("a union of a stored quantity with a literal", "1 'g' | value.ofType(Quantity)"),
        arguments("a union of two literals", "1 'g' | 2 'kg'"));
  }

  @Test
  void quantityUdfRejectsAStructThatIsNotTheFixedShape() {
    assertThatThrownBy(
            () ->
                spark
                    .sql("SELECT quantity_to_literal(named_struct('code', 'mg', 'value', '1.5'))")
                    .collectAsList())
        .hasStackTraceContaining("Expected a System quantity");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("producers")
  void engineBuiltQuantityHasTheFixedShape(
      @Nonnull final String producer, @Nonnull final String expression) {
    assertThat(elementTypeOf(expression)).isEqualTo(SYSTEM_QUANTITY);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("producers")
  void engineBuiltQuantityCarriesNoLegacyCompanion(
      @Nonnull final String producer, @Nonnull final String expression) {
    final StructType type = (StructType) elementTypeOf(expression);
    assertThat(type.fieldNames()).doesNotContainAnyElementsOf(LEGACY_FIELDS);
    assertThat(Arrays.stream(type.fields()).map(StructField::dataType))
        .isNotEmpty()
        .allMatch(DataTypes.StringType::equals);
  }

  @Nonnull
  Stream<Arguments> values() {
    return Stream.of(
        // .value decodes the text to the engine's decimal type, at six places.
        arguments("(1.50 'mg').value", "1.500000"),
        // The value is the text of the decimal, at its own scale.
        arguments("1.50 'mg'", "[null,1.50,null,mg,http://unitsofmeasure.org,mg]"),
        // A promoted number has no known scale, so it is given its shortest text.
        arguments("(1 + 2).toQuantity()", "[null,3,null,1,http://unitsofmeasure.org,1]"),
        arguments("(1.5 * 2).toQuantity()", "[null,3,null,1,http://unitsofmeasure.org,1]"),
        arguments("1.25.toQuantity()", "[null,1.25,null,1,http://unitsofmeasure.org,1]"),
        arguments("100.toQuantity()", "[null,100,null,1,http://unitsofmeasure.org,1]"),
        arguments("0.toQuantity()", "[null,0,null,1,http://unitsofmeasure.org,1]"),
        arguments("true.toQuantity()", "[null,1,null,1,http://unitsofmeasure.org,1]"),
        arguments("(1 'g').toQuantity('mg')", "[null,1000,null,mg,http://unitsofmeasure.org,mg]"),
        // A stored quantity met by a System one keeps its id and comparator, but not its
        // extensions.
        arguments(
            "(value.ofType(Quantity) | 1 'g').first()",
            "[q1,1.5,<,g,http://unitsofmeasure.org,g]"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  void engineBuiltQuantityHoldsItsElements(
      @Nonnull final String expression, @Nonnull final String expected) {
    assertThat(evaluate(expression)).isEqualTo(expected);
  }

  @Nonnull
  Stream<Arguments> comparisons() {
    // A definite calendar duration is compared through its UCUM equivalent, so its canonical form
    // depends on the system as well as the code.
    return Stream.of(
        arguments("1 second = 1000 'ms'", "true"),
        arguments("1000 milliseconds = 1 's'", "true"),
        arguments("1 second < 1001 'ms'", "true"),
        arguments("1.50 'g' = 1500 'mg'", "true"),
        arguments("(1 'g').toQuantity('mg') = 1 'g'", "true"),
        arguments("(1 + 2).toQuantity() = 3 '1'", "true"),
        arguments("5 '1' = 500 '%'", "true"),
        arguments("(value.ofType(Quantity) | 1 'g').where($this > 1000 'mg').count()", "1"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("comparisons")
  void engineBuiltQuantityComparesAsBefore(
      @Nonnull final String expression, @Nonnull final String expected) {
    assertThat(evaluate(expression)).isEqualTo(expected);
  }

  @Nonnull
  Stream<Arguments> promotedNumberComparisons() {
    return Stream.of(
        arguments("5.toQuantity() = 500 '%'", "true"),
        arguments("0.5.toQuantity() = 50 '%'", "true"),
        arguments("5.toQuantity() = 5 '1'", "true"));
  }

  /**
   * A number promoted with {@code toQuantity()} has the default unit {@code '1'}, so it compares
   * across dimensionless units as the literal {@code 5 '1'} does. This result was empty before the
   * fixed shape, which did not follow the specification.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("promotedNumberComparisons")
  void promotedNumberComparesAsTheDimensionlessLiteral(
      @Nonnull final String expression, @Nonnull final String expected) {
    assertThat(evaluate(expression)).isEqualTo(expected);
  }

  @Nonnull
  private DataType elementTypeOf(@Nonnull final String expression) {
    return SparkTypes.elementTypeOf(
        dataset
            .select(evaluator().evaluateToCollection(PARSER.parse(expression)).getColumnValue())
            .schema()
            .fields()[0]
            .dataType());
  }

  @Nonnull
  private String evaluate(@Nonnull final String expression) {
    final Row row =
        dataset
            .select(evaluator().evaluateToCollection(PARSER.parse(expression)).getColumnValue())
            .first();
    return Objects.toString(row.get(0));
  }

  @Nonnull
  private DatasetEvaluator evaluator() {
    return DatasetEvaluatorBuilder.create(ResourceType.OBSERVATION, fhirContext)
        .withDataset(dataset)
        .build();
  }
}
