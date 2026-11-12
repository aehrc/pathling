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
import au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.layout.PofDatasets;
import au.csiro.pathling.utilities.SparkTypes;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
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
 * Pins the shape of a {@code System.Quantity} that the engine builds itself (FR-059, decision 150).
 *
 * <p>Whatever produces it, such a quantity has the fields its literal can set, its content: {@code
 * value}, the engine's System.Decimal, then {@code unit}, {@code code} and {@code system}. Beside
 * them, flat, it carries its canonical annotations, {@code __canonical_code} and {@code
 * __canonical_value_exact}, only where they are available without computing (decision 152): a
 * literal has them as constants, and a function result, or a merge with an operand that lacks them,
 * has none. It has no {@code id}, {@code comparator} or {@code _value}, and none of the previous
 * layout's companions. Returned to a caller, it has its content only.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class SystemQuantityShapeTest {

  private static final Parser PARSER = new Parser();

  /** The content of a System.Quantity, written out field by field, as it is returned. */
  private static final StructType CONTENT =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("value", DataTypes.createDecimalType(38, 6), true),
            DataTypes.createStructField("unit", DataTypes.StringType, true),
            DataTypes.createStructField("code", DataTypes.StringType, true),
            DataTypes.createStructField("system", DataTypes.StringType, true)
          });

  /** The canonical annotations of a System.Quantity, where they are available. */
  private static final StructType ANNOTATED =
      CONTENT
          .add("__canonical_code", DataTypes.StringType, true)
          .add("__canonical_value_exact", QuantityCanonicalKind.EXACT_VALUE_TYPE, true);

  /** The fields of the shape that are the content of a System.Quantity. */
  private static final List<String> ELEMENTS = List.of("value", "unit", "code", "system");

  /** The field names by which a structure is recognised as a previous-layout or stored quantity. */
  private static final List<String> LEGACY_FIELDS =
      List.of(
          "value_scale",
          "_value_canonicalized",
          "_code_canonicalized",
          "_fid",
          "id",
          "comparator",
          "_value");

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

  /**
   * Each producer, and whether its quantity carries the canonical annotations: a literal with a
   * canonical form carries them as constants; a calendar duration longer than a second has no
   * canonical form; a function result carries none (rule A); and a merge keeps them only where
   * every operand has them, which the unannotated stored quantity here does not (decision 142, D9).
   */
  @Nonnull
  Stream<Arguments> producers() {
    return Stream.of(
        arguments("a literal", "1.50 'mg'", true),
        arguments("a calendar duration literal", "3 days", false),
        arguments("an arithmetic result promoted to a quantity", "(1 + 2).toQuantity()", false),
        arguments(
            "a decimal arithmetic result promoted to a quantity", "(1.5 * 2).toQuantity()", false),
        arguments("a string converted to a quantity", "'5.5 \\'mg\\''.toQuantity()", false),
        arguments("a unit conversion", "(1 'g').toQuantity('mg')", false),
        arguments(
            "a stored quantity converted to a unit",
            "value.ofType(Quantity).toQuantity('mg')",
            false),
        arguments(
            "a union of a literal with a stored quantity", "value.ofType(Quantity) | 1 'g'", false),
        arguments(
            "a union of a stored quantity with a literal", "1 'g' | value.ofType(Quantity)", false),
        arguments("a union of two literals", "1 'g' | 2 'kg'", true));
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
  void engineBuiltQuantityHasTheShapeOfDecision150(
      @Nonnull final String producer, @Nonnull final String expression, final boolean annotated) {
    assertThat(elementTypeOf(expression)).isEqualTo(annotated ? ANNOTATED : CONTENT);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("producers")
  void returnedQuantityHasItsContentOnly(
      @Nonnull final String producer,
      @Nonnull final String expression,
      final boolean ignoredAnnotated) {
    assertThat(returnedTypeOf(expression)).isEqualTo(CONTENT);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("producers")
  void engineBuiltQuantityCarriesNoLegacyCompanion(
      @Nonnull final String producer,
      @Nonnull final String expression,
      final boolean ignoredAnnotated) {
    final StructType type = (StructType) elementTypeOf(expression);
    assertThat(type.fieldNames()).doesNotContainAnyElementsOf(LEGACY_FIELDS);
  }

  @Nonnull
  Stream<Arguments> values() {
    return Stream.of(
        // .value is the engine's decimal, at six places.
        arguments("(1.50 'mg').value", "1.500000"),
        // The value is the engine's System.Decimal, at six places.
        arguments("1.50 'mg'", "[1.500000,mg,mg,http://unitsofmeasure.org]"),
        arguments("(1 + 2).toQuantity()", "[3.000000,1,1,http://unitsofmeasure.org]"),
        arguments("(1.5 * 2).toQuantity()", "[3.000000,1,1,http://unitsofmeasure.org]"),
        arguments("1.25.toQuantity()", "[1.250000,1,1,http://unitsofmeasure.org]"),
        arguments("100.toQuantity()", "[100.000000,1,1,http://unitsofmeasure.org]"),
        arguments("0.toQuantity()", "[0.000000,1,1,http://unitsofmeasure.org]"),
        arguments("true.toQuantity()", "[1.000000,1,1,http://unitsofmeasure.org]"),
        arguments("(1 'g').toQuantity('mg')", "[1000.000000,mg,mg,http://unitsofmeasure.org]"),
        // A stored quantity with a comparator has no System form, so it drops out of a merge with
        // a System quantity, and the literal is first.
        arguments(
            "(value.ofType(Quantity) | 1 'g').first()",
            "[1.000000,g,g,http://unitsofmeasure.org]"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  void engineBuiltQuantityHoldsItsElements(
      @Nonnull final String expression, @Nonnull final String expected) {
    assertThat(evaluateElements(expression)).isEqualTo(expected);
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
        // The stored quantity is a bound, so its comparison is empty and it is not kept.
        arguments("(value.ofType(Quantity) | 1 'g').where($this > 1000 'mg').count()", "0"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("comparisons")
  void engineBuiltQuantityComparesAsBefore(
      @Nonnull final String expression, @Nonnull final String expected) {
    assertThat(evaluate(expression)).isEqualTo(expected);
  }

  /**
   * A System quantity's value is a System.Decimal, which its string renders as a decimal's string
   * renders, without trailing fractional zeros.
   */
  @Nonnull
  Stream<Arguments> strings() {
    return Stream.of(
        arguments("(7.5 'mg').toString()", "7.5 'mg'"),
        arguments("(1.50 'mg').toString()", "1.5 'mg'"),
        arguments("(1 + 2).toQuantity().toString()", "3 '1'"),
        arguments("(1 'g').toQuantity('mg').toString()", "1000 'mg'"),
        arguments("(4 days).toString()", "4 days"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("strings")
  void systemQuantityStringHasNoTrailingFractionalZeros(
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
  private DataType returnedTypeOf(@Nonnull final String expression) {
    return SparkTypes.elementTypeOf(
        dataset
            .select(
                evaluator()
                    .evaluateToCollection(PARSER.parse(expression))
                    .getOutputColumn()
                    .getValue())
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

  /** Evaluates an expression, keeping only the elements of a quantity it gives. */
  @Nonnull
  private String evaluateElements(@Nonnull final String expression) {
    final Object value =
        dataset
            .select(evaluator().evaluateToCollection(PARSER.parse(expression)).getColumnValue())
            .first()
            .get(0);
    return value instanceof final Row quantity
        ? ELEMENTS.stream()
            .map(name -> Objects.toString(quantity.<Object>getAs(name)))
            .collect(Collectors.joining(",", "[", "]"))
        : Objects.toString(value);
  }

  @Nonnull
  private DatasetEvaluator evaluator() {
    return DatasetEvaluatorBuilder.create(ResourceType.OBSERVATION, fhirContext)
        .withDataset(dataset)
        .build();
  }
}
