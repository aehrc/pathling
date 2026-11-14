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

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.transform;
import static org.apache.spark.sql.functions.when;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.Annotator;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
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
 * Tests that each annotated type, a date, a decimal and a quantity, gives the same results over
 * data written with its annotations and without them (FR-022, FR-023; design §8), including values
 * whose text and number order differently: {@code "10"} against {@code "9"}, and {@code 1.0}
 * against {@code 1.00}.
 *
 * <p>A decimal is read and written through a double as JSON is ingested, so the stored texts are
 * set directly, before the annotations are computed from them.
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class AnnotatedTypesParityTest {

  private static final Parser PARSER = new Parser();

  private static final String QUANTITY =
      "{\"value\":1,\"unit\":\"g\",\"system\":\"http://unitsofmeasure.org\",\"code\":\"g\"}";

  private static final String NANOSECOND =
      "{\"value\":1,\"unit\":\"ns\",\"system\":\"http://unitsofmeasure.org\",\"code\":\"ns\"}";

  private static final List<String> OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"effectiveDateTime\":\"2020-03\","
              + "\"valueQuantity\":"
              + QUANTITY
              + ",\"component\":["
              + String.join(",", List.of(component(), component(), component(), component()))
              + "]}",
          "{\"resourceType\":\"Observation\",\"id\":\"o2\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"effectiveDateTime\":\"2018-01-01T10:00:00Z\","
              + "\"valueQuantity\":"
              + QUANTITY
              + "}");

  @Nonnull
  private static String component() {
    return "{\"code\":{\"text\":\"c\"},\"valueQuantity\":" + QUANTITY + "}";
  }

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Dataset<Row> read;

  private Dataset<Row> annotated;

  /** Observations with a decimal the engine cannot hold, and a quantity without a code. */
  private static final List<String> EDGE_OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o3\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":"
              + QUANTITY
              + ",\"component\":["
              + component()
              + ","
              + component()
              + "]}",
          "{\"resourceType\":\"Observation\",\"id\":\"o4\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":{\"value\":5,\"unit\":\"mg\","
              + "\"system\":\"http://unitsofmeasure.org\"}}");

  private Dataset<Row> edgeRead;

  private Dataset<Row> edgeAnnotated;

  /**
   * Observations with a quantity whose value does not decode, quantities whose texts have more
   * fractional digits than the engine's System.Decimal keeps, and quantities in a small unit.
   */
  private static final List<String> ROUNDING_OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o5\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":"
              + QUANTITY
              + ",\"component\":["
              + String.join(",", List.of(component(), component(), component()))
              + "]}",
          "{\"resourceType\":\"Observation\",\"id\":\"o6\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":"
              + NANOSECOND
              + ",\"component\":[{\"code\":{\"text\":\"c\"},\"valueQuantity\":"
              + NANOSECOND
              + "}]}");

  private Dataset<Row> roundingRead;

  /** Quantities whose stored texts have more fractional digits than the System.Decimal keeps. */
  private static final List<String> CONVERSION_OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o8\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":"
              + QUANTITY
              + "}",
          "{\"resourceType\":\"Observation\",\"id\":\"o9\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":"
              + QUANTITY
              + "}");

  private Dataset<Row> conversionRead;

  private Dataset<Row> conversionAnnotated;

  private Dataset<Row> roundingAnnotated;

  @BeforeAll
  void setUp() {
    final Dataset<Row> parsed =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Observation", OBSERVATIONS);
    // The stored texts: "10" against "9" orders differently as text and as a number, and "1.0"
    // and "1.00" are one number with two texts. "1.0000001" has more fractional digits than the
    // engine's System.Decimal keeps.
    final Dataset<Row> resources =
        parsed
            .withColumn(
                "valueQuantity",
                col("valueQuantity")
                    .withField(
                        "value", when(col("id").equalTo("o1"), lit("10")).otherwise("1.0000001")))
            .withColumn(
                "component",
                transform(
                    col("component"),
                    (component, index) ->
                        component.withField(
                            "valueQuantity.value",
                            element_at(
                                array(lit("9"), lit("1.0"), lit("1.00"), lit("10")),
                                index.plus(1)))));
    read = PrunedSchemaReader.write(resources, tempDir.resolve("read").toString()).read();
    // "1.0E40" has more integer digits than the engine's System.Decimal holds, so it does not
    // decode.
    final Dataset<Row> edge =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Observation", EDGE_OBSERVATIONS)
            .withColumn(
                "valueQuantity",
                when(
                        col("id").equalTo("o3"),
                        col("valueQuantity").withField("value", lit("1.0E40")))
                    .otherwise(col("valueQuantity")))
            .withColumn(
                "component",
                transform(
                    col("component"),
                    (component, index) ->
                        component.withField(
                            "valueQuantity.value",
                            element_at(array(lit("1.0E40"), lit("2")), index.plus(1)))));
    edgeRead = PrunedSchemaReader.write(edge, tempDir.resolve("edge-read").toString()).read();
    edgeAnnotated =
        PrunedSchemaReader.write(
                Annotator.of(
                        FhirDefinitionContext.of(fhirEncoders.getContext()),
                        SchemaConfiguration.builder().build())
                    .annotate("Observation", edge),
                tempDir.resolve("edge-annotated").toString())
            .read();
    final Dataset<Row> rounding =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Observation", ROUNDING_OBSERVATIONS)
            .withColumn(
                "valueQuantity",
                when(
                        col("id").equalTo("o5"),
                        col("valueQuantity").withField("value", lit("1.0E33")))
                    .otherwise(col("valueQuantity")))
            .withColumn(
                "component",
                transform(
                    col("component"),
                    (component, index) ->
                        component.withField(
                            "valueQuantity.value",
                            when(
                                    col("id").equalTo("o5"),
                                    element_at(
                                        array(lit("1.0000001"), lit("1"), lit("0.1234567")),
                                        index.plus(1)))
                                .otherwise(lit("0.000001")))));
    roundingRead =
        PrunedSchemaReader.write(rounding, tempDir.resolve("rounding-read").toString()).read();
    roundingAnnotated =
        PrunedSchemaReader.write(
                Annotator.of(
                        FhirDefinitionContext.of(fhirEncoders.getContext()),
                        SchemaConfiguration.builder().build())
                    .annotate("Observation", rounding),
                tempDir.resolve("rounding-annotated").toString())
            .read();
    final Dataset<Row> conversion =
        PofDatasets.fromJson(
                spark, fhirEncoders.getContext(), "Observation", CONVERSION_OBSERVATIONS)
            .withColumn(
                "valueQuantity",
                col("valueQuantity")
                    .withField(
                        "value",
                        when(col("id").equalTo("o8"), lit("0.0000005")).otherwise("1.0000001")));
    conversionRead =
        PrunedSchemaReader.write(conversion, tempDir.resolve("conversion-read").toString()).read();
    conversionAnnotated =
        PrunedSchemaReader.write(
                Annotator.of(
                        FhirDefinitionContext.of(fhirEncoders.getContext()),
                        SchemaConfiguration.builder().build())
                    .annotate("Observation", conversion),
                tempDir.resolve("conversion-annotated").toString())
            .read();
    annotated =
        PrunedSchemaReader.write(
                Annotator.of(
                        FhirDefinitionContext.of(fhirEncoders.getContext()),
                        SchemaConfiguration.builder().build())
                    .annotate("Observation", resources),
                tempDir.resolve("annotated").toString())
            .read();
  }

  @Test
  void theAnnotatedDataCarriesTheAnnotations() {
    final String schema = annotated.schema().simpleString();
    assertThat(schema)
        .contains("__numeric")
        .contains("__canonical_code")
        .contains("__start")
        .contains("__end");
    assertThat(read.schema().simpleString()).doesNotContain("__");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void annotatedAndUnannotatedDataGiveTheSameResults(
      @Nonnull final String expression, @Nonnull final List<String> expected) {
    assertThat(rows(read, expression)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(rows(annotated, expression)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> cases() {
    return Stream.of(
        // Decimals: compared, ordered and deduplicated by their numbers.
        Arguments.of("component.valueQuantity.value.distinct().count()", List.of("o1=3", "o2=0")),
        Arguments.of("component.valueQuantity.value.isDistinct()", List.of("o1=false", "o2=true")),
        Arguments.of(
            "(component.valueQuantity.value | valueQuantity.value).count()",
            List.of("o1=3", "o2=1")),
        Arguments.of(
            "valueQuantity.value > component.valueQuantity.value.first()",
            List.of("o1=true", "o2=null")),
        Arguments.of(
            "component.valueQuantity.value.where($this > 5).count()", List.of("o1=2", "o2=0")),
        Arguments.of(
            "component.valueQuantity.value[1] = component.valueQuantity.value[2]",
            List.of("o1=true", "o2=null")),
        Arguments.of("component.valueQuantity.value contains 1.00", List.of("o1=true", "o2=false")),
        Arguments.of("valueQuantity.value = 10", List.of("o1=true", "o2=false")),
        Arguments.of("(valueQuantity.value + 1).toString()", List.of("o1=11", "o2=2")),
        // A union is stored only if every operand is, whichever side the computed one is on.
        Arguments.of("(valueQuantity.value | 1.5).count()", List.of("o1=2", "o2=2")),
        Arguments.of("(1.5 | valueQuantity.value).count()", List.of("o1=2", "o2=2")),
        Arguments.of("(valueQuantity.value | 10).count()", List.of("o1=1", "o2=2")),
        Arguments.of("(10 | valueQuantity.value).count()", List.of("o1=1", "o2=2")),
        // A stored decimal's string is its stored text; a computed one's is its number.
        Arguments.of("valueQuantity.value.toString()", List.of("o1=10", "o2=1.0000001")),
        Arguments.of("component.valueQuantity.value[2].toString()", List.of("o1=1.00", "o2=null")),
        Arguments.of(
            "(component.valueQuantity.value[1] | component.valueQuantity.value[2]).toString()",
            List.of("o1=1.0", "o2=null")),
        Arguments.of("(valueQuantity.value | 1.5).first().toString()", List.of("o1=10", "o2=1")),
        Arguments.of("(1.5 | valueQuantity.value)[1].toString()", List.of("o1=10", "o2=1")),
        Arguments.of("(1.0 | 1.00).count()", List.of("o1=1", "o2=1")),
        Arguments.of("1.0 = 1.00", List.of("o1=true", "o2=true")),
        // Quantities: compared by their canonical forms, computed from the System.Decimal value
        // where
        // the stored annotation is absent, as ingest computes it (owner, 2026-11-12).
        Arguments.of("valueQuantity > 9 'g'", List.of("o1=true", "o2=false")),
        // The stored "1.0000001 g" is 1 g (owner, 2026-11-12, decision 153).
        Arguments.of("valueQuantity > 1 'g'", List.of("o1=true", "o2=false")),
        Arguments.of("valueQuantity > 1000 'mg'", List.of("o1=true", "o2=false")),
        Arguments.of(
            "component.valueQuantity[0] < component.valueQuantity[3]",
            List.of("o1=true", "o2=null")),
        // The canonical form of a stored quantity is that of its System.Decimal value, so the
        // stored "1.0000001 g" canonicalises to 1 g, whether it is stored or computed, and however
        // the quantity is reached (owner, 2026-11-12).
        Arguments.of("(valueQuantity | 1 'g').count()", List.of("o1=2", "o2=1")),
        Arguments.of("valueQuantity.toQuantity() = 1 'g'", List.of("o1=false", "o2=true")),
        Arguments.of("valueQuantity.toQuantity('g') = 1 'g'", List.of("o1=false", "o2=true")),
        Arguments.of("valueQuantity.toQuantity('mg') = 1000 'mg'", List.of("o1=false", "o2=true")),
        Arguments.of("valueQuantity.value = 1", List.of("o1=false", "o2=true")),
        Arguments.of(
            "component.valueQuantity.where($this > 5 'g').count()", List.of("o1=2", "o2=0")),
        Arguments.of("component.valueQuantity.distinct().count()", List.of("o1=3", "o2=0")),
        Arguments.of("(component.valueQuantity | 10000 'mg').count()", List.of("o1=3", "o2=1")),
        // Dates.
        Arguments.of("effectiveDateTime > @2019", List.of("o1=true", "o2=false")),
        Arguments.of("(effectiveDateTime | @2020-03).count()", List.of("o1=1", "o2=2")));
  }

  /**
   * A stored decimal whose number the engine's System.Decimal cannot hold is absent where it is
   * singular, and a null item under a repeating parent, as decoding its text alone gave before the
   * element held its text; and a stored quantity without a code has no System form, so it does not
   * convert to a unit (FHIR R4 FHIRPath binding, §2.9.1.3).
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("edgeCases")
  void edgeCasesGiveTheSameResultsOverAnnotatedAndUnannotatedData(
      @Nonnull final String expression, @Nonnull final List<String> expected) {
    assertThat(rows(edgeRead, expression)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(rows(edgeAnnotated, expression)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> edgeCases() {
    return Stream.of(
        Arguments.of("valueQuantity.value.exists()", List.of("o3=false", "o4=true")),
        Arguments.of("valueQuantity.value.count()", List.of("o3=0", "o4=1")),
        Arguments.of("valueQuantity.value = 1", List.of("o3=null", "o4=false")),
        Arguments.of("component.valueQuantity.value.count()", List.of("o3=2", "o4=0")),
        Arguments.of("component.valueQuantity.value[0].exists()", List.of("o3=false", "o4=false")),
        Arguments.of(
            "component.valueQuantity.value.where($this > 1).count()", List.of("o3=1", "o4=0")),
        Arguments.of("valueQuantity.where(code.empty()).exists()", List.of("o3=false", "o4=true")),
        Arguments.of(
            "valueQuantity.where(code.empty()).toQuantity('mg').exists()",
            List.of("o3=false", "o4=false")),
        // The null item of a repeating decimal that does not decode, through deduplication,
        // ordering, combining and its string form.
        Arguments.of("component.valueQuantity.value.distinct().count()", List.of("o3=2", "o4=0")),
        Arguments.of("component.valueQuantity.value.isDistinct()", List.of("o3=true", "o4=true")),
        Arguments.of("(component.valueQuantity.value | 2).count()", List.of("o3=2", "o4=1")),
        Arguments.of("component.valueQuantity.value.combine(2).count()", List.of("o3=3", "o4=1")),
        Arguments.of("component.valueQuantity.value[0] < 3", List.of("o3=null", "o4=null")),
        Arguments.of(
            "component.valueQuantity.value[0] < component.valueQuantity.value[1]",
            List.of("o3=null", "o4=null")),
        Arguments.of("component.valueQuantity.value[0].toString()", List.of("o3=null", "o4=null")),
        Arguments.of("component.valueQuantity.value[1].toString()", List.of("o3=2", "o4=null")),
        // A quantity whose value does not decode has no canonical form and no System form, so it
        // is neither compared nor merged as a quantity without a value: it is absent where it is
        // singular, and a null item under a repeating parent, as its value is.
        Arguments.of("valueQuantity > 1 'g'", List.of("o3=null", "o4=null")),
        Arguments.of("(valueQuantity | 1 'g').count()", List.of("o3=1", "o4=2")),
        Arguments.of("component.valueQuantity.distinct().count()", List.of("o3=2", "o4=0")),
        Arguments.of("(component.valueQuantity | 2 'g').count()", List.of("o3=2", "o4=1")),
        Arguments.of(
            "component.valueQuantity.where($this < 3 'g').count()", List.of("o3=1", "o4=0")));
  }

  /**
   * The canonical form of a stored quantity is a function of its System.Decimal value and its unit
   * (owner, 2026-11-12): digits of the stored text beyond those the System.Decimal holds do not
   * take part, a value the System.Decimal cannot hold has no canonical form even where the
   * canonical form would fit, and the exact canonical value still keeps the canonicalisation of a
   * small unit from losing digits.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("roundingCases")
  void theCanonicalFormIsThatOfTheSystemDecimalOverAnnotatedAndUnannotatedData(
      @Nonnull final String expression, @Nonnull final List<String> expected) {
    assertThat(rows(roundingRead, expression)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(rows(roundingAnnotated, expression)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> roundingCases() {
    return Stream.of(
        // "1.0E33 g" does not decode, though its canonical form would fit.
        Arguments.of("valueQuantity > 1 'g'", List.of("o5=null", "o6=null")),
        Arguments.of("(valueQuantity | 1 'g').count()", List.of("o5=1", "o6=2")),
        Arguments.of("valueQuantity.toQuantity().exists()", List.of("o5=false", "o6=true")),
        // Nor does it convert to a unit, even its own.
        Arguments.of("valueQuantity.toQuantity('g').exists()", List.of("o5=false", "o6=false")),
        Arguments.of("valueQuantity.convertsToQuantity('g')", List.of("o5=false", "o6=false")),
        Arguments.of("valueQuantity.convertsToQuantity()", List.of("o5=false", "o6=true")),
        Arguments.of("valueQuantity.toQuantity('ps').exists()", List.of("o5=false", "o6=true")),
        Arguments.of("valueQuantity.toString()", List.of("o5=null", "o6=1 'ns'")),
        Arguments.of(
            "component.valueQuantity[0].toString()",
            List.of("o5=1.0000001 'g'", "o6=0.000001 'ns'")),
        Arguments.of("valueQuantity.toQuantity('kg').exists()", List.of("o5=false", "o6=false")),
        // "1.0000001 g" and "1 g" are one System quantity, and "0.1234567 g" is 0.123457 g, so a
        // union keeps one more item than the distinct quantities, whether or not the merge keeps
        // the canonical forms.
        Arguments.of("component.valueQuantity.distinct().count()", List.of("o5=2", "o6=1")),
        Arguments.of("(component.valueQuantity | 1 month).count()", List.of("o5=3", "o6=2")),
        Arguments.of("(component.valueQuantity | 1 'g').count()", List.of("o5=2", "o6=2")),
        Arguments.of("component.valueQuantity[2] = 0.123457 'g'", List.of("o5=true", "o6=null")),
        // "1 ns" canonicalises to 0.000000001 s, and "0.000001 ns" to 0.000000000000001 s, beyond
        // the scale of the System.Decimal.
        Arguments.of("valueQuantity = 1000 'ps'", List.of("o5=null", "o6=true")),
        // A literal is canonicalised from its System.Decimal value too, so 0.000000001 's' is 0 s:
        // a known limitation of the six fractional digits, though 1 'ns' is exactly 1e-9 s.
        Arguments.of("valueQuantity = 0.000000001 's'", List.of("o5=null", "o6=false")),
        Arguments.of("1 'ns' = 0.000000001 's'", List.of("o5=false", "o6=false")),
        Arguments.of("0.000000001 's' = 0 's'", List.of("o5=true", "o6=true")),
        Arguments.of("valueQuantity < 1 'us'", List.of("o5=null", "o6=true")),
        Arguments.of("component.valueQuantity.first() > 0 's'", List.of("o5=null", "o6=true")),
        Arguments.of("component.valueQuantity.first() = 1 'fs'", List.of("o5=null", "o6=true")));
  }

  /**
   * A unit conversion computes from the System.Decimal value, as every computation does (owner,
   * 2026-11-12, decision 153): the stored 0.0000005 g of o8 is 0.000001 g, and the stored 1.0000001
   * g of o9 is 1 g, before either is converted.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("conversionCases")
  void aUnitConversionComputesFromTheSystemDecimalOverAnnotatedAndUnannotatedData(
      @Nonnull final String expression, @Nonnull final List<String> expected) {
    assertThat(rows(conversionRead, expression)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(rows(conversionAnnotated, expression)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> conversionCases() {
    return Stream.of(
        Arguments.of(
            "valueQuantity.toQuantity('mg') = valueQuantity", List.of("o8=true", "o9=true")),
        Arguments.of("valueQuantity.toQuantity('mg') = 0.001 'mg'", List.of("o8=true", "o9=false")),
        Arguments.of("valueQuantity.toQuantity('mg') = 1000 'mg'", List.of("o8=false", "o9=true")),
        Arguments.of("valueQuantity.convertsToQuantity('mg')", List.of("o8=true", "o9=true")));
  }

  /** The value of an expression for each Observation, as {@code id=value}. */
  @Nonnull
  private List<String> rows(@Nonnull final Dataset<Row> data, @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(ResourceType.OBSERVATION, fhirEncoders.getContext())
            .withDataset(data)
            .build();
    return evaluator.evaluate(PARSER.parse(expression)).toIdValueDataset().collectAsList().stream()
        .map(row -> row.getString(0) + "=" + row.get(1))
        .toList();
  }
}
