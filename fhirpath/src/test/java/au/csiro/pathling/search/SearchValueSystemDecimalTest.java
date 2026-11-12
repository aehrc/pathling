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

package au.csiro.pathling.search;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.DatasetDataSource;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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
 * Tests that a number or quantity search takes its search value as the engine's System.Decimal
 * holds it, as it takes the element (owner, 2026-11-12, decision 153): a value with more than six
 * fractional digits is rounded to six, its implicit range is that of the rounded value, and a value
 * outside the System.Decimal's range is not available, so it matches nothing, under {@code ne} too.
 * A search on an integer element rounds its search value too, before it applies the integer rule.
 *
 * <p>The fixtures include a value with the System.Decimal's 32 integer digits and six fractional
 * digits, so that a comparison with a search value, or with the boundary of its implicit range,
 * that Spark widened beyond the System.Decimal's scale would lose it.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class SearchValueSystemDecimalTest {

  private static final String UCUM = "http://unitsofmeasure.org";

  /** A value with as many integer and fractional digits as the System.Decimal holds. */
  private static final String WIDEST = "12345678901234567890123456789012.123456";

  private static final List<String> OBSERVATIONS =
      List.of(
          observation("d1", "1.5"),
          observation("d2", "1"),
          observation("d3", "0"),
          observation("d4", "1"),
          observation("d5", "1"));

  private final SearchParameterRegistry registry =
      SearchParameterRegistry.fromSearchParameters(
          List.of(
              parameter("value-number", SearchParamType.NUMBER, "Observation.valueQuantity.value"),
              parameter(
                  "value-quantity", SearchParamType.QUANTITY, "Observation.value.ofType(Quantity)"),
              parameter(
                  "value-integer", SearchParamType.NUMBER, "Observation.value.ofType(integer)"),
              parameter(
                  "component-number",
                  SearchParamType.NUMBER,
                  "Observation.component.valueQuantity.value")));

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  @TempDir static Path tempDir;

  private Dataset<Row> dataset;

  private Dataset<Row> integers;

  private Dataset<Row> edges;

  private Dataset<Row> components;

  @BeforeAll
  void setUp() {
    // The stored texts are set directly, as JSON ingest would store them through a double. d2 has
    // more fractional digits than the System.Decimal holds, and is 0 there; d5 has more integer
    // digits, and has no value there.
    final Column text =
        when(col("id").equalTo("d2"), lit("0.0000002"))
            .when(col("id").equalTo("d4"), lit(WIDEST))
            .when(col("id").equalTo("d5"), lit("1.0E33"))
            .otherwise(col("valueQuantity.value"));
    final Dataset<Row> observations =
        PofDatasets.fromJson(spark, encoders.getContext(), "Observation", OBSERVATIONS)
            .withColumn("valueQuantity", col("valueQuantity").withField("value", text));
    dataset =
        PrunedSchemaReader.write(observations, tempDir.resolve("Observation").toString()).read();
    integers =
        PrunedSchemaReader.write(
                PofDatasets.fromJson(
                    spark,
                    encoders.getContext(),
                    "Observation",
                    List.of(
                        integerObservation("i1", 1),
                        integerObservation("i2", 2),
                        integerObservation("i3", 10),
                        integerObservation("i4", 14))),
                tempDir.resolve("integers").toString())
            .read();
    // e1 is the largest value the System.Decimal holds, and e2 is close below a power of ten.
    final Column edgeText =
        when(col("id").equalTo("e1"), lit("99999999999999999999999999999999.999999"))
            .when(col("id").equalTo("e2"), lit("999999999999999.999999"))
            .when(col("id").equalTo("e3"), lit("1000000000000000"))
            .otherwise(lit("1000"));
    edges =
        PrunedSchemaReader.write(
                PofDatasets.fromJson(
                        spark,
                        encoders.getContext(),
                        "Observation",
                        List.of(
                            observation("e1", "1"),
                            observation("e2", "1"),
                            observation("e3", "1"),
                            observation("e4", "1")))
                    .withColumn("valueQuantity", col("valueQuantity").withField("value", edgeText)),
                tempDir.resolve("edges").toString())
            .read();
    components =
        PrunedSchemaReader.write(
                PofDatasets.fromJson(
                    spark,
                    encoders.getContext(),
                    "Observation",
                    List.of(
                        componentObservation("c1", "1.0000001", "5"),
                        componentObservation("c2", "3"))),
                tempDir.resolve("components").toString())
            .read();
  }

  @Nonnull
  Stream<Arguments> numberCases() {
    return Stream.of(
        // More than six fractional digits: rounded, with the range of the rounded value.
        arguments("eq0.0000002", List.of("d2", "d3")),
        arguments("eq2e-7", List.of("d2", "d3")),
        arguments("ne0.0000002", List.of("d1", "d4")),
        arguments("gt0.0000002", List.of("d1", "d4")),
        arguments("ge0.0000004", List.of("d1", "d2", "d3", "d4")),
        arguments("lt0.0000006", List.of("d2", "d3")),
        arguments("le0.0000004", List.of("d2", "d3")),
        // Six fractional digits or fewer: unchanged.
        arguments("eq0", List.of("d2", "d3")),
        arguments("eq1.5", List.of("d1")),
        // Outside the System.Decimal's range: not available, so nothing matches.
        arguments("eq1E40", List.of()),
        arguments("ne1E40", List.of()),
        arguments("gt1E40", List.of()),
        arguments("lt1E40", List.of()),
        // The widest value, against a search value and range boundaries of its own scale and more.
        arguments("eq" + WIDEST, List.of("d4")),
        arguments("eq" + WIDEST + "4", List.of("d4")),
        arguments("ne" + WIDEST, List.of("d1", "d2", "d3")),
        arguments("ge" + WIDEST, List.of("d4")),
        arguments("gt12345678901234567890123456789012.1234554", List.of("d4")),
        arguments("lt" + WIDEST, List.of("d1", "d2", "d3")));
  }

  @ParameterizedTest(name = "value-number={0}")
  @MethodSource("numberCases")
  void numberSearchTakesTheSearchValueAsTheSystemDecimal(
      @Nonnull final String searchValue, @Nonnull final List<String> expectedIds) {
    assertThat(search("value-number", searchValue))
        .containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  @Nonnull
  Stream<Arguments> quantityCases() {
    return Stream.of(
        // Canonicalised from the rounded value: 0.0000002 g is 0 g, as d2 is.
        arguments("eq0.0000002|" + UCUM + "|g", List.of("d2", "d3")),
        // The value is rounded as it is written, before it is canonicalised, so 0.0002 mg is
        // exactly 0.0000002 g, which finds no 0 g.
        arguments("eq0.0002|" + UCUM + "|mg", List.of()),
        arguments("gt0.0000002|" + UCUM + "|g", List.of("d1", "d4")),
        // Without a system, the value is matched with the range of the rounded value.
        arguments("eq0.0000002||g", List.of("d2", "d3")),
        arguments("lt0.0000006||g", List.of("d2", "d3")),
        // Outside the System.Decimal's range: not available, so nothing matches.
        arguments("eq1E40|" + UCUM + "|g", List.of()),
        arguments("ne1E40|" + UCUM + "|g", List.of()),
        arguments("gt1E40||g", List.of()),
        arguments("ne1E40||g", List.of()),
        // The widest value, canonicalised and not.
        arguments("eq" + WIDEST + "|" + UCUM + "|g", List.of("d4")),
        arguments("eq" + WIDEST + "||g", List.of("d4")),
        arguments("ge" + WIDEST + "4||g", List.of("d4")));
  }

  @ParameterizedTest(name = "value-quantity={0}")
  @MethodSource("quantityCases")
  void quantitySearchTakesTheSearchValueAsTheSystemDecimal(
      @Nonnull final String searchValue, @Nonnull final List<String> expectedIds) {
    assertThat(search("value-quantity", searchValue))
        .containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  @Nonnull
  Stream<Arguments> integerCases() {
    return Stream.of(
        arguments("eq1", List.of("i1")),
        // The integer rule is exact: the implicit range of 1e1, [5, 15), would find 14 too.
        arguments("eq1e1", List.of("i3")),
        // Rounded to six fractional digits first: 1.0000001 is 1, and 1.0000005 is 1.000001, which
        // no integer equals.
        arguments("eq1.0000001", List.of("i1")),
        arguments("ne1.0000001", List.of("i2", "i3", "i4")),
        arguments("eq1.0000005", List.of()),
        arguments("ne1.0000005", List.of("i1", "i2", "i3", "i4")),
        arguments("gt1.0000001", List.of("i2", "i3", "i4")),
        // At a rounding boundary: 2.0000004 and 1.9999996 are both 2.
        arguments("ge2.0000004", List.of("i2", "i3", "i4")),
        arguments("le1.9999996", List.of("i1", "i2")),
        // Outside the System.Decimal's range: not available, so nothing matches.
        arguments("eq1E40", List.of()),
        arguments("ne1E40", List.of()),
        arguments("lt1E40", List.of()));
  }

  @ParameterizedTest(name = "value-integer={0}")
  @MethodSource("integerCases")
  void integerSearchTakesTheSearchValueAsTheSystemDecimal(
      @Nonnull final String searchValue, @Nonnull final List<String> expectedIds) {
    assertThat(search(integers, "value-integer", searchValue))
        .containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  @Nonnull
  Stream<Arguments> edgeCases() {
    return Stream.of(
        // The largest value: its range's upper boundary, 10^32, is beyond the System.Decimal, so
        // it bounds nothing.
        arguments("eq99999999999999999999999999999999.999999", List.of("e1")),
        arguments("ne99999999999999999999999999999999.999999", List.of("e2", "e3", "e4")),
        arguments("eq99999999999999999999999999999999.99999", List.of()),
        arguments("ge99999999999999999999999999999999.99999", List.of("e1")),
        // Close below a power of ten: the range is [999999999999999.9999985, 1E15 + 0.0000005),
        // which does not reach 1E15.
        arguments("eq999999999999999.999999", List.of("e2")),
        arguments("eq1000000000000000", List.of("e2", "e3")),
        // A search value with a negative scale.
        arguments("gt1E3", List.of("e1", "e2", "e3")),
        arguments("ge1E3", List.of("e1", "e2", "e3", "e4")),
        arguments("eq1E3", List.of("e4")));
  }

  @ParameterizedTest(name = "value-number={0}")
  @MethodSource("edgeCases")
  void numberSearchMatchesAtTheEdgesOfTheSystemDecimal(
      @Nonnull final String searchValue, @Nonnull final List<String> expectedIds) {
    assertThat(search(edges, "value-number", searchValue))
        .containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  @Nonnull
  Stream<Arguments> componentCases() {
    return Stream.of(
        arguments("eq1", List.of("c1")),
        arguments("eq1.0000001", List.of("c1")),
        arguments("gt4", List.of("c1")),
        arguments("eq3", List.of("c2")),
        arguments("lt1.0000004", List.of()),
        arguments("eq1E40", List.of()));
  }

  @ParameterizedTest(name = "component-number={0}")
  @MethodSource("componentCases")
  void numberSearchOverARepeatingDecimalTakesTheSearchValueAsTheSystemDecimal(
      @Nonnull final String searchValue, @Nonnull final List<String> expectedIds) {
    assertThat(search(components, "component-number", searchValue))
        .containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  /** The ids of the Observations a search finds. */
  @Nonnull
  private List<String> search(@Nonnull final String code, @Nonnull final String searchValue) {
    return search(dataset, code, searchValue);
  }

  /** The ids of the Observations in some data that a search finds. */
  @Nonnull
  private List<String> search(
      @Nonnull final Dataset<Row> data,
      @Nonnull final String code,
      @Nonnull final String searchValue) {
    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(
            encoders.getContext(), new DatasetDataSource(Map.of("Observation", data)), registry);
    return executor
        .execute(
            ResourceType.OBSERVATION, FhirSearch.builder().criterion(code, searchValue).build())
        .select("id")
        .collectAsList()
        .stream()
        .map(row -> row.getString(0))
        .toList();
  }

  @Nonnull
  private static SearchParameter parameter(
      @Nonnull final String code,
      @Nonnull final SearchParamType type,
      @Nonnull final String expression) {
    final SearchParameter parameter = new SearchParameter();
    parameter.setCode(code);
    parameter.setType(type);
    parameter.addBase("Observation");
    parameter.setExpression(expression);
    return parameter;
  }

  @Nonnull
  private static String componentObservation(
      @Nonnull final String id, @Nonnull final String... values) {
    return "{\"resourceType\":\"Observation\",\"id\":\""
        + id
        + "\",\"status\":\"final\",\"code\":{\"text\":\"x\"},\"component\":["
        + String.join(
            ",",
            Stream.of(values)
                .map(
                    value ->
                        "{\"code\":{\"text\":\"c\"},\"valueQuantity\":{\"value\":" + value + "}}")
                .toList())
        + "]}";
  }

  @Nonnull
  private static String integerObservation(@Nonnull final String id, final int value) {
    return "{\"resourceType\":\"Observation\",\"id\":\""
        + id
        + "\",\"status\":\"final\",\"code\":{\"text\":\"x\"},\"valueInteger\":"
        + value
        + "}";
  }

  @Nonnull
  private static String observation(@Nonnull final String id, @Nonnull final String value) {
    return "{\"resourceType\":\"Observation\",\"id\":\""
        + id
        + "\",\"status\":\"final\",\"code\":{\"text\":\"x\"},\"valueQuantity\":{\"value\":"
        + value
        + ",\"unit\":\"g\",\"system\":\""
        + UCUM
        + "\",\"code\":\"g\"}}";
  }
}
