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
 * Tests that a quantity search with a UCUM system matches the implicit range of the search value
 * under {@code eq} and {@code ne}, against stored quantities, as a search without a UCUM system
 * does (https://hl7.org/fhir/R4/search.html#quantity). So {@code 5.4|http://unitsofmeasure.org|mg}
 * is the range [5.35, 5.45) mg, carried into the canonical unit, and matches a stored {@code 5.41
 * mg} and {@code 0.00541 g}.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class UcumQuantityRangeSearchTest {

  private static final String UCUM = "http://unitsofmeasure.org";

  /**
   * The value of {@code huge}, 10^27 Tg, is held by the System.Decimal, but its canonical value,
   * 10^39 g, has too many digits to have an exact form, so a search falls back to its own unit for
   * it.
   */
  private static final String HUGE_VALUE = "1000000000000000000000000000";

  private static final List<String> OBSERVATIONS =
      List.of(
          observation("exact", "5.4", "mg"),
          observation("within", "5.41", "mg"),
          observation("lower", "5.35", "mg"),
          observation("upper", "5.45", "mg"),
          observation("grams", "0.00541", "g"),
          observation("teragrams", "5.41", "Tg"),
          observation("huge", HUGE_VALUE, "Tg"));

  private final SearchParameterRegistry registry =
      SearchParameterRegistry.fromSearchParameters(List.of(quantityParameter()));

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  @TempDir static Path tempDir;

  private Dataset<Row> dataset;

  @BeforeAll
  void setUp() {
    dataset = stored();
  }

  @Nonnull
  Stream<Arguments> cases() {
    return Stream.of(
        // The range of 5.4 mg is [5.35, 5.45) mg, in any unit of mass.
        arguments("5.4|" + UCUM + "|mg", List.of("exact", "within", "lower", "grams")),
        // The other unit of the fallback row, Tg, keeps it out of a search in mg.
        arguments("ne5.4|" + UCUM + "|mg", List.of("upper", "teragrams")),
        // The same range written in grams.
        arguments("0.0054|" + UCUM + "|g", List.of("exact", "within", "lower", "grams")),
        // Trailing zeros narrow the range to [5.395, 5.405) mg.
        arguments("5.40|" + UCUM + "|mg", List.of("exact")),
        arguments(
            "ne5.40|" + UCUM + "|mg", List.of("within", "lower", "upper", "grams", "teragrams")),
        // Comparison prefixes compare with the search value itself, not its range.
        arguments("gt5.4|" + UCUM + "|mg", List.of("within", "upper", "grams", "teragrams")),
        arguments("le5.4|" + UCUM + "|mg", List.of("exact", "lower")),
        // A search value beyond the range of the System.Decimal matches nothing, under ne as under
        // eq.
        arguments("1e33|" + UCUM + "|mg", List.of()),
        arguments("ne1e33|" + UCUM + "|mg", List.of()),
        // A quantity with no canonical form is matched by the range in its own unit, so one search
        // compares canonical ranges for some rows and falls back for others, by the same rule.
        arguments("5.4|" + UCUM + "|Tg", List.of("teragrams")),
        arguments(
            "ne5.4|" + UCUM + "|Tg", List.of("exact", "within", "lower", "upper", "grams", "huge")),
        // A search value whose own canonical form has no exact form falls back to its own unit.
        arguments(HUGE_VALUE + ".0|" + UCUM + "|Tg", List.of("huge")),
        arguments("ne" + HUGE_VALUE + ".0|" + UCUM + "|Tg", List.of("teragrams")));
  }

  @ParameterizedTest(name = "value-quantity={0}")
  @MethodSource("cases")
  void ucumSearchMatchesTheImplicitRange(
      @Nonnull final String searchValue, @Nonnull final List<String> expectedIds) {
    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(
            encoders.getContext(), new DatasetDataSource(Map.of("Observation", dataset)), registry);
    final Dataset<Row> results =
        executor.execute(
            ResourceType.OBSERVATION,
            FhirSearch.builder().criterion("value-quantity", searchValue).build());
    assertThat(results.select("id").collectAsList().stream().map(row -> row.getString(0)))
        .containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  /**
   * Builds the fixtures, and writes them to Parquet and reads them back, so that the search reads
   * stored files.
   */
  @Nonnull
  private Dataset<Row> stored() {
    final Dataset<Row> observations =
        PofDatasets.fromJson(spark, encoders.getContext(), "Observation", OBSERVATIONS);
    return PrunedSchemaReader.write(observations, tempDir.resolve("Observation").toString()).read();
  }

  @Nonnull
  private static SearchParameter quantityParameter() {
    final SearchParameter parameter = new SearchParameter();
    parameter.setCode("value-quantity");
    parameter.setType(SearchParamType.QUANTITY);
    parameter.addBase("Observation");
    parameter.setExpression("Observation.value.ofType(Quantity)");
    return parameter;
  }

  @Nonnull
  private static String observation(
      @Nonnull final String id, @Nonnull final String value, @Nonnull final String code) {
    return "{\"resourceType\":\"Observation\",\"id\":\""
        + id
        + "\",\"status\":\"final\",\"code\":{\"text\":\"x\"},\"valueQuantity\":{\"value\":"
        + value
        + ",\"unit\":\""
        + code
        + "\",\"system\":\""
        + UCUM
        + "\",\"code\":\""
        + code
        + "\"}}";
  }
}
