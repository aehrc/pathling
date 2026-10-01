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
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
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
 * Tests that a quantity search matches against the value as it is stored, so that on the new layout
 * a value with more than six fractional digits keeps them through canonicalisation (decision 77).
 *
 * <p>The engine decodes a quantity to a {@code DECIMAL(32,6)} value for computation, which keeps
 * six fractional digits. The new layout stores the source text, so {@code 0.0000002 kg}, which is
 * {@code 0.2 mg}, is found by a search in either unit. The previous layout's value column holds six
 * digits, so there the value is zero and the search finds nothing, as decision 77 accepts. A value
 * with six digits or fewer is found on both layouts.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class QuantitySearchPrecisionTest {

  private static final String UCUM = "http://unitsofmeasure.org";

  private static final List<String> OBSERVATIONS =
      List.of(observation("q1", "1.5", "g"), observation("q2", "0.0000002", "kg"));

  private final SearchParameterRegistry registry =
      SearchParameterRegistry.fromSearchParameters(List.of(quantityParameter()));

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  @TempDir static Path tempDir;

  private Map<TestLayout, Dataset<Row>> datasets;

  @BeforeAll
  void setUp() {
    datasets =
        Map.of(
            TestLayout.PREVIOUS, stored(TestLayout.PREVIOUS),
            TestLayout.POF, stored(TestLayout.POF));
  }

  @Nonnull
  Stream<Arguments> cases() {
    return Stream.of(
        // A value with more than six fractional digits, found in its own unit and in others.
        arguments(TestLayout.POF, "eq0.0000002|" + UCUM + "|kg", List.of("q2")),
        arguments(TestLayout.POF, "eq2e-7|" + UCUM + "|kg", List.of("q2")),
        arguments(TestLayout.POF, "eq0.2|" + UCUM + "|mg", List.of("q2")),
        arguments(TestLayout.POF, "eq200|" + UCUM + "|ug", List.of("q2")),
        arguments(TestLayout.POF, "gt0.1|" + UCUM + "|mg", List.of("q1", "q2")),
        arguments(TestLayout.POF, "lt0.3|" + UCUM + "|mg", List.of("q2")),
        // The previous layout keeps six digits, so the value is zero there.
        arguments(TestLayout.PREVIOUS, "eq0.2|" + UCUM + "|mg", List.of()),
        arguments(TestLayout.PREVIOUS, "gt0.1|" + UCUM + "|mg", List.of("q1")),
        // A value with six digits or fewer is found on both layouts.
        arguments(TestLayout.POF, "eq1500|" + UCUM + "|mg", List.of("q1")),
        arguments(TestLayout.PREVIOUS, "eq1500|" + UCUM + "|mg", List.of("q1")),
        arguments(TestLayout.POF, "eq1.5|" + UCUM + "|g", List.of("q1")),
        arguments(TestLayout.PREVIOUS, "eq1.5|" + UCUM + "|g", List.of("q1")));
  }

  @ParameterizedTest(name = "value-quantity={1} over the {0} layout")
  @MethodSource("cases")
  void searchMatchesTheStoredValue(
      @Nonnull final TestLayout layout,
      @Nonnull final String searchValue,
      @Nonnull final List<String> expectedIds) {
    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(
            encoders.getContext(),
            new DatasetDataSource(Map.of("Observation", datasets.get(layout))),
            registry);
    final Dataset<Row> results =
        executor.execute(
            ResourceType.OBSERVATION,
            FhirSearch.builder().criterion("value-quantity", searchValue).build());
    assertThat(results.select("id").collectAsList().stream().map(row -> row.getString(0)))
        .containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  /**
   * Builds the fixtures in a layout, and writes them to Parquet and reads them back, so that the
   * search reads stored files.
   */
  @Nonnull
  private Dataset<Row> stored(@Nonnull final TestLayout layout) {
    final Dataset<Row> dataset =
        LayoutDatasets.fromJson(spark, encoders, layout, "Observation", OBSERVATIONS);
    return PrunedSchemaReader.write(dataset, tempDir.resolve(layout + "-Observation").toString())
        .read();
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
