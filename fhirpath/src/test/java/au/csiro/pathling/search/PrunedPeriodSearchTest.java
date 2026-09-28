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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Coverage;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Period;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that a date search over a Period tolerates a schema that does not carry the Period's start
 * or its end (FR-024, FR-054).
 *
 * <p>A pruned schema carries only the elements the data populates, so a Period whose bounds are all
 * open at one end has no field for that bound. An absent bound is open, as a null one is.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class PrunedPeriodSearchTest {

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  @TempDir static Path tempDir;

  private final SearchParameterRegistry registry = new TestSearchParameterRegistry();

  private Map<String, Dataset<Row>> datasets;

  @BeforeAll
  void setUp() {
    // Every Period in the first set is open at its end, and every one in the second at its start.
    final Coverage openEnded = coverage("1", "2024-01-01", null);
    final Coverage openStarted = coverage("2", null, "2022-12-31");
    final Coverage noPeriod = coverage("3", null, null);
    datasets =
        Map.of(
            "withoutEnd",
            PrunedSchemaReader.write(
                    encode(List.of(openEnded, noPeriod)), tempDir.resolve("end").toString())
                .readWithout("period.end"),
            "withoutStart",
            PrunedSchemaReader.write(
                    encode(List.of(openStarted, noPeriod)), tempDir.resolve("start").toString())
                .readWithout("period.start"));
  }

  @Nonnull
  Stream<Arguments> cases() {
    return Stream.of(
        arguments("withoutEnd", "ge2024-01-01", List.of("1")),
        arguments("withoutEnd", "gt2030-01-01", List.of("1")),
        arguments("withoutEnd", "2025-06-01", List.of("1")),
        arguments("withoutEnd", "lt2023-01-01", List.of()),
        arguments("withoutStart", "lt2022-06-01", List.of("2")),
        arguments("withoutStart", "le2022-12-31", List.of("2")),
        arguments("withoutStart", "2000-01-01", List.of("2")),
        arguments("withoutStart", "gt2023-01-01", List.of()));
  }

  @ParameterizedTest(name = "period={1} over a Period {0}")
  @MethodSource("cases")
  void periodSearchToleratesAnAbsentBound(
      @Nonnull final String dataset,
      @Nonnull final String searchValue,
      @Nonnull final List<String> expectedIds) {
    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(
            encoders.getContext(),
            new DatasetDataSource(Map.of("Coverage", datasets.get(dataset))),
            registry);
    final Dataset<Row> results =
        executor.execute(
            ResourceType.COVERAGE, FhirSearch.builder().criterion("period", searchValue).build());
    assertThat(results.select("id").collectAsList().stream().map(row -> row.getString(0)))
        .containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  @Nonnull
  private Dataset<Row> encode(@Nonnull final List<Coverage> resources) {
    return spark
        .createDataset(List.<IBaseResource>copyOf(resources), encoders.of("Coverage"))
        .toDF();
  }

  @Nonnull
  private static Coverage coverage(
      @Nonnull final String id, @Nullable final String start, @Nullable final String end) {
    final Coverage coverage = new Coverage();
    coverage.setId(id);
    if (start != null || end != null) {
      final Period period = new Period();
      if (start != null) {
        period.setStartElement(new DateTimeType(start));
      }
      if (end != null) {
        period.setEndElement(new DateTimeType(end));
      }
      coverage.setPeriod(period);
    }
    return coverage;
  }
}
