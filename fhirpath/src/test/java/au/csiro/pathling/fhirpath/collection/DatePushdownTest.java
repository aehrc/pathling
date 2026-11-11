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

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.Annotator;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.search.SearchColumnBuilder;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Tests where a date comparison over annotated Parquet pushes a filter on the stored bound to the
 * scan (decision 144), beyond the root date of {@code AnnotationPlanBaselineTest}: a date under a
 * singular parent, a choice, and an extension.
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class DatePushdownTest {

  private static final String URL = "http://example.org/date";

  private static final List<String> ENCOUNTERS =
      List.of(
          "{\"resourceType\":\"Encounter\",\"id\":\"e1\",\"status\":\"finished\","
              + "\"class\":{\"code\":\"AMB\"},\"period\":{\"start\":\"2021-06-01\"}}",
          "{\"resourceType\":\"Encounter\",\"id\":\"e2\",\"status\":\"finished\","
              + "\"class\":{\"code\":\"AMB\"},\"period\":{\"start\":\"1999-06-01\"}}");

  private static final List<String> OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"effectiveDateTime\":\"2021-06-01T10:00:00Z\"}",
          "{\"resourceType\":\"Observation\",\"id\":\"o2\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"effectiveDateTime\":\"1999-06-01T10:00:00Z\"}");

  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"extension\":[{\"url\":\""
              + URL
              + "\",\"valueDate\":\"2021-06-01\"}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"extension\":[{\"url\":\""
              + URL
              + "\",\"valueDate\":\"1999-06-01\"}]}");

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Dataset<Row> encounters;

  private Dataset<Row> observations;

  private Dataset<Row> patients;

  @BeforeAll
  void setUp() {
    encounters = annotated("Encounter", ENCOUNTERS);
    observations = annotated("Observation", OBSERVATIONS);
    patients = annotated("Patient", PATIENTS);
  }

  @Test
  void aDateUnderASingularParentPushesAFilterOnItsStoredBound() {
    final Dataset<Row> query = filter(encounters, ResourceType.ENCOUNTER, "period.start >= @2000");
    assertThat(pushedFilters(query)).contains("GreaterThanOrEqual(period._start.__end,");
    assertThat(ids(query)).containsExactly("e1");
  }

  @Test
  void aChoiceDatePushesAFilterOnItsStoredBound() {
    final Dataset<Row> query =
        filter(observations, ResourceType.OBSERVATION, "effective.ofType(dateTime) >= @2000");
    assertThat(pushedFilters(query)).contains("GreaterThanOrEqual(_effectiveDateTime.__end,");
    assertThat(ids(query)).containsExactly("o1");
  }

  /**
   * An extension repeats, so its date is compared inside a lambda over the extensions, which
   * Parquet cannot test: no filter on the stored bound is pushed, and the result is the same.
   */
  @Test
  void aDateInAnExtensionIsComparedInsideALambdaAndPushesNothing() {
    final Dataset<Row> query =
        filter(
            patients,
            ResourceType.PATIENT,
            "extension('" + URL + "').value.ofType(date).where($this >= @2000).exists()");
    assertThat(pushedFilters(query)).doesNotContain("__end").doesNotContain("__start");
    assertThat(ids(query)).containsExactly("p1");
  }

  @Nonnull
  private Dataset<Row> filter(
      @Nonnull final Dataset<Row> data,
      @Nonnull final ResourceType resourceType,
      @Nonnull final String expression) {
    return data.filter(
        SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
            .fromExpression(resourceType, expression));
  }

  @Nonnull
  private static List<String> ids(@Nonnull final Dataset<Row> query) {
    return query.select("id").collectAsList().stream().map(row -> row.getString(0)).toList();
  }

  @Nonnull
  private static String pushedFilters(@Nonnull final Dataset<Row> query) {
    return CollectionConverters.asJava(query.queryExecution().executedPlan().collectLeaves())
        .stream()
        .filter(FileSourceScanExec.class::isInstance)
        .map(FileSourceScanExec.class::cast)
        .findFirst()
        .orElseThrow()
        .metadata()
        .get("PushedFilters")
        .getOrElse(() -> "");
  }

  /** The resources annotated with every annotation on, written to Parquet and read back. */
  @Nonnull
  private Dataset<Row> annotated(
      @Nonnull final String resourceType, @Nonnull final List<String> json) {
    final Dataset<Row> resources =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), resourceType, json);
    return PrunedSchemaReader.write(
            Annotator.of(
                    FhirDefinitionContext.of(fhirEncoders.getContext()),
                    SchemaConfiguration.builder().build())
                .annotate(resourceType, resources),
            tempDir.resolve(resourceType).toString())
        .read();
  }
}
