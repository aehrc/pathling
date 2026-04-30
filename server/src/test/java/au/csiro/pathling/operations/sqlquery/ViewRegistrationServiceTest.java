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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.config.ServerConfiguration;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * Tests for {@link ViewRegistrationService}, with particular attention to the request-id
 * namespacing that prevents concurrent {@code $sqlquery-run} requests from clobbering one another's
 * temporary views in Spark's session-global catalog.
 */
@TestInstance(Lifecycle.PER_CLASS)
class ViewRegistrationServiceTest {

  private SparkSession spark;
  private ViewRegistrationService service;

  @BeforeAll
  void setUp() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("ViewRegistrationServiceTest")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", false)
            .config("spark.sql.shuffle.partitions", 1)
            .getOrCreate();
    service = new ViewRegistrationService(spark, FhirContext.forR4(), new ServerConfiguration());
  }

  @AfterAll
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  // ---------------------------------------------------------------------------
  // Temp view name resolution.
  // ---------------------------------------------------------------------------

  @Test
  void resolveTempViewNamePrefixesRequestIdAndLabel() {
    assertThat(ViewRegistrationService.resolveTempViewName("abc123", "patients"))
        .isEqualTo("sqlquery_abc123_patients");
  }

  @Test
  void resolveTempViewNameStripsUnsafeCharactersFromRequestId() {
    // X-Request-ID may carry arbitrary characters; the resulting identifier must remain a
    // legal Spark temp view name.
    assertThat(ViewRegistrationService.resolveTempViewName("req-A:b/c", "patients"))
        .isEqualTo("sqlquery_reqAbc_patients");
  }

  @Test
  void resolveTempViewNameProducesDistinctNamesForDistinctRequestIds() {
    final String a = ViewRegistrationService.resolveTempViewName("requestA", "patients");
    final String b = ViewRegistrationService.resolveTempViewName("requestB", "patients");
    assertThat(a).isNotEqualTo(b);
  }

  // ---------------------------------------------------------------------------
  // SQL rewriting.
  // ---------------------------------------------------------------------------

  @Test
  void rewriteSqlSubstitutesLabelsWithViewNames() {
    final String rewritten =
        service.rewriteSql("SELECT * FROM patients", Map.of("patients", "sqlquery_req1_patients"));
    assertThat(rewritten).isEqualTo("SELECT * FROM sqlquery_req1_patients");
  }

  @Test
  void rewriteSqlMatchesOnWordBoundariesOnly() {
    // The label "patients" should not be substituted inside the unrelated identifier
    // "patients_archive".
    final String rewritten =
        service.rewriteSql(
            "SELECT * FROM patients JOIN patients_archive ON patients.id = patients_archive.id",
            Map.of("patients", "sqlquery_req1_patients"));
    assertThat(rewritten).contains("FROM sqlquery_req1_patients JOIN patients_archive");
    assertThat(rewritten).contains("ON sqlquery_req1_patients.id = patients_archive.id");
  }

  @Test
  void rewriteSqlPrefersLongerLabelsToAvoidPartialReplacement() {
    final String rewritten =
        service.rewriteSql(
            "SELECT * FROM obs JOIN obs_summary ON obs.id = obs_summary.id",
            Map.of("obs", "sqlquery_req1_obs", "obs_summary", "sqlquery_req1_obs_summary"));
    // The longer label is rewritten cleanly; the shorter label only matches the bare token.
    assertThat(rewritten).contains("FROM sqlquery_req1_obs ");
    assertThat(rewritten).contains("JOIN sqlquery_req1_obs_summary ");
  }

  // ---------------------------------------------------------------------------
  // Concurrent registration regression.
  // ---------------------------------------------------------------------------

  /**
   * Regression test for two concurrent requests registering a view under the same label. Without
   * request-id namespacing, the second {@code createOrReplaceTempView} call would overwrite the
   * first, so request A would observe request B's data. With namespacing, each request resolves a
   * distinct temp view name and the two queries remain isolated.
   */
  @Test
  void concurrentRegistrationsWithSameLabelAreIsolated() throws Exception {
    final Dataset<Row> datasetA = singleColumnDataset("value", List.of("A1", "A2"));
    final Dataset<Row> datasetB = singleColumnDataset("value", List.of("B1", "B2", "B3"));

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Callable<List<String>> taskA =
          () -> runRegisterAndRead("requestA", "patients", datasetA);
      final Callable<List<String>> taskB =
          () -> runRegisterAndRead("requestB", "patients", datasetB);

      final Future<List<String>> futureA = executor.submit(taskA);
      final Future<List<String>> futureB = executor.submit(taskB);

      final List<String> resultA = futureA.get(60, TimeUnit.SECONDS);
      final List<String> resultB = futureB.get(60, TimeUnit.SECONDS);

      assertThat(resultA).containsExactlyInAnyOrder("A1", "A2");
      assertThat(resultB).containsExactlyInAnyOrder("B1", "B2", "B3");
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Reads back the registered view through SQL and then drops it, so the test exercises the same
   * register / query / drop sequence the production code performs for a single request.
   */
  private List<String> runRegisterAndRead(
      final String requestId, final String label, final Dataset<Row> dataset) {
    final String tempViewName = service.registerDataset(label, dataset, requestId);
    try {
      final String rewrittenSql =
          service.rewriteSql("SELECT value FROM " + label, Map.of(label, tempViewName));
      return spark.sql(rewrittenSql).collectAsList().stream().map(row -> row.getString(0)).toList();
    } finally {
      service.dropViews(List.of(tempViewName));
    }
  }

  private Dataset<Row> singleColumnDataset(final String columnName, final List<String> values) {
    final StructType schema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField(columnName, DataTypes.StringType, false)
            });
    final List<Row> rows =
        Arrays.stream(values.toArray(new String[0])).map(v -> (Row) RowFactory.create(v)).toList();
    return spark.createDataFrame(rows, schema);
  }
}
