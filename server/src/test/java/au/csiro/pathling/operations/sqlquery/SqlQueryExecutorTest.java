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
import static org.mockito.Mockito.mock;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Tests for {@link SqlQueryExecutor}. Three concerns are covered:
 *
 * <ul>
 *   <li>Row limiting: the caller's {@code _limit} is applied when they supply one, and no limit at
 *       all is imposed when they do not. A server-side cap used to truncate every result, which
 *       silently truncated every {@code $sql-export} at one million rows (spec 041 US1, US4).
 *   <li>Spark job groups: execution runs under whatever job group the caller established, which is
 *       how an asynchronous job's stages are attributed to it and how a cancellation of that job
 *       reaches the query in flight (spec 041 US3).
 *   <li>That a {@code DESCRIBE <label>} statement flows through the same validate → execute →
 *       {@code validateAnalyzed} sequence the executor uses and yields the engine's describe rows.
 *       This exercises the analysed-mode carve-out end to end in the JVM, since the executor calls
 *       {@link SqlValidator#validateAnalyzed} on the eagerly-executed command plan (spec 029 US1).
 * </ul>
 */
@Import(SqlValidator.class)
@SpringBootUnitTest
class SqlQueryExecutorTest {

  private static final String REQUEST_ID = "test-request";

  private static final String VIEW_NAME = "patients";

  /** SQL that needs no dependencies, so it can run against an empty dependency graph. */
  private static final String SELF_CONTAINED_SQL =
      "SELECT * FROM (VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')) AS t(id, name)";

  @Autowired private SqlValidator sqlValidator;

  @Autowired private SparkSession sparkSession;

  @Autowired private FhirContext fhirContext;

  // -------------------------------------------------------------------------
  // DESCRIBE <label> flows through the executor's validate/execute/analyse
  // sequence and returns the engine's describe rows.
  // -------------------------------------------------------------------------

  @Test
  void describeReturnsColumnRowsMatchingSchema() {
    final Dataset<Row> backing =
        sparkSession.sql("SELECT CAST(1 AS INT) AS id, CAST('x' AS STRING) AS name");
    final List<Row> rows = runDescribe("DESCRIBE " + VIEW_NAME, backing, null);

    // One row per column of the backing dataset, carrying the three describe fields.
    final var typesByColumn =
        rows.stream().collect(Collectors.toMap(row -> row.getString(0), row -> row.getString(1)));
    assertThat(typesByColumn).containsEntry("id", "int").containsEntry("name", "string");
    // The describe result schema is always col_name, data_type, comment.
    assertThat(rows.get(0).schema().fieldNames())
        .containsExactly("col_name", "data_type", "comment");
  }

  @Test
  void limitAppliesToDescribeRows() {
    final Dataset<Row> backing = sparkSession.sql("SELECT 1 AS a, 2 AS b, 3 AS c");
    final List<Row> rows = runDescribe("DESCRIBE " + VIEW_NAME, backing, 1);
    assertThat(rows).hasSize(1);
  }

  @Test
  void describeQueryReturnsProjectedColumnTypes() {
    final Dataset<Row> backing =
        sparkSession.sql("SELECT CAST(1 AS INT) AS id, CAST('x' AS STRING) AS name");
    final List<Row> rows =
        runDescribe(
            "DESCRIBE QUERY SELECT id, count(*) AS n FROM " + VIEW_NAME + " GROUP BY id",
            backing,
            null);

    final var typesByColumn =
        rows.stream().collect(Collectors.toMap(row -> row.getString(0), row -> row.getString(1)));
    // count(*) projects a bigint; the passed-through id keeps its int type.
    assertThat(typesByColumn).containsEntry("id", "int").containsEntry("n", "bigint");
  }

  @Test
  void describeQueryBindsParameterMarker() {
    final Dataset<Row> backing = sparkSession.sql("SELECT 1 AS id");
    final List<Row> rows =
        runDescribe(
            "DESCRIBE QUERY SELECT :threshold AS v FROM " + VIEW_NAME,
            backing,
            null,
            Map.of("threshold", 5));
    // The bound parameter yields a single projected column, described without scanning the view.
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getString(0)).isEqualTo("v");
  }

  /**
   * Reproduces the executor's describe pipeline in the JVM: register the backing dataset as the
   * label-named temp view, run the parse-time gate, execute through {@code sparkSession.sql}, run
   * the analysed-mode gate on the eagerly-executed plan, apply any limit, and collect.
   */
  @Nonnull
  private List<Row> runDescribe(
      @Nonnull final String sql, @Nonnull final Dataset<Row> backing, final Integer limit) {
    return runDescribe(sql, backing, limit, Map.of());
  }

  @Nonnull
  private List<Row> runDescribe(
      @Nonnull final String sql,
      @Nonnull final Dataset<Row> backing,
      final Integer limit,
      @Nonnull final Map<String, Object> parameters) {
    backing.createOrReplaceTempView(VIEW_NAME);
    try {
      sqlValidator.validate(sql, Set.of(VIEW_NAME));
      Dataset<Row> result =
          parameters.isEmpty() ? sparkSession.sql(sql) : sparkSession.sql(sql, parameters);
      sqlValidator.validateAnalyzed(result.queryExecution().analyzed(), Set.of(VIEW_NAME));
      if (limit != null) {
        result = result.limit(limit);
      }
      return result.collectAsList();
    } finally {
      sparkSession.catalog().dropTempView(VIEW_NAME);
    }
  }

  // -------------------------------------------------------------------------
  // Row limiting: the caller's _limit is the only limit that is ever applied.
  // -------------------------------------------------------------------------

  @Test
  void imposesNoRowLimitWhenCallerSuppliesNone() {
    // A caller that supplies no _limit must receive the complete result, so nothing may add a limit
    // node to the plan handed to the consumer. This is what previously truncated every
    // $sql-export at the configured server cap.
    final LogicalPlan consumedPlan = executeAndCapturePlan(SELF_CONTAINED_SQL, null);

    assertThat(findLimitValue(consumedPlan)).isNull();
  }

  @Test
  void appliesTheCallerLimitWhenSupplied() {
    // The caller's _limit remains the means of bounding a synchronous response, so it must appear
    // in the plan with exactly the value that was asked for.
    final LogicalPlan consumedPlan = executeAndCapturePlan(SELF_CONTAINED_SQL, 2);

    assertThat(findLimitValue(consumedPlan)).isEqualTo(2);
  }

  // -------------------------------------------------------------------------
  // Spark job group: execution runs under whatever group the caller established.
  // -------------------------------------------------------------------------

  @Test
  void preservesTheAmbientSparkJobGroup() {
    // The asynchronous machinery sets a job group named for the job before calling the executor.
    // Stage attribution and cancellation both key off spark.jobGroup.id, so the executor must
    // neither replace that group nor clear it, either during or after execution.
    final String ambientJobGroup = "job-" + REQUEST_ID;
    sparkSession.sparkContext().setJobGroup(ambientJobGroup, "ambient", true);
    try {
      final AtomicReference<String> groupInsideConsumer = new AtomicReference<>();
      newExecutor()
          .execute(
              request(SELF_CONTAINED_SQL, null),
              new ResolvedDependencyGraph(List.of(), Map.of(), Map.of()),
              mock(DataSource.class),
              REQUEST_ID,
              dataset -> groupInsideConsumer.set(currentJobGroup()));

      assertThat(groupInsideConsumer.get()).isEqualTo(ambientJobGroup);
      assertThat(currentJobGroup()).isEqualTo(ambientJobGroup);
    } finally {
      sparkSession.sparkContext().clearJobGroup();
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Executes the given SQL through a real executor and returns the plan the consumer received. */
  @Nonnull
  private LogicalPlan executeAndCapturePlan(
      @Nonnull final String sql, @Nullable final Integer limit) {
    final AtomicReference<LogicalPlan> consumedPlan = new AtomicReference<>();
    newExecutor()
        .execute(
            request(sql, limit),
            new ResolvedDependencyGraph(List.of(), Map.of(), Map.of()),
            mock(DataSource.class),
            REQUEST_ID,
            dataset -> consumedPlan.set(dataset.queryExecution().analyzed()));
    return Objects.requireNonNull(consumedPlan.get());
  }

  /**
   * Returns the value of the first {@link GlobalLimit} node found in the plan, or null when the
   * plan carries no limit at all. {@code Dataset.limit(n)} is the only thing that introduces one
   * here, since none of the test SQL expresses a limit of its own.
   */
  @Nullable
  private static Integer findLimitValue(@Nonnull final LogicalPlan plan) {
    if (plan instanceof final GlobalLimit globalLimit
        && globalLimit.limitExpr() instanceof final Literal literal) {
      return (Integer) literal.value();
    }
    for (final LogicalPlan child : CollectionConverters.asJava(plan.children())) {
      final Integer found = findLimitValue(child);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /** Returns the job group currently set on the Spark context, or null when none is set. */
  @Nullable
  private String currentJobGroup() {
    return sparkSession.sparkContext().getLocalProperty("spark.jobGroup.id");
  }

  /** Builds a request over self-contained SQL with no dependencies. */
  @Nonnull
  private static SqlQueryRequest request(@Nonnull final String sql, @Nullable final Integer limit) {
    return new SqlQueryRequest(
        new ParsedSqlQuery(sql, List.of(), List.of(), SqlLibraryParser.SQL_QUERY_TYPE_CODE),
        SqlQueryOutputFormat.NDJSON,
        /* includeHeader= */ true,
        limit,
        Map.of());
  }

  /** Builds an executor over the real Spark session, view registration service and validator. */
  @Nonnull
  private SqlQueryExecutor newExecutor() {
    return new SqlQueryExecutor(
        sparkSession,
        new ViewRegistrationService(sparkSession, fhirContext, new ServerConfiguration()),
        sqlValidator);
  }
}
