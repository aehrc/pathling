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
import au.csiro.pathling.config.SqlQueryConfiguration;
import au.csiro.pathling.test.SpringBootUnitTest;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests for {@link SqlQueryExecutor}. Two concerns are covered:
 *
 * <ul>
 *   <li>The row-cap clamp logic in {@link SqlQueryExecutor#effectiveLimit(Integer, String)}, which
 *       verifies that the server-configured cap is always honoured and that a caller-supplied
 *       {@code _limit} can only narrow, never widen, the result set.
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

  @Autowired private SqlValidator sqlValidator;

  @Autowired private SparkSession sparkSession;

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

  /**
   * Reproduces the executor's describe pipeline in the JVM: register the backing dataset as the
   * label-named temp view, run the parse-time gate, execute through {@code sparkSession.sql}, run
   * the analysed-mode gate on the eagerly-executed plan, apply any limit, and collect.
   */
  @Nonnull
  private List<Row> runDescribe(
      @Nonnull final String sql, @Nonnull final Dataset<Row> backing, final Integer limit) {
    backing.createOrReplaceTempView(VIEW_NAME);
    try {
      sqlValidator.validate(sql, Set.of(VIEW_NAME));
      Dataset<Row> result = sparkSession.sql(sql);
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
  // Row-cap clamp logic.
  // -------------------------------------------------------------------------

  @Test
  void appliesServerCapWhenCallerHasNoLimit() {
    final SqlQueryExecutor executor = newExecutor(2);
    assertThat(executor.effectiveLimit(null, REQUEST_ID)).isEqualTo(2);
  }

  @Test
  void appliesCallerLimitWhenLowerThanCap() {
    final SqlQueryExecutor executor = newExecutor(1000);
    assertThat(executor.effectiveLimit(5, REQUEST_ID)).isEqualTo(5);
  }

  @Test
  void appliesServerCapWhenCallerLimitExceedsIt() {
    final SqlQueryExecutor executor = newExecutor(10);
    assertThat(executor.effectiveLimit(1_000_000, REQUEST_ID)).isEqualTo(10);
  }

  @Test
  void clampsConfiguredCapToIntegerMaxValue() {
    // A "disable the cap" value larger than Integer.MAX_VALUE must clamp down so it can be passed
    // to Spark's Dataset.limit(int) API.
    final SqlQueryExecutor executor = newExecutor(Long.MAX_VALUE);
    assertThat(executor.effectiveLimit(null, REQUEST_ID)).isEqualTo(Integer.MAX_VALUE);
  }

  @Nonnull
  private static SqlQueryExecutor newExecutor(final long maxRows) {
    final SqlQueryConfiguration sqlQueryConfig = new SqlQueryConfiguration();
    sqlQueryConfig.setMaxRows(maxRows);
    final ServerConfiguration serverConfiguration = new ServerConfiguration();
    serverConfiguration.setSqlQuery(sqlQueryConfig);
    return new SqlQueryExecutor(
        mock(SparkSession.class),
        mock(ViewRegistrationService.class),
        mock(SqlValidator.class),
        serverConfiguration,
        mock(SqlQueryWatchdog.class));
  }
}
