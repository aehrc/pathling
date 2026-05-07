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
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the row-cap clamp logic in {@link SqlQueryExecutor#effectiveLimit(Integer,
 * String)}. These verify that the server-configured cap is always honoured and that a
 * caller-supplied {@code _limit} can only narrow, never widen, the result set.
 */
class SqlQueryExecutorTest {

  private static final String REQUEST_ID = "test-request";

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
        serverConfiguration);
  }
}
