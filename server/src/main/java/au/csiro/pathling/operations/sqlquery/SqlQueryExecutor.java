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

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Executes the SQL of a {@link SqlQueryRequest} against the configured Spark session, owning the
 * lifecycle of the request-scoped temporary views the query references. The only piece of the
 * pipeline that touches Spark.
 */
@Component
public class SqlQueryExecutor {

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final ViewRegistrationService viewRegistrationService;

  @Nonnull private final SqlValidator sqlValidator;

  /**
   * Constructs a new SqlQueryExecutor.
   *
   * @param sparkSession the Spark session
   * @param viewRegistrationService manages temp-view registration / cleanup and SQL rewriting
   * @param sqlValidator validates the SQL before execution
   */
  @Autowired
  public SqlQueryExecutor(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final ViewRegistrationService viewRegistrationService,
      @Nonnull final SqlValidator sqlValidator) {
    this.sparkSession = sparkSession;
    this.viewRegistrationService = viewRegistrationService;
    this.sqlValidator = sqlValidator;
  }

  /**
   * Validates and executes the SQL, registering the resolved views under request-scoped temp view
   * names for the duration of the call. The {@code consumer} is invoked with the result dataset
   * before the temp views are dropped, so streaming and other terminal operations can complete
   * before cleanup.
   *
   * @param request the parsed and validated request
   * @param resolvedViews the views referenced by the SQL, keyed by table label
   * @param dataSource the data source backing FhirView execution
   * @param requestId the HAPI per-request id used to namespace temp view names
   * @param consumer terminal consumer of the result dataset
   */
  public void execute(
      @Nonnull final SqlQueryRequest request,
      @Nonnull final Map<String, FhirView> resolvedViews,
      @Nonnull final DataSource dataSource,
      @Nonnull final String requestId,
      @Nonnull final Consumer<Dataset<Row>> consumer) {

    sqlValidator.validate(request.getParsedQuery().getSql());

    Map<String, String> registeredViews = Map.of();
    try {
      registeredViews = viewRegistrationService.registerViews(resolvedViews, dataSource, requestId);

      final String rewrittenSql =
          viewRegistrationService.rewriteSql(request.getParsedQuery().getSql(), registeredViews);

      Dataset<Row> result = runSql(rewrittenSql, request.getParameterBindings());

      sqlValidator.validateAnalyzed(result.queryExecution().analyzed());

      if (request.getLimit() != null) {
        result = result.limit(request.getLimit());
      }

      consumer.accept(result);
    } finally {
      viewRegistrationService.dropViews(registeredViews.values());
    }
  }

  @Nonnull
  private Dataset<Row> runSql(
      @Nonnull final String sql, @Nonnull final Map<String, Object> parameterBindings) {
    if (parameterBindings.isEmpty()) {
      return sparkSession.sql(sql);
    }
    return sparkSession.sql(sql, parameterBindings);
  }
}
