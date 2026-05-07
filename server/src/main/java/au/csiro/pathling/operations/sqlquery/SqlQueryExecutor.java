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

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.SqlQueryConfiguration;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
@Component
public class SqlQueryExecutor {

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final ViewRegistrationService viewRegistrationService;

  @Nonnull private final SqlValidator sqlValidator;

  @Nonnull private final SqlQueryConfiguration sqlQueryConfig;

  /**
   * Constructs a new SqlQueryExecutor.
   *
   * @param sparkSession the Spark session
   * @param viewRegistrationService manages temp-view registration / cleanup and SQL rewriting
   * @param sqlValidator validates the SQL before execution
   * @param serverConfiguration the server configuration, used to resolve the resource limits
   *     applied to each query
   */
  @Autowired
  public SqlQueryExecutor(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final ViewRegistrationService viewRegistrationService,
      @Nonnull final SqlValidator sqlValidator,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.sparkSession = sparkSession;
    this.viewRegistrationService = viewRegistrationService;
    this.sqlValidator = sqlValidator;
    this.sqlQueryConfig = serverConfiguration.getSqlQuery();
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

    final Set<String> declaredLabels =
        request.getParsedQuery().getViewReferences().stream()
            .map(ViewArtifactReference::getLabel)
            .collect(Collectors.toUnmodifiableSet());
    sqlValidator.validate(request.getParsedQuery().getSql(), declaredLabels);

    Map<String, String> registeredViews = Map.of();
    try {
      registeredViews = viewRegistrationService.registerViews(resolvedViews, dataSource, requestId);

      final String rewrittenSql =
          viewRegistrationService.rewriteSql(request.getParsedQuery().getSql(), registeredViews);

      Dataset<Row> result = runSql(rewrittenSql, request.getParameterBindings());

      sqlValidator.validateAnalyzed(
          result.queryExecution().analyzed(), Set.copyOf(registeredViews.values()));

      result = result.limit(effectiveLimit(request.getLimit(), requestId));

      consumer.accept(result);
    } finally {
      viewRegistrationService.dropViews(registeredViews.values());
    }
  }

  /**
   * Resolves the row limit applied to the result dataset. The configured server cap is always
   * applied; when the caller supplies a {@code _limit}, the lower of the two values wins. The
   * server cap is clamped to {@link Integer#MAX_VALUE} so that it can be passed to Spark's {@code
   * Dataset.limit(int)} API.
   */
  int effectiveLimit(final Integer callerLimit, @Nonnull final String requestId) {
    final int cap = (int) Math.min(sqlQueryConfig.getMaxRows(), Integer.MAX_VALUE);
    if (callerLimit == null) {
      return cap;
    }
    if (callerLimit > cap) {
      log.info(
          "Caller-supplied _limit of {} clamped to server cap of {} for request {}.",
          callerLimit,
          cap,
          requestId);
      return cap;
    }
    return callerLimit;
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
