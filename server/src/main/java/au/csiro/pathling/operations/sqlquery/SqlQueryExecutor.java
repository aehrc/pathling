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
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Executes the SQL of a {@link SqlQueryRequest} against the configured Spark session, materialising
 * the request's resolved dependency graph as request-scoped temporary views and owning their
 * lifecycle. The only piece of the pipeline that touches Spark.
 *
 * <p>Each node of the graph is materialised in topological order: a {@code ViewDefinition} leaf is
 * executed as a view, and a {@code SQLView} node's SQL is rewritten against the temp views of its
 * already-materialised children, validated, and run. The top-level SQL is then rewritten against
 * its own direct dependencies' temp views and run. Every node's SQL is validated statically before
 * execution and against its analysed plan during execution.
 *
 * @author John Grimes
 */
@Slf4j
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
   * Runs the static, read-only SQL validation for every node of the graph and the top-level query,
   * each against its own declared labels, without touching Spark. Used both at export kick-off and
   * before execution so malformed or disallowed SQL is caught early.
   *
   * @param request the parsed request
   * @param graph the resolved dependency graph
   */
  public void validateStatically(
      @Nonnull final SqlQueryRequest request, @Nonnull final ResolvedDependencyGraph graph) {
    for (final ResolvedDependency node : graph.getOrderedNodes()) {
      if (node instanceof final ResolvedSqlView sqlView) {
        sqlValidator.validate(sqlView.getSql(), sqlView.getChildKeysByLabel().keySet());
      }
    }
    sqlValidator.validate(
        request.getParsedQuery().getSql(), graph.getTopLevelKeysByLabel().keySet());
  }

  /**
   * Validates and executes the query, materialising the resolved dependency graph under
   * request-scoped temp view names for the duration of the call. The {@code consumer} is invoked
   * with the result dataset before the temp views are dropped, so streaming and other terminal
   * operations can complete before cleanup.
   *
   * <p>The only row limit applied is the caller's {@code _limit}, when they supply one. Execution
   * runs under whatever Spark job group the caller established, so an asynchronous job's Spark
   * stages remain attributed to it and a cancellation of that group reaches the work in flight.
   *
   * @param request the parsed and validated request
   * @param graph the resolved dependency graph the SQL references
   * @param dataSource the data source backing FhirView execution
   * @param requestId the HAPI per-request id used to namespace temp view names
   * @param consumer terminal consumer of the result dataset
   */
  public void execute(
      @Nonnull final SqlQueryRequest request,
      @Nonnull final ResolvedDependencyGraph graph,
      @Nonnull final DataSource dataSource,
      @Nonnull final String requestId,
      @Nonnull final Consumer<Dataset<Row>> consumer) {

    validateStatically(request, graph);

    final Map<String, String> registeredByKey = new LinkedHashMap<>();
    try {
      for (final ResolvedDependency node : graph.getOrderedNodes()) {
        materialiseNode(node, dataSource, requestId, registeredByKey);
      }

      final Map<String, String> topLevelViews =
          resolveLabelToViewName(graph.getTopLevelKeysByLabel(), registeredByKey);
      final String rewrittenSql =
          viewRegistrationService.rewriteSql(request.getParsedQuery().getSql(), topLevelViews);

      Dataset<Row> result = runSql(rewrittenSql, request.getParameterBindings());

      sqlValidator.validateAnalyzed(
          result.queryExecution().analyzed(), Set.copyOf(topLevelViews.values()));

      final Integer callerLimit = request.getLimit();
      if (callerLimit != null) {
        result = result.limit(callerLimit);
      }

      consumer.accept(result);
    } finally {
      viewRegistrationService.dropViews(registeredByKey.values());
    }
  }

  /**
   * Materialises a single graph node as a request-scoped temp view, recording its name by canonical
   * key. A {@code SQLView} node's analysed plan is validated against its own children's temp views
   * before registration, so it cannot reach an unauthorised data source.
   */
  private void materialiseNode(
      @Nonnull final ResolvedDependency node,
      @Nonnull final DataSource dataSource,
      @Nonnull final String requestId,
      @Nonnull final Map<String, String> registeredByKey) {
    final Dataset<Row> dataset;
    if (node instanceof final ResolvedViewDefinition viewDefinition) {
      dataset = viewRegistrationService.buildViewDefinition(viewDefinition.getView(), dataSource);
    } else if (node instanceof final ResolvedSqlView sqlView) {
      dataset = viewRegistrationService.buildSqlView(sqlView, registeredByKey);
      final Set<String> childViewNames =
          Set.copyOf(
              resolveLabelToViewName(sqlView.getChildKeysByLabel(), registeredByKey).values());
      sqlValidator.validateAnalyzed(dataset.queryExecution().analyzed(), childViewNames);
    } else {
      throw new InvalidRequestException(
          "Unsupported dependency node type: " + node.getClass().getSimpleName());
    }
    final String tempViewName =
        viewRegistrationService.registerDataset(node.getCanonicalKey(), dataset, requestId);
    registeredByKey.put(node.getCanonicalKey(), tempViewName);
    log.debug(
        "Materialised temp view '{}' for dependency '{}'", tempViewName, node.getCanonicalKey());
  }

  /**
   * Maps a node's local {@code label -> child canonical key} to {@code label -> temp view name},
   * resolving each child key against the views materialised so far.
   */
  @Nonnull
  private static Map<String, String> resolveLabelToViewName(
      @Nonnull final Map<String, String> keysByLabel,
      @Nonnull final Map<String, String> registeredByKey) {
    final Map<String, String> labelToViewName = new LinkedHashMap<>();
    for (final Map.Entry<String, String> entry : keysByLabel.entrySet()) {
      final String viewName = registeredByKey.get(entry.getValue());
      if (viewName == null) {
        throw new IllegalStateException(
            "Dependency '"
                + entry.getValue()
                + "' for label '"
                + entry.getKey()
                + "' was not materialised before it was referenced");
      }
      labelToViewName.put(entry.getKey(), viewName);
    }
    return labelToViewName;
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
