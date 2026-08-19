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
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import jakarta.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
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
 * <p>A failure raised by Spark's analyser while analysing the user's SQL is a client mistake rather
 * than a server fault, so it is translated into a {@code 422 Unprocessable Entity} carrying the
 * analyser message; see {@link #analysisFailure(String)}.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class SqlQueryExecutor {

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final ViewRegistrationService viewRegistrationService;

  @Nonnull private final SqlValidator sqlValidator;

  @Nonnull private final SqlQueryConfiguration sqlQueryConfig;

  @Nonnull private final SqlQueryWatchdog watchdog;

  /**
   * Constructs a new SqlQueryExecutor.
   *
   * @param sparkSession the Spark session
   * @param viewRegistrationService manages temp-view registration / cleanup and SQL rewriting
   * @param sqlValidator validates the SQL before execution
   * @param serverConfiguration the server configuration, used to resolve the resource limits
   *     applied to each query
   * @param watchdog the watchdog used to schedule wall-clock timeouts and Spark job-group
   *     cancellation for each query
   */
  @Autowired
  public SqlQueryExecutor(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final ViewRegistrationService viewRegistrationService,
      @Nonnull final SqlValidator sqlValidator,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final SqlQueryWatchdog watchdog) {
    this.sparkSession = sparkSession;
    this.viewRegistrationService = viewRegistrationService;
    this.sqlValidator = sqlValidator;
    this.sqlQueryConfig = serverConfiguration.getSqlQuery();
    this.watchdog = watchdog;
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

    final String jobGroupId = "sqlquery-" + requestId;
    sparkSession
        .sparkContext()
        .setJobGroup(jobGroupId, "$sqlquery-run " + requestId, /* interruptOnCancel= */ true);

    final Map<String, String> registeredByKey = new LinkedHashMap<>();
    final SqlQueryWatchdog.Watch watch = watchdog.start(jobGroupId);
    try {
      for (final ResolvedDependency node : graph.getOrderedNodes()) {
        materialiseNode(node, dataSource, requestId, registeredByKey);
      }

      final Map<String, String> topLevelViews =
          resolveLabelToViewName(graph.getTopLevelKeysByLabel(), registeredByKey);
      final String rewrittenSql =
          viewRegistrationService.rewriteSql(request.getParsedQuery().getSql(), topLevelViews);

      final Dataset<Row> analysed;
      try {
        analysed = runSql(rewrittenSql, request.getParameterBindings());
        sqlValidator.validateAnalyzed(
            analysed.queryExecution().analyzed(), Set.copyOf(topLevelViews.values()));
      } catch (final Exception e) {
        // Spark analyses eagerly, so a mistake in the user's SQL surfaces here as an
        // AnalysisException. Scala-compiled methods declare no checked exceptions, so
        // AnalysisException cannot be named in a catch clause around sparkSession.sql; the same
        // pattern is used in ExportFileWriter.
        if (e instanceof final AnalysisException analysisException) {
          throw analysisFailure("SQL analysis failed: " + analysisException.getMessage());
        }
        if (e instanceof final RuntimeException runtimeException) {
          throw runtimeException;
        }
        throw new RuntimeException(e);
      }

      // The terminal consumer sits outside the wrapped region: a failure while streaming or
      // writing the result is an execution fault, not a client SQL mistake, and keeps its 500.
      consumer.accept(analysed.limit(effectiveLimit(request.getLimit(), requestId)));
    } catch (final RuntimeException e) {
      if (watch.timedOut()) {
        throw new InvalidRequestException(
            "Query exceeded the configured timeout of "
                + sqlQueryConfig.getTimeoutSeconds()
                + " seconds.");
      }
      throw e;
    } finally {
      watch.complete();
      sparkSession.sparkContext().clearJobGroup();
      viewRegistrationService.dropViews(registeredByKey.values());
    }
  }

  /**
   * Materialises a single graph node as a request-scoped temp view, recording its name by canonical
   * key.
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
      dataset = materialiseSqlView(sqlView, registeredByKey);
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
   * Builds the dataset for a {@code SQLView} node from its own SQL, rewritten against the temp
   * views of its already-materialised children. The node's analysed plan is validated against those
   * temp views before it is returned, so it cannot reach an unauthorised data source.
   *
   * <p>The node's SQL is the user's, so an analysis failure here is a client mistake and is
   * translated into the same 422 as a top-level failure, with the failing dependency named so the
   * client knows it is not the SQL it just submitted that is at fault.
   */
  @Nonnull
  private Dataset<Row> materialiseSqlView(
      @Nonnull final ResolvedSqlView sqlView, @Nonnull final Map<String, String> registeredByKey) {
    try {
      final Dataset<Row> dataset = viewRegistrationService.buildSqlView(sqlView, registeredByKey);
      final Set<String> childViewNames =
          Set.copyOf(
              resolveLabelToViewName(sqlView.getChildKeysByLabel(), registeredByKey).values());
      sqlValidator.validateAnalyzed(dataset.queryExecution().analyzed(), childViewNames);
      return dataset;
    } catch (final Exception e) {
      // The catch clause cannot name AnalysisException, for the reason given in execute.
      if (e instanceof final AnalysisException analysisException) {
        throw analysisFailure(
            "SQL analysis failed for dependency '"
                + sqlView.getCanonicalKey()
                + "': "
                + analysisException.getMessage());
      }
      if (e instanceof final RuntimeException runtimeException) {
        throw runtimeException;
      }
      throw new RuntimeException(e);
    }
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

  /**
   * Builds the {@code 422 Unprocessable Entity} response for a Spark analysis failure. Spark's own
   * analyser message is the most useful thing the client can be told, so it is carried through as
   * the diagnostics of a single issue with {@code code = invalid}: the SQL is well-formed but
   * cannot be processed as submitted. No {@code expression} is set, because the faulting element is
   * the SQL text rather than a request parameter.
   *
   * <p>The outcome is built by hand rather than left to HAPI, whose default outcome for this
   * exception uses {@code code = processing} and so does not distinguish a client mistake from a
   * processing fault.
   *
   * @param diagnostics the diagnostics message, already prefixed with the identity of the failing
   *     node
   * @return the exception to throw
   */
  @Nonnull
  static UnprocessableEntityException analysisFailure(@Nonnull final String diagnostics) {
    final OperationOutcome outcome = new OperationOutcome();
    final OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setSeverity(IssueSeverity.ERROR);
    issue.setCode(IssueType.INVALID);
    issue.setDiagnostics(diagnostics);
    outcome.addIssue(issue);
    return new UnprocessableEntityException(diagnostics, outcome);
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
