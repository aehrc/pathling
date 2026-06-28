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
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The shared request/execution pipeline for SQL queries. Given a SQLQuery Library, optional runtime
 * parameters, and optional request-supplied views, it parses the query, resolves its ViewDefinition
 * table sources, statically validates the SQL, and executes it against Spark.
 *
 * <p>Both the synchronous {@code $sqlquery-run} operation (which streams the single result) and the
 * asynchronous {@code $sqlquery-export} operation (which writes each result to files) call this
 * pipeline, so the parsing, view resolution, validation, and execution semantics are identical
 * across the two. Only the terminal step (stream-to-response vs. write-to-files) differs and is
 * supplied by the caller as a {@link Consumer} of the result dataset.
 *
 * @author John Grimes
 */
@Component
public class SqlQueryPipeline {

  @Nonnull private final SqlQueryRequestParser requestParser;

  @Nonnull private final SqlDependencyResolver dependencyResolver;

  @Nonnull private final SqlQueryExecutor executor;

  /**
   * Constructs a new SqlQueryPipeline.
   *
   * @param requestParser parses a SQLQuery or SQLView Library and binds runtime parameters
   * @param dependencyResolver resolves the transitive dependency graph, preferring request-supplied
   *     views over server storage
   * @param executor validates and runs the SQL against Spark
   */
  @Autowired
  public SqlQueryPipeline(
      @Nonnull final SqlQueryRequestParser requestParser,
      @Nonnull final SqlDependencyResolver dependencyResolver,
      @Nonnull final SqlQueryExecutor executor) {
    this.requestParser = requestParser;
    this.dependencyResolver = dependencyResolver;
    this.executor = executor;
  }

  /**
   * Parses the SQLQuery Library and resolves its view table sources, producing a {@link
   * PreparedSqlQuery} ready for validation and execution. Performs all structural FHIR-level
   * validation (query parsing, parameter binding and type checking) and view resolution (preferring
   * request-supplied views, falling back to server storage), but does not touch Spark.
   *
   * @param library the SQLQuery Library resource (inline or already resolved from a reference)
   * @param format the explicit {@code _format} parameter, if any
   * @param acceptHeader the HTTP {@code Accept} header value, used as a fallback for {@code format}
   * @param includeHeader whether to include a CSV header row; {@code null} defaults to {@code true}
   * @param limit optional row cap
   * @param parameters runtime parameter bindings as a {@code Parameters} resource
   * @param suppliedViews request-supplied views keyed by the ViewDefinition id they satisfy
   * @return the prepared query
   */
  @Nonnull
  @SuppressWarnings("java:S107")
  public PreparedSqlQuery prepare(
      @Nonnull final IBaseResource library,
      @Nullable final String format,
      @Nullable final String acceptHeader,
      @Nullable final BooleanType includeHeader,
      @Nullable final IntegerType limit,
      @Nullable final Parameters parameters,
      @Nonnull final Map<String, FhirView> suppliedViews) {
    final SqlQueryRequest request =
        requestParser.parse(library, format, acceptHeader, includeHeader, limit, parameters);
    final ResolvedDependencyGraph dependencyGraph =
        dependencyResolver.resolve(request.getParsedQuery(), suppliedViews);
    return new PreparedSqlQuery(request, dependencyGraph);
  }

  /**
   * Runs the static SQL validation that does not require executing the query, so that malformed or
   * disallowed SQL is detected before any Spark work. Validates the top-level SQL and every SQLView
   * node's SQL against its own declared labels. Used by the asynchronous export to surface these
   * failures synchronously at kick-off.
   *
   * @param prepared the prepared query
   */
  public void validateStatically(@Nonnull final PreparedSqlQuery prepared) {
    executor.validateStatically(prepared.getRequest(), prepared.getDependencyGraph());
  }

  /**
   * Executes the prepared query against Spark, materialising the resolved dependency graph under
   * request-scoped temp views for the duration of the call and invoking {@code consumer} with the
   * result dataset before they are dropped.
   *
   * @param prepared the prepared query
   * @param dataSource the data source backing FhirView execution (filtered for the export filters)
   * @param requestId the HAPI per-request id used to namespace temp view names
   * @param consumer terminal consumer of the result dataset
   */
  public void execute(
      @Nonnull final PreparedSqlQuery prepared,
      @Nonnull final DataSource dataSource,
      @Nonnull final String requestId,
      @Nonnull final Consumer<Dataset<Row>> consumer) {
    executor.execute(
        prepared.getRequest(), prepared.getDependencyGraph(), dataSource, requestId, consumer);
  }
}
