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
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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

  @Nonnull private final ViewResolver viewResolver;

  @Nonnull private final SqlQueryExecutor executor;

  @Nonnull private final SqlValidator sqlValidator;

  /**
   * Constructs a new SqlQueryPipeline.
   *
   * @param requestParser parses a SQLQuery Library and binds runtime parameters
   * @param viewResolver resolves the view references to parsed FhirViews, preferring
   *     request-supplied views over server storage
   * @param executor validates and runs the SQL against Spark
   * @param sqlValidator the static SQL validator, used for kick-off-time validation that does not
   *     require executing the query
   */
  @Autowired
  public SqlQueryPipeline(
      @Nonnull final SqlQueryRequestParser requestParser,
      @Nonnull final ViewResolver viewResolver,
      @Nonnull final SqlQueryExecutor executor,
      @Nonnull final SqlValidator sqlValidator) {
    this.requestParser = requestParser;
    this.viewResolver = viewResolver;
    this.executor = executor;
    this.sqlValidator = sqlValidator;
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
    final Map<String, FhirView> resolvedViews =
        viewResolver.resolve(request.getParsedQuery().getViewReferences(), suppliedViews);
    return new PreparedSqlQuery(request, resolvedViews);
  }

  /**
   * Runs the static SQL validation that does not require executing the query, so that malformed or
   * disallowed SQL is detected before any Spark work. Used by the asynchronous export to surface
   * these failures synchronously at kick-off.
   *
   * @param prepared the prepared query
   */
  public void validateStatically(@Nonnull final PreparedSqlQuery prepared) {
    final Set<String> declaredLabels =
        prepared.getRequest().getParsedQuery().getViewReferences().stream()
            .map(ViewArtifactReference::getLabel)
            .collect(Collectors.toUnmodifiableSet());
    sqlValidator.validate(prepared.getRequest().getParsedQuery().getSql(), declaredLabels);
  }

  /**
   * Executes the prepared query against Spark, registering the resolved views under request-scoped
   * temp views for the duration of the call and invoking {@code consumer} with the result dataset
   * before they are dropped.
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
        prepared.getRequest(), prepared.getResolvedViews(), dataSource, requestId, consumer);
  }
}
