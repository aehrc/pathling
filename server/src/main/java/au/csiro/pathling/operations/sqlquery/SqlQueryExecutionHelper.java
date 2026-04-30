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

import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Orchestrates the {@code $sqlquery-run} operation by chaining the parser, view resolver, executor
 * and result streamer.
 */
@Component
public class SqlQueryExecutionHelper {

  @Nonnull private final SqlQueryRequestParser requestParser;

  @Nonnull private final ViewResolver viewResolver;

  @Nonnull private final SqlQueryExecutor executor;

  @Nonnull private final SqlQueryResultStreamer streamer;

  @Nonnull private final QueryableDataSource deltaLake;

  /**
   * Constructs a new SqlQueryExecutionHelper.
   *
   * @param requestParser parses raw HTTP inputs into a validated request
   * @param viewResolver resolves view references to parsed FhirViews with auth checks
   * @param executor validates and runs the SQL against Spark
   * @param streamer streams the result dataset in the requested format
   * @param deltaLake the queryable data source backing FhirView execution
   */
  @Autowired
  public SqlQueryExecutionHelper(
      @Nonnull final SqlQueryRequestParser requestParser,
      @Nonnull final ViewResolver viewResolver,
      @Nonnull final SqlQueryExecutor executor,
      @Nonnull final SqlQueryResultStreamer streamer,
      @Nonnull final QueryableDataSource deltaLake) {
    this.requestParser = requestParser;
    this.viewResolver = viewResolver;
    this.executor = executor;
    this.streamer = streamer;
    this.deltaLake = deltaLake;
  }

  /**
   * Executes a SQL query from a Library resource and streams results to the HTTP response.
   *
   * @param libraryResource the Library resource containing the SQLQuery
   * @param format the output format, overrides Accept header if provided
   * @param acceptHeader the HTTP Accept header value, used as fallback if format is not provided
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param parameterValues runtime parameter values to bind to the SQL query
   * @param requestId the HAPI per-request id used to namespace registered temp views
   * @param response the HTTP response for streaming output
   */
  @SuppressWarnings("java:S107")
  public void executeSqlQuery(
      @Nonnull final IBaseResource libraryResource,
      @Nullable final String format,
      @Nullable final String acceptHeader,
      @Nullable final BooleanType includeHeader,
      @Nullable final IntegerType limit,
      @Nullable final List<ParametersParameterComponent> parameterValues,
      @Nonnull final String requestId,
      @Nullable final HttpServletResponse response) {

    if (response == null) {
      throw new InvalidRequestException("HTTP response is required for this operation");
    }

    final SqlQueryRequest request =
        requestParser.parse(
            libraryResource, format, acceptHeader, includeHeader, limit, parameterValues);

    final Map<String, FhirView> resolvedViews =
        viewResolver.resolve(request.getParsedQuery().getViewReferences());

    executor.execute(
        request,
        resolvedViews,
        deltaLake,
        requestId,
        result ->
            streamer.stream(
                result, request.getOutputFormat(), request.isIncludeHeader(), response));
  }
}
