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
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Map;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Orchestrates the {@code $sqlquery-run} operation by selecting the query Library, running it
 * through the shared {@link SqlQueryPipeline}, and streaming the single result.
 *
 * @author John Grimes
 */
@Component
public class SqlQueryExecutionHelper {

  @Nonnull private final SqlQueryPipeline pipeline;

  @Nonnull private final SqlQueryResultStreamer streamer;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final LibraryReferenceResolver libraryReferenceResolver;

  /**
   * Constructs a new SqlQueryExecutionHelper.
   *
   * @param pipeline the shared SQL query pipeline (parse, resolve, validate, execute)
   * @param streamer streams the result dataset in the requested format
   * @param deltaLake the queryable data source backing FhirView execution
   * @param libraryReferenceResolver resolves a queryReference to a stored Library
   */
  @Autowired
  public SqlQueryExecutionHelper(
      @Nonnull final SqlQueryPipeline pipeline,
      @Nonnull final SqlQueryResultStreamer streamer,
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final LibraryReferenceResolver libraryReferenceResolver) {
    this.pipeline = pipeline;
    this.streamer = streamer;
    this.deltaLake = deltaLake;
    this.libraryReferenceResolver = libraryReferenceResolver;
  }

  /**
   * Rejects the unsupported {@code source} parameter (external data source). Pathling does not
   * implement external data sources, so a supplied {@code source} value is rejected rather than
   * silently ignored. This shared guard backs the {@code source} rejection at the system, type, and
   * instance levels, over both POST and GET.
   *
   * @param source the {@code source} parameter value, if supplied
   * @throws InvalidRequestException if {@code source} is present and non-blank
   */
  public void rejectSourceParameter(@Nullable final String source) {
    if (source != null && !source.isBlank()) {
      throw new InvalidRequestException(
          "The 'source' parameter (external data source) is not supported by this server.");
    }
  }

  /**
   * Executes a {@code $sqlquery-run} request and streams results to the HTTP response. Exactly one
   * of {@code queryResource} and {@code queryReference} must be provided.
   *
   * @param queryResource the inline SQLQuery Library resource, if supplied
   * @param queryReference reference to a stored SQLQuery Library, if supplied
   * @param format the output format, overrides Accept header if provided
   * @param acceptHeader the HTTP Accept header value, used as fallback if format is not provided
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param parameters runtime parameter bindings as a Parameters resource
   * @param requestId the HAPI per-request id used to namespace registered temp views
   * @param response the HTTP response for streaming output
   */
  @SuppressWarnings("java:S107")
  public void executeSqlQuery(
      @Nullable final IBaseResource queryResource,
      @Nullable final Reference queryReference,
      @Nullable final String format,
      @Nullable final String acceptHeader,
      @Nullable final BooleanType includeHeader,
      @Nullable final IntegerType limit,
      @Nullable final Parameters parameters,
      @Nonnull final String requestId,
      @Nullable final HttpServletResponse response) {

    if (response == null) {
      throw new InvalidRequestException("HTTP response is required for this operation");
    }

    final IBaseResource library = selectLibrary(queryResource, queryReference);

    final PreparedSqlQuery prepared =
        pipeline.prepare(library, format, acceptHeader, includeHeader, limit, parameters, Map.of());

    pipeline.execute(
        prepared,
        deltaLake,
        requestId,
        result ->
            streamer.stream(
                result,
                prepared.getRequest().getOutputFormat(),
                prepared.getRequest().isIncludeHeader(),
                response));
  }

  /**
   * Enforces the OperationDefinition's "exactly one of queryResource / queryReference" contract and
   * returns the resolved Library resource. Returns 400 if neither or both are provided; bubbles up
   * the resolver's 404 if a queryReference doesn't match a stored Library.
   */
  @Nonnull
  private IBaseResource selectLibrary(
      @Nullable final IBaseResource queryResource, @Nullable final Reference queryReference) {
    final boolean hasResource = queryResource != null;
    final boolean hasReference = queryReference != null && !queryReference.isEmpty();

    if (hasResource && hasReference) {
      throw new InvalidRequestException(
          "Exactly one of 'queryResource' and 'queryReference' must be provided, not both");
    }
    if (!hasResource && !hasReference) {
      throw new InvalidRequestException(
          "One of 'queryResource' or 'queryReference' must be provided");
    }
    if (hasResource) {
      return queryResource;
    }
    return libraryReferenceResolver.resolve(queryReference);
  }
}
