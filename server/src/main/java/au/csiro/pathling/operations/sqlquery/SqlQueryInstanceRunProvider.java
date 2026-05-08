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

import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provider for the type-level and instance-level {@code $sqlquery-run} operations for Library
 * resources.
 *
 * <p>This provides operations at:
 *
 * <ul>
 *   <li>{@code POST /fhir/Library/$sqlquery-run} — Type-level operation with queryResource
 *       parameter
 *   <li>{@code POST /fhir/Library/[id]/$sqlquery-run} — Instance-level operation using stored
 *       Library
 * </ul>
 *
 * @see <a
 *     href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/OperationDefinition-SQLQueryRun.html">SQLQueryRun</a>
 * @see SqlQueryRunProvider for system-level $sqlquery-run operation
 */
@Component
public class SqlQueryInstanceRunProvider implements IResourceProvider {

  @Nonnull private final SqlQueryExecutionHelper executionHelper;

  @Nonnull private final LibraryReferenceResolver libraryReferenceResolver;

  /**
   * Constructs a new SqlQueryInstanceRunProvider.
   *
   * @param executionHelper the helper for executing SQL queries
   * @param libraryReferenceResolver resolves a Library reference to a stored resource, with the
   *     same 404 semantics used by the type-level operation
   */
  @Autowired
  public SqlQueryInstanceRunProvider(
      @Nonnull final SqlQueryExecutionHelper executionHelper,
      @Nonnull final LibraryReferenceResolver libraryReferenceResolver) {
    this.executionHelper = executionHelper;
    this.libraryReferenceResolver = libraryReferenceResolver;
  }

  @Override
  public Class<Library> getResourceType() {
    return Library.class;
  }

  /**
   * Type-level {@code $sqlquery-run} operation that accepts a SQLQuery Library either inline
   * ({@code queryResource}) or by reference ({@code queryReference}).
   *
   * @param queryResource the inline SQLQuery Library resource
   * @param queryReference reference to a stored SQLQuery Library
   * @param format the output format (ndjson, csv, json, or parquet), overrides Accept header
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param parameters runtime parameter bindings as a Parameters resource
   * @param requestDetails the servlet request details containing HTTP headers
   * @param response the HTTP response for streaming output
   */
  @Operation(name = "$sqlquery-run", idempotent = true, manualResponse = true)
  @OperationAccess("sqlquery-run")
  @SuppressWarnings("java:S107")
  public void runTypeLevel(
      @Nullable @OperationParam(name = "queryResource") final IBaseResource queryResource,
      @Nullable @OperationParam(name = "queryReference") final Reference queryReference,
      @Nullable @OperationParam(name = "_format") final String format,
      @Nullable @OperationParam(name = "header") final BooleanType includeHeader,
      @Nullable @OperationParam(name = "_limit") final IntegerType limit,
      @Nullable @OperationParam(name = "parameters") final Parameters parameters,
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final HttpServletResponse response) {

    final String acceptHeader = requestDetails.getServletRequest().getHeader("Accept");

    executionHelper.executeSqlQuery(
        queryResource,
        queryReference,
        format,
        acceptHeader,
        includeHeader,
        limit,
        parameters,
        requestDetails.getRequestId(),
        response);
  }

  /**
   * Instance-level {@code $sqlquery-run} operation that uses a stored Library by ID.
   *
   * @param libraryId the ID of the stored Library resource
   * @param format the output format (ndjson, csv, json, or parquet), overrides Accept header
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param parameters runtime parameter bindings as a Parameters resource
   * @param requestDetails the servlet request details containing HTTP headers
   * @param response the HTTP response for streaming output
   */
  @Operation(name = "$sqlquery-run", idempotent = true, manualResponse = true)
  @OperationAccess("sqlquery-run")
  public void runById(
      @IdParam final IdType libraryId,
      @Nullable @OperationParam(name = "_format") final String format,
      @Nullable @OperationParam(name = "header") final BooleanType includeHeader,
      @Nullable @OperationParam(name = "_limit") final IntegerType limit,
      @Nullable @OperationParam(name = "parameters") final Parameters parameters,
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final HttpServletResponse response) {

    final IBaseResource libraryResource =
        libraryReferenceResolver.resolve(new Reference("Library/" + libraryId.getIdPart()));
    final String acceptHeader = requestDetails.getServletRequest().getHeader("Accept");

    executionHelper.executeSqlQuery(
        libraryResource,
        null,
        format,
        acceptHeader,
        includeHeader,
        limit,
        parameters,
        requestDetails.getRequestId(),
        response);
  }
}
