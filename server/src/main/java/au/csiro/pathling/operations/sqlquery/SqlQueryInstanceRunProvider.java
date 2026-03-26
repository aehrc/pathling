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

import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
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

  @Nonnull private final ReadExecutor readExecutor;

  /**
   * Constructs a new SqlQueryInstanceRunProvider.
   *
   * @param executionHelper the helper for executing SQL queries
   * @param readExecutor the read executor for reading stored Library resources
   */
  @Autowired
  public SqlQueryInstanceRunProvider(
      @Nonnull final SqlQueryExecutionHelper executionHelper,
      @Nonnull final ReadExecutor readExecutor) {
    this.executionHelper = executionHelper;
    this.readExecutor = readExecutor;
  }

  @Override
  public Class<Library> getResourceType() {
    return Library.class;
  }

  /**
   * Type-level {@code $sqlquery-run} operation that accepts a SQLQuery Library inline.
   *
   * @param queryResource the SQLQuery Library resource
   * @param format the output format (ndjson, csv, json, or parquet), overrides Accept header
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param parameterValues query parameter values to bind
   * @param requestDetails the servlet request details containing HTTP headers
   * @param response the HTTP response for streaming output
   */
  @Operation(name = "$sqlquery-run", idempotent = true, manualResponse = true)
  @OperationAccess("sqlquery-run")
  public void runTypeLevel(
      @Nonnull @OperationParam(name = "queryResource") final IBaseResource queryResource,
      @Nullable @OperationParam(name = "_format") final String format,
      @Nullable @OperationParam(name = "header") final BooleanType includeHeader,
      @Nullable @OperationParam(name = "_limit") final IntegerType limit,
      @Nullable @OperationParam(name = "parameter")
          final List<ParametersParameterComponent> parameterValues,
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final HttpServletResponse response) {

    final String acceptHeader = requestDetails.getServletRequest().getHeader("Accept");

    executionHelper.executeSqlQuery(
        queryResource, format, acceptHeader, includeHeader, limit, parameterValues, response);
  }

  /**
   * Instance-level {@code $sqlquery-run} operation that uses a stored Library by ID.
   *
   * @param libraryId the ID of the stored Library resource
   * @param format the output format (ndjson, csv, json, or parquet), overrides Accept header
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param parameterValues query parameter values to bind
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
      @Nullable @OperationParam(name = "parameter")
          final List<ParametersParameterComponent> parameterValues,
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final HttpServletResponse response) {

    final IBaseResource libraryResource = readLibrary(libraryId);
    final String acceptHeader = requestDetails.getServletRequest().getHeader("Accept");

    executionHelper.executeSqlQuery(
        libraryResource, format, acceptHeader, includeHeader, limit, parameterValues, response);
  }

  /** Reads a Library resource by ID. */
  @Nonnull
  private IBaseResource readLibrary(@Nonnull final IdType libraryId) {
    try {
      return readExecutor.read("Library", libraryId.getIdPart());
    } catch (final ResourceNotFoundError e) {
      throw new ResourceNotFoundException(
          "Library with ID '" + libraryId.getIdPart() + "' not found");
    } catch (final IllegalArgumentException e) {
      if (e.getMessage() != null && e.getMessage().contains("No data found for resource type")) {
        throw new ResourceNotFoundException(
            "Library with ID '" + libraryId.getIdPart() + "' not found");
      }
      throw e;
    }
  }
}
