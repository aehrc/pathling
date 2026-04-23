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
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provider for the system-level {@code $sqlquery-run} operation from the SQL on FHIR v2
 * specification. Executes a SQL query against materialised ViewDefinition tables.
 *
 * <p>This provides a system-level operation at {@code /fhir/$sqlquery-run} that accepts a SQLQuery
 * Library resource inline or by reference.
 *
 * @see <a
 *     href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/OperationDefinition-SQLQueryRun.html">SQLQueryRun</a>
 * @see SqlQueryInstanceRunProvider for type-level and instance-level operations
 */
@Component
public class SqlQueryRunProvider {

  @Nonnull private final SqlQueryExecutionHelper executionHelper;

  /**
   * Constructs a new SqlQueryRunProvider.
   *
   * @param executionHelper the helper for executing SQL queries
   */
  @Autowired
  public SqlQueryRunProvider(@Nonnull final SqlQueryExecutionHelper executionHelper) {
    this.executionHelper = executionHelper;
  }

  /**
   * Executes a SQL query provided inline as a Library resource and returns the results in the
   * requested format. This is the system-level operation at {@code /fhir/$sqlquery-run}.
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
  public void run(
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
}
