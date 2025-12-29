/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.view;

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
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provider for the system-level $viewdefinition-run operation from the SQL on FHIR specification.
 *
 * <p>This provides a system-level operation at /fhir/$viewdefinition-run that accepts a
 * ViewDefinition as a parameter.
 *
 * @author John Grimes
 * @see <a
 *     href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/OperationDefinition-ViewDefinitionRun.html">ViewDefinitionRun</a>
 * @see ViewDefinitionInstanceRunProvider for type-level and instance-level $run operations
 */
@Component
public class ViewDefinitionRunProvider {

  @Nonnull private final ViewExecutionHelper viewExecutionHelper;

  /**
   * Constructs a new ViewDefinitionRunProvider.
   *
   * @param viewExecutionHelper the helper for executing view queries
   */
  @Autowired
  public ViewDefinitionRunProvider(@Nonnull final ViewExecutionHelper viewExecutionHelper) {
    this.viewExecutionHelper = viewExecutionHelper;
  }

  /**
   * Executes a ViewDefinition provided inline and returns the results in NDJSON or CSV format with
   * chunked streaming. This is the system-level operation at /fhir/$viewdefinition-run.
   *
   * @param viewResource the ViewDefinition resource
   * @param format the output format (ndjson or csv), overrides Accept header if provided
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param patientIds patient IDs to filter by
   * @param groupIds group IDs to filter by
   * @param since filter by meta.lastUpdated >= value
   * @param inlineResources FHIR resources to use instead of the main data source
   * @param requestDetails the servlet request details containing HTTP headers
   * @param response the HTTP response for streaming output
   */
  @SuppressWarnings("java:S107")
  @Operation(name = "$viewdefinition-run", idempotent = true, manualResponse = true)
  @OperationAccess("view-run")
  public void run(
      @Nonnull @OperationParam(name = "viewResource") final IBaseResource viewResource,
      @Nullable @OperationParam(name = "_format") final String format,
      @Nullable @OperationParam(name = "header") final BooleanType includeHeader,
      @Nullable @OperationParam(name = "_limit") final IntegerType limit,
      @Nullable @OperationParam(name = "patient") final List<String> patientIds,
      @Nullable @OperationParam(name = "group") final List<IdType> groupIds,
      @Nullable @OperationParam(name = "_since") final InstantType since,
      @Nullable @OperationParam(name = "resource") final List<String> inlineResources,
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final HttpServletResponse response) {

    final String acceptHeader = requestDetails.getServletRequest().getHeader("Accept");

    viewExecutionHelper.executeView(
        viewResource,
        format,
        acceptHeader,
        includeHeader,
        limit,
        patientIds,
        groupIds,
        since,
        inlineResources,
        response);
  }
}
