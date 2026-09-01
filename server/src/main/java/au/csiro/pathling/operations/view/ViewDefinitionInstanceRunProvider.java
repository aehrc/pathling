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

package au.csiro.pathling.operations.view;

import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
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
import org.hl7.fhir.r4.model.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provider for the type-level and instance-level $run operations for ViewDefinition resources.
 *
 * <p>This provides operations at:
 *
 * <ul>
 *   <li>{@code POST /fhir/ViewDefinition/$run} - Type-level operation with a viewResource or
 *       viewReference parameter
 *   <li>{@code GET /fhir/ViewDefinition/[id]/$run} - Instance-level operation using stored
 *       ViewDefinition
 *   <li>{@code POST /fhir/ViewDefinition/[id]/$run} - Instance-level operation with additional
 *       parameters
 * </ul>
 *
 * @author John Grimes
 * @see <a
 *     href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/OperationDefinition-ViewDefinitionRun.html">ViewDefinitionRun</a>
 * @see ViewDefinitionRunProvider for system-level $viewdefinition-run operation
 */
@Component
public class ViewDefinitionInstanceRunProvider implements IResourceProvider {

  @Nonnull private final ViewExecutionHelper viewExecutionHelper;

  /**
   * Constructs a new ViewDefinitionInstanceRunProvider.
   *
   * @param viewExecutionHelper the helper for executing view queries and resolving stored
   *     ViewDefinitions
   */
  @Autowired
  public ViewDefinitionInstanceRunProvider(@Nonnull final ViewExecutionHelper viewExecutionHelper) {
    this.viewExecutionHelper = viewExecutionHelper;
  }

  @Override
  public Class<ViewDefinitionResource> getResourceType() {
    return ViewDefinitionResource.class;
  }

  /**
   * Type-level $run operation that accepts a ViewDefinition inline ({@code viewResource}) or by
   * reference ({@code viewReference}).
   *
   * @param viewResource the inline ViewDefinition resource (mutually exclusive with viewReference)
   * @param viewReference a reference to a stored ViewDefinition (mutually exclusive with
   *     viewResource)
   * @param format the output format (ndjson, csv, or json), overrides Accept header if provided
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param patient a single patient reference to filter by
   * @param group group references to filter by
   * @param since filter by meta.lastUpdated >= value
   * @param inlineResources FHIR resources to use instead of the main data source
   * @param source the unsupported external data source parameter, rejected when supplied
   * @param requestDetails the servlet request details containing HTTP headers
   * @param response the HTTP response for streaming output
   */
  @SuppressWarnings("java:S107")
  @Operation(name = "$run", idempotent = true, manualResponse = true)
  @OperationAccess("view-run")
  public void runTypeLevel(
      @Nullable @OperationParam(name = "viewResource") final IBaseResource viewResource,
      @Nullable @OperationParam(name = "viewReference") final Reference viewReference,
      @Nullable @OperationParam(name = "_format") final String format,
      @Nullable @OperationParam(name = "header") final BooleanType includeHeader,
      @Nullable @OperationParam(name = "_limit") final IntegerType limit,
      @Nullable @OperationParam(name = "patient", max = OperationParam.MAX_UNLIMITED)
          final List<Reference> patient,
      @Nullable @OperationParam(name = "group", max = OperationParam.MAX_UNLIMITED)
          final List<Reference> group,
      @Nullable @OperationParam(name = "_since") final InstantType since,
      @Nullable @OperationParam(name = "resource") final List<String> inlineResources,
      @Nullable @OperationParam(name = "source") final String source,
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final HttpServletResponse response) {

    viewExecutionHelper.rejectSourceParameter(source);

    final IBaseResource view = viewExecutionHelper.resolveViewInput(viewResource, viewReference);
    final List<String> patientIds = viewExecutionHelper.toPatientIds(patient);
    final List<IdType> groupIds = viewExecutionHelper.toGroupIds(group);
    final String acceptHeader = requestDetails.getServletRequest().getHeader("Accept");

    viewExecutionHelper.executeView(
        view,
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

  /**
   * Instance-level $run operation that uses a stored ViewDefinition by ID. The view is inferred
   * from the path; a {@code viewReference} parameter is neither required nor consulted. Supports
   * both GET and POST requests.
   *
   * @param viewDefinitionId the ID of the stored ViewDefinition
   * @param format the output format (ndjson, csv, or json), overrides Accept header if provided
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param patient a single patient reference to filter by
   * @param group group references to filter by
   * @param since filter by meta.lastUpdated >= value
   * @param inlineResources FHIR resources to use instead of the main data source
   * @param source the unsupported external data source parameter, rejected when supplied
   * @param requestDetails the servlet request details containing HTTP headers
   * @param response the HTTP response for streaming output
   */
  @SuppressWarnings("java:S107")
  @Operation(name = "$run", idempotent = true, manualResponse = true)
  @OperationAccess("view-run")
  public void runById(
      @IdParam final IdType viewDefinitionId,
      @Nullable @OperationParam(name = "_format") final String format,
      @Nullable @OperationParam(name = "header") final BooleanType includeHeader,
      @Nullable @OperationParam(name = "_limit") final IntegerType limit,
      @Nullable @OperationParam(name = "patient", max = OperationParam.MAX_UNLIMITED)
          final List<Reference> patient,
      @Nullable @OperationParam(name = "group", max = OperationParam.MAX_UNLIMITED)
          final List<Reference> group,
      @Nullable @OperationParam(name = "_since") final InstantType since,
      @Nullable @OperationParam(name = "resource") final List<String> inlineResources,
      @Nullable @OperationParam(name = "source") final String source,
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final HttpServletResponse response) {

    viewExecutionHelper.rejectSourceParameter(source);

    // The view is inferred from the path id; viewReference is not consulted at the instance level.
    final IBaseResource view =
        viewExecutionHelper.readStoredViewDefinition(viewDefinitionId.getIdPart());
    final List<String> patientIds = viewExecutionHelper.toPatientIds(patient);
    final List<IdType> groupIds = viewExecutionHelper.toGroupIds(group);
    final String acceptHeader = requestDetails.getServletRequest().getHeader("Accept");

    viewExecutionHelper.executeView(
        view,
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
