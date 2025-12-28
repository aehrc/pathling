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

import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
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
 * Provider for the type-level and instance-level $run operations for ViewDefinition resources.
 *
 * <p>This provides operations at:
 *
 * <ul>
 *   <li>{@code POST /fhir/ViewDefinition/$run} - Type-level operation with viewResource parameter
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

  @Nonnull private final ReadExecutor readExecutor;

  /**
   * Constructs a new ViewDefinitionInstanceRunProvider.
   *
   * @param viewExecutionHelper the helper for executing view queries
   * @param readExecutor the read executor for reading stored ViewDefinitions
   */
  @Autowired
  public ViewDefinitionInstanceRunProvider(
      @Nonnull final ViewExecutionHelper viewExecutionHelper,
      @Nonnull final ReadExecutor readExecutor) {
    this.viewExecutionHelper = viewExecutionHelper;
    this.readExecutor = readExecutor;
  }

  @Override
  public Class<ViewDefinitionResource> getResourceType() {
    return ViewDefinitionResource.class;
  }

  /**
   * Type-level $run operation that accepts a ViewDefinition inline.
   *
   * @param viewResource the ViewDefinition resource
   * @param format the output format (ndjson or csv)
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param patientIds patient IDs to filter by
   * @param groupIds group IDs to filter by
   * @param since filter by meta.lastUpdated >= value
   * @param inlineResources FHIR resources to use instead of the main data source
   * @param response the HTTP response for streaming output
   */
  @SuppressWarnings("java:S107")
  @Operation(name = "$run", idempotent = true, manualResponse = true)
  @OperationAccess("view-run")
  public void runTypeLevel(
      @Nonnull @OperationParam(name = "viewResource") final IBaseResource viewResource,
      @Nullable @OperationParam(name = "_format") final String format,
      @Nullable @OperationParam(name = "header") final BooleanType includeHeader,
      @Nullable @OperationParam(name = "_limit") final IntegerType limit,
      @Nullable @OperationParam(name = "patient") final List<String> patientIds,
      @Nullable @OperationParam(name = "group") final List<IdType> groupIds,
      @Nullable @OperationParam(name = "_since") final InstantType since,
      @Nullable @OperationParam(name = "resource") final List<String> inlineResources,
      @Nullable final HttpServletResponse response) {

    viewExecutionHelper.executeView(
        viewResource,
        format,
        includeHeader,
        limit,
        patientIds,
        groupIds,
        since,
        inlineResources,
        response);
  }

  /**
   * Instance-level $run operation that uses a stored ViewDefinition by ID. Supports both GET and
   * POST requests.
   *
   * @param viewDefinitionId the ID of the stored ViewDefinition
   * @param format the output format (ndjson or csv)
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param patientIds patient IDs to filter by
   * @param groupIds group IDs to filter by
   * @param since filter by meta.lastUpdated >= value
   * @param inlineResources FHIR resources to use instead of the main data source
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
      @Nullable @OperationParam(name = "patient") final List<String> patientIds,
      @Nullable @OperationParam(name = "group") final List<IdType> groupIds,
      @Nullable @OperationParam(name = "_since") final InstantType since,
      @Nullable @OperationParam(name = "resource") final List<String> inlineResources,
      @Nullable final HttpServletResponse response) {

    // Read the ViewDefinition by ID.
    final IBaseResource viewResource = readViewDefinition(viewDefinitionId);

    viewExecutionHelper.executeView(
        viewResource,
        format,
        includeHeader,
        limit,
        patientIds,
        groupIds,
        since,
        inlineResources,
        response);
  }

  /** Reads a ViewDefinition resource by ID. */
  @Nonnull
  private IBaseResource readViewDefinition(@Nonnull final IdType viewDefinitionId) {
    try {
      return readExecutor.read("ViewDefinition", viewDefinitionId.getIdPart());
    } catch (final ResourceNotFoundError e) {
      throw new ResourceNotFoundException(
          "ViewDefinition with ID '" + viewDefinitionId.getIdPart() + "' not found");
    } catch (final IllegalArgumentException e) {
      // Handle case where no ViewDefinition data exists in the data source.
      if (e.getMessage() != null && e.getMessage().contains("No data found for resource type")) {
        throw new ResourceNotFoundException(
            "ViewDefinition with ID '" + viewDefinitionId.getIdPart() + "' not found");
      }
      throw e;
    }
  }
}
