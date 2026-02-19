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

package au.csiro.pathling.operations.bulkexport;

import static au.csiro.pathling.operations.bulkexport.SystemExportProvider.ELEMENTS_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.SystemExportProvider.OUTPUT_FORMAT_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.SystemExportProvider.SINCE_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.SystemExportProvider.TYPE_FILTER_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.SystemExportProvider.TYPE_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.SystemExportProvider.UNTIL_PARAM_NAME;

import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.bulkexport.ExportRequest.ExportLevel;
import au.csiro.pathling.operations.compartment.GroupMemberService;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Group;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provider for group-level bulk export operations.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class GroupExportProvider implements IResourceProvider, PreAsyncValidation<ExportRequest> {

  @Nonnull private final ExportOperationValidator exportOperationValidator;

  @Nonnull private final ExportOperationHelper exportOperationHelper;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final GroupMemberService groupMemberService;

  @Override
  public Class<Group> getResourceType() {
    return Group.class;
  }

  /**
   * Constructs a new GroupExportProvider.
   *
   * @param exportOperationValidator the export operation validator
   * @param exportOperationHelper the export operation helper
   * @param deltaLake the queryable data source
   * @param groupMemberService the group member service
   */
  @Autowired
  public GroupExportProvider(
      @Nonnull final ExportOperationValidator exportOperationValidator,
      @Nonnull final ExportOperationHelper exportOperationHelper,
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final GroupMemberService groupMemberService) {
    this.exportOperationValidator = exportOperationValidator;
    this.exportOperationHelper = exportOperationHelper;
    this.deltaLake = deltaLake;
    this.groupMemberService = groupMemberService;
  }

  /**
   * Handles the $export operation at the Group instance level (/Group/[id]/$export). Exports all
   * resources in the Patient compartment for all patients in the specified group.
   *
   * @param groupId the group ID
   * @param outputFormat the output format parameter
   * @param since the since date parameter
   * @param until the until date parameter
   * @param type the type parameter
   * @param typeFilter the type filter parameter
   * @param elements the elements parameter
   * @param requestDetails the request details
   * @return the binary result, or null if the job was cancelled
   */
  @Operation(name = "export", idempotent = true)
  @OperationAccess("export")
  @AsyncSupported
  @Nullable
  @SuppressWarnings("unused")
  public Parameters exportGroup(
      @IdParam final IdType groupId,
      @Nullable @OperationParam(name = OUTPUT_FORMAT_PARAM_NAME) final String outputFormat,
      @Nullable @OperationParam(name = SINCE_PARAM_NAME) final InstantType since,
      @Nullable @OperationParam(name = UNTIL_PARAM_NAME) final InstantType until,
      @Nullable @OperationParam(name = TYPE_PARAM_NAME) final List<String> type,
      @Nullable @OperationParam(name = TYPE_FILTER_PARAM_NAME) final List<String> typeFilter,
      @Nullable @OperationParam(name = ELEMENTS_PARAM_NAME) final List<String> elements,
      @Nonnull final ServletRequestDetails requestDetails) {
    return exportOperationHelper.executeExport(requestDetails);
  }

  @Override
  @SuppressWarnings("unchecked")
  @Nonnull
  public PreAsyncValidationResult<ExportRequest> preAsyncValidate(
      @Nonnull final ServletRequestDetails servletRequestDetails, @Nonnull final Object[] args) {
    // args = [groupId, outputFormat, since, until, type, typeFilter, elements, requestDetails]
    final IdType groupId = (IdType) args[0];
    final String outputFormat = (String) args[1];
    final InstantType since = (InstantType) args[2];
    final InstantType until = (InstantType) args[3];
    final List<String> type = (List<String>) args[4];
    final List<String> typeFilter = (List<String>) args[5];
    final List<String> elements = (List<String>) args[6];

    // Extract patient IDs from the group during validation.
    final Set<String> patientIds =
        groupMemberService.extractPatientIdsFromGroup(groupId.getIdPart());

    return exportOperationValidator.validatePatientExportRequest(
        servletRequestDetails,
        ExportLevel.GROUP,
        patientIds,
        outputFormat,
        since,
        until,
        type,
        typeFilter,
        elements);
  }
}
