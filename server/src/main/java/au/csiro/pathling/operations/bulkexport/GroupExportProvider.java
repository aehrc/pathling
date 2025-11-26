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

package au.csiro.pathling.operations.bulkexport;

import static au.csiro.pathling.operations.bulkexport.ExportProvider.ELEMENTS_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.ExportProvider.OUTPUT_FORMAT_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.ExportProvider.SINCE_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.ExportProvider.TYPE_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.ExportProvider.UNTIL_PARAM_NAME;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.bulkexport.ExportRequest.ExportLevel;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.Group;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
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

  private static final String PATIENT_REFERENCE_PREFIX = "Patient/";

  @Nonnull
  private final ExportOperationValidator exportOperationValidator;

  @Nonnull
  private final ExportOperationHelper exportOperationHelper;

  @Nonnull
  private final QueryableDataSource deltaLake;

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
   */
  @Autowired
  public GroupExportProvider(@Nonnull final ExportOperationValidator exportOperationValidator,
      @Nonnull final ExportOperationHelper exportOperationHelper,
      @Nonnull final QueryableDataSource deltaLake) {
    this.exportOperationValidator = exportOperationValidator;
    this.exportOperationHelper = exportOperationHelper;
    this.deltaLake = deltaLake;
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
   * @param elements the elements parameter
   * @param requestDetails the request details
   * @return the binary result, or null if the job was cancelled
   */
  @Operation(name = "export", idempotent = true)
  @OperationAccess("export")
  @AsyncSupported
  @Nullable
  @SuppressWarnings("unused")
  public Binary exportGroup(
      @IdParam final IdType groupId,
      @Nullable @OperationParam(name = OUTPUT_FORMAT_PARAM_NAME) final String outputFormat,
      @Nullable @OperationParam(name = SINCE_PARAM_NAME) final InstantType since,
      @Nullable @OperationParam(name = UNTIL_PARAM_NAME) final InstantType until,
      @Nullable @OperationParam(name = TYPE_PARAM_NAME) final List<String> type,
      @Nullable @OperationParam(name = ELEMENTS_PARAM_NAME) final List<String> elements,
      @Nonnull final ServletRequestDetails requestDetails
  ) {
    return exportOperationHelper.executeExport(requestDetails);
  }

  /**
   * Extracts patient IDs from a Group resource's members.
   *
   * @param groupId the group ID
   * @return the set of patient IDs
   */
  @Nonnull
  private Set<String> extractPatientIdsFromGroup(@Nonnull final String groupId) {
    // Read the Group dataset and filter by ID.
    final Dataset<Row> groupDataset = deltaLake.read("Group");
    final Dataset<Row> filtered = groupDataset.filter(col("id").equalTo(groupId));

    if (filtered.isEmpty()) {
      throw new ResourceNotFoundException("Group/" + groupId);
    }

    // Extract patient references from member.entity.reference.
    // The member array contains references to the group members, and entity.reference contains the
    // reference string (e.g., "Patient/123").
    final Dataset<Row> memberRefs = filtered
        .select(explode(col("member")).as("member"))
        .select(col("member.entity.reference").as("reference"))
        .filter(col("reference").startsWith(PATIENT_REFERENCE_PREFIX));

    final Set<String> patientIds = new HashSet<>();
    final List<Row> rows = memberRefs.collectAsList();
    for (final Row row : rows) {
      final String reference = row.getString(0);
      if (reference != null && reference.startsWith(PATIENT_REFERENCE_PREFIX)) {
        patientIds.add(reference.substring(PATIENT_REFERENCE_PREFIX.length()));
      }
    }

    log.debug("Extracted {} patient IDs from Group/{}", patientIds.size(), groupId);
    return patientIds;
  }

  @Override
  @SuppressWarnings("unchecked")
  @Nonnull
  public PreAsyncValidationResult<ExportRequest> preAsyncValidate(
      @Nonnull final ServletRequestDetails servletRequestDetails, @Nonnull final Object[] args) {
    // args = [groupId, outputFormat, since, until, type, elements, requestDetails]
    final IdType groupId = (IdType) args[0];
    final String outputFormat = (String) args[1];
    final InstantType since = (InstantType) args[2];
    final InstantType until = (InstantType) args[3];
    final List<String> type = (List<String>) args[4];
    final List<String> elements = (List<String>) args[5];

    // Extract patient IDs from the group during validation.
    final Set<String> patientIds = extractPatientIdsFromGroup(groupId.getIdPart());

    return exportOperationValidator.validatePatientExportRequest(
        servletRequestDetails,
        ExportLevel.GROUP,
        patientIds,
        outputFormat,
        since,
        until,
        type,
        elements);
  }

}
