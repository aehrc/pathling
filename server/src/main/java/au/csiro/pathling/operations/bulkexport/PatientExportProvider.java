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

import static au.csiro.pathling.operations.bulkexport.SystemExportProvider.ELEMENTS_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.SystemExportProvider.OUTPUT_FORMAT_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.SystemExportProvider.SINCE_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.SystemExportProvider.TYPE_PARAM_NAME;
import static au.csiro.pathling.operations.bulkexport.SystemExportProvider.UNTIL_PARAM_NAME;

import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.operations.bulkexport.ExportRequest.ExportLevel;
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
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Patient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provider for patient-level bulk export operations.
 *
 * @author John Grimes
 */
@Component
public class PatientExportProvider implements IResourceProvider, PreAsyncValidation<ExportRequest> {

  @Nonnull private final ExportOperationValidator exportOperationValidator;

  @Nonnull private final ExportOperationHelper exportOperationHelper;

  @Override
  public Class<Patient> getResourceType() {
    return Patient.class;
  }

  /**
   * Constructs a new PatientExportProvider.
   *
   * @param exportOperationValidator the export operation validator
   * @param exportOperationHelper the export operation helper
   */
  @Autowired
  public PatientExportProvider(
      @Nonnull final ExportOperationValidator exportOperationValidator,
      @Nonnull final ExportOperationHelper exportOperationHelper) {
    this.exportOperationValidator = exportOperationValidator;
    this.exportOperationHelper = exportOperationHelper;
  }

  /**
   * Handles the $export operation at the Patient type level (/Patient/$export). Exports all
   * resources in the Patient compartment for all patients.
   *
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
  public Parameters exportAllPatients(
      @Nullable @OperationParam(name = OUTPUT_FORMAT_PARAM_NAME) final String outputFormat,
      @Nullable @OperationParam(name = SINCE_PARAM_NAME) final InstantType since,
      @Nullable @OperationParam(name = UNTIL_PARAM_NAME) final InstantType until,
      @Nullable @OperationParam(name = TYPE_PARAM_NAME) final List<String> type,
      @Nullable @OperationParam(name = ELEMENTS_PARAM_NAME) final List<String> elements,
      @Nonnull final ServletRequestDetails requestDetails) {
    return exportOperationHelper.executeExport(requestDetails);
  }

  /**
   * Handles the $export operation at the Patient instance level (/Patient/[id]/$export). Exports
   * all resources in the Patient compartment for a specific patient.
   *
   * @param patientId the patient ID
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
  public Parameters exportSinglePatient(
      @IdParam final IdType patientId,
      @Nullable @OperationParam(name = OUTPUT_FORMAT_PARAM_NAME) final String outputFormat,
      @Nullable @OperationParam(name = SINCE_PARAM_NAME) final InstantType since,
      @Nullable @OperationParam(name = UNTIL_PARAM_NAME) final InstantType until,
      @Nullable @OperationParam(name = TYPE_PARAM_NAME) final List<String> type,
      @Nullable @OperationParam(name = ELEMENTS_PARAM_NAME) final List<String> elements,
      @Nonnull final ServletRequestDetails requestDetails) {
    return exportOperationHelper.executeExport(requestDetails);
  }

  @Override
  @SuppressWarnings("unchecked")
  @Nonnull
  public PreAsyncValidationResult<ExportRequest> preAsyncValidate(
      @Nonnull final ServletRequestDetails servletRequestDetails, @Nonnull final Object[] args) {
    // Determine if this is a type-level or instance-level operation based on args.
    // Type-level: args = [outputFormat, since, until, type, elements, requestDetails]
    // Instance-level: args = [patientId, outputFormat, since, until, type, elements,
    // requestDetails]
    final boolean isInstanceLevel = args.length > 0 && args[0] instanceof IdType;

    final ExportLevel exportLevel;
    final Set<String> patientIds;
    final String outputFormat;
    final InstantType since;
    final InstantType until;
    final List<String> type;
    final List<String> elements;

    if (isInstanceLevel) {
      final IdType patientId = (IdType) args[0];
      exportLevel = ExportLevel.PATIENT_INSTANCE;
      patientIds = Set.of(patientId.getIdPart());
      outputFormat = (String) args[1];
      since = (InstantType) args[2];
      until = (InstantType) args[3];
      type = (List<String>) args[4];
      elements = (List<String>) args[5];
    } else {
      exportLevel = ExportLevel.PATIENT_TYPE;
      patientIds = Set.of();
      outputFormat = (String) args[0];
      since = (InstantType) args[1];
      until = (InstantType) args[2];
      type = (List<String>) args[3];
      elements = (List<String>) args[4];
    }

    return exportOperationValidator.validatePatientExportRequest(
        servletRequestDetails, exportLevel, patientIds, outputFormat, since, until, type, elements);
  }
}
