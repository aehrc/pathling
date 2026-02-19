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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.operations.bulkexport.ExportRequest.ExportLevel;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link PatientExportProvider} covering resource type, pre-async validation for both
 * type-level and instance-level export, and delegation to the helper.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class PatientExportProviderTest {

  @Mock private ExportOperationValidator exportOperationValidator;
  @Mock private ExportOperationHelper exportOperationHelper;

  private PatientExportProvider provider;

  @BeforeEach
  void setUp() {
    provider = new PatientExportProvider(exportOperationValidator, exportOperationHelper);
  }

  @Test
  void getResourceTypeReturnsPatient() {
    // The provider should return Patient as its resource type.
    assertEquals(Patient.class, provider.getResourceType());
  }

  @Test
  void exportAllPatientsDelegatesToHelper() {
    // The exportAllPatients method should delegate to the helper.
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    final Parameters expected = new Parameters();
    when(exportOperationHelper.executeExport(requestDetails)).thenReturn(expected);

    final Parameters result =
        provider.exportAllPatients(null, null, null, null, null, null, requestDetails);

    assertEquals(expected, result);
  }

  @Test
  void exportSinglePatientDelegatesToHelper() {
    // The exportSinglePatient method should delegate to the helper.
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    final Parameters expected = new Parameters();
    when(exportOperationHelper.executeExport(requestDetails)).thenReturn(expected);

    final Parameters result =
        provider.exportSinglePatient(
            new IdType("Patient/123"), null, null, null, null, null, null, requestDetails);

    assertEquals(expected, result);
  }

  @Test
  void preAsyncValidateHandlesTypeLevelExport() {
    // Type-level export: args without IdType first arg.
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    final ExportRequest exportRequest =
        new ExportRequest(
            "http://localhost/fhir/Patient/$export",
            "http://localhost/fhir",
            null,
            null,
            null,
            List.of(),
            Map.of(),
            List.of(),
            false,
            ExportLevel.PATIENT_TYPE,
            Set.of());
    when(exportOperationValidator.validatePatientExportRequest(
            requestDetails, ExportLevel.PATIENT_TYPE, Set.of(), null, null, null, null, null, null))
        .thenReturn(
            new PreAsyncValidation.PreAsyncValidationResult<>(
                exportRequest, Collections.emptyList()));

    // Type-level args: [outputFormat, since, until, type, typeFilter, elements, requestDetails].
    final Object[] args = new Object[] {null, null, null, null, null, null, requestDetails};

    final PreAsyncValidation.PreAsyncValidationResult<ExportRequest> result =
        provider.preAsyncValidate(requestDetails, args);

    assertNotNull(result);
  }

  @Test
  void preAsyncValidateHandlesInstanceLevelExport() {
    // Instance-level export: args with IdType as first arg.
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    final IdType patientId = new IdType("Patient/456");
    final ExportRequest exportRequest =
        new ExportRequest(
            "http://localhost/fhir/Patient/456/$export",
            "http://localhost/fhir",
            null,
            null,
            null,
            List.of(),
            Map.of(),
            List.of(),
            false,
            ExportLevel.PATIENT_INSTANCE,
            Set.of("456"));
    when(exportOperationValidator.validatePatientExportRequest(
            requestDetails,
            ExportLevel.PATIENT_INSTANCE,
            Set.of("456"),
            null,
            null,
            null,
            null,
            null,
            null))
        .thenReturn(
            new PreAsyncValidation.PreAsyncValidationResult<>(
                exportRequest, Collections.emptyList()));

    // Instance-level args: [patientId, outputFormat, since, until, type, typeFilter, elements,
    // requestDetails].
    final Object[] args =
        new Object[] {patientId, null, null, null, null, null, null, requestDetails};

    final PreAsyncValidation.PreAsyncValidationResult<ExportRequest> result =
        provider.preAsyncValidate(requestDetails, args);

    assertNotNull(result);
  }
}
