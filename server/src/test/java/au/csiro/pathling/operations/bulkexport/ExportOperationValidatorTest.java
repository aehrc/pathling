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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ExportOperationValidator.
 *
 * @author John Grimes
 */
class ExportOperationValidatorTest {

  private ExportOperationValidator validator;
  private RequestDetails requestDetails;

  @BeforeEach
  void setUp() {
    final FhirContext fhirContext = FhirContext.forR4();
    final ServerConfiguration serverConfiguration = new ServerConfiguration();
    // Configure auth to be disabled to avoid NPE.
    final AuthorizationConfiguration authConfig = new AuthorizationConfiguration();
    authConfig.setEnabled(false);
    serverConfiguration.setAuth(authConfig);
    final PatientCompartmentService patientCompartmentService =
        new PatientCompartmentService(fhirContext);
    validator = new ExportOperationValidator(fhirContext, serverConfiguration,
        patientCompartmentService);

    requestDetails = mock(RequestDetails.class);
    when(requestDetails.getCompleteUrl()).thenReturn("http://localhost:8080/fhir/$export");
    // Mock the header methods to return valid values.
    when(requestDetails.getHeader(FhirServer.ACCEPT_HEADER.headerName())).thenReturn(
        "application/fhir+json");
    when(requestDetails.getHeaders(FhirServer.PREFER_RESPOND_TYPE_HEADER.headerName())).thenReturn(
        List.of("respond-async"));
    when(requestDetails.getParameters()).thenReturn(java.util.Collections.emptyMap());
  }

  @Test
  @DisplayName("validateRequest should accept null outputFormat parameter")
  void validateRequest_shouldAcceptNullOutputFormat() {
    assertThatNoException().isThrownBy(() -> {
      final PreAsyncValidationResult<ExportRequest> result = validator.validateRequest(
          requestDetails,
          null,  // outputFormat
          null,  // since
          null,  // until
          null,  // type
          null   // elements
      );
      assertThat(result).isNotNull();
      assertThat(result.result()).isNotNull();
    });
  }

  @Test
  @DisplayName("validateRequest should accept null since parameter")
  void validateRequest_shouldAcceptNullSince() {
    assertThatNoException().isThrownBy(() -> {
      final PreAsyncValidationResult<ExportRequest> result = validator.validateRequest(
          requestDetails,
          "application/fhir+ndjson",
          null,  // since - this was incorrectly marked as @Nonnull
          null,  // until
          null,  // type
          null   // elements
      );
      assertThat(result).isNotNull();
      assertThat(result.result()).isNotNull();
    });
  }

  @Test
  @DisplayName("validateRequest should accept all null optional parameters")
  void validateRequest_shouldAcceptAllNullOptionalParameters() {
    assertThatNoException().isThrownBy(() -> {
      final PreAsyncValidationResult<ExportRequest> result = validator.validateRequest(
          requestDetails,
          null,  // outputFormat
          null,  // since
          null,  // until
          null,  // type
          null   // elements
      );
      assertThat(result).isNotNull();
      assertThat(result.result()).isNotNull();

      // Verify the created export request handles nulls correctly.
      final ExportRequest exportRequest = result.result();
      assertThat(exportRequest.since()).isNull();
      assertThat(exportRequest.until()).isNull();
      assertThat(exportRequest.outputFormat()).isNotNull(); // Should default to ND_JSON
      assertThat(exportRequest.includeResourceTypeFilters()).isEmpty();
      assertThat(exportRequest.elements()).isEmpty();
    });
  }

  @Test
  @DisplayName("validateRequest should reject duplicate resource types in _type parameter")
  void validateRequest_shouldRejectDuplicateResourceTypes() {
    // When: Creating an export request with duplicate resource types.
    final List<String> duplicateTypes = List.of("Patient", "Observation", "Patient");

    // Then: Should throw InvalidRequestException.
    assertThatThrownBy(() -> validator.validateRequest(
        requestDetails,
        null,
        null,
        null,
        duplicateTypes,
        null
    ))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Duplicate resource type")
        .hasMessageContaining("Patient");
  }

  @Test
  @DisplayName("validateRequest should reject duplicate types provided via comma-separated string")
  void validateRequest_shouldRejectDuplicateTypesInCommaSeparatedString() {
    // When: Creating an export request with duplicates in comma-separated format.
    final List<String> duplicateTypes = List.of("Patient,Observation,Patient");

    // Then: Should throw InvalidRequestException.
    assertThatThrownBy(() -> validator.validateRequest(
        requestDetails,
        null,
        null,
        null,
        duplicateTypes,
        null
    ))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Duplicate resource type")
        .hasMessageContaining("Patient");
  }
}
