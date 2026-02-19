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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
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
    final FhirContext fhirContext = FhirEncoders.contextFor(FhirVersionEnum.R4);
    final ServerConfiguration serverConfiguration = new ServerConfiguration();
    // Configure auth to be disabled to avoid NPE.
    final AuthorizationConfiguration authConfig = new AuthorizationConfiguration();
    authConfig.setEnabled(false);
    serverConfiguration.setAuth(authConfig);
    final PatientCompartmentService patientCompartmentService =
        new PatientCompartmentService(fhirContext);
    validator =
        new ExportOperationValidator(fhirContext, serverConfiguration, patientCompartmentService);

    requestDetails = mock(RequestDetails.class);
    when(requestDetails.getCompleteUrl()).thenReturn("http://localhost:8080/fhir/$export");
    // Mock the header methods to return valid values.
    when(requestDetails.getHeader(FhirServer.ACCEPT_HEADER.headerName()))
        .thenReturn("application/fhir+json");
    when(requestDetails.getHeaders(FhirServer.PREFER_RESPOND_TYPE_HEADER.headerName()))
        .thenReturn(List.of("respond-async"));
    when(requestDetails.getParameters()).thenReturn(java.util.Collections.emptyMap());
  }

  @Test
  @DisplayName("validateRequest should accept null outputFormat parameter")
  void validateRequest_shouldAcceptNullOutputFormat() {
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  validator.validateRequest(
                      requestDetails,
                      null, // outputFormat
                      null, // since
                      null, // until
                      null, // type
                      null // elements
                      );
              assertThat(result).isNotNull();
              assertThat(result.result()).isNotNull();
            });
  }

  @Test
  @DisplayName("validateRequest should accept null since parameter")
  void validateRequest_shouldAcceptNullSince() {
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  validator.validateRequest(
                      requestDetails,
                      "application/fhir+ndjson",
                      null, // since - this was incorrectly marked as @Nonnull
                      null, // until
                      null, // type
                      null // elements
                      );
              assertThat(result).isNotNull();
              assertThat(result.result()).isNotNull();
            });
  }

  @Test
  @DisplayName("validateRequest should accept all null optional parameters")
  void validateRequest_shouldAcceptAllNullOptionalParameters() {
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  validator.validateRequest(
                      requestDetails,
                      null, // outputFormat
                      null, // since
                      null, // until
                      null, // type
                      null // elements
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
    assertThatThrownBy(
            () -> validator.validateRequest(requestDetails, null, null, null, duplicateTypes, null))
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
    assertThatThrownBy(
            () -> validator.validateRequest(requestDetails, null, null, null, duplicateTypes, null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Duplicate resource type")
        .hasMessageContaining("Patient");
  }

  @Test
  @DisplayName("validateRequest should reject invalid output format")
  void validateRequest_shouldRejectInvalidOutputFormat() {
    assertThatThrownBy(
            () ->
                validator.validateRequest(
                    requestDetails,
                    "application/xml", // invalid format
                    null,
                    null,
                    null,
                    null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Unknown")
        .hasMessageContaining("_outputFormat");
  }

  @Test
  @DisplayName("validateRequest should reject invalid resource type in _type parameter")
  void validateRequest_shouldRejectInvalidResourceType() {
    final List<String> invalidTypes = List.of("NotAValidResourceType");

    assertThatThrownBy(
            () -> validator.validateRequest(requestDetails, null, null, null, invalidTypes, null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Failed to map '_type' value");
  }

  @Test
  @DisplayName("validateRequest should reject invalid element format in _elements parameter")
  void validateRequest_shouldRejectInvalidElementFormat() {
    final List<String> invalidElements = List.of("Patient.name.given.extra");

    assertThatThrownBy(
            () ->
                validator.validateRequest(requestDetails, null, null, null, null, invalidElements))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Failed to parse '_elements'");
  }

  @Test
  @DisplayName("validateRequest should reject invalid element for resource type")
  void validateRequest_shouldRejectInvalidElementForResourceType() {
    final List<String> invalidElements = List.of("Patient.notAValidElement");

    assertThatThrownBy(
            () ->
                validator.validateRequest(requestDetails, null, null, null, null, invalidElements))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Failed to parse element");
  }

  @Test
  @DisplayName("validateRequest should reject invalid resource type in _elements parameter")
  void validateRequest_shouldRejectInvalidResourceTypeInElements() {
    final List<String> invalidElements = List.of("InvalidResourceType.name");

    assertThatThrownBy(
            () ->
                validator.validateRequest(requestDetails, null, null, null, null, invalidElements))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Failed to parse resource type");
  }

  @Test
  @DisplayName("validateRequest should accept valid ndjson output format")
  void validateRequest_shouldAcceptValidOutputFormat() {
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  validator.validateRequest(
                      requestDetails, "application/fhir+ndjson", null, null, null, null);
              assertThat(result.result().outputFormat()).isEqualTo(ExportOutputFormat.NDJSON);
            });
  }

  @Test
  @DisplayName("validateRequest should accept parquet output format")
  void validateRequest_shouldAcceptParquetOutputFormat() {
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  validator.validateRequest(
                      requestDetails, "application/vnd.apache.parquet", null, null, null, null);
              assertThat(result.result().outputFormat()).isEqualTo(ExportOutputFormat.PARQUET);
            });
  }

  @Test
  @DisplayName("validateRequest should accept parquet shorthand format")
  void validateRequest_shouldAcceptParquetShorthandFormat() {
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  validator.validateRequest(requestDetails, "parquet", null, null, null, null);
              assertThat(result.result().outputFormat()).isEqualTo(ExportOutputFormat.PARQUET);
            });
  }

  @Test
  @DisplayName("validateRequest should reject delta output format")
  void validateRequest_shouldRejectDeltaOutputFormat() {
    // Delta is no longer supported for bulk export because it requires directory structure
    // that cannot be flattened for download.
    assertThatThrownBy(
            () ->
                validator.validateRequest(
                    requestDetails, "application/x-pathling-delta+parquet", null, null, null, null))
        .isInstanceOf(InvalidRequestException.class);
  }

  @Test
  @DisplayName("validateRequest should reject delta shorthand format")
  void validateRequest_shouldRejectDeltaShorthandFormat() {
    // Delta is no longer supported for bulk export.
    assertThatThrownBy(
            () -> validator.validateRequest(requestDetails, "delta", null, null, null, null))
        .isInstanceOf(InvalidRequestException.class);
  }

  @Test
  @DisplayName("validateRequest should reject unsupported query parameters in strict mode")
  void validateRequest_shouldRejectUnsupportedQueryParams() {
    when(requestDetails.getParameters())
        .thenReturn(java.util.Map.of("includeAssociatedData", new String[] {"value"}));

    assertThatThrownBy(
            () -> validator.validateRequest(requestDetails, null, null, null, null, null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("not supported");
  }

  @Test
  @DisplayName("validateRequest should accept unsupported query params in lenient mode")
  void validateRequest_shouldAcceptUnsupportedQueryParamsInLenientMode() {
    when(requestDetails.getParameters())
        .thenReturn(java.util.Map.of("includeAssociatedData", new String[] {"value"}));
    when(requestDetails.getHeaders(FhirServer.PREFER_LENIENT_HEADER.headerName()))
        .thenReturn(List.of("handling=lenient"));

    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  validator.validateRequest(requestDetails, null, null, null, null, null);
              assertThat(result).isNotNull();
              // Should have an informational warning about the unsupported param.
              assertThat(result.warnings()).isNotEmpty();
            });
  }

  @Test
  @DisplayName("validateRequest should accept element with no resource type")
  void validateRequest_shouldAcceptElementWithoutResourceType() {
    final List<String> elements = List.of("id");

    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  validator.validateRequest(requestDetails, null, null, null, null, elements);
              assertThat(result.result().elements()).hasSize(1);
              assertThat(result.result().elements().get(0).elementName()).isEqualTo("id");
              assertThat(result.result().elements().get(0).resourceTypeCode()).isNull();
            });
  }

  @Test
  @DisplayName("validateRequest should accept valid resource type and element combination")
  void validateRequest_shouldAcceptValidResourceTypeAndElement() {
    final List<String> elements = List.of("Patient.name");

    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  validator.validateRequest(requestDetails, null, null, null, null, elements);
              assertThat(result.result().elements()).hasSize(1);
              assertThat(result.result().elements().get(0).elementName()).isEqualTo("name");
              assertThat(result.result().elements().get(0).resourceTypeCode()).isEqualTo("Patient");
            });
  }

  // _typeFilter tests.

  @Test
  @DisplayName("validateRequest should parse valid _typeFilter into typeFilters map")
  void validateRequest_shouldParseValidTypeFilter() {
    // A valid _typeFilter value should be parsed into the typeFilters map on the ExportRequest.
    final List<String> typeFilter = List.of("Observation?code=8867-4");
    final PreAsyncValidationResult<ExportRequest> result =
        validator.validateRequest(requestDetails, null, null, null, null, typeFilter, null);

    assertThat(result.result().typeFilters()).containsKey("Observation");
    assertThat(result.result().typeFilters().get("Observation")).containsExactly("code=8867-4");
  }

  @Test
  @DisplayName("validateRequest should parse multiple _typeFilter values for same resource type")
  void validateRequest_shouldParseMultipleTypeFiltersForSameType() {
    // Multiple _typeFilter values targeting the same resource type should be grouped together.
    final List<String> typeFilter = List.of("Observation?code=8867-4", "Observation?code=8310-5");
    final PreAsyncValidationResult<ExportRequest> result =
        validator.validateRequest(requestDetails, null, null, null, null, typeFilter, null);

    assertThat(result.result().typeFilters()).containsKey("Observation");
    assertThat(result.result().typeFilters().get("Observation"))
        .containsExactly("code=8867-4", "code=8310-5");
  }

  @Test
  @DisplayName("validateRequest should parse _typeFilter values for different resource types")
  void validateRequest_shouldParseTypeFiltersForDifferentTypes() {
    // _typeFilter values for different resource types should be separated into different keys.
    final List<String> typeFilter = List.of("Observation?code=8867-4", "Condition?code=73211009");
    final PreAsyncValidationResult<ExportRequest> result =
        validator.validateRequest(requestDetails, null, null, null, null, typeFilter, null);

    assertThat(result.result().typeFilters()).hasSize(2);
    assertThat(result.result().typeFilters().get("Observation")).containsExactly("code=8867-4");
    assertThat(result.result().typeFilters().get("Condition")).containsExactly("code=73211009");
  }

  @Test
  @DisplayName("validateRequest should reject _typeFilter without question mark separator")
  void validateRequest_shouldRejectTypeFilterWithoutQuestionMark() {
    // A _typeFilter value must contain a '?' separator.
    final List<String> typeFilter = List.of("Observation");

    assertThatThrownBy(
            () ->
                validator.validateRequest(requestDetails, null, null, null, null, typeFilter, null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("_typeFilter")
        .hasMessageContaining("format");
  }

  @Test
  @DisplayName("validateRequest should reject _typeFilter with empty search query")
  void validateRequest_shouldRejectTypeFilterWithEmptySearchQuery() {
    // A _typeFilter value must have a non-empty search query after the '?'.
    final List<String> typeFilter = List.of("Observation?");

    assertThatThrownBy(
            () ->
                validator.validateRequest(requestDetails, null, null, null, null, typeFilter, null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("_typeFilter")
        .hasMessageContaining("empty");
  }

  @Test
  @DisplayName(
      "validateRequest should reject _typeFilter with invalid resource type in strict mode")
  void validateRequest_shouldRejectTypeFilterWithInvalidResourceType() {
    // An invalid resource type in _typeFilter should be rejected in strict mode.
    final List<String> typeFilter = List.of("FakeResource?status=active");

    assertThatThrownBy(
            () ->
                validator.validateRequest(requestDetails, null, null, null, null, typeFilter, null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("FakeResource");
  }

  @Test
  @DisplayName(
      "validateRequest should ignore _typeFilter with invalid resource type in lenient mode")
  void validateRequest_shouldIgnoreTypeFilterWithInvalidResourceTypeInLenientMode() {
    // In lenient mode, an invalid resource type in _typeFilter should be ignored.
    when(requestDetails.getHeaders(FhirServer.PREFER_LENIENT_HEADER.headerName()))
        .thenReturn(List.of("handling=lenient"));
    final List<String> typeFilter = List.of("FakeResource?status=active");

    final PreAsyncValidationResult<ExportRequest> result =
        validator.validateRequest(requestDetails, null, null, null, null, typeFilter, null);

    assertThat(result.result().typeFilters()).isEmpty();
    assertThat(result.warnings()).isNotEmpty();
  }

  @Test
  @DisplayName("validateRequest should reject _typeFilter type not in _type in strict mode")
  void validateRequest_shouldRejectTypeFilterTypeNotInType() {
    // When _type is specified and _typeFilter references a type not in _type, reject in strict
    // mode.
    final List<String> type = List.of("Patient");
    final List<String> typeFilter = List.of("Observation?code=8867-4");

    assertThatThrownBy(
            () ->
                validator.validateRequest(requestDetails, null, null, null, type, typeFilter, null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("_typeFilter")
        .hasMessageContaining("Observation")
        .hasMessageContaining("_type");
  }

  @Test
  @DisplayName("validateRequest should ignore _typeFilter type not in _type in lenient mode")
  void validateRequest_shouldIgnoreTypeFilterTypeNotInTypeInLenientMode() {
    // In lenient mode, _typeFilter entries for types not in _type should be silently ignored.
    when(requestDetails.getHeaders(FhirServer.PREFER_LENIENT_HEADER.headerName()))
        .thenReturn(List.of("handling=lenient"));
    final List<String> type = List.of("Patient");
    final List<String> typeFilter = List.of("Observation?code=8867-4");

    final PreAsyncValidationResult<ExportRequest> result =
        validator.validateRequest(requestDetails, null, null, null, type, typeFilter, null);

    assertThat(result.result().typeFilters()).isEmpty();
    assertThat(result.warnings()).isNotEmpty();
  }

  @Test
  @DisplayName("validateRequest should accept _typeFilter that matches subset of _type")
  void validateRequest_shouldAcceptTypeFilterMatchingSubsetOfType() {
    // When _type includes multiple types and _typeFilter targets a subset, it should be accepted.
    final List<String> type = List.of("Patient", "Observation");
    final List<String> typeFilter = List.of("Observation?code=8867-4");

    final PreAsyncValidationResult<ExportRequest> result =
        validator.validateRequest(requestDetails, null, null, null, type, typeFilter, null);

    assertThat(result.result().typeFilters()).containsKey("Observation");
    assertThat(result.result().includeResourceTypeFilters())
        .containsExactlyInAnyOrder("Patient", "Observation");
  }

  @Test
  @DisplayName(
      "validateRequest should implicitly include resource types from _typeFilter when _type is"
          + " absent")
  void validateRequest_shouldImplicitlyIncludeTypesFromTypeFilter() {
    // When _type is not provided but _typeFilter is, the resource types from _typeFilter should
    // become the effective type filter.
    final List<String> typeFilter = List.of("Observation?code=8867-4", "Condition?code=73211009");

    final PreAsyncValidationResult<ExportRequest> result =
        validator.validateRequest(requestDetails, null, null, null, null, typeFilter, null);

    assertThat(result.result().includeResourceTypeFilters())
        .containsExactlyInAnyOrder("Observation", "Condition");
    assertThat(result.result().typeFilters()).hasSize(2);
  }

  @Test
  @DisplayName("validateRequest should accept null _typeFilter parameter")
  void validateRequest_shouldAcceptNullTypeFilter() {
    // Null _typeFilter should result in an empty typeFilters map.
    final PreAsyncValidationResult<ExportRequest> result =
        validator.validateRequest(requestDetails, null, null, null, null, null, null);

    assertThat(result.result().typeFilters()).isEmpty();
  }

  @Test
  @DisplayName(
      "validateRequest should handle _typeFilter with multiple search criteria (compound query)")
  void validateRequest_shouldHandleTypeFilterWithMultipleSearchCriteria() {
    // A _typeFilter value can have multiple search criteria separated by '&'.
    final List<String> typeFilter = List.of("Observation?code=8867-4&date=ge2024-01-01");

    final PreAsyncValidationResult<ExportRequest> result =
        validator.validateRequest(requestDetails, null, null, null, null, typeFilter, null);

    assertThat(result.result().typeFilters()).containsKey("Observation");
    assertThat(result.result().typeFilters().get("Observation"))
        .containsExactly("code=8867-4&date=ge2024-01-01");
  }

  @Test
  @DisplayName(
      "validateRequest should handle _typeFilter for patient export with non-compartment type")
  void validateRequest_shouldFilterNonCompartmentTypeFilterInPatientExport() {
    // For patient-level exports, _typeFilter values referencing non-compartment resource types
    // should be silently ignored.
    final List<String> typeFilter = List.of("Organization?name=test");

    final PreAsyncValidationResult<ExportRequest> result =
        validator.validatePatientExportRequest(
            requestDetails,
            ExportRequest.ExportLevel.PATIENT_TYPE,
            java.util.Set.of(),
            null,
            null,
            null,
            null,
            typeFilter,
            null);

    assertThat(result.result().typeFilters()).isEmpty();
  }
}
