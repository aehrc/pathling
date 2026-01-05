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

import static au.csiro.pathling.operations.bulkexport.ExportOutputFormat.NDJSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.bulkexport.ExportOperationValidator;
import au.csiro.pathling.operations.bulkexport.ExportOutputFormat;
import au.csiro.pathling.operations.bulkexport.ExportRequest;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.MockUtil;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import java.util.List;
import org.hl7.fhir.r4.model.InstantType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

/**
 * Tests for exporting ViewDefinition resources via the $export operation.
 *
 * <p>These tests verify that ViewDefinition resources can be included in bulk export operations
 * using the _type parameter. ViewDefinition is a custom resource type from the SQL on FHIR
 * specification that should be treated like any other exportable resource.
 *
 * @author John Grimes
 */
@Import({
  ExportOperationValidator.class,
  FhirServerTestConfiguration.class,
  PatientCompartmentService.class
})
@SpringBootUnitTest
class ViewDefinitionExportTest {

  @Autowired private ExportOperationValidator exportOperationValidator;

  @SuppressWarnings("unused")
  @MockBean
  private QueryableDataSource queryableDataSource;

  private RequestDetails mockRequest;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
    mockRequest = MockUtil.mockRequest("application/fhir+json", "respond-async", false);
  }

  // -------------------------------------------------------------------------
  // System-level export tests
  // -------------------------------------------------------------------------

  @Test
  void exportValidatorAcceptsViewDefinitionInTypeParameter() {
    // Given: a system-level export request with _type=ViewDefinition.
    final String outputFormat = ExportOutputFormat.asParam(NDJSON);
    final InstantType now = InstantType.now();
    final List<String> types = List.of("ViewDefinition");

    // When: validating the request.
    // Then: validation should pass and ViewDefinition should be in the resource type filters.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  exportOperationValidator.validateRequest(
                      mockRequest, outputFormat, now, null, types, null);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().includeResourceTypeFilters()).contains("ViewDefinition");
            });
  }

  @Test
  void exportValidatorAcceptsViewDefinitionWithOtherTypes() {
    // Given: an export request with multiple resource types including ViewDefinition.
    final String outputFormat = ExportOutputFormat.asParam(NDJSON);
    final InstantType now = InstantType.now();
    final List<String> types = List.of("Patient", "ViewDefinition", "Observation");

    // When: validating the request.
    // Then: all resource types should be accepted.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  exportOperationValidator.validateRequest(
                      mockRequest, outputFormat, now, null, types, null);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().includeResourceTypeFilters()).hasSize(3);
            });
  }

  @Test
  void exportValidatorAcceptsViewDefinitionInLenientMode() {
    // Given: a lenient export request with ViewDefinition type.
    final RequestDetails lenientRequest =
        MockUtil.mockRequest("application/fhir+json", "respond-async", true);
    final String outputFormat = ExportOutputFormat.asParam(NDJSON);
    final InstantType now = InstantType.now();
    final List<String> types = List.of("ViewDefinition");

    // When: validating the request in lenient mode.
    // Then: validation should pass.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  exportOperationValidator.validateRequest(
                      lenientRequest, outputFormat, now, null, types, null);
              assertThat(result.result()).isNotNull();
            });
  }

  @Test
  void exportWithViewDefinitionOnlyReturnsViewDefinitions() {
    // Given: an export request with only ViewDefinition type.
    final String outputFormat = ExportOutputFormat.asParam(NDJSON);
    final InstantType now = InstantType.now();
    final List<String> types = List.of("ViewDefinition");

    // When: validating the request.
    // Then: only ViewDefinition should be in the filter.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  exportOperationValidator.validateRequest(
                      mockRequest, outputFormat, now, null, types, null);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().includeResourceTypeFilters()).hasSize(1);
            });
  }

  // -------------------------------------------------------------------------
  // Export format tests
  // -------------------------------------------------------------------------

  @Test
  void viewDefinitionExportSupportsNdjsonFormat() {
    // Given: an export request for ViewDefinition in NDJSON format.
    final String outputFormat = ExportOutputFormat.asParam(NDJSON);
    final List<String> types = List.of("ViewDefinition");

    // When: validating the request.
    // Then: the format should be NDJSON.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  exportOperationValidator.validateRequest(
                      mockRequest, outputFormat, null, null, types, null);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().outputFormat()).isEqualTo(NDJSON);
            });
  }

  // -------------------------------------------------------------------------
  // Time-based filtering tests
  // -------------------------------------------------------------------------

  @Test
  void viewDefinitionExportWithSinceParameter() {
    // Given: an export request with _since parameter.
    final String outputFormat = ExportOutputFormat.asParam(NDJSON);
    final InstantType since = InstantType.now();
    final List<String> types = List.of("ViewDefinition");

    // When: validating the request.
    // Then: the since parameter should be captured.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  exportOperationValidator.validateRequest(
                      mockRequest, outputFormat, since, null, types, null);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().since()).isEqualTo(since);
            });
  }

  @Test
  void viewDefinitionExportWithUntilParameter() {
    // Given: an export request with _until parameter.
    final String outputFormat = ExportOutputFormat.asParam(NDJSON);
    final InstantType since = InstantType.now();
    final InstantType until = InstantType.now();
    final List<String> types = List.of("ViewDefinition");

    // When: validating the request.
    // Then: both time parameters should be captured.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ExportRequest> result =
                  exportOperationValidator.validateRequest(
                      mockRequest, outputFormat, since, until, types, null);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().since()).isEqualTo(since);
              assertThat(result.result().until()).isEqualTo(until);
            });
  }
}
