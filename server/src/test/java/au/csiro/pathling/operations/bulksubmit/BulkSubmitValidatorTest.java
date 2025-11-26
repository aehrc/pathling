/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.bulksubmit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNoException;

import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.config.BulkSubmitConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.util.MockUtil;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import java.util.List;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UrlType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for BulkSubmitValidator.
 *
 * @author John Grimes
 */
class BulkSubmitValidatorTest {

  private static final String SUBMITTER_SYSTEM = "https://example.org/submitters";
  private static final String SUBMITTER_VALUE = "test-submitter";

  private BulkSubmitValidator validator;
  private ServerConfiguration serverConfiguration;

  @BeforeEach
  void setUp() {
    final BulkSubmitConfiguration bulkSubmitConfig = new BulkSubmitConfiguration();
    bulkSubmitConfig.setEnabled(true);
    bulkSubmitConfig.setAllowedSubmitters(
        List.of(new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE)));
    bulkSubmitConfig.setAllowableSources(List.of("https://"));

    serverConfiguration = new ServerConfiguration();
    serverConfiguration.setBulkSubmit(bulkSubmitConfig);

    validator = new BulkSubmitValidator(serverConfiguration);
  }

  @Test
  void validInProgressRequest() {
    final Parameters params = minimalInProgressParams();
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatNoException().isThrownBy(() -> {
      final PreAsyncValidationResult<BulkSubmitRequest> result =
          validator.validateRequest(mockRequest, params);
      assertThat(result.result()).isNotNull();
      assertThat(result.result().submissionId()).isEqualTo("test-submission-id");
      assertThat(result.result().submitter().system()).isEqualTo(SUBMITTER_SYSTEM);
      assertThat(result.result().submitter().value()).isEqualTo(SUBMITTER_VALUE);
      assertThat(result.result().isInProgress()).isTrue();
    });
  }

  @Test
  void validCompleteRequest() {
    final Parameters params = completeParams();
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatNoException().isThrownBy(() -> {
      final PreAsyncValidationResult<BulkSubmitRequest> result =
          validator.validateRequest(mockRequest, params);
      assertThat(result.result()).isNotNull();
      assertThat(result.result().isComplete()).isTrue();
      assertThat(result.result().manifestUrl()).isEqualTo("https://example.org/manifest.json");
      assertThat(result.result().fhirBaseUrl()).isEqualTo("https://example.org/fhir");
    });
  }

  @Test
  void validAbortedRequest() {
    final Parameters params = abortedParams();
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatNoException().isThrownBy(() -> {
      final PreAsyncValidationResult<BulkSubmitRequest> result =
          validator.validateRequest(mockRequest, params);
      assertThat(result.result()).isNotNull();
      assertThat(result.result().isAborted()).isTrue();
    });
  }

  @Test
  void missingSubmissionId() {
    final Parameters params = new Parameters();
    addSubmitter(params, SUBMITTER_SYSTEM, SUBMITTER_VALUE);
    addSubmissionStatus(params, BulkSubmitRequest.STATUS_IN_PROGRESS);

    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatCode(() -> validator.validateRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("submissionId");
  }

  @Test
  void missingSubmitter() {
    final Parameters params = new Parameters();
    params.addParameter().setName("submissionId").setValue(new StringType("test-id"));
    addSubmissionStatus(params, BulkSubmitRequest.STATUS_IN_PROGRESS);

    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatCode(() -> validator.validateRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("submitter");
  }

  @Test
  void missingSubmissionStatus() {
    final Parameters params = new Parameters();
    params.addParameter().setName("submissionId").setValue(new StringType("test-id"));
    addSubmitter(params, SUBMITTER_SYSTEM, SUBMITTER_VALUE);

    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatCode(() -> validator.validateRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("submissionStatus");
  }

  @Test
  void invalidSubmissionStatus() {
    final Parameters params = new Parameters();
    params.addParameter().setName("submissionId").setValue(new StringType("test-id"));
    addSubmitter(params, SUBMITTER_SYSTEM, SUBMITTER_VALUE);
    addSubmissionStatus(params, "invalid-status");

    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatCode(() -> validator.validateRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("Invalid submissionStatus");
  }

  @Test
  void completeStatusWithoutManifestUrl() {
    final Parameters params = new Parameters();
    params.addParameter().setName("submissionId").setValue(new StringType("test-id"));
    addSubmitter(params, SUBMITTER_SYSTEM, SUBMITTER_VALUE);
    addSubmissionStatus(params, BulkSubmitRequest.STATUS_COMPLETE);

    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatCode(() -> validator.validateRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("manifestUrl");
  }

  @Test
  void completeStatusWithoutFhirBaseUrl() {
    final Parameters params = new Parameters();
    params.addParameter().setName("submissionId").setValue(new StringType("test-id"));
    addSubmitter(params, SUBMITTER_SYSTEM, SUBMITTER_VALUE);
    addSubmissionStatus(params, BulkSubmitRequest.STATUS_COMPLETE);
    params.addParameter().setName("manifestUrl")
        .setValue(new UrlType("https://example.org/manifest.json"));

    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatCode(() -> validator.validateRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("fhirBaseUrl");
  }

  @Test
  void unauthorisedSubmitter() {
    final Parameters params = new Parameters();
    params.addParameter().setName("submissionId").setValue(new StringType("test-id"));
    addSubmitter(params, "https://example.org/submitters", "unauthorised-submitter");
    addSubmissionStatus(params, BulkSubmitRequest.STATUS_IN_PROGRESS);

    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatCode(() -> validator.validateRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("not in the list of allowed submitters");
  }

  @Test
  void invalidManifestUrlPrefix() {
    final BulkSubmitConfiguration config = new BulkSubmitConfiguration();
    config.setEnabled(true);
    config.setAllowedSubmitters(
        List.of(new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE)));
    config.setAllowableSources(List.of("https://allowed.org/"));
    serverConfiguration.setBulkSubmit(config);

    final Parameters params = completeParams();
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatCode(() -> validator.validateRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("does not match any allowed source prefixes");
  }

  @Test
  void requestWithFileRequestHeaders() {
    final Parameters params = completeParams();
    addFileRequestHeader(params, "Authorization", "Bearer token123");
    addFileRequestHeader(params, "X-Custom-Header", "custom-value");

    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    final PreAsyncValidationResult<BulkSubmitRequest> result =
        validator.validateRequest(mockRequest, params);

    assertThat(result.result().fileRequestHeaders()).hasSize(2);
    assertThat(result.result().fileRequestHeaders())
        .extracting(FileRequestHeader::name)
        .containsExactlyInAnyOrder("Authorization", "X-Custom-Header");
  }

  // ========================================
  // Helper Methods
  // ========================================

  private Parameters minimalInProgressParams() {
    final Parameters params = new Parameters();
    params.addParameter().setName("submissionId").setValue(new StringType("test-submission-id"));
    addSubmitter(params, SUBMITTER_SYSTEM, SUBMITTER_VALUE);
    addSubmissionStatus(params, BulkSubmitRequest.STATUS_IN_PROGRESS);
    return params;
  }

  private Parameters completeParams() {
    final Parameters params = new Parameters();
    params.addParameter().setName("submissionId").setValue(new StringType("test-submission-id"));
    addSubmitter(params, SUBMITTER_SYSTEM, SUBMITTER_VALUE);
    addSubmissionStatus(params, BulkSubmitRequest.STATUS_COMPLETE);
    params.addParameter().setName("manifestUrl")
        .setValue(new UrlType("https://example.org/manifest.json"));
    params.addParameter().setName("fhirBaseUrl").setValue(new UrlType("https://example.org/fhir"));
    return params;
  }

  private Parameters abortedParams() {
    final Parameters params = new Parameters();
    params.addParameter().setName("submissionId").setValue(new StringType("test-submission-id"));
    addSubmitter(params, SUBMITTER_SYSTEM, SUBMITTER_VALUE);
    addSubmissionStatus(params, BulkSubmitRequest.STATUS_ABORTED);
    return params;
  }

  private void addSubmitter(Parameters params, String system, String value) {
    final Identifier identifier = new Identifier();
    identifier.setSystem(system);
    identifier.setValue(value);
    params.addParameter().setName("submitter").setValue(identifier);
  }

  private void addSubmissionStatus(Parameters params, String status) {
    final Coding coding = new Coding();
    coding.setCode(status);
    params.addParameter().setName("submissionStatus").setValue(coding);
  }

  private void addFileRequestHeader(Parameters params, String name, String value) {
    final ParametersParameterComponent headerParam = params.addParameter();
    headerParam.setName("fileRequestHeader");
    headerParam.addPart().setName("name").setValue(new StringType(name));
    headerParam.addPart().setName("value").setValue(new StringType(value));
  }

}
