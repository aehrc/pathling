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

package au.csiro.pathling.operations.bulksubmit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Unit tests for BulkSubmitProvider.
 *
 * @author John Grimes
 */
class BulkSubmitProviderTest {

  private static final String SUBMITTER_SYSTEM = "https://example.org/submitters";
  private static final String SUBMITTER_VALUE = "test-submitter";
  private static final String SUBMISSION_ID = "test-submission-123";
  private static final String FHIR_SERVER_BASE = "http://localhost:8080/fhir";
  private static final String MANIFEST_URL = "https://example.org/manifest.json";
  private static final String FHIR_BASE_URL = "https://example.org/fhir";

  private BulkSubmitValidator validator;
  private SubmissionRegistry submissionRegistry;
  private BulkSubmitExecutor executor;
  private BulkSubmitProvider provider;
  private ServletRequestDetails requestDetails;
  private SubmitterIdentifier submitterIdentifier;

  @BeforeEach
  void setUp() {
    validator = mock(BulkSubmitValidator.class);
    submissionRegistry = mock(SubmissionRegistry.class);
    executor = mock(BulkSubmitExecutor.class);

    provider = new BulkSubmitProvider(validator, submissionRegistry, executor);

    requestDetails = mock(ServletRequestDetails.class);
    when(requestDetails.getFhirServerBase()).thenReturn(FHIR_SERVER_BASE);

    submitterIdentifier = new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE);

    // Set up security context.
    final SecurityContext securityContext = mock(SecurityContext.class);
    final Authentication authentication = mock(Authentication.class);
    when(securityContext.getAuthentication()).thenReturn(authentication);
    SecurityContextHolder.setContext(securityContext);
  }

  // ========================================
  // In-Progress Submission Tests
  // ========================================

  @Test
  void createsNewSubmissionForInProgressRequest() {
    // Given: an in-progress request with manifest URL.
    final BulkSubmitRequest request =
        new BulkSubmitRequest(
            FHIR_SERVER_BASE + "/$bulk-submit",
            SUBMISSION_ID,
            submitterIdentifier,
            BulkSubmitRequest.STATUS_IN_PROGRESS,
            MANIFEST_URL,
            FHIR_BASE_URL,
            null,
            null,
            null,
            List.of());
    when(validator.validateAndExtract(any(), any())).thenReturn(request);
    when(submissionRegistry.get(submitterIdentifier, SUBMISSION_ID)).thenReturn(Optional.empty());

    // When: calling the operation.
    final Parameters result = provider.bulkSubmitOperation(new Parameters(), requestDetails);

    // Then: should create submission and return acknowledgement.
    verify(submissionRegistry, org.mockito.Mockito.atLeast(1)).put(any(Submission.class));
    verify(executor).downloadManifestJob(any(), any(), any(), eq(FHIR_SERVER_BASE));
    assertThat(result.getParameter("submissionId")).isNotNull();
    assertThat(((StringType) result.getParameter("submissionId").getValue()).getValue())
        .isEqualTo(SUBMISSION_ID);
  }

  @Test
  void updatesExistingPendingSubmission() {
    // Given: an in-progress request for existing pending submission.
    final BulkSubmitRequest request =
        new BulkSubmitRequest(
            FHIR_SERVER_BASE + "/$bulk-submit",
            SUBMISSION_ID,
            submitterIdentifier,
            BulkSubmitRequest.STATUS_IN_PROGRESS,
            MANIFEST_URL,
            FHIR_BASE_URL,
            null,
            null,
            null,
            List.of());
    when(validator.validateAndExtract(any(), any())).thenReturn(request);

    final Submission existingSubmission =
        Submission.createPending(SUBMISSION_ID, submitterIdentifier, Optional.empty());
    when(submissionRegistry.get(submitterIdentifier, SUBMISSION_ID))
        .thenReturn(Optional.of(existingSubmission));

    // When: calling the operation.
    final Parameters result = provider.bulkSubmitOperation(new Parameters(), requestDetails);

    // Then: should update submission.
    verify(submissionRegistry).put(any(Submission.class));
    assertThat(result.getParameter("status")).isNotNull();
  }

  @Test
  void rejectsUpdateToCompletedSubmission() {
    // Given: an in-progress request for already completed submission.
    final BulkSubmitRequest request =
        new BulkSubmitRequest(
            FHIR_SERVER_BASE + "/$bulk-submit",
            SUBMISSION_ID,
            submitterIdentifier,
            BulkSubmitRequest.STATUS_IN_PROGRESS,
            MANIFEST_URL,
            FHIR_BASE_URL,
            null,
            null,
            null,
            List.of());
    when(validator.validateAndExtract(any(), any())).thenReturn(request);

    final Submission completedSubmission =
        Submission.createPending(SUBMISSION_ID, submitterIdentifier, Optional.empty())
            .withState(SubmissionState.COMPLETED);
    when(submissionRegistry.get(submitterIdentifier, SUBMISSION_ID))
        .thenReturn(Optional.of(completedSubmission));

    // When/Then: should throw InvalidRequestException.
    assertThatThrownBy(() -> provider.bulkSubmitOperation(new Parameters(), requestDetails))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Cannot update submission")
        .hasMessageContaining("COMPLETED");
  }

  @Test
  void handlesInProgressWithoutManifestUrl() {
    // Given: an in-progress request without manifest URL (notification only).
    final BulkSubmitRequest request =
        new BulkSubmitRequest(
            FHIR_SERVER_BASE + "/$bulk-submit",
            SUBMISSION_ID,
            submitterIdentifier,
            BulkSubmitRequest.STATUS_IN_PROGRESS,
            null, // no manifest URL
            null,
            null,
            null,
            null,
            List.of());
    when(validator.validateAndExtract(any(), any())).thenReturn(request);
    when(submissionRegistry.get(submitterIdentifier, SUBMISSION_ID)).thenReturn(Optional.empty());

    // When: calling the operation.
    final Parameters result = provider.bulkSubmitOperation(new Parameters(), requestDetails);

    // Then: should create submission but not start download.
    verify(submissionRegistry).put(any(Submission.class));
    verify(executor, never()).downloadManifestJob(any(), any(), any(), any());
    assertThat(result.getParameter("submissionId")).isNotNull();
  }

  @Test
  void handlesReplacesManifestUrl() {
    // Given: a request that replaces an existing manifest.
    final String oldManifestUrl = "https://example.org/old-manifest.json";
    final BulkSubmitRequest request =
        new BulkSubmitRequest(
            FHIR_SERVER_BASE + "/$bulk-submit",
            SUBMISSION_ID,
            submitterIdentifier,
            BulkSubmitRequest.STATUS_IN_PROGRESS,
            MANIFEST_URL,
            FHIR_BASE_URL,
            oldManifestUrl,
            null,
            null,
            List.of());
    when(validator.validateAndExtract(any(), any())).thenReturn(request);

    // Create existing submission with the old manifest job.
    final ManifestJob oldJob =
        ManifestJob.createPending("old-job-id", oldManifestUrl, FHIR_BASE_URL, null);
    final Submission existingSubmission =
        new Submission(
            SUBMISSION_ID,
            submitterIdentifier,
            SubmissionState.PROCESSING,
            new ArrayList<>(List.of(oldJob)),
            "2024-01-01T00:00:00Z",
            null,
            null,
            null,
            null);
    when(submissionRegistry.get(submitterIdentifier, SUBMISSION_ID))
        .thenReturn(Optional.of(existingSubmission));

    // When: calling the operation.
    final Parameters result = provider.bulkSubmitOperation(new Parameters(), requestDetails);

    // Then: should abort old job and add new one.
    verify(executor).abortManifestJob(eq(existingSubmission), eq(oldJob));
    verify(executor).downloadManifestJob(any(), any(), any(), eq(FHIR_SERVER_BASE));
    assertThat(result.getParameter("status")).isNotNull();
  }

  @Test
  void rejectsReplacesManifestUrlWhenNotFound() {
    // Given: a request with replacesManifestUrl that doesn't exist.
    final BulkSubmitRequest request =
        new BulkSubmitRequest(
            FHIR_SERVER_BASE + "/$bulk-submit",
            SUBMISSION_ID,
            submitterIdentifier,
            BulkSubmitRequest.STATUS_IN_PROGRESS,
            MANIFEST_URL,
            FHIR_BASE_URL,
            "https://example.org/nonexistent.json",
            null,
            null,
            List.of());
    when(validator.validateAndExtract(any(), any())).thenReturn(request);

    final Submission existingSubmission =
        Submission.createPending(SUBMISSION_ID, submitterIdentifier, Optional.empty())
            .withState(SubmissionState.PROCESSING);
    when(submissionRegistry.get(submitterIdentifier, SUBMISSION_ID))
        .thenReturn(Optional.of(existingSubmission));

    // When/Then: should throw InvalidRequestException.
    assertThatThrownBy(() -> provider.bulkSubmitOperation(new Parameters(), requestDetails))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Cannot replace manifest");
  }

  // ========================================
  // Complete Submission Tests
  // ========================================

  @Test
  void rejectsCompleteWithManifestUrl() {
    // Given: a complete request with manifest URL (not allowed).
    final BulkSubmitRequest request =
        new BulkSubmitRequest(
            FHIR_SERVER_BASE + "/$bulk-submit",
            SUBMISSION_ID,
            submitterIdentifier,
            BulkSubmitRequest.STATUS_COMPLETE,
            MANIFEST_URL, // not allowed for complete
            FHIR_BASE_URL,
            null,
            null,
            null,
            List.of());
    when(validator.validateAndExtract(any(), any())).thenReturn(request);

    final Submission existingSubmission =
        Submission.createPending(SUBMISSION_ID, submitterIdentifier, Optional.empty());
    when(submissionRegistry.get(submitterIdentifier, SUBMISSION_ID))
        .thenReturn(Optional.of(existingSubmission));

    // When/Then: should throw InvalidRequestException.
    assertThatThrownBy(() -> provider.bulkSubmitOperation(new Parameters(), requestDetails))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Cannot add manifest when completing");
  }

  @Test
  void rejectsCompleteWithNoManifests() {
    // Given: a complete request for submission with no manifests.
    final BulkSubmitRequest request =
        new BulkSubmitRequest(
            FHIR_SERVER_BASE + "/$bulk-submit",
            SUBMISSION_ID,
            submitterIdentifier,
            BulkSubmitRequest.STATUS_COMPLETE,
            null,
            null,
            null,
            null,
            null,
            List.of());
    when(validator.validateAndExtract(any(), any())).thenReturn(request);

    final Submission existingSubmission =
        Submission.createPending(SUBMISSION_ID, submitterIdentifier, Optional.empty());
    when(submissionRegistry.get(submitterIdentifier, SUBMISSION_ID))
        .thenReturn(Optional.of(existingSubmission));

    // When/Then: should throw InvalidRequestException.
    assertThatThrownBy(() -> provider.bulkSubmitOperation(new Parameters(), requestDetails))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("no manifests have been submitted");
  }

  // ========================================
  // Abort Submission Tests
  // ========================================

  @Test
  void abortsSubmission() {
    // Given: an abort request for existing submission.
    final BulkSubmitRequest request =
        new BulkSubmitRequest(
            FHIR_SERVER_BASE + "/$bulk-submit",
            SUBMISSION_ID,
            submitterIdentifier,
            BulkSubmitRequest.STATUS_ABORTED,
            null,
            null,
            null,
            null,
            null,
            List.of());
    when(validator.validateAndExtract(any(), any())).thenReturn(request);

    final Submission existingSubmission =
        Submission.createPending(SUBMISSION_ID, submitterIdentifier, Optional.empty())
            .withState(SubmissionState.PROCESSING);
    when(submissionRegistry.get(submitterIdentifier, SUBMISSION_ID))
        .thenReturn(Optional.of(existingSubmission));

    // When: calling the operation.
    final Parameters result = provider.bulkSubmitOperation(new Parameters(), requestDetails);

    // Then: should abort and update state.
    verify(executor).abortSubmission(existingSubmission);
    verify(submissionRegistry).put(any(Submission.class));
    assertThat(((StringType) result.getParameter("status").getValue()).getValue())
        .isEqualTo("aborted");
  }

  @Test
  void rejectsAbortForNonexistentSubmission() {
    // Given: an abort request for nonexistent submission.
    final BulkSubmitRequest request =
        new BulkSubmitRequest(
            FHIR_SERVER_BASE + "/$bulk-submit",
            SUBMISSION_ID,
            submitterIdentifier,
            BulkSubmitRequest.STATUS_ABORTED,
            null,
            null,
            null,
            null,
            null,
            List.of());
    when(validator.validateAndExtract(any(), any())).thenReturn(request);
    when(submissionRegistry.get(submitterIdentifier, SUBMISSION_ID)).thenReturn(Optional.empty());

    // When/Then: should throw ResourceNotFoundException.
    assertThatThrownBy(() -> provider.bulkSubmitOperation(new Parameters(), requestDetails))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessageContaining(SUBMISSION_ID);
  }

  @Test
  void rejectsAbortForAlreadyCompletedSubmission() {
    // Given: an abort request for already completed submission.
    final BulkSubmitRequest request =
        new BulkSubmitRequest(
            FHIR_SERVER_BASE + "/$bulk-submit",
            SUBMISSION_ID,
            submitterIdentifier,
            BulkSubmitRequest.STATUS_ABORTED,
            null,
            null,
            null,
            null,
            null,
            List.of());
    when(validator.validateAndExtract(any(), any())).thenReturn(request);

    final Submission completedSubmission =
        Submission.createPending(SUBMISSION_ID, submitterIdentifier, Optional.empty())
            .withState(SubmissionState.COMPLETED);
    when(submissionRegistry.get(submitterIdentifier, SUBMISSION_ID))
        .thenReturn(Optional.of(completedSubmission));

    // When/Then: should throw InvalidRequestException.
    assertThatThrownBy(() -> provider.bulkSubmitOperation(new Parameters(), requestDetails))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Cannot abort submission")
        .hasMessageContaining("COMPLETED");
  }

  // ========================================
  // Provider Without Executor Tests
  // ========================================

  @Test
  void worksWithoutExecutor() {
    // Given: provider created without executor (early startup scenario).
    final BulkSubmitProvider providerWithoutExecutor =
        new BulkSubmitProvider(validator, submissionRegistry, null);

    final BulkSubmitRequest request =
        new BulkSubmitRequest(
            FHIR_SERVER_BASE + "/$bulk-submit",
            SUBMISSION_ID,
            submitterIdentifier,
            BulkSubmitRequest.STATUS_IN_PROGRESS,
            MANIFEST_URL,
            FHIR_BASE_URL,
            null,
            null,
            null,
            List.of());
    when(validator.validateAndExtract(any(), any())).thenReturn(request);
    when(submissionRegistry.get(submitterIdentifier, SUBMISSION_ID)).thenReturn(Optional.empty());

    // When: calling the operation.
    final Parameters result =
        providerWithoutExecutor.bulkSubmitOperation(new Parameters(), requestDetails);

    // Then: should create submission but not call executor.
    verify(submissionRegistry, org.mockito.Mockito.atLeast(1)).put(any(Submission.class));
    assertThat(result.getParameter("submissionId")).isNotNull();
  }
}
