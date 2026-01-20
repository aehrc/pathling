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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.ProcessingNotCompletedException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for BulkSubmitStatusProvider.
 *
 * @author John Grimes
 */
class BulkSubmitStatusProviderTest {

  private static final String SUBMITTER_SYSTEM = "https://example.org/submitters";
  private static final String SUBMITTER_VALUE = "test-submitter";
  private static final String SUBMISSION_ID = "test-submission-123";
  private static final String FHIR_SERVER_BASE = "http://localhost:8080/fhir";

  private SubmissionRegistry submissionRegistry;
  private BulkSubmitResultBuilder resultBuilder;
  private BulkSubmitValidator validator;
  private JobRegistry jobRegistry;
  private BulkSubmitStatusProvider provider;
  private ServletRequestDetails requestDetails;
  private HttpServletResponse servletResponse;

  @BeforeEach
  void setUp() {
    submissionRegistry = mock(SubmissionRegistry.class);
    resultBuilder = mock(BulkSubmitResultBuilder.class);
    validator = mock(BulkSubmitValidator.class);
    jobRegistry = mock(JobRegistry.class);

    provider =
        new BulkSubmitStatusProvider(submissionRegistry, resultBuilder, validator, jobRegistry);

    requestDetails = mock(ServletRequestDetails.class);
    servletResponse = mock(HttpServletResponse.class);
    final HttpServletRequest servletRequest = mock(HttpServletRequest.class);

    when(requestDetails.getFhirServerBase()).thenReturn(FHIR_SERVER_BASE);
    when(requestDetails.getServletResponse()).thenReturn(servletResponse);
    when(requestDetails.getServletRequest()).thenReturn(servletRequest);
    when(requestDetails.getRequestPath()).thenReturn("/$bulk-submit-status");
    when(servletRequest.getQueryString()).thenReturn("submissionId=" + SUBMISSION_ID);
  }

  @Test
  void throwsExceptionWhenSubmissionIdIsNull() {
    // Given: null submission ID.
    final Identifier submitter = createSubmitterIdentifier();

    // When/Then: Should throw InvalidRequestException.
    assertThatThrownBy(() -> provider.bulkSubmitStatusOperation(null, submitter, requestDetails))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("submissionId");
  }

  @Test
  void throwsExceptionWhenSubmissionIdIsEmpty() {
    // Given: empty submission ID.
    final StringType emptySubmissionId = new StringType("");
    final Identifier submitter = createSubmitterIdentifier();

    // When/Then: Should throw InvalidRequestException.
    assertThatThrownBy(
            () -> provider.bulkSubmitStatusOperation(emptySubmissionId, submitter, requestDetails))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("submissionId");
  }

  @Test
  void throwsExceptionWhenSubmitterIsNull() {
    // Given: valid submission ID but null submitter.
    final StringType submissionId = new StringType(SUBMISSION_ID);

    // When/Then: Should throw InvalidRequestException.
    assertThatThrownBy(() -> provider.bulkSubmitStatusOperation(submissionId, null, requestDetails))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("submitter");
  }

  @Test
  void throwsExceptionWhenSubmitterSystemIsNull() {
    // Given: submitter with null system.
    final StringType submissionId = new StringType(SUBMISSION_ID);
    final Identifier submitter = new Identifier();
    submitter.setValue(SUBMITTER_VALUE);
    // system is null

    // When/Then: Should throw InvalidRequestException.
    assertThatThrownBy(
            () -> provider.bulkSubmitStatusOperation(submissionId, submitter, requestDetails))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("submitter");
  }

  @Test
  void throwsExceptionWhenSubmitterValueIsNull() {
    // Given: submitter with null value.
    final StringType submissionId = new StringType(SUBMISSION_ID);
    final Identifier submitter = new Identifier();
    submitter.setSystem(SUBMITTER_SYSTEM);
    // value is null

    // When/Then: Should throw InvalidRequestException.
    assertThatThrownBy(
            () -> provider.bulkSubmitStatusOperation(submissionId, submitter, requestDetails))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("submitter");
  }

  @Test
  void throwsResourceNotFoundWhenSubmissionDoesNotExist() {
    // Given: submission does not exist.
    final StringType submissionId = new StringType(SUBMISSION_ID);
    final Identifier submitter = createSubmitterIdentifier();
    when(submissionRegistry.get(any(SubmitterIdentifier.class), eq(SUBMISSION_ID)))
        .thenReturn(Optional.empty());

    // When/Then: Should throw ResourceNotFoundException.
    assertThatThrownBy(
            () -> provider.bulkSubmitStatusOperation(submissionId, submitter, requestDetails))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessageContaining(SUBMISSION_ID);
  }

  @Test
  void returnsResultWhenSubmissionIsCompleted() {
    // Given: a completed submission.
    final StringType submissionId = new StringType(SUBMISSION_ID);
    final Identifier submitter = createSubmitterIdentifier();
    final SubmitterIdentifier submitterIdentifier =
        new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE);
    final Submission completedSubmission =
        Submission.createPending(SUBMISSION_ID, submitterIdentifier, Optional.empty())
            .withState(SubmissionState.COMPLETED);

    when(submissionRegistry.get(any(SubmitterIdentifier.class), eq(SUBMISSION_ID)))
        .thenReturn(Optional.of(completedSubmission));

    final Parameters expectedResult = new Parameters();
    expectedResult.addParameter().setName("result").setValue(new StringType("success"));
    when(resultBuilder.buildStatusManifest(any(), any(), any())).thenReturn(expectedResult);

    // When: calling the operation.
    final Parameters result =
        provider.bulkSubmitStatusOperation(submissionId, submitter, requestDetails);

    // Then: should return the built result.
    assertThat(result).isEqualTo(expectedResult);
    verify(resultBuilder).buildStatusManifest(eq(completedSubmission), any(), eq(FHIR_SERVER_BASE));
  }

  @Test
  void returnsResultWhenSubmissionIsCompletedWithErrors() {
    // Given: a submission completed with errors.
    final StringType submissionId = new StringType(SUBMISSION_ID);
    final Identifier submitter = createSubmitterIdentifier();
    final SubmitterIdentifier submitterIdentifier =
        new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE);
    final Submission completedWithErrors =
        Submission.createPending(SUBMISSION_ID, submitterIdentifier, Optional.empty())
            .withState(SubmissionState.COMPLETED_WITH_ERRORS);

    when(submissionRegistry.get(any(SubmitterIdentifier.class), eq(SUBMISSION_ID)))
        .thenReturn(Optional.of(completedWithErrors));

    final Parameters expectedResult = new Parameters();
    when(resultBuilder.buildStatusManifest(any(), any(), any())).thenReturn(expectedResult);

    // When: calling the operation.
    final Parameters result =
        provider.bulkSubmitStatusOperation(submissionId, submitter, requestDetails);

    // Then: should return the built result (not throw exception).
    assertThat(result).isEqualTo(expectedResult);
  }

  @Test
  void throwsInternalErrorWhenSubmissionIsAborted() {
    // Given: an aborted submission.
    final StringType submissionId = new StringType(SUBMISSION_ID);
    final Identifier submitter = createSubmitterIdentifier();
    final SubmitterIdentifier submitterIdentifier =
        new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE);
    final Submission abortedSubmission =
        Submission.createPending(SUBMISSION_ID, submitterIdentifier, Optional.empty())
            .withState(SubmissionState.ABORTED);

    when(submissionRegistry.get(any(SubmitterIdentifier.class), eq(SUBMISSION_ID)))
        .thenReturn(Optional.of(abortedSubmission));

    // When/Then: Should throw InternalErrorException.
    assertThatThrownBy(
            () -> provider.bulkSubmitStatusOperation(submissionId, submitter, requestDetails))
        .isInstanceOf(ca.uhn.fhir.rest.server.exceptions.InternalErrorException.class)
        .hasMessageContaining("aborted");
  }

  @Test
  void redirectsToJobWhenJobIsStillRunning() {
    // Given: a pending submission with a running job.
    final StringType submissionId = new StringType(SUBMISSION_ID);
    final Identifier submitter = createSubmitterIdentifier();
    final SubmitterIdentifier submitterIdentifier =
        new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE);
    final String jobId = "job-123";

    final ManifestJob manifestJob =
        ManifestJob.createPending(
                "manifest-job-123",
                "https://example.org/manifest.json",
                "https://example.org/fhir",
                null)
            .withState(ManifestJobState.PROCESSING)
            .withJobId(jobId);
    final Submission pendingSubmission =
        new Submission(
            SUBMISSION_ID,
            submitterIdentifier,
            SubmissionState.PROCESSING,
            List.of(manifestJob),
            "2024-01-01T00:00:00Z",
            null,
            null,
            null,
            null);

    when(submissionRegistry.get(any(SubmitterIdentifier.class), eq(SUBMISSION_ID)))
        .thenReturn(Optional.of(pendingSubmission));
    when(submissionRegistry.getByJobId(jobId)).thenReturn(Optional.of(pendingSubmission));

    @SuppressWarnings("unchecked")
    final Job<IBaseResource> job = mock(Job.class);
    final CompletableFuture<IBaseResource> incompleteFuture = new CompletableFuture<>();
    when(job.getResult()).thenReturn(incompleteFuture);
    when(jobRegistry.get(jobId)).thenAnswer(inv -> job);

    // When/Then: Should throw ProcessingNotCompletedException with redirect.
    assertThatThrownBy(
            () -> provider.bulkSubmitStatusOperation(submissionId, submitter, requestDetails))
        .isInstanceOf(ProcessingNotCompletedException.class);

    // Verify headers were set.
    verify(servletResponse).setHeader(eq("Content-Location"), any());
    verify(servletResponse).setHeader(eq("X-Progress"), any());
  }

  @Test
  void returnsJobResultWhenJobIsComplete() {
    // Given: a pending submission with a completed job.
    final StringType submissionId = new StringType(SUBMISSION_ID);
    final Identifier submitter = createSubmitterIdentifier();
    final SubmitterIdentifier submitterIdentifier =
        new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE);
    final String jobId = "job-123";

    final ManifestJob manifestJob =
        ManifestJob.createPending(
                "manifest-job-123",
                "https://example.org/manifest.json",
                "https://example.org/fhir",
                null)
            .withState(ManifestJobState.PROCESSING)
            .withJobId(jobId);
    final Submission pendingSubmission =
        new Submission(
            SUBMISSION_ID,
            submitterIdentifier,
            SubmissionState.PROCESSING,
            List.of(manifestJob),
            "2024-01-01T00:00:00Z",
            null,
            null,
            null,
            null);

    when(submissionRegistry.get(any(SubmitterIdentifier.class), eq(SUBMISSION_ID)))
        .thenReturn(Optional.of(pendingSubmission));

    @SuppressWarnings("unchecked")
    final Job<IBaseResource> job = mock(Job.class);
    final Parameters completedResult = new Parameters();
    completedResult.addParameter().setName("status").setValue(new StringType("complete"));
    final CompletableFuture<IBaseResource> completedFuture =
        CompletableFuture.completedFuture(completedResult);
    when(job.getResult()).thenReturn(completedFuture);
    when(jobRegistry.get(jobId)).thenAnswer(inv -> job);

    // When: calling the operation.
    final Parameters result =
        provider.bulkSubmitStatusOperation(submissionId, submitter, requestDetails);

    // Then: should return the job result.
    assertThat(result).isEqualTo(completedResult);
  }

  @Test
  void worksWithNullJobRegistry() {
    // Given: provider created without job registry (async disabled).
    final BulkSubmitStatusProvider providerWithoutAsync =
        new BulkSubmitStatusProvider(submissionRegistry, resultBuilder, validator, null);

    final StringType submissionId = new StringType(SUBMISSION_ID);
    final Identifier submitter = createSubmitterIdentifier();
    final SubmitterIdentifier submitterIdentifier =
        new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE);

    // When: submission is pending with job IDs but no job registry.
    final ManifestJob manifestJob =
        ManifestJob.createPending(
                "job-123", "https://example.org/manifest.json", "https://example.org/fhir", null)
            .withState(ManifestJobState.PROCESSING);
    final Submission pendingSubmission =
        new Submission(
            SUBMISSION_ID,
            submitterIdentifier,
            SubmissionState.PROCESSING,
            List.of(manifestJob),
            "2024-01-01T00:00:00Z",
            null,
            null,
            null,
            null);

    when(submissionRegistry.get(any(SubmitterIdentifier.class), eq(SUBMISSION_ID)))
        .thenReturn(Optional.of(pendingSubmission));

    // Then: Should throw ProcessingNotCompletedException (can't redirect without job registry).
    assertThatThrownBy(
            () ->
                providerWithoutAsync.bulkSubmitStatusOperation(
                    submissionId, submitter, requestDetails))
        .isInstanceOf(ProcessingNotCompletedException.class);

    // Verify Retry-After header was set.
    verify(servletResponse).setHeader("Retry-After", "5");
  }

  // ========================================
  // Helper Methods
  // ========================================

  private Identifier createSubmitterIdentifier() {
    final Identifier identifier = new Identifier();
    identifier.setSystem(SUBMITTER_SYSTEM);
    identifier.setValue(SUBMITTER_VALUE);
    return identifier;
  }
}
