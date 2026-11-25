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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.ProcessingNotCompletedException;
import au.csiro.pathling.errors.InvalidUserInputError;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

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
  private static final String OWNER = "hospital-user";
  private static final String OTHER_USER = "mallory";

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

  @AfterEach
  void tearDown() {
    SecurityContextHolder.clearContext();
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
  void returnsCurrentManifestWhenJobIsComplete() {
    // Given: a processing submission whose only download job has completed.
    final StringType submissionId = new StringType(SUBMISSION_ID);
    final Identifier submitter = createSubmitterIdentifier();
    final ManifestJob manifestJob = downloadedManifestJob("manifest-job-123", "job-123");
    final Submission submission = processingSubmission(List.of(manifestJob));
    when(submissionRegistry.get(any(SubmitterIdentifier.class), eq(SUBMISSION_ID)))
        .thenReturn(Optional.of(submission));
    registerCompletedJob("job-123", new Parameters());

    final Parameters currentManifest = new Parameters();
    when(resultBuilder.buildStatusManifest(eq(submission), any(), eq(FHIR_SERVER_BASE)))
        .thenReturn(currentManifest);

    // When: calling the operation.
    final Parameters result =
        provider.bulkSubmitStatusOperation(submissionId, submitter, requestDetails);

    // Then: the manifest is built from the current state of the submission.
    assertThat(result).isSameAs(currentManifest);
  }

  @Test
  void includesLaterManifestsWhenAllJobsAreComplete() {
    // Given: a submission with two manifests, both downloaded. Each download job stores the
    // manifest as it was when that job finished, so the first job's copy does not include the
    // second manifest.
    final StringType submissionId = new StringType(SUBMISSION_ID);
    final Identifier submitter = createSubmitterIdentifier();
    final Submission submission =
        processingSubmission(
            List.of(
                downloadedManifestJob("manifest-job-1", "job-1"),
                downloadedManifestJob("manifest-job-2", "job-2")));
    when(submissionRegistry.get(any(SubmitterIdentifier.class), eq(SUBMISSION_ID)))
        .thenReturn(Optional.of(submission));
    final Parameters staleManifest = new Parameters();
    registerCompletedJob("job-1", staleManifest);
    registerCompletedJob("job-2", new Parameters());

    final Parameters currentManifest = new Parameters();
    when(resultBuilder.buildStatusManifest(eq(submission), any(), eq(FHIR_SERVER_BASE)))
        .thenReturn(currentManifest);

    // When: calling the operation.
    final Parameters result =
        provider.bulkSubmitStatusOperation(submissionId, submitter, requestDetails);

    // Then: the manifest reflects both manifest jobs, rather than the first job's stored copy.
    assertThat(result).isSameAs(currentManifest).isNotSameAs(staleManifest);
    verify(resultBuilder).buildStatusManifest(eq(submission), any(), eq(FHIR_SERVER_BASE));
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
  // Ownership and Submitter Tests
  // ========================================

  @Test
  void returnsNotFoundForCompletedSubmissionWhenCallerIsNotOwner() {
    // Given: a completed submission owned by another user.
    givenExistingSubmission(submissionOwnedBy(OWNER).withState(SubmissionState.COMPLETED));
    authenticateAs(OTHER_USER);

    // When/Then: the caller is told the submission does not exist, and no manifest is built.
    assertThatThrownBy(
            () ->
                provider.bulkSubmitStatusOperation(
                    new StringType(SUBMISSION_ID), createSubmitterIdentifier(), requestDetails))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessage("Submission not found: " + SUBMISSION_ID);
    verify(resultBuilder, never()).buildStatusManifest(any(), any(), any());
  }

  @Test
  void returnsNotFoundForProcessingSubmissionWhenCallerIsNotOwner() {
    // Given: a submission owned by another user with a running job.
    givenExistingSubmission(
        submissionOwnedBy(OWNER).withManifestJob(downloadedManifestJob("manifest-1", "job-1")));
    authenticateAs(OTHER_USER);

    // When/Then: the caller is not redirected to the job, and no progress is disclosed.
    assertThatThrownBy(
            () ->
                provider.bulkSubmitStatusOperation(
                    new StringType(SUBMISSION_ID), createSubmitterIdentifier(), requestDetails))
        .isInstanceOf(ResourceNotFoundException.class);
    verify(jobRegistry, never()).get(any(String.class));
    verify(servletResponse, never()).setHeader(any(), any());
  }

  @Test
  void returnsNotFoundForSubmissionWithoutOwnerWhenCallerIsAuthenticated() {
    // Given: a submission created by a caller without a subject.
    givenExistingSubmission(
        Submission.createPending(
                SUBMISSION_ID,
                new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE),
                Optional.empty())
            .withState(SubmissionState.COMPLETED));
    authenticateAs(OTHER_USER);

    // When/Then: the authenticated caller does not own the submission.
    assertThatThrownBy(
            () ->
                provider.bulkSubmitStatusOperation(
                    new StringType(SUBMISSION_ID), createSubmitterIdentifier(), requestDetails))
        .isInstanceOf(ResourceNotFoundException.class);
  }

  @Test
  void returnsStatusManifestToOwner() {
    // Given: a completed submission owned by the caller.
    final Submission submission = submissionOwnedBy(OWNER).withState(SubmissionState.COMPLETED);
    givenExistingSubmission(submission);
    authenticateAs(OWNER);
    final Parameters expectedResult = new Parameters();
    when(resultBuilder.buildStatusManifest(any(), any(), any())).thenReturn(expectedResult);

    // When: calling the operation.
    final Parameters result =
        provider.bulkSubmitStatusOperation(
            new StringType(SUBMISSION_ID), createSubmitterIdentifier(), requestDetails);

    // Then: the owner receives the status manifest.
    assertThat(result).isSameAs(expectedResult);
    verify(resultBuilder).buildStatusManifest(eq(submission), any(), eq(FHIR_SERVER_BASE));
  }

  @Test
  void rejectsSubmitterThatIsNotAllowed() {
    // Given: a submitter that the validator rejects.
    doThrow(new InvalidUserInputError("Submitter is not in the list of allowed submitters."))
        .when(validator)
        .validateSubmitterAllowed(new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE));

    // When/Then: the request is rejected before the submission is looked up.
    assertThatThrownBy(
            () ->
                provider.bulkSubmitStatusOperation(
                    new StringType(SUBMISSION_ID), createSubmitterIdentifier(), requestDetails))
        .isInstanceOf(InvalidUserInputError.class);
    verify(submissionRegistry, never()).get(any(), any());
  }

  // ========================================
  // Helper Methods
  // ========================================

  private ManifestJob downloadedManifestJob(final String manifestJobId, final String jobId) {
    return ManifestJob.createPending(
            manifestJobId, "https://example.org/" + manifestJobId + ".json", null, null)
        .withState(ManifestJobState.DOWNLOADED)
        .withJobId(jobId);
  }

  private Submission processingSubmission(final List<ManifestJob> manifestJobs) {
    return new Submission(
        SUBMISSION_ID,
        new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE),
        SubmissionState.PROCESSING,
        manifestJobs,
        "2024-01-01T00:00:00Z",
        null,
        null,
        null,
        null);
  }

  private Submission submissionOwnedBy(final String owner) {
    return Submission.createPending(
            SUBMISSION_ID,
            new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE),
            Optional.of(owner))
        .withState(SubmissionState.PROCESSING);
  }

  private void givenExistingSubmission(final Submission submission) {
    when(submissionRegistry.get(any(SubmitterIdentifier.class), eq(SUBMISSION_ID)))
        .thenReturn(Optional.of(submission));
  }

  private static void authenticateAs(final String subject) {
    final Jwt jwt =
        Jwt.withTokenValue("mock-token").header("alg", "none").claim("sub", subject).build();
    SecurityContextHolder.setContext(new SecurityContextImpl(new JwtAuthenticationToken(jwt)));
  }

  @SuppressWarnings("unchecked")
  private void registerCompletedJob(final String jobId, final Parameters storedResult) {
    final Job<IBaseResource> job = mock(Job.class);
    when(job.getResult()).thenReturn(CompletableFuture.completedFuture(storedResult));
    when(jobRegistry.get(jobId)).thenAnswer(inv -> job);
  }

  private Identifier createSubmitterIdentifier() {
    final Identifier identifier = new Identifier();
    identifier.setSystem(SUBMITTER_SYSTEM);
    identifier.setValue(SUBMITTER_VALUE);
    return identifier;
  }
}
