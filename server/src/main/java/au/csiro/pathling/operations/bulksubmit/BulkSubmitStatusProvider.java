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

import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.ProcessingNotCompletedException;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.StringType;
import org.springframework.stereotype.Component;

/**
 * Provides the $bulk-submit-status operation for checking submission status.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Component
@Slf4j
public class BulkSubmitStatusProvider {

  private static final int MAX_JOB_WAIT_ATTEMPTS = 10;
  private static final long JOB_WAIT_INTERVAL_MS = 500;

  @Nonnull
  private final SubmissionRegistry submissionRegistry;

  @Nonnull
  private final BulkSubmitResultBuilder resultBuilder;

  @Nullable
  private final JobRegistry jobRegistry;

  /**
   * Creates a new BulkSubmitStatusProvider.
   *
   * @param submissionRegistry The registry for tracking submissions.
   * @param resultBuilder The builder for generating result manifests.
   * @param jobRegistry The job registry for looking up async jobs (may be null if async is
   * disabled).
   */
  public BulkSubmitStatusProvider(
      @Nonnull final SubmissionRegistry submissionRegistry,
      @Nonnull final BulkSubmitResultBuilder resultBuilder,
      @Nullable final JobRegistry jobRegistry
  ) {
    this.submissionRegistry = submissionRegistry;
    this.resultBuilder = resultBuilder;
    this.jobRegistry = jobRegistry;
  }

  /**
   * The $bulk-submit-status operation endpoint.
   * <p>
   * This operation returns a 202 Accepted response with a Content-Location header pointing to the
   * $job endpoint for polling progress. The Job is created by BulkSubmitExecutor when processing
   * begins. Clients should poll the $job endpoint to track progress and retrieve the final result.
   *
   * @param submissionId The submission ID to check status for.
   * @param submitter The submitter identifier.
   * @param requestDetails The servlet request details.
   * @return A Binary resource containing the status manifest JSON (when complete).
   */
  @Operation(name = "$bulk-submit-status")
  @OperationAccess("bulk-submit")
  @Nonnull
  public Binary bulkSubmitStatusOperation(
      @OperationParam(name = "submissionId") final StringType submissionId,
      @OperationParam(name = "submitter") final Identifier submitter,
      @Nonnull final ServletRequestDetails requestDetails
  ) {
    if (submissionId == null || submissionId.isEmpty()) {
      throw new ca.uhn.fhir.rest.server.exceptions.InvalidRequestException(
          "Missing required parameter: submissionId"
      );
    }
    if (submitter == null || submitter.getSystem() == null || submitter.getValue() == null) {
      throw new ca.uhn.fhir.rest.server.exceptions.InvalidRequestException(
          "Missing required parameter: submitter"
      );
    }

    final SubmitterIdentifier submitterIdentifier = new SubmitterIdentifier(
        submitter.getSystem(),
        submitter.getValue()
    );

    // Look up the submission.
    final Submission submission = getSubmission(submitterIdentifier, submissionId.getValue());

    // Build the request URL for the status manifest.
    final String requestUrl = buildRequestUrl(requestDetails);

    // If the submission has completed or failed, return the result directly.
    if (submission.state() == SubmissionState.COMPLETED
        || submission.state() == SubmissionState.COMPLETED_WITH_ERRORS
        || submission.state() == SubmissionState.ABORTED) {
      return handleCompletedSubmission(submission, requestUrl);
    }

    // Get all job IDs from manifest jobs.
    final List<String> jobIds = submission.getAllJobIds();

    // If async is enabled and we have job IDs, redirect to the first running job.
    if (jobRegistry != null && !jobIds.isEmpty()) {
      // Find the first job that is still running, or the first job if all are done.
      for (final String jobId : jobIds) {
        final Job<?> job = jobRegistry.get(jobId);
        if (job != null && !job.getResult().isDone()) {
          return redirectToJob(jobId, requestDetails);
        }
      }
      // All jobs are done, use the first job ID.
      return redirectToJob(jobIds.get(0), requestDetails);
    }

    // If no job IDs yet (executor hasn't started), wait briefly for one to be created.
    final String jobId = waitForJobId(submitterIdentifier, submissionId.getValue());
    if (jobId != null) {
      return redirectToJob(jobId, requestDetails);
    }

    // If we still don't have a job ID, return 202 with Retry-After.
    log.warn("No job ID available for submission {}, returning 202 with Retry-After",
        submissionId.getValue());
    final HttpServletResponse response = requestDetails.getServletResponse();
    response.setHeader("Retry-After", "5");
    throw new ProcessingNotCompletedException(
        "Submission processing has not yet started",
        buildRetryOutcome()
    );
  }

  @Nonnull
  private Binary handleCompletedSubmission(
      @Nonnull final Submission submission,
      @Nonnull final String requestUrl
  ) {
    // Check for error state and throw exception with error details.
    if (submission.state() == SubmissionState.COMPLETED_WITH_ERRORS) {
      final String errorMessage = submission.errorMessage() != null
                                  ? submission.errorMessage()
                                  : "Unknown error";
      throw new InternalErrorException("Submission failed: " + errorMessage);
    }

    if (submission.state() == SubmissionState.ABORTED) {
      throw new InternalErrorException("Submission was aborted");
    }

    return resultBuilder.buildStatusManifest(submission, requestUrl);
  }

  @Nonnull
  private Binary redirectToJob(
      @Nonnull final String jobId,
      @Nonnull final ServletRequestDetails requestDetails
  ) {
    // Check if the job has completed.
    if (jobRegistry != null) {
      final Job<?> job = jobRegistry.get(jobId);
      if (job != null && job.getResult().isDone()) {
        // Job is done, return the result directly.
        try {
          final IBaseResource result = job.getResult().get();
          if (result instanceof final Binary binary) {
            return binary;
          }
          // Unexpected result type - should not happen.
          throw new InternalErrorException("Unexpected job result type: " + result.getClass());
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InternalErrorException("Interrupted while getting job result", e);
        } catch (final ExecutionException e) {
          // Job failed, unwrap and rethrow the cause.
          final Throwable cause = e.getCause();
          if (cause instanceof final RuntimeException runtimeException) {
            throw runtimeException;
          }
          throw new InternalErrorException("Job execution failed", cause);
        }
      }
    }

    // Job is still running, return 202 with Content-Location.
    final HttpServletResponse response = requestDetails.getServletResponse();
    final String jobUrl = requestDetails.getFhirServerBase() + "/$job?id=" + jobId;
    response.setHeader("Content-Location", jobUrl);

    throw new ProcessingNotCompletedException(
        "Processing",
        buildProcessingOutcome(jobUrl)
    );
  }

  @Nullable
  private String waitForJobId(
      @Nonnull final SubmitterIdentifier submitterIdentifier,
      @Nonnull final String submissionId
  ) {
    // Wait briefly for a job ID to be created by BulkSubmitExecutor.
    for (int i = 0; i < MAX_JOB_WAIT_ATTEMPTS; i++) {
      try {
        Thread.sleep(JOB_WAIT_INTERVAL_MS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }

      final Submission submission = submissionRegistry.get(submitterIdentifier, submissionId)
          .orElse(null);
      if (submission != null) {
        final List<String> jobIds = submission.getAllJobIds();
        if (!jobIds.isEmpty()) {
          return jobIds.get(0);
        }
      }

      // If submission completed while we were waiting, stop waiting.
      if (submission != null && (submission.state() == SubmissionState.COMPLETED
          || submission.state() == SubmissionState.COMPLETED_WITH_ERRORS
          || submission.state() == SubmissionState.ABORTED)) {
        return null;
      }
    }
    return null;
  }

  @Nonnull
  private Submission getSubmission(
      @Nonnull final SubmitterIdentifier submitterIdentifier,
      @Nonnull final String submissionId
  ) {
    return submissionRegistry.get(submitterIdentifier, submissionId)
        .orElseThrow(() -> new ResourceNotFoundException(
            "Submission not found: " + submissionId
        ));
  }

  @Nonnull
  private static OperationOutcome buildProcessingOutcome(@Nonnull final String jobUrl) {
    final OperationOutcome outcome = new OperationOutcome();
    outcome.addIssue()
        .setCode(IssueType.INFORMATIONAL)
        .setSeverity(IssueSeverity.INFORMATION)
        .setDiagnostics("Job accepted for processing, see the Content-Location header for the "
            + "URL at which status can be queried: " + jobUrl);
    return outcome;
  }

  @Nonnull
  private static OperationOutcome buildRetryOutcome() {
    final OperationOutcome outcome = new OperationOutcome();
    outcome.addIssue()
        .setCode(IssueType.INFORMATIONAL)
        .setSeverity(IssueSeverity.INFORMATION)
        .setDiagnostics("Submission processing has not yet started. Please retry after a few "
            + "seconds.");
    return outcome;
  }

  /**
   * Builds the request URL for the status manifest from the servlet request details.
   *
   * @param requestDetails The servlet request details.
   * @return The complete request URL.
   */
  @Nonnull
  private static String buildRequestUrl(@Nonnull final ServletRequestDetails requestDetails) {
    final String requestPath = requestDetails.getRequestPath();
    final String queryString = requestDetails.getServletRequest().getQueryString();
    final String fhirServerBase = requestDetails.getFhirServerBase();

    final StringBuilder url = new StringBuilder(fhirServerBase);
    if (requestPath != null && !requestPath.isEmpty()) {
      if (!fhirServerBase.endsWith("/") && !requestPath.startsWith("/")) {
        url.append("/");
      }
      url.append(requestPath);
    }
    if (queryString != null && !queryString.isEmpty()) {
      url.append("?").append(queryString);
    }
    return url.toString();
  }

}
