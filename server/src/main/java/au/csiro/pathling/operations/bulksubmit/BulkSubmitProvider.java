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

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Provides the $bulk-submit operation for receiving bulk data from external systems.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Component
@Slf4j
public class BulkSubmitProvider {

  @Nonnull private final BulkSubmitValidator validator;

  @Nonnull private final SubmissionRegistry submissionRegistry;

  @Nullable private final BulkSubmitExecutor executor;

  /**
   * Creates a new BulkSubmitProvider.
   *
   * @param validator The validator for bulk submit requests.
   * @param submissionRegistry The registry for tracking submissions.
   * @param executor The executor for processing submissions (may be null during early startup).
   */
  public BulkSubmitProvider(
      @Nonnull final BulkSubmitValidator validator,
      @Nonnull final SubmissionRegistry submissionRegistry,
      @Nullable final BulkSubmitExecutor executor) {
    this.validator = validator;
    this.submissionRegistry = submissionRegistry;
    this.executor = executor;
  }

  /**
   * The $bulk-submit operation endpoint. This is a synchronous operation that acknowledges receipt
   * of the submission request and triggers background processing.
   *
   * @param parameters The FHIR Parameters resource containing the submission request.
   * @param requestDetails The servlet request details.
   * @return A FHIR Parameters resource describing the result.
   */
  @Operation(name = "$bulk-submit")
  @SuppressWarnings("UnusedReturnValue")
  @OperationAccess("bulk-submit")
  @Nonnull
  public Parameters bulkSubmitOperation(
      @ResourceParam final Parameters parameters,
      @Nonnull final ServletRequestDetails requestDetails) {
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    final BulkSubmitRequest request = validator.validateAndExtract(requestDetails, parameters);
    final Optional<String> ownerId = getCurrentUserId(authentication);
    final String fhirServerBase = requestDetails.getFhirServerBase();
    return handleSubmission(request, ownerId, fhirServerBase);
  }

  @Nonnull
  private Parameters handleSubmission(
      @Nonnull final BulkSubmitRequest request,
      @Nonnull final Optional<String> ownerId,
      @Nonnull final String fhirServerBase) {
    final Optional<Submission> existingSubmission =
        submissionRegistry.get(request.submitter(), request.submissionId());

    if (request.isInProgress()) {
      return handleInProgressSubmission(request, existingSubmission, ownerId, fhirServerBase);
    } else if (request.isComplete()) {
      return handleCompleteSubmission(request, existingSubmission, ownerId, fhirServerBase);
    } else if (request.isAborted()) {
      return handleAbortedSubmission(request, existingSubmission);
    } else {
      throw new InvalidRequestException("Unknown submission status: " + request.submissionStatus());
    }
  }

  @SuppressWarnings("java:S3776") // Complexity is inherent to submission state management.
  @Nonnull
  private Parameters handleInProgressSubmission(
      @Nonnull final BulkSubmitRequest request,
      @Nonnull final Optional<Submission> existingSubmission,
      @Nonnull final Optional<String> ownerId,
      @Nonnull final String fhirServerBase) {
    // For "in-progress" status, create or update the submission and process manifest if provided.
    Submission submission;
    if (existingSubmission.isPresent()) {
      submission = existingSubmission.get();
      if (submission.state() != SubmissionState.PENDING
          && submission.state() != SubmissionState.PROCESSING) {
        throw new InvalidRequestException(
            "Cannot update submission %s: current state is %s."
                .formatted(request.submissionId(), submission.state()));
      }
    } else {
      // Create new submission.
      submission = Submission.createPending(request.submissionId(), request.submitter(), ownerId);
      submissionRegistry.put(submission);
      log.info("Created new submission: {}", request.submissionId());
    }

    // Handle replacesManifestUrl - abort the old manifest job if specified.
    if (request.replacesManifestUrl() != null) {
      final Optional<ManifestJob> jobToReplace =
          submission.findManifestJobByUrl(request.replacesManifestUrl());
      if (jobToReplace.isEmpty()) {
        throw new InvalidRequestException(
            "Cannot replace manifest: no job found for URL " + request.replacesManifestUrl());
      }

      // Abort the old job.
      if (executor != null) {
        executor.abortManifestJob(submission, jobToReplace.get());
      }

      // Remove the old job from submission.
      submission = submission.withoutManifestJob(jobToReplace.get().manifestJobId());
      submissionRegistry.put(submission);
      log.info(
          "Replaced manifest job for URL {} in submission {}",
          request.replacesManifestUrl(),
          request.submissionId());
    }

    // If manifest URL provided, create a manifest job and start processing.
    if (request.manifestUrl() != null) {
      final ManifestJob manifestJob =
          ManifestJob.createPending(
              UUID.randomUUID().toString(),
              request.manifestUrl(),
              request.fhirBaseUrl(),
              request.oauthMetadataUrl());

      // Add manifest job to submission.
      submission = submission.withManifestJob(manifestJob);
      if (request.metadata() != null) {
        submission = submission.withMetadata(request.metadata());
      }

      // Transition to PROCESSING if not already.
      if (submission.state() == SubmissionState.PENDING) {
        submission = submission.withState(SubmissionState.PROCESSING);
      }
      submissionRegistry.put(submission);

      log.info(
          "Added manifest job {} to submission {} for manifest: {}",
          manifestJob.manifestJobId(),
          request.submissionId(),
          request.manifestUrl());

      // Start downloading the manifest files.
      if (executor != null) {
        executor.downloadManifestJob(
            submission, manifestJob, request.fileRequestHeaders(), fhirServerBase);
      } else {
        log.warn(
            "BulkSubmitExecutor not available - manifest job {} will not be processed",
            manifestJob.manifestJobId());
      }
    } else {
      log.debug("Received in-progress notification for submission: {}", request.submissionId());
    }

    return createAcknowledgementResponse(request.submissionId(), "in-progress");
  }

  @Nonnull
  private Parameters handleCompleteSubmission(
      @Nonnull final BulkSubmitRequest request,
      @Nonnull final Optional<Submission> existingSubmission,
      @Nonnull final Optional<String> ownerId,
      @Nonnull final String fhirServerBase) {
    // For "complete" status, the provider is signalling that no more manifests will be added.
    Submission submission;
    if (existingSubmission.isPresent()) {
      submission = existingSubmission.get();
      if (submission.state() != SubmissionState.PENDING
          && submission.state() != SubmissionState.PROCESSING) {
        throw new InvalidRequestException(
            "Cannot complete submission %s: current state is %s."
                .formatted(request.submissionId(), submission.state()));
      }
    } else {
      // No prior in-progress notification - create submission now.
      submission = Submission.createPending(request.submissionId(), request.submitter(), ownerId);
      submissionRegistry.put(submission);
      log.info("Created new submission: {}", request.submissionId());
    }

    // If manifest URL provided with complete request, this is an error - cannot add manifests
    // when completing. The client must submit all manifests via in-progress requests first.
    if (request.manifestUrl() != null) {
      throw new InvalidRequestException(
          "Cannot add manifest when completing submission. "
              + "Submit manifests via in-progress requests first.");
    }

    // Check that we have manifests to process.
    if (submission.manifestJobs().isEmpty()) {
      throw new InvalidRequestException(
          "Cannot complete submission %s: no manifests have been submitted."
              .formatted(request.submissionId()));
    }

    // Check if all downloads have completed.
    if (!submission.allDownloadsComplete()) {
      throw new InvalidUserInputError(
          "Cannot complete submission %s: downloads are still in progress."
              .formatted(request.submissionId()));
    }

    // Check if any downloads failed.
    if (submission.hasFailedJobs()) {
      final SubmissionState finalState = SubmissionState.COMPLETED_WITH_ERRORS;
      submission = submission.withState(finalState);
      submissionRegistry.put(submission);
      log.info("Submission {} completed with errors", request.submissionId());
      return createAcknowledgementResponse(request.submissionId(), finalState.name().toLowerCase());
    }

    // All downloads successful - start the background import.
    final SubmissionState finalState = SubmissionState.COMPLETED;
    submission = submission.withState(finalState);
    submissionRegistry.put(submission);

    if (executor != null) {
      executor.importSubmission(submission);
      log.info("Submission {} marked complete, background import started", request.submissionId());
    } else {
      log.warn(
          "BulkSubmitExecutor not available - import will not be executed for submission {}",
          request.submissionId());
    }

    return createAcknowledgementResponse(request.submissionId(), finalState.name().toLowerCase());
  }

  @Nonnull
  private Parameters handleAbortedSubmission(
      @Nonnull final BulkSubmitRequest request,
      @Nonnull final Optional<Submission> existingSubmission) {
    // Validate that submission exists.
    final Submission submission =
        existingSubmission.orElseThrow(
            () ->
                new ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException(
                    "Submission not found: " + request.submissionId()));

    // Validate that submission can be aborted.
    if (submission.state() == SubmissionState.COMPLETED
        || submission.state() == SubmissionState.COMPLETED_WITH_ERRORS
        || submission.state() == SubmissionState.ABORTED) {
      throw new InvalidRequestException(
          "Cannot abort submission %s: current state is %s."
              .formatted(request.submissionId(), submission.state()));
    }

    // Abort the submission.
    if (executor != null) {
      executor.abortSubmission(submission);
    }

    // Update submission state to ABORTED.
    final Submission abortedSubmission = submission.withState(SubmissionState.ABORTED);
    submissionRegistry.put(abortedSubmission);
    log.info("Submission {} aborted", request.submissionId());

    return createAcknowledgementResponse(request.submissionId(), "aborted");
  }

  @Nonnull
  private Parameters createAcknowledgementResponse(
      @Nonnull final String submissionId, @Nonnull final String status) {
    final Parameters response = new Parameters();
    response
        .addParameter()
        .setName("submissionId")
        .setValue(new org.hl7.fhir.r4.model.StringType(submissionId));
    response
        .addParameter()
        .setName("status")
        .setValue(new org.hl7.fhir.r4.model.StringType(status));
    return response;
  }
}
