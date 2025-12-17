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

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;

import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
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

  @Nonnull
  private final BulkSubmitValidator validator;

  @Nonnull
  private final SubmissionRegistry submissionRegistry;

  @Nullable
  private final BulkSubmitExecutor executor;

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
      @Nullable final BulkSubmitExecutor executor
  ) {
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
      @Nonnull final ServletRequestDetails requestDetails
  ) {
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
      @Nonnull final String fhirServerBase
  ) {
    final Optional<Submission> existingSubmission = submissionRegistry.get(
        request.submitter(),
        request.submissionId()
    );

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

  @Nonnull
  private Parameters handleInProgressSubmission(
      @Nonnull final BulkSubmitRequest request,
      @Nonnull final Optional<Submission> existingSubmission,
      @Nonnull final Optional<String> ownerId,
      @Nonnull final String fhirServerBase
  ) {
    // For "in-progress" status, create or update the submission.
    // Per the Argonaut spec, processing should begin immediately when a manifest URL is provided.
    Submission submission;
    if (existingSubmission.isPresent()) {
      submission = existingSubmission.get();
      if (submission.state() != SubmissionState.PENDING
          && submission.state() != SubmissionState.PROCESSING) {
        throw new InvalidRequestException(
            "Cannot update submission %s: current state is %s."
                .formatted(request.submissionId(), submission.state())
        );
      }
      // Update with new manifest details if provided.
      if (request.manifestUrl() != null) {
        submission = submission.withManifestDetails(
            request.manifestUrl(),
            request.fhirBaseUrl(),
            request.metadata()
        );
        submissionRegistry.put(submission);
        log.info("Updated submission {} with manifest details", request.submissionId());
      } else {
        log.debug("Received in-progress notification for submission: {}",
            request.submissionId());
      }
    } else {
      // Create new submission.
      submission = Submission.createPending(
          request.submissionId(),
          request.submitter(),
          ownerId
      );
      // Store manifest details if provided.
      if (request.manifestUrl() != null) {
        submission = submission.withManifestDetails(
            request.manifestUrl(),
            request.fhirBaseUrl(),
            request.metadata()
        );
      }
      submissionRegistry.put(submission);
      log.info("Created new submission: {}", request.submissionId());
    }

    // Per the Argonaut spec, processing should begin immediately when a manifest URL is provided.
    if (request.manifestUrl() != null && submission.state() == SubmissionState.PENDING) {
      submission = submission.withState(SubmissionState.PROCESSING);
      submissionRegistry.put(submission);
      log.info("Submission {} starting processing for manifest: {}", request.submissionId(),
          request.manifestUrl());
      if (executor != null) {
        executor.execute(submission, request.fileRequestHeaders(), fhirServerBase);
      } else {
        log.warn("BulkSubmitExecutor not available - submission {} will not be processed",
            request.submissionId());
      }
    }

    return createAcknowledgementResponse(request.submissionId(), "in-progress");
  }

  @Nonnull
  private Parameters handleCompleteSubmission(
      @Nonnull final BulkSubmitRequest request,
      @Nonnull final Optional<Submission> existingSubmission,
      @Nonnull final Optional<String> ownerId,
      @Nonnull final String fhirServerBase
  ) {
    // For "complete" status, the provider is signalling that no more manifests will be added.
    // Processing may already be in progress from a previous "in-progress" request with manifest.
    Submission submission;
    boolean alreadyProcessing = false;
    if (existingSubmission.isPresent()) {
      final Submission existing = existingSubmission.get();
      if (existing.state() != SubmissionState.PENDING
          && existing.state() != SubmissionState.PROCESSING) {
        throw new InvalidRequestException(
            "Cannot complete submission %s: current state is %s."
                .formatted(request.submissionId(), existing.state())
        );
      }
      alreadyProcessing = existing.state() == SubmissionState.PROCESSING;
      // Use request manifest details if provided, otherwise use stored details from in-progress.
      if (request.manifestUrl() != null) {
        submission = existing.withManifestDetails(
            request.manifestUrl(),
            request.fhirBaseUrl(),
            request.metadata()
        );
      } else if (existing.manifestUrl() != null) {
        // Use existing manifest details from previous in-progress request.
        submission = existing;
      } else {
        throw new InvalidRequestException(
            "Cannot complete submission %s: no manifest URL provided."
                .formatted(request.submissionId())
        );
      }
    } else {
      // No prior in-progress notification - must have manifest details in this request.
      if (request.manifestUrl() == null) {
        throw new InvalidRequestException(
            "Cannot complete submission %s: no prior submission found and no manifest URL provided."
                .formatted(request.submissionId())
        );
      }
      submission = Submission.createPending(
          request.submissionId(),
          request.submitter(),
          ownerId
      ).withManifestDetails(
          request.manifestUrl(),
          request.fhirBaseUrl(),
          request.metadata()
      );
    }

    // Only start processing if not already in progress.
    if (!alreadyProcessing) {
      submission = submission.withState(SubmissionState.PROCESSING);
      submissionRegistry.put(submission);
      log.info("Submission {} marked complete, starting processing", request.submissionId());

      if (executor != null) {
        executor.execute(submission, request.fileRequestHeaders(), fhirServerBase);
      } else {
        log.warn("BulkSubmitExecutor not available - submission {} will not be processed",
            request.submissionId());
      }
    } else {
      log.info("Submission {} marked complete, processing already in progress",
          request.submissionId());
    }

    return createAcknowledgementResponse(request.submissionId(), "processing");
  }

  @Nonnull
  private Parameters handleAbortedSubmission(
      @Nonnull final BulkSubmitRequest request,
      @Nonnull final Optional<Submission> existingSubmission
  ) {
    // For "aborted" status, mark the submission as aborted.
    if (existingSubmission.isEmpty()) {
      throw new InvalidRequestException(
          "Cannot abort submission %s: submission not found.".formatted(request.submissionId())
      );
    }

    final Submission existing = existingSubmission.get();
    if (existing.state() == SubmissionState.COMPLETED
        || existing.state() == SubmissionState.COMPLETED_WITH_ERRORS) {
      throw new InvalidRequestException(
          "Cannot abort submission %s: submission has already completed."
              .formatted(request.submissionId())
      );
    }

    submissionRegistry.updateState(
        request.submitter(),
        request.submissionId(),
        SubmissionState.ABORTED
    );
    log.info("Submission {} aborted", request.submissionId());

    return createAcknowledgementResponse(request.submissionId(), "aborted");
  }

  @Nonnull
  private Parameters createAcknowledgementResponse(
      @Nonnull final String submissionId,
      @Nonnull final String status
  ) {
    final Parameters response = new Parameters();
    response.addParameter()
        .setName("submissionId")
        .setValue(new org.hl7.fhir.r4.model.StringType(submissionId));
    response.addParameter()
        .setName("status")
        .setValue(new org.hl7.fhir.r4.model.StringType(status));
    return response;
  }

}
