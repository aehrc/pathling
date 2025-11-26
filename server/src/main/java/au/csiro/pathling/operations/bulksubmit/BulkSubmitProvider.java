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

import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.async.RequestTag;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
@ConditionalOnProperty(prefix = "pathling.bulk-submit", name = "enabled", havingValue = "true")
public class BulkSubmitProvider implements PreAsyncValidation<BulkSubmitRequest> {

  @Nonnull
  private final BulkSubmitValidator validator;

  @Nonnull
  private final SubmissionRegistry submissionRegistry;

  @Nonnull
  private final RequestTagFactory requestTagFactory;

  @Nonnull
  private final JobRegistry jobRegistry;

  @Nullable
  private final BulkSubmitExecutor executor;

  /**
   * Creates a new BulkSubmitProvider.
   *
   * @param validator The validator for bulk submit requests.
   * @param submissionRegistry The registry for tracking submissions.
   * @param requestTagFactory The factory for creating request tags.
   * @param jobRegistry The registry for async jobs.
   * @param executor The executor for processing submissions (may be null during early startup).
   */
  public BulkSubmitProvider(
      @Nonnull final BulkSubmitValidator validator,
      @Nonnull final SubmissionRegistry submissionRegistry,
      @Nonnull final RequestTagFactory requestTagFactory,
      @Nonnull final JobRegistry jobRegistry,
      @Nullable final BulkSubmitExecutor executor
  ) {
    this.validator = validator;
    this.submissionRegistry = submissionRegistry;
    this.requestTagFactory = requestTagFactory;
    this.jobRegistry = jobRegistry;
    this.executor = executor;
  }

  /**
   * The $bulk-submit operation endpoint.
   *
   * @param parameters The FHIR Parameters resource containing the submission request.
   * @param requestDetails The servlet request details.
   * @return A FHIR Parameters resource describing the result, or null for async processing.
   */
  @Operation(name = "$bulk-submit")
  @SuppressWarnings("UnusedReturnValue")
  @OperationAccess("bulk-submit")
  @AsyncSupported
  @Nullable
  public Parameters bulkSubmitOperation(
      @ResourceParam final Parameters parameters,
      @Nonnull final ServletRequestDetails requestDetails
  ) {
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    final RequestTag ownTag = requestTagFactory.createTag(requestDetails, authentication);
    final Job<BulkSubmitRequest> ownJob = jobRegistry.get(ownTag);

    if (ownJob == null) {
      throw new InvalidRequestException("Missing 'Prefer: respond-async' header value.");
    }

    // Check that the user requesting is the same user that started the job.
    final Optional<String> currentUserId = getCurrentUserId(authentication);
    if (currentUserId.isPresent() && !ownJob.getOwnerId().equals(currentUserId)) {
      throw new AccessDeniedError(
          "The requested result is not owned by the current user '%s'.".formatted(
              currentUserId.orElse("null")));
    }

    final BulkSubmitRequest request = ownJob.getPreAsyncValidationResult();
    if (ownJob.isCancelled()) {
      return null;
    }

    // Handle the submission based on status.
    return handleSubmission(request, currentUserId);
  }

  @Nonnull
  private Parameters handleSubmission(
      @Nonnull final BulkSubmitRequest request,
      @Nonnull final Optional<String> ownerId
  ) {
    final Optional<Submission> existingSubmission = submissionRegistry.get(
        request.submitter(),
        request.submissionId()
    );

    if (request.isInProgress()) {
      return handleInProgressSubmission(request, existingSubmission, ownerId);
    } else if (request.isComplete()) {
      return handleCompleteSubmission(request, existingSubmission, ownerId);
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
      @Nonnull final Optional<String> ownerId
  ) {
    // For "in-progress" status, create or update the submission in PENDING state.
    if (existingSubmission.isPresent()) {
      final Submission existing = existingSubmission.get();
      if (existing.state() != SubmissionState.PENDING) {
        throw new InvalidRequestException(
            "Cannot update submission %s: current state is %s, not PENDING."
                .formatted(request.submissionId(), existing.state())
        );
      }
      // Already in PENDING state, this is a duplicate notification - that's OK.
      log.debug("Received duplicate in-progress notification for submission: {}",
          request.submissionId());
    } else {
      // Create new submission in PENDING state.
      final Submission submission = Submission.createPending(
          request.submissionId(),
          request.submitter(),
          ownerId
      );
      submissionRegistry.put(submission);
      log.info("Created new submission: {}", request.submissionId());
    }

    return createAcknowledgementResponse(request.submissionId(), "in-progress");
  }

  @Nonnull
  private Parameters handleCompleteSubmission(
      @Nonnull final BulkSubmitRequest request,
      @Nonnull final Optional<Submission> existingSubmission,
      @Nonnull final Optional<String> ownerId
  ) {
    // For "complete" status, update the submission and trigger processing.
    final Submission submission;
    if (existingSubmission.isPresent()) {
      final Submission existing = existingSubmission.get();
      if (existing.state() != SubmissionState.PENDING) {
        throw new InvalidRequestException(
            "Cannot complete submission %s: current state is %s, not PENDING."
                .formatted(request.submissionId(), existing.state())
        );
      }
      submission = existing.withManifestDetails(
          request.manifestUrl(),
          request.fhirBaseUrl(),
          request.fileRequestHeaders(),
          request.metadata()
      );
    } else {
      // No prior in-progress notification - create directly in PROCESSING state.
      submission = Submission.createPending(
          request.submissionId(),
          request.submitter(),
          ownerId
      ).withManifestDetails(
          request.manifestUrl(),
          request.fhirBaseUrl(),
          request.fileRequestHeaders(),
          request.metadata()
      );
    }

    submissionRegistry.put(submission);
    log.info("Submission {} marked complete, starting processing", request.submissionId());

    // Execute the submission processing.
    if (executor != null) {
      executor.execute(submission);
    } else {
      log.warn("BulkSubmitExecutor not available - submission {} will not be processed",
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

  @Override
  @Nonnull
  public PreAsyncValidationResult<BulkSubmitRequest> preAsyncValidate(
      @Nonnull final ServletRequestDetails servletRequestDetails,
      @Nonnull final Object[] params
  ) throws InvalidRequestException {
    return validator.validateRequest(servletRequestDetails, (Parameters) params[0]);
  }

}
