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

import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.Identifier;
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

  @Nonnull
  private final SubmissionRegistry submissionRegistry;

  @Nonnull
  private final BulkSubmitResultBuilder resultBuilder;

  /**
   * Creates a new BulkSubmitStatusProvider.
   *
   * @param submissionRegistry The registry for tracking submissions.
   * @param resultBuilder The builder for generating result manifests.
   */
  public BulkSubmitStatusProvider(
      @Nonnull final SubmissionRegistry submissionRegistry,
      @Nonnull final BulkSubmitResultBuilder resultBuilder
  ) {
    this.submissionRegistry = submissionRegistry;
    this.resultBuilder = resultBuilder;
  }

  /**
   * The $bulk-submit-status operation endpoint.
   * <p>
   * When called with {@code Prefer: respond-async}, this operation integrates with the async Job
   * framework. The method blocks until the submission completes, while the framework handles
   * returning 202 Accepted with a Content-Location header pointing to the $job polling endpoint.
   *
   * @param submissionId The submission ID to check status for.
   * @param submitter The submitter identifier.
   * @param requestDetails The servlet request details.
   * @return A Binary resource containing the status manifest JSON.
   */
  @Operation(name = "$bulk-submit-status")
  @OperationAccess("bulk-submit")
  @AsyncSupported
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

    // Block until the submission completes. The async framework handles returning 202 with
    // Content-Location pointing to $job while this method is running.
    Submission submission = getSubmission(submitterIdentifier, submissionId.getValue());
    while (submission.state() == SubmissionState.PENDING
        || submission.state() == SubmissionState.PROCESSING) {
      log.debug("Submission {} still processing, waiting...", submission.submissionId());
      try {
        Thread.sleep(1000);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Status check interrupted", e);
      }
      submission = getSubmission(submitterIdentifier, submissionId.getValue());
    }

    log.debug("Submission {} completed with state: {}", submission.submissionId(),
        submission.state());

    // Check for error state and throw exception with error details.
    if (submission.state() == SubmissionState.COMPLETED_WITH_ERRORS) {
      final String errorMessage = submission.errorMessage() != null
                                  ? submission.errorMessage()
                                  : "Unknown error";
      throw new InternalErrorException("Submission failed: " + errorMessage);
    }

    // Get the result if available.
    final SubmissionResult result = submissionRegistry.getResult(submission.submissionId())
        .orElse(null);

    return resultBuilder.buildStatusManifest(submission, result,
        requestDetails.getFhirServerBase());
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

}
