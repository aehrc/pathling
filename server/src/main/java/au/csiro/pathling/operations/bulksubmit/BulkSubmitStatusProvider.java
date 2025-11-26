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

import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.StringType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Provides the $bulk-submit-status operation for checking submission status.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Component
@Slf4j
@ConditionalOnProperty(prefix = "pathling.bulk-submit", name = "enabled", havingValue = "true")
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
   *
   * @param submissionId The submission ID to check status for.
   * @param submitter The submitter identifier.
   * @param requestDetails The servlet request details.
   * @return A Binary resource containing the status manifest JSON.
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

    final Submission submission = submissionRegistry.get(submitterIdentifier,
            submissionId.getValue())
        .orElseThrow(() -> new ResourceNotFoundException(
            "Submission not found: " + submissionId.getValue()
        ));

    log.debug("Status check for submission {}: state={}", submission.submissionId(),
        submission.state());

    // If the submission is still processing, return 202 Accepted.
    if (submission.state() == SubmissionState.PENDING
        || submission.state() == SubmissionState.PROCESSING) {
      // For now, return a simple status response.
      // In a full implementation, this would use ProcessingNotCompletedException.
      return resultBuilder.buildProcessingResponse(submission, requestDetails.getFhirServerBase());
    }

    // Get the result if available.
    final SubmissionResult result = submissionRegistry.getResult(submission.submissionId())
        .orElse(null);

    return resultBuilder.buildStatusManifest(submission, result,
        requestDetails.getFhirServerBase());
  }

}
