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
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.StringType;
import org.springframework.stereotype.Component;

/**
 * Provides the $bulk-submit-error operation for retrieving error details from failed manifest jobs.
 * <p>
 * This endpoint returns OperationOutcome resources containing error details for manifest jobs that
 * failed during processing. The URLs in the status manifest error section point to this operation.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Component
@Slf4j
public class BulkSubmitErrorProvider {

  @Nonnull
  private final SubmissionRegistry submissionRegistry;

  /**
   * Creates a new BulkSubmitErrorProvider.
   *
   * @param submissionRegistry The registry for looking up submissions.
   */
  public BulkSubmitErrorProvider(@Nonnull final SubmissionRegistry submissionRegistry) {
    this.submissionRegistry = submissionRegistry;
  }

  /**
   * The $bulk-submit-error operation endpoint.
   * <p>
   * Returns an OperationOutcome resource containing error details for a failed manifest job.
   *
   * @param submissionId The submission ID.
   * @param manifestJobId The manifest job ID that failed.
   * @return An OperationOutcome resource with error details.
   */
  @Operation(name = "$bulk-submit-error", idempotent = true)
  @OperationAccess("bulk-submit")
  @Nonnull
  public OperationOutcome bulkSubmitErrorOperation(
      @OperationParam(name = "submissionId") final StringType submissionId,
      @OperationParam(name = "manifestJobId") final StringType manifestJobId
  ) {
    if (submissionId == null || submissionId.isEmpty()) {
      throw new ca.uhn.fhir.rest.server.exceptions.InvalidRequestException(
          "Missing required parameter: submissionId"
      );
    }
    if (manifestJobId == null || manifestJobId.isEmpty()) {
      throw new ca.uhn.fhir.rest.server.exceptions.InvalidRequestException(
          "Missing required parameter: manifestJobId"
      );
    }

    // Find the submission by ID (without requiring submitter).
    final Submission submission = submissionRegistry.getBySubmissionId(submissionId.getValue())
        .orElseThrow(() -> new ResourceNotFoundException(
            "Submission not found: " + submissionId.getValue()
        ));

    // Find the manifest job.
    final ManifestJob manifestJob = submission.manifestJobs().stream()
        .filter(job -> job.manifestJobId().equals(manifestJobId.getValue()))
        .findFirst()
        .orElseThrow(() -> new ResourceNotFoundException(
            "Manifest job not found: " + manifestJobId.getValue()
        ));

    // Build the OperationOutcome.
    return buildErrorOutcome(manifestJob);
  }

  @Nonnull
  private static OperationOutcome buildErrorOutcome(@Nonnull final ManifestJob manifestJob) {
    final OperationOutcome outcome = new OperationOutcome();

    final String errorMessage = manifestJob.errorMessage() != null
                                ? manifestJob.errorMessage()
                                : "Unknown error";

    outcome.addIssue()
        .setCode(IssueType.EXCEPTION)
        .setSeverity(IssueSeverity.ERROR)
        .setDiagnostics(errorMessage);

    return outcome;
  }

}
