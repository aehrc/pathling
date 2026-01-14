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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.springframework.stereotype.Component;

/**
 * Builds status manifest responses for bulk submissions.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Component
@Slf4j
public class BulkSubmitResultBuilder {

  private static final String SUBMISSION_ID_EXT_URL =
      "http://hl7.org/fhir/uv/bulkdata/StructureDefinition/bulk-submit-submission-id";
  private static final String EXTENSION = "extension";

  /**
   * Builds a status manifest for a submission.
   *
   * <p>This method is used for both completed (HTTP 200) and in-progress (HTTP 202) responses. The
   * manifest structure is the same in both cases, as per the Argonaut bulk submit specification.
   *
   * <p>Once files have been downloaded, the output array includes entries with URLs pointing to the
   * $result operation where clients can download the files.
   *
   * @param submission The submission.
   * @param requestUrl The URL of the status request endpoint.
   * @param fhirServerBase The FHIR server base URL for building result file URLs.
   * @return A Parameters resource containing the manifest.
   */
  @Nonnull
  public Parameters buildStatusManifest(
      @Nonnull final Submission submission,
      @Nonnull final String requestUrl,
      @Nonnull final String fhirServerBase) {

    final Parameters parameters = new Parameters();

    // Add submission ID extension parameter.
    final ParametersParameterComponent extensionParam = parameters.addParameter();
    extensionParam.setName(EXTENSION);
    extensionParam.addPart().setName("url").setValue(new UriType(SUBMISSION_ID_EXT_URL));
    extensionParam
        .addPart()
        .setName("valueString")
        .setValue(new StringType(submission.submissionId()));

    // Determine transaction time from submission or latest manifest job completion.
    final String transactionTime = getTransactionTime(submission);
    if (transactionTime != null) {
      parameters
          .addParameter()
          .setName("transactionTime")
          .setValue(new InstantType(transactionTime));
    }

    // Add request URL.
    parameters.addParameter().setName("request").setValue(new UriType(requestUrl));

    // Add requiresAccessToken (always false for now).
    parameters.addParameter().setName("requiresAccessToken").setValue(new BooleanType(false));

    // Build output parameters with downloaded files.
    for (final ManifestJob job : submission.manifestJobs()) {
      if (job.downloadedFiles() != null) {
        for (final DownloadedFile file : job.downloadedFiles()) {
          final ParametersParameterComponent outputParam = parameters.addParameter();
          outputParam.setName("output");
          outputParam.addPart().setName("type").setValue(new CodeType(file.resourceType()));
          outputParam
              .addPart()
              .setName("url")
              .setValue(
                  new UriType(
                      buildResultUrl(fhirServerBase, submission.submissionId(), file.fileName())));

          // Add manifestUrl extension per spec.
          final ParametersParameterComponent extPart = outputParam.addPart();
          extPart.setName(EXTENSION);
          extPart.addPart().setName("url").setValue(new UriType("manifestUrl"));
          extPart.addPart().setName("valueUrl").setValue(new UriType(file.manifestUrl()));
        }
      }
    }

    // Build error parameters from failed manifest jobs that have error files.
    for (final ManifestJob job : submission.manifestJobs()) {
      if (job.state() == ManifestJobState.FAILED && job.errorFileName() != null) {
        final ParametersParameterComponent errorParam = parameters.addParameter();
        errorParam.setName("error");
        errorParam.addPart().setName("type").setValue(new CodeType("OperationOutcome"));
        errorParam
            .addPart()
            .setName("url")
            .setValue(
                new UriType(
                    buildResultUrl(
                        fhirServerBase, submission.submissionId(), job.errorFileName())));

        // Add manifestUrl extension per spec.
        final ParametersParameterComponent extPart = errorParam.addPart();
        extPart.setName(EXTENSION);
        extPart.addPart().setName("url").setValue(new UriType("manifestUrl"));
        extPart.addPart().setName("valueUrl").setValue(new UriType(job.manifestUrl()));
      }
    }

    return parameters;
  }

  /**
   * Builds a URL for downloading a result file via the $result operation.
   *
   * @param fhirServerBase The FHIR server base URL.
   * @param submissionId The submission ID (used as the job ID).
   * @param fileName The name of the file.
   * @return The complete $result URL.
   */
  @Nonnull
  private static String buildResultUrl(
      @Nonnull final String fhirServerBase,
      @Nonnull final String submissionId,
      @Nonnull final String fileName) {
    return fhirServerBase + "/$result?job=" + submissionId + "&file=" + fileName;
  }

  /**
   * Determines the transaction time for the status manifest.
   *
   * @param submission The submission.
   * @return The transaction time, or null if not available.
   */
  @Nullable
  private String getTransactionTime(@Nonnull final Submission submission) {
    // Use submission's completedAt if available.
    if (submission.completedAt() != null) {
      return submission.completedAt();
    }

    // Otherwise, find the latest completedAt from manifest jobs.
    return submission.manifestJobs().stream()
        .map(ManifestJob::completedAt)
        .filter(Objects::nonNull)
        .max(String::compareTo)
        .orElse(null);
  }
}
