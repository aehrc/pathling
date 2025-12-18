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

import au.csiro.pathling.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Binary;
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

  private static final String CONTENT_TYPE_JSON = "application/json";

  @Nonnull
  private final ObjectMapper objectMapper;

  /**
   * Creates a new BulkSubmitResultBuilder.
   */
  public BulkSubmitResultBuilder() {
    this.objectMapper = new ObjectMapper();
  }

  /**
   * Builds a status manifest for a submission.
   * <p>
   * This method is used for both completed (HTTP 200) and in-progress (HTTP 202) responses. The
   * manifest structure is the same in both cases, as per the Argonaut bulk submit specification.
   * <p>
   * Once files have been downloaded, the output array includes entries with URLs pointing to the
   * $result operation where clients can download the files.
   *
   * @param submission The submission.
   * @param requestUrl The URL of the status request endpoint.
   * @param fhirServerBase The FHIR server base URL for building result file URLs.
   * @return A Binary resource containing the manifest JSON.
   */
  @Nonnull
  public Binary buildStatusManifest(
      @Nonnull final Submission submission,
      @Nonnull final String requestUrl,
      @Nonnull final String fhirServerBase
  ) {
    try {
      final ObjectNode manifest = objectMapper.createObjectNode();

      // Add submission ID extension.
      final ArrayNode extensions = objectMapper.createArrayNode();
      final ObjectNode submissionIdExt = objectMapper.createObjectNode();
      submissionIdExt.put("url",
          "http://hl7.org/fhir/uv/bulkdata/StructureDefinition/bulk-submit-submission-id");
      submissionIdExt.put("valueString", submission.submissionId());
      extensions.add(submissionIdExt);
      manifest.set("extension", extensions);

      // Determine transaction time from submission or latest manifest job completion.
      final String transactionTime = getTransactionTime(submission);
      if (transactionTime != null) {
        manifest.put("transactionTime", transactionTime);
      }

      // Add request URL.
      manifest.put("request", requestUrl);

      // Add requiresAccessToken (always false for now).
      manifest.put("requiresAccessToken", false);

      // Build output array with downloaded files.
      final ArrayNode outputArray = objectMapper.createArrayNode();
      for (final ManifestJob job : submission.manifestJobs()) {
        if (job.downloadedFiles() != null) {
          for (final DownloadedFile file : job.downloadedFiles()) {
            final ObjectNode outputEntry = objectMapper.createObjectNode();
            outputEntry.put("type", file.resourceType());
            outputEntry.put("url", buildResultUrl(fhirServerBase, submission.submissionId(),
                file.fileName()));
            outputArray.add(outputEntry);
          }
        }
      }
      manifest.set("output", outputArray);

      // Add empty error array (errors are communicated via submission state).
      final ArrayNode errorArray = objectMapper.createArrayNode();
      manifest.set("error", errorArray);

      final String json = objectMapper.writerWithDefaultPrettyPrinter()
          .writeValueAsString(manifest);

      final Binary binary = new Binary();
      binary.setContentType(CONTENT_TYPE_JSON);
      binary.setData(json.getBytes(StandardCharsets.UTF_8));
      return binary;

    } catch (final JsonProcessingException e) {
      log.error("Failed to build status manifest", e);
      throw new RuntimeException("Failed to build status manifest", e);
    }
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
      @Nonnull final String fileName
  ) {
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
        .filter(completedAt -> completedAt != null)
        .max(String::compareTo)
        .orElse(null);
  }

}
