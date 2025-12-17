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
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Binary;
import org.springframework.stereotype.Component;

/**
 * Builds status manifest responses for bulk submissions in export-style format.
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
   * Builds a status manifest for a completed or errored submission.
   * <p>
   * Aggregates output files from all completed manifest jobs within the submission.
   *
   * @param submission The submission.
   * @param result The submission result (deprecated, output files are now aggregated from manifest
   * jobs).
   * @param serverBaseUrl The server base URL for constructing result URLs.
   * @return A Binary resource containing the manifest JSON.
   */
  @Nonnull
  public Binary buildStatusManifest(
      @Nonnull final Submission submission,
      @Nullable final SubmissionResult result,
      @Nonnull final String serverBaseUrl
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

      // Aggregate output files from all completed manifest jobs.
      final List<OutputFile> aggregatedOutputFiles = aggregateOutputFiles(submission);

      // Determine transaction time from submission or latest manifest job completion.
      final String transactionTime = getTransactionTime(submission);
      if (transactionTime != null) {
        manifest.put("transactionTime", transactionTime);
      }

      // Add requiresAccessToken (always false for now).
      manifest.put("requiresAccessToken", false);

      // Add output array with aggregated files from all manifest jobs.
      final ArrayNode outputArray = objectMapper.createArrayNode();
      for (final OutputFile outputFile : aggregatedOutputFiles) {
        final ObjectNode outputNode = objectMapper.createObjectNode();
        outputNode.put("type", outputFile.type());
        outputNode.put("url", outputFile.url());
        if (outputFile.count() != null) {
          outputNode.put("count", outputFile.count());
        }
        outputArray.add(outputNode);
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
   * Aggregates output files from all completed manifest jobs in the submission.
   *
   * @param submission The submission containing manifest jobs.
   * @return A list of all output files from completed manifest jobs.
   */
  @Nonnull
  private List<OutputFile> aggregateOutputFiles(@Nonnull final Submission submission) {
    final List<OutputFile> allOutputFiles = new ArrayList<>();
    for (final ManifestJob job : submission.manifestJobs()) {
      if (job.state() == ManifestJobState.COMPLETED) {
        allOutputFiles.addAll(job.outputFiles());
      }
    }
    return allOutputFiles;
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

  /**
   * Builds a processing status response for submissions still in progress.
   *
   * @param submission The submission being processed.
   * @param serverBaseUrl The server base URL.
   * @return A Binary resource containing the status JSON.
   */
  @Nonnull
  public Binary buildProcessingResponse(
      @Nonnull final Submission submission,
      @Nonnull final String serverBaseUrl
  ) {
    try {
      final ObjectNode status = objectMapper.createObjectNode();

      // Add submission ID extension.
      final ArrayNode extensions = objectMapper.createArrayNode();
      final ObjectNode submissionIdExt = objectMapper.createObjectNode();
      submissionIdExt.put("url",
          "http://hl7.org/fhir/uv/bulkdata/StructureDefinition/bulk-submit-submission-id");
      submissionIdExt.put("valueString", submission.submissionId());
      extensions.add(submissionIdExt);
      status.set("extension", extensions);

      status.put("status", submission.state().getCode());
      status.put("message", "Submission is being processed");

      final String json = objectMapper.writerWithDefaultPrettyPrinter()
          .writeValueAsString(status);

      final Binary binary = new Binary();
      binary.setContentType(CONTENT_TYPE_JSON);
      binary.setData(json.getBytes(StandardCharsets.UTF_8));
      return binary;

    } catch (final JsonProcessingException e) {
      log.error("Failed to build processing response", e);
      throw new RuntimeException("Failed to build processing response", e);
    }
  }

}
