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
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Binary;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Builds status manifest responses for bulk submissions in export-style format.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Component
@Slf4j
@ConditionalOnProperty(prefix = "pathling.bulk-submit", name = "enabled", havingValue = "true")
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
   *
   * @param submission The submission.
   * @param result The submission result (may be null if processing failed early).
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

      // Add transaction time.
      if (result != null) {
        manifest.put("transactionTime", result.transactionTime());
      } else if (submission.completedAt() != null) {
        manifest.put("transactionTime", submission.completedAt());
      }

      // Add requiresAccessToken.
      manifest.put("requiresAccessToken", result != null && result.requiresAccessToken());

      // Add output array.
      final ArrayNode outputArray = objectMapper.createArrayNode();
      if (result != null) {
        for (final OutputFile outputFile : result.outputFiles()) {
          final ObjectNode outputNode = objectMapper.createObjectNode();
          outputNode.put("type", outputFile.type());
          outputNode.put("url", outputFile.url());
          if (outputFile.count() != null) {
            outputNode.put("count", outputFile.count());
          }
          outputArray.add(outputNode);
        }
      }
      manifest.set("output", outputArray);

      // Add error array.
      final ArrayNode errorArray = objectMapper.createArrayNode();
      if (result != null) {
        for (final ErrorFile errorFile : result.errorFiles()) {
          final ObjectNode errorNode = objectMapper.createObjectNode();
          errorNode.put("type", errorFile.type());
          errorNode.put("url", errorFile.url());

          // Add countSeverity extension if present.
          if (errorFile.countBySeverity() != null && !errorFile.countBySeverity().isEmpty()) {
            final ArrayNode countExtensions = objectMapper.createArrayNode();
            for (final Map.Entry<String, Long> entry : errorFile.countBySeverity().entrySet()) {
              final ObjectNode countExt = objectMapper.createObjectNode();
              countExt.put("url",
                  "http://hl7.org/fhir/uv/bulkdata/StructureDefinition/bulk-submit-count-severity");
              final ObjectNode valueNode = objectMapper.createObjectNode();
              valueNode.put("severity", entry.getKey());
              valueNode.put("count", entry.getValue());
              countExt.set("valueCoding", valueNode);
              countExtensions.add(countExt);
            }
            errorNode.set("extension", countExtensions);
          }
          errorArray.add(errorNode);
        }
      }
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
