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

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.operations.bulkimport.ImportExecutor;
import au.csiro.pathling.operations.bulkimport.ImportFormat;
import au.csiro.pathling.operations.bulkimport.ImportRequest;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Executes bulk submission processing by fetching manifests and delegating to ImportExecutor.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Component
@Slf4j
@ConditionalOnProperty(prefix = "pathling.bulk-submit", name = "enabled", havingValue = "true")
public class BulkSubmitExecutor {

  private static final Duration HTTP_TIMEOUT = Duration.ofMinutes(5);
  private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT;

  @Nonnull
  private final ImportExecutor importExecutor;

  @Nonnull
  private final SubmissionRegistry submissionRegistry;

  @Nonnull
  private final ObjectMapper objectMapper;

  @Nonnull
  private final HttpClient httpClient;

  /**
   * Creates a new BulkSubmitExecutor.
   *
   * @param importExecutor The import executor for processing files.
   * @param submissionRegistry The submission registry for tracking state.
   */
  public BulkSubmitExecutor(
      @Nonnull final ImportExecutor importExecutor,
      @Nonnull final SubmissionRegistry submissionRegistry
  ) {
    this.importExecutor = importExecutor;
    this.submissionRegistry = submissionRegistry;
    this.objectMapper = new ObjectMapper();
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(HTTP_TIMEOUT)
        .build();
  }

  /**
   * Executes the bulk submission processing asynchronously.
   *
   * @param submission The submission to process.
   */
  public void execute(@Nonnull final Submission submission) {
    // Execute asynchronously to not block the request thread.
    CompletableFuture.runAsync(() -> executeInternal(submission));
  }

  private void executeInternal(@Nonnull final Submission submission) {
    log.info("Starting processing for submission: {}", submission.submissionId());

    final List<OutputFile> outputFiles = new ArrayList<>();
    final List<ErrorFile> errorFiles = new ArrayList<>();

    try {
      // Fetch and parse the manifest.
      final JsonNode manifest = fetchManifest(submission);

      // Extract file URLs from the manifest.
      final Map<String, Collection<String>> fileUrls = extractUrlsFromManifest(manifest);

      if (fileUrls.isEmpty()) {
        throw new InvalidUserInputError("No files found in manifest");
      }

      // Create ImportRequest and delegate to ImportExecutor.
      final ImportRequest importRequest = new ImportRequest(
          submission.submissionId(),
          submission.fhirBaseUrl() != null
          ? submission.fhirBaseUrl()
          : "unknown",
          fileUrls,
          SaveMode.MERGE,
          ImportFormat.NDJSON
      );

      // Execute the import.
      final String jobId = submission.submissionId();
      importExecutor.execute(importRequest, jobId);

      // Build output file list.
      for (final Map.Entry<String, Collection<String>> entry : fileUrls.entrySet()) {
        final String resourceType = entry.getKey();
        final long count = entry.getValue().size();
        outputFiles.add(new OutputFile(resourceType, "", count));
      }

      // Update submission state to COMPLETED.
      final SubmissionState finalState = errorFiles.isEmpty()
                                         ? SubmissionState.COMPLETED
                                         : SubmissionState.COMPLETED_WITH_ERRORS;
      submissionRegistry.updateState(
          submission.submitter(),
          submission.submissionId(),
          finalState
      );

      // Store the result.
      final SubmissionResult result = new SubmissionResult(
          submission.submissionId(),
          ISO_FORMATTER.format(Instant.now()),
          outputFiles,
          errorFiles,
          false
      );
      submissionRegistry.putResult(result);

      log.info("Submission {} completed with state: {}", submission.submissionId(), finalState);

    } catch (final Exception e) {
      log.error("Failed to process submission {}: {}", submission.submissionId(), e.getMessage(),
          e);

      // Update submission state to COMPLETED_WITH_ERRORS.
      submissionRegistry.updateState(
          submission.submitter(),
          submission.submissionId(),
          SubmissionState.COMPLETED_WITH_ERRORS
      );

      // Store error result.
      errorFiles.add(ErrorFile.create("error://internal", Map.of("error", 1L)));
      final SubmissionResult result = new SubmissionResult(
          submission.submissionId(),
          ISO_FORMATTER.format(Instant.now()),
          outputFiles,
          errorFiles,
          false
      );
      submissionRegistry.putResult(result);
    }
  }

  @Nonnull
  private JsonNode fetchManifest(@Nonnull final Submission submission) throws IOException {
    final String manifestUrl = submission.manifestUrl();
    if (manifestUrl == null) {
      throw new InvalidUserInputError("Manifest URL is required");
    }

    log.info("Fetching manifest from: {}", manifestUrl);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(manifestUrl))
        .timeout(HTTP_TIMEOUT)
        .GET()
        .build();

    try {
      final HttpResponse<InputStream> response = httpClient.send(
          request,
          HttpResponse.BodyHandlers.ofInputStream()
      );

      if (response.statusCode() != 200) {
        throw new InvalidUserInputError(
            "Failed to fetch manifest: HTTP " + response.statusCode()
        );
      }

      try (final InputStream body = response.body()) {
        return objectMapper.readTree(body);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InvalidUserInputError("Manifest fetch was interrupted", e);
    }
  }

  /**
   * Extracts file URLs from the manifest, organised by resource type.
   *
   * @param manifest The parsed manifest JSON.
   * @return A map of resource type to collection of file URLs.
   */
  @Nonnull
  private Map<String, Collection<String>> extractUrlsFromManifest(
      @Nonnull final JsonNode manifest) {
    final Map<String, Collection<String>> result = new HashMap<>();
    final JsonNode outputNode = manifest.get("output");

    if (outputNode == null || !outputNode.isArray()) {
      log.warn("Manifest has no output array");
      return result;
    }

    for (final JsonNode fileNode : outputNode) {
      final String type = fileNode.has("type")
                          ? fileNode.get("type").asText()
                          : null;
      final String url = fileNode.has("url")
                         ? fileNode.get("url").asText()
                         : null;

      if (type == null || url == null) {
        log.warn("Skipping manifest entry with missing type or url");
        continue;
      }

      result.computeIfAbsent(type, k -> new ArrayList<>()).add(url);
    }

    log.info("Extracted URLs for resource types: {}", result.keySet());
    return result;
  }

}
