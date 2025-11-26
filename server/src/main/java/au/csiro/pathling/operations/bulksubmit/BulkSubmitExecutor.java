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

import au.csiro.pathling.config.BulkSubmitConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Executes bulk submission processing by fetching manifests, downloading files, and delegating to
 * ImportExecutor.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Component
@Slf4j
@ConditionalOnProperty(prefix = "pathling.bulk-submit", name = "enabled", havingValue = "true")
public class BulkSubmitExecutor {

  // Pattern to match resource type with optional qualifier, e.g. "Patient.0000" -> "Patient".
  private static final Pattern BASE_NAME_WITH_QUALIFIER = Pattern.compile(
      "^([A-Za-z]+)(\\.[^.]+)?$");

  private static final Duration HTTP_TIMEOUT = Duration.ofMinutes(5);
  private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT;

  @Nonnull
  private final ServerConfiguration serverConfiguration;

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
   * @param serverConfiguration The server configuration.
   * @param importExecutor The import executor for processing files.
   * @param submissionRegistry The submission registry for tracking state.
   */
  public BulkSubmitExecutor(
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final ImportExecutor importExecutor,
      @Nonnull final SubmissionRegistry submissionRegistry
  ) {
    this.serverConfiguration = serverConfiguration;
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

    final BulkSubmitConfiguration config = getBulkSubmitConfig();
    Path tempDir = null;
    final List<OutputFile> outputFiles = new ArrayList<>();
    final List<ErrorFile> errorFiles = new ArrayList<>();

    try {
      // Create temporary directory for downloaded files.
      tempDir = createTempDirectory(config, submission.submissionId());

      // Fetch and parse the manifest.
      final JsonNode manifest = fetchManifest(submission);

      // Download files from the manifest.
      final Map<String, Collection<String>> downloadedFiles = downloadFilesFromManifest(
          manifest, submission, tempDir, config
      );

      if (downloadedFiles.isEmpty()) {
        throw new InvalidUserInputError("No files were downloaded from the manifest");
      }

      // Create ImportRequest and delegate to ImportExecutor.
      final ImportRequest importRequest = new ImportRequest(
          submission.submissionId(),
          submission.fhirBaseUrl() != null
          ? submission.fhirBaseUrl()
          : "unknown",
          downloadedFiles,
          SaveMode.MERGE,
          ImportFormat.NDJSON
      );

      // Execute the import.
      final String jobId = submission.submissionId();
      importExecutor.execute(importRequest, jobId);

      // Build output file list.
      for (final Map.Entry<String, Collection<String>> entry : downloadedFiles.entrySet()) {
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

    } finally {
      // Clean up temporary directory.
      if (tempDir != null) {
        cleanupTempDirectory(tempDir);
      }
    }
  }

  @Nonnull
  private BulkSubmitConfiguration getBulkSubmitConfig() {
    final BulkSubmitConfiguration config = serverConfiguration.getBulkSubmit();
    if (config == null) {
      throw new InvalidUserInputError("Bulk submit configuration is missing");
    }
    return config;
  }

  @Nonnull
  private Path createTempDirectory(
      @Nonnull final BulkSubmitConfiguration config,
      @Nonnull final String submissionId
  ) throws IOException {
    final String stagingLocation = config.getStagingLocation();
    final Path baseDir = Path.of(URI.create(stagingLocation).getPath());

    if (!Files.exists(baseDir)) {
      Files.createDirectories(baseDir);
      log.debug("Created staging directory: {}", baseDir);
    }

    final String tempDirName = "bulk-submit-" + submissionId;
    final Path tempDir = baseDir.resolve(tempDirName);
    if (!Files.exists(tempDir)) {
      Files.createDirectories(tempDir);
    }
    log.debug("Using staging directory: {}", tempDir);

    return tempDir;
  }

  @Nonnull
  private JsonNode fetchManifest(@Nonnull final Submission submission) throws IOException {
    final String manifestUrl = submission.manifestUrl();
    if (manifestUrl == null) {
      throw new InvalidUserInputError("Manifest URL is required");
    }

    log.info("Fetching manifest from: {}", manifestUrl);

    final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
        .uri(URI.create(manifestUrl))
        .timeout(HTTP_TIMEOUT)
        .GET();

    // Add custom headers from the submission.
    for (final FileRequestHeader header : submission.fileRequestHeaders()) {
      requestBuilder.header(header.name(), header.value());
    }

    final HttpRequest request = requestBuilder.build();

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

  @Nonnull
  private Map<String, Collection<String>> downloadFilesFromManifest(
      @Nonnull final JsonNode manifest,
      @Nonnull final Submission submission,
      @Nonnull final Path tempDir,
      @Nonnull final BulkSubmitConfiguration config
  ) {
    final Map<String, Collection<String>> result = new HashMap<>();
    final JsonNode outputNode = manifest.get("output");

    if (outputNode == null || !outputNode.isArray()) {
      log.warn("Manifest has no output array");
      return result;
    }

    final int maxConcurrent = config.getMaxConcurrentDownloads();
    final ExecutorService downloadExecutor = Executors.newFixedThreadPool(maxConcurrent);

    try {
      final List<CompletableFuture<DownloadResult>> futures = new ArrayList<>();

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

        futures.add(CompletableFuture.supplyAsync(
            () -> downloadFile(url, type, tempDir, submission.fileRequestHeaders()),
            downloadExecutor
        ));
      }

      // Wait for all downloads to complete.
      for (final CompletableFuture<DownloadResult> future : futures) {
        try {
          final DownloadResult downloadResult = future.join();
          if (downloadResult != null && downloadResult.localPath != null) {
            result.computeIfAbsent(downloadResult.resourceType, k -> new ArrayList<>())
                .add(downloadResult.localPath.toUri().toString());
          }
        } catch (final Exception e) {
          log.error("Download failed: {}", e.getMessage());
        }
      }

    } finally {
      downloadExecutor.shutdown();
    }

    log.info("Downloaded files by resource type: {}", result.keySet());
    return result;
  }

  @Nonnull
  private DownloadResult downloadFile(
      @Nonnull final String url,
      @Nonnull final String resourceType,
      @Nonnull final Path tempDir,
      @Nonnull final List<FileRequestHeader> headers
  ) {
    log.debug("Downloading file: {} (type: {})", url, resourceType);

    try {
      final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(HTTP_TIMEOUT)
          .GET();

      for (final FileRequestHeader header : headers) {
        requestBuilder.header(header.name(), header.value());
      }

      final HttpRequest request = requestBuilder.build();
      final HttpResponse<InputStream> response = httpClient.send(
          request,
          HttpResponse.BodyHandlers.ofInputStream()
      );

      if (response.statusCode() != 200) {
        log.error("Failed to download {}: HTTP {}", url, response.statusCode());
        return new DownloadResult(resourceType, null);
      }

      // Generate a unique filename.
      final String filename = resourceType + "." + System.nanoTime() + ".ndjson";
      final Path localPath = tempDir.resolve(filename);

      try (final InputStream body = response.body()) {
        Files.copy(body, localPath);
      }

      log.debug("Downloaded {} to {}", url, localPath);
      return new DownloadResult(resourceType, localPath);

    } catch (final IOException | InterruptedException e) {
      log.error("Failed to download {}: {}", url, e.getMessage());
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return new DownloadResult(resourceType, null);
    }
  }

  private void cleanupTempDirectory(@Nonnull final Path tempDir) {
    try {
      if (Files.exists(tempDir)) {
        try (final var paths = Files.walk(tempDir)) {
          paths.sorted(Comparator.reverseOrder())
              .forEach(path -> {
                try {
                  Files.delete(path);
                } catch (final IOException e) {
                  log.warn("Failed to delete temporary file: {}", path, e);
                }
              });
        }
        log.debug("Cleaned up temporary directory: {}", tempDir);
      }
    } catch (final IOException e) {
      log.warn("Failed to clean up temporary directory: {}", tempDir, e);
    }
  }

  private record DownloadResult(
      @Nonnull String resourceType,
      Path localPath
  ) {

  }

}
