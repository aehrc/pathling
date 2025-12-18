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

import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
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
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.stereotype.Component;

/**
 * Executes bulk submission processing by fetching manifests and delegating to ImportExecutor.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Component
@Slf4j
public class BulkSubmitExecutor {

  private static final Duration HTTP_TIMEOUT = Duration.ofMinutes(5);

  @Nonnull
  private final ImportExecutor importExecutor;

  @Nonnull
  private final SubmissionRegistry submissionRegistry;

  @Nonnull
  private final ServerConfiguration serverConfiguration;

  @Nonnull
  private final BulkSubmitResultBuilder resultBuilder;

  @Nullable
  private final JobRegistry jobRegistry;

  @Nullable
  private final SparkSession sparkSession;

  @Nonnull
  private final ObjectMapper objectMapper;

  @Nonnull
  private final HttpClient httpClient;

  /**
   * Creates a new BulkSubmitExecutor.
   *
   * @param importExecutor The import executor for processing files.
   * @param submissionRegistry The submission registry for tracking state.
   * @param serverConfiguration The server configuration.
   * @param resultBuilder The builder for creating status manifests.
   * @param jobRegistry The job registry for tracking async jobs (may be null if async is
   * disabled).
   * @param sparkSession The Spark session for setting job groups (may be null if async is
   * disabled).
   */
  public BulkSubmitExecutor(
      @Nonnull final ImportExecutor importExecutor,
      @Nonnull final SubmissionRegistry submissionRegistry,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final BulkSubmitResultBuilder resultBuilder,
      @Nullable final JobRegistry jobRegistry,
      @Nullable final SparkSession sparkSession
  ) {
    this.importExecutor = importExecutor;
    this.submissionRegistry = submissionRegistry;
    this.serverConfiguration = serverConfiguration;
    this.resultBuilder = resultBuilder;
    this.jobRegistry = jobRegistry;
    this.sparkSession = sparkSession;
    this.objectMapper = new ObjectMapper();
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(HTTP_TIMEOUT)
        .build();
  }

  /**
   * Executes processing for a specific manifest job asynchronously. Creates a Job in the
   * JobRegistry for progress tracking if async support is enabled.
   *
   * @param submission The submission containing the manifest job.
   * @param manifestJob The manifest job to process.
   * @param fileRequestHeaders Custom HTTP headers to include when downloading files.
   * @param fhirServerBase The FHIR server base URL for building result manifests.
   */
  public void executeManifestJob(
      @Nonnull final Submission submission,
      @Nonnull final ManifestJob manifestJob,
      @Nonnull final List<FileRequestHeader> fileRequestHeaders,
      @Nonnull final String fhirServerBase
  ) {
    // Create a Job for progress tracking if async is enabled.
    final String jobId;
    final CompletableFuture<IBaseResource> resultFuture;

    if (jobRegistry != null && sparkSession != null) {
      jobId = UUID.randomUUID().toString();
      resultFuture = new CompletableFuture<>();

      // Create and register the Job.
      final Optional<String> ownerId = Optional.ofNullable(submission.ownerId());
      final Job<Object> job = new Job<>(jobId, "bulk-submit-manifest", resultFuture, ownerId);
      jobRegistry.register(job);

      // Store the job ID in the manifest job.
      submissionRegistry.updateManifestJob(
          submission.submitter(),
          submission.submissionId(),
          manifestJob.manifestJobId(),
          mj -> mj.withJobId(jobId)
      );
      log.info("Created job {} for manifest job {} in submission {}",
          jobId, manifestJob.manifestJobId(), submission.submissionId());
    } else {
      jobId = null;
      resultFuture = null;
    }

    // Execute asynchronously to not block the request thread.
    CompletableFuture.runAsync(
        () -> executeManifestJobInternal(
            submission, manifestJob, fileRequestHeaders, fhirServerBase, jobId, resultFuture
        )
    );
  }

  private void executeManifestJobInternal(
      @Nonnull final Submission submission,
      @Nonnull final ManifestJob manifestJob,
      @Nonnull final List<FileRequestHeader> fileRequestHeaders,
      @Nonnull final String fhirServerBase,
      @Nullable final String jobId,
      @Nullable final CompletableFuture<IBaseResource> resultFuture
  ) {
    log.info("Starting processing for manifest job: {} in submission: {}",
        manifestJob.manifestJobId(), submission.submissionId());

    Path stagingDir = null;

    // Update manifest job to PROCESSING state.
    submissionRegistry.updateManifestJob(
        submission.submitter(),
        submission.submissionId(),
        manifestJob.manifestJobId(),
        mj -> mj.withState(ManifestJobState.PROCESSING)
    );

    // Set Spark job group for progress tracking if a job ID was provided.
    if (jobId != null && sparkSession != null) {
      sparkSession.sparkContext().setJobGroup(jobId, jobId, true);
      log.debug("Set Spark job group to {}", jobId);
    }

    try {
      // Fetch and parse the manifest.
      final JsonNode manifest = fetchManifest(manifestJob, fileRequestHeaders);

      // Extract file URLs from the manifest.
      final Map<String, Collection<String>> fileUrls = extractUrlsFromManifest(manifest);

      if (fileUrls.isEmpty()) {
        throw new InvalidUserInputError("No files found in manifest");
      }

      // Create staging directory and download files.
      stagingDir = createStagingDirectory(submission.submissionId(), manifestJob.manifestJobId());
      final Map<String, Collection<String>> localFilePaths = downloadFiles(
          fileUrls, stagingDir, fileRequestHeaders
      );

      // Create ImportRequest with local file paths and delegate to ImportExecutor.
      final ImportRequest importRequest = new ImportRequest(
          manifestJob.manifestJobId(),
          manifestJob.fhirBaseUrl() != null
          ? manifestJob.fhirBaseUrl()
          : "unknown",
          localFilePaths,
          SaveMode.MERGE,
          ImportFormat.NDJSON
      );

      // Execute the import with the bulk submit allowable sources.
      final BulkSubmitConfiguration bulkSubmitConfig = serverConfiguration.getBulkSubmit();
      final List<String> allowableSources = bulkSubmitConfig != null
                                            ? bulkSubmitConfig.getAllowableSources()
                                            : List.of();
      importExecutor.execute(importRequest, submission.submissionId(), allowableSources);

      // Update manifest job state to COMPLETED.
      submissionRegistry.updateManifestJob(
          submission.submitter(),
          submission.submissionId(),
          manifestJob.manifestJobId(),
          mj -> mj.withState(ManifestJobState.COMPLETED)
      );

      log.info("Manifest job {} completed successfully", manifestJob.manifestJobId());

      // Complete the async job if enabled.
      if (resultFuture != null) {
        // Build a simple status manifest for this manifest job.
        final Submission updatedSubmission = submissionRegistry.get(
            submission.submitter(), submission.submissionId()
        ).orElse(submission);
        // Construct the status request URL.
        final String requestUrl = buildStatusRequestUrl(fhirServerBase, submission);
        final IBaseResource statusManifest = resultBuilder.buildStatusManifest(
            updatedSubmission, requestUrl
        );
        resultFuture.complete(statusManifest);
      }

    } catch (final Exception e) {
      log.error("Failed to process manifest job {}: {}",
          manifestJob.manifestJobId(), e.getMessage(), e);

      // Update manifest job with error message.
      submissionRegistry.updateManifestJob(
          submission.submitter(),
          submission.submissionId(),
          manifestJob.manifestJobId(),
          mj -> mj.withError(e.getMessage())
      );

      // Complete the job with an exception if async is enabled.
      if (resultFuture != null) {
        resultFuture.completeExceptionally(e);
      }
    } finally {
      // Clear Spark job group.
      if (sparkSession != null) {
        sparkSession.sparkContext().clearJobGroup();
      }

      // Clean up staging directory.
      if (stagingDir != null) {
        cleanupStagingDirectory(stagingDir);
      }
    }
  }

  @Nonnull
  private JsonNode fetchManifest(
      @Nonnull final ManifestJob manifestJob,
      @Nonnull final List<FileRequestHeader> fileRequestHeaders
  ) throws IOException {
    final String manifestUrl = manifestJob.manifestUrl();

    log.info("Fetching manifest from: {}", manifestUrl);

    final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
        .uri(URI.create(manifestUrl))
        .timeout(HTTP_TIMEOUT)
        .GET();

    // Apply custom headers.
    for (final FileRequestHeader header : fileRequestHeaders) {
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

  @Nonnull
  private Path createStagingDirectory(
      @Nonnull final String submissionId,
      @Nonnull final String manifestJobId
  ) throws IOException {
    final BulkSubmitConfiguration config = serverConfiguration.getBulkSubmit();
    final String baseStagingDir = config != null
                                  ? config.getStagingDirectory()
                                  : "/usr/local/staging/bulk-submit-fetch";

    final Path stagingDir = Path.of(baseStagingDir, submissionId, manifestJobId);
    Files.createDirectories(stagingDir);
    log.info("Created staging directory: {}", stagingDir);
    return stagingDir;
  }

  @Nonnull
  private Map<String, Collection<String>> downloadFiles(
      @Nonnull final Map<String, Collection<String>> fileUrls,
      @Nonnull final Path stagingDir,
      @Nonnull final List<FileRequestHeader> fileRequestHeaders
  ) throws IOException {
    final Map<String, Collection<String>> localPaths = new HashMap<>();
    final AtomicInteger fileCounter = new AtomicInteger(0);

    for (final Map.Entry<String, Collection<String>> entry : fileUrls.entrySet()) {
      final String resourceType = entry.getKey();
      final Collection<String> urls = entry.getValue();
      final Collection<String> localFiles = new ArrayList<>();

      for (final String url : urls) {
        final int fileNum = fileCounter.incrementAndGet();
        final String fileName = resourceType + "-" + fileNum + ".ndjson";
        final Path localPath = stagingDir.resolve(fileName);

        downloadFile(url, localPath, fileRequestHeaders);
        localFiles.add(localPath.toUri().toString());
      }

      localPaths.put(resourceType, localFiles);
    }

    return localPaths;
  }

  private void downloadFile(
      @Nonnull final String url,
      @Nonnull final Path destPath,
      @Nonnull final List<FileRequestHeader> fileRequestHeaders
  ) throws IOException {
    log.debug("Downloading file from {} to {}", url, destPath);

    final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(HTTP_TIMEOUT)
        .GET();

    // Apply custom headers.
    for (final FileRequestHeader header : fileRequestHeaders) {
      requestBuilder.header(header.name(), header.value());
    }

    final HttpRequest request = requestBuilder.build();

    try {
      final HttpResponse<InputStream> response = httpClient.send(
          request,
          HttpResponse.BodyHandlers.ofInputStream()
      );

      if (response.statusCode() != 200) {
        throw new IOException("Failed to download file from " + url + ": HTTP "
            + response.statusCode());
      }

      try (final InputStream body = response.body()) {
        Files.copy(body, destPath, StandardCopyOption.REPLACE_EXISTING);
      }

      log.debug("Downloaded file to {}", destPath);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("File download was interrupted: " + url, e);
    }
  }

  private void cleanupStagingDirectory(@Nonnull final Path stagingDir) {
    try {
      if (Files.exists(stagingDir)) {
        try (final Stream<Path> walk = Files.walk(stagingDir)) {
          walk.sorted(Comparator.reverseOrder())
              .forEach(path -> {
                try {
                  Files.delete(path);
                } catch (final IOException e) {
                  log.warn("Failed to delete {}: {}", path, e.getMessage());
                }
              });
        }
        log.info("Cleaned up staging directory: {}", stagingDir);
      }
    } catch (final IOException e) {
      log.warn("Failed to clean up staging directory {}: {}", stagingDir, e.getMessage());
    }
  }

  /**
   * Builds the status request URL for a submission.
   *
   * @param fhirServerBase The FHIR server base URL.
   * @param submission The submission.
   * @return The status request URL.
   */
  @Nonnull
  private static String buildStatusRequestUrl(
      @Nonnull final String fhirServerBase,
      @Nonnull final Submission submission
  ) {
    return fhirServerBase + "/$bulk-submit-status?submissionId="
        + submission.submissionId()
        + "&submitter=" + submission.submitter().system()
        + "|" + submission.submitter().value();
  }

}
