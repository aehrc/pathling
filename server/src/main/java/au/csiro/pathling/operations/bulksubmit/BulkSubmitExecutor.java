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
import au.csiro.pathling.operations.bulkexport.ExportResult;
import au.csiro.pathling.operations.bulkexport.ExportResultRegistry;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import ca.uhn.fhir.context.FhirContext;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Executes bulk submission processing by fetching manifests and downloading files. Import is
 * deferred until the submission is marked as complete.
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
  private final ExportResultRegistry exportResultRegistry;

  @Nonnull
  private final ServerConfiguration serverConfiguration;

  @Nonnull
  private final BulkSubmitResultBuilder resultBuilder;

  @Nullable
  private final JobRegistry jobRegistry;

  @Nullable
  private final SparkSession sparkSession;

  @Nonnull
  private final String databasePath;

  @Nonnull
  private final ObjectMapper objectMapper;

  @Nonnull
  private final HttpClient httpClient;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final BulkSubmitAuthProvider authProvider;

  /**
   * Creates a new BulkSubmitExecutor.
   *
   * @param importExecutor The import executor for processing files.
   * @param submissionRegistry The submission registry for tracking state.
   * @param exportResultRegistry The registry for tracking downloadable results.
   * @param serverConfiguration The server configuration.
   * @param resultBuilder The builder for creating status manifests.
   * @param jobRegistry The job registry for tracking async jobs (may be null if async is
   * disabled).
   * @param sparkSession The Spark session for setting job groups (may be null if async is
   * disabled).
   * @param databasePath The path to the database storage location.
   * @param fhirContext The FHIR context for serialising resources.
   * @param authProvider The authentication provider for OAuth2 token acquisition.
   */
  public BulkSubmitExecutor(
      @Nonnull final ImportExecutor importExecutor,
      @Nonnull final SubmissionRegistry submissionRegistry,
      @Nonnull final ExportResultRegistry exportResultRegistry,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final BulkSubmitResultBuilder resultBuilder,
      @Nullable final JobRegistry jobRegistry,
      @Nullable final SparkSession sparkSession,
      @Nonnull @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
      final String databasePath,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final BulkSubmitAuthProvider authProvider
  ) {
    this.importExecutor = importExecutor;
    this.submissionRegistry = submissionRegistry;
    this.exportResultRegistry = exportResultRegistry;
    this.serverConfiguration = serverConfiguration;
    this.resultBuilder = resultBuilder;
    this.jobRegistry = jobRegistry;
    this.sparkSession = sparkSession;
    this.databasePath = databasePath;
    this.objectMapper = new ObjectMapper();
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(HTTP_TIMEOUT)
        .build();
    this.fhirContext = fhirContext;
    this.authProvider = authProvider;
  }

  /**
   * Downloads files for a specific manifest job asynchronously. Creates a Job in the JobRegistry
   * for progress tracking if async support is enabled. This method only downloads the files; the
   * actual import is deferred until the submission is marked as complete.
   *
   * @param submission The submission containing the manifest job.
   * @param manifestJob The manifest job to process.
   * @param fileRequestHeaders Custom HTTP headers to include when downloading files.
   * @param fhirServerBase The FHIR server base URL for building result manifests.
   */
  public void downloadManifestJob(
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
        () -> downloadManifestJobInternal(
            submission, manifestJob, fileRequestHeaders, fhirServerBase, jobId, resultFuture
        )
    );
  }

  private void downloadManifestJobInternal(
      @Nonnull final Submission submission,
      @Nonnull final ManifestJob manifestJob,
      @Nonnull final List<FileRequestHeader> fileRequestHeaders,
      @Nonnull final String fhirServerBase,
      @Nullable final String jobId,
      @Nullable final CompletableFuture<IBaseResource> resultFuture
  ) {
    log.info("Starting download for manifest job: {} in submission: {}",
        manifestJob.manifestJobId(), submission.submissionId());

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
      // Acquire OAuth2 access token if credentials are configured for this submitter.
      final String accessToken = acquireAccessToken(
          submission.submitter(),
          manifestJob.fhirBaseUrl(),
          manifestJob.oauthMetadataUrl()
      );

      // Fetch and parse the manifest.
      final JsonNode manifest = fetchManifest(manifestJob, fileRequestHeaders, accessToken);

      // Check if files require authentication based on the manifest's requiresAccessToken field.
      final boolean requiresAccessToken = manifest.has("requiresAccessToken")
                                          && manifest.get("requiresAccessToken").asBoolean(false);
      final String fileAccessToken = requiresAccessToken ? accessToken : null;

      // Log authentication status for debugging.
      if (requiresAccessToken) {
        if (fileAccessToken != null) {
          log.info("Manifest requires authentication - using OAuth2 access token for file downloads");
        } else {
          log.warn("Manifest requires authentication but no OAuth credentials configured for "
              + "submitter {} - file downloads may fail", submission.submitter().toKey());
        }
      }

      // Extract file URLs from the manifest.
      final Map<String, Collection<String>> fileUrls = extractUrlsFromManifest(manifest);

      if (fileUrls.isEmpty()) {
        throw new InvalidUserInputError("No files found in manifest");
      }

      // Create persistent directory for downloaded files (same structure as export).
      final Path downloadDir = createDownloadDirectory(submission.submissionId());

      // Download files to persistent storage.
      final List<DownloadedFile> downloadedFiles = downloadFilesToPersistentStorage(
          fileUrls, downloadDir, manifestJob.manifestJobId(), manifestJob.manifestUrl(),
          fileRequestHeaders, fileAccessToken
      );

      // Register submission in ExportResultRegistry so files can be served via $result.
      final Optional<String> ownerId = Optional.ofNullable(submission.ownerId());
      exportResultRegistry.put(submission.submissionId(), new ExportResult(ownerId));
      log.info("Registered submission {} in export result registry", submission.submissionId());

      // Update manifest job with downloaded files and DOWNLOADED state.
      submissionRegistry.updateManifestJob(
          submission.submitter(),
          submission.submissionId(),
          manifestJob.manifestJobId(),
          mj -> mj.withDownloadedFiles(downloadedFiles).withState(ManifestJobState.DOWNLOADED)
      );

      log.info("Manifest job {} download completed successfully with {} files",
          manifestJob.manifestJobId(), downloadedFiles.size());

      // Complete the async job if enabled.
      if (resultFuture != null) {
        final Submission updatedSubmission = submissionRegistry.get(
            submission.submitter(), submission.submissionId()
        ).orElse(submission);
        final String requestUrl = buildStatusRequestUrl(fhirServerBase, submission);
        final IBaseResource statusManifest = resultBuilder.buildStatusManifest(
            updatedSubmission, requestUrl, fhirServerBase
        );
        resultFuture.complete(statusManifest);
      }

    } catch (final Exception e) {
      log.error("Failed to download manifest job {}: {}",
          manifestJob.manifestJobId(), e.getMessage(), e);

      // Write error to NDJSON file and update manifest job.
      String errorFileName = null;
      try {
        // Register in export registry so error file can be served via $result.
        final Optional<String> ownerId = Optional.ofNullable(submission.ownerId());
        exportResultRegistry.put(submission.submissionId(), new ExportResult(ownerId));

        errorFileName = writeErrorFile(
            submission.submissionId(),
            manifestJob.manifestJobId(),
            e.getMessage()
        );
      } catch (final IOException ioEx) {
        log.error("Failed to write error file for manifest job {}: {}",
            manifestJob.manifestJobId(), ioEx.getMessage(), ioEx);
      }

      final String finalErrorFileName = errorFileName;
      submissionRegistry.updateManifestJob(
          submission.submitter(),
          submission.submissionId(),
          manifestJob.manifestJobId(),
          mj -> mj.withError(e.getMessage(), finalErrorFileName)
      );

      // Complete the job normally with a status manifest containing errors.
      // Per the bulk submit spec, errors should be reported in the manifest error section,
      // not as HTTP exceptions.
      if (resultFuture != null) {
        final Submission updatedSubmission = submissionRegistry.get(
            submission.submitter(), submission.submissionId()
        ).orElse(submission);
        final String requestUrl = buildStatusRequestUrl(fhirServerBase, submission);
        final IBaseResource statusManifest = resultBuilder.buildStatusManifest(
            updatedSubmission, requestUrl, fhirServerBase
        );
        resultFuture.complete(statusManifest);
      }
    } finally {
      // Clear Spark job group.
      if (sparkSession != null) {
        sparkSession.sparkContext().clearJobGroup();
      }
      // Note: Downloaded files are NOT cleaned up - they persist until explicit deletion.
    }
  }

  /**
   * Aborts a submission by cancelling any running async jobs and updating manifest job states. Note
   * that downloaded files are NOT deleted - cleanup is handled by a separate mechanism.
   *
   * @param submission The submission to abort.
   */
  public void abortSubmission(@Nonnull final Submission submission) {
    log.info("Aborting submission: {}", submission.submissionId());

    // Cancel any running async jobs.
    for (final ManifestJob job : submission.manifestJobs()) {
      if (job.jobId() != null && jobRegistry != null) {
        final Job<?> asyncJob = jobRegistry.get(job.jobId());
        if (asyncJob != null && !asyncJob.getResult().isDone()) {
          asyncJob.getResult().cancel(true);
          log.debug("Cancelled async job {} for manifest job {}", job.jobId(),
              job.manifestJobId());
        }
      }
    }

    // Remove from export registry so files are no longer served.
    exportResultRegistry.remove(submission.submissionId());
    log.debug("Removed submission {} from export result registry", submission.submissionId());

    // Update all non-terminal manifest jobs to ABORTED state.
    for (final ManifestJob job : submission.manifestJobs()) {
      if (!job.isTerminal()) {
        submissionRegistry.updateManifestJob(
            submission.submitter(),
            submission.submissionId(),
            job.manifestJobId(),
            mj -> mj.withState(ManifestJobState.ABORTED)
        );
        log.debug("Marked manifest job {} as ABORTED", job.manifestJobId());
      }
    }

    log.info("Submission {} aborted successfully", submission.submissionId());
  }

  /**
   * Aborts a single manifest job within a submission. Cancels the async job if running and updates
   * the manifest job state. Downloaded files are NOT deleted.
   *
   * @param submission The submission containing the manifest job.
   * @param manifestJob The manifest job to abort.
   */
  public void abortManifestJob(
      @Nonnull final Submission submission,
      @Nonnull final ManifestJob manifestJob
  ) {
    log.info("Aborting manifest job {} in submission {}", manifestJob.manifestJobId(),
        submission.submissionId());

    // Cancel the async job if running.
    if (manifestJob.jobId() != null && jobRegistry != null) {
      final Job<?> asyncJob = jobRegistry.get(manifestJob.jobId());
      if (asyncJob != null && !asyncJob.getResult().isDone()) {
        asyncJob.getResult().cancel(true);
        log.debug("Cancelled async job {} for manifest job {}", manifestJob.jobId(),
            manifestJob.manifestJobId());
      }
    }

    // Update manifest job to ABORTED state.
    submissionRegistry.updateManifestJob(
        submission.submitter(),
        submission.submissionId(),
        manifestJob.manifestJobId(),
        mj -> mj.withState(ManifestJobState.ABORTED)
    );

    log.info("Manifest job {} aborted successfully", manifestJob.manifestJobId());
  }

  /**
   * Imports all downloaded files from a submission. This method runs the import asynchronously in
   * the background (fire-and-forget) and returns immediately.
   *
   * @param submission The submission to import.
   */
  public void importSubmission(@Nonnull final Submission submission) {
    log.info("Starting background import for submission: {}", submission.submissionId());

    // Execute import asynchronously (fire-and-forget).
    CompletableFuture.runAsync(() -> importSubmissionInternal(submission));
  }

  private void importSubmissionInternal(@Nonnull final Submission submission) {
    try {
      // Collect all downloaded file paths from all manifest jobs.
      final Map<String, Collection<String>> allFilePaths = new HashMap<>();

      for (final ManifestJob job : submission.manifestJobs()) {
        if (job.downloadedFiles() != null) {
          for (final DownloadedFile file : job.downloadedFiles()) {
            allFilePaths.computeIfAbsent(file.resourceType(), k -> new ArrayList<>())
                .add(file.localPath());
          }
        }
      }

      if (allFilePaths.isEmpty()) {
        log.warn("No files to import for submission {}", submission.submissionId());
        return;
      }

      // Create ImportRequest with aggregated file paths.
      final ImportRequest importRequest = new ImportRequest(
          submission.submissionId(),
          "bulk-submit",
          allFilePaths,
          SaveMode.MERGE,
          ImportFormat.NDJSON
      );

      // Execute the import with the bulk submit allowable sources.
      final BulkSubmitConfiguration bulkSubmitConfig = serverConfiguration.getBulkSubmit();
      final List<String> allowableSources = bulkSubmitConfig != null
                                            ? bulkSubmitConfig.getAllowableSources()
                                            : List.of();
      importExecutor.execute(importRequest, submission.submissionId(), allowableSources);

      // Update all manifest jobs to COMPLETED state.
      for (final ManifestJob job : submission.manifestJobs()) {
        if (job.state() == ManifestJobState.DOWNLOADED) {
          submissionRegistry.updateManifestJob(
              submission.submitter(),
              submission.submissionId(),
              job.manifestJobId(),
              mj -> mj.withState(ManifestJobState.COMPLETED)
          );
        }
      }

      log.info("Import completed successfully for submission {}", submission.submissionId());

    } catch (final Exception e) {
      log.error("Failed to import submission {}: {}", submission.submissionId(), e.getMessage(), e);

      // Update all DOWNLOADED manifest jobs to FAILED state with error files.
      for (final ManifestJob job : submission.manifestJobs()) {
        if (job.state() == ManifestJobState.DOWNLOADED) {
          String errorFileName = null;
          try {
            errorFileName = writeErrorFile(
                submission.submissionId(),
                job.manifestJobId(),
                "Import failed: " + e.getMessage()
            );
          } catch (final IOException ioEx) {
            log.error("Failed to write error file for manifest job {}: {}",
                job.manifestJobId(), ioEx.getMessage(), ioEx);
          }

          final String finalErrorFileName = errorFileName;
          submissionRegistry.updateManifestJob(
              submission.submitter(),
              submission.submissionId(),
              job.manifestJobId(),
              mj -> mj.withError("Import failed: " + e.getMessage(), finalErrorFileName)
          );
        }
      }
    }
  }

  @Nonnull
  private JsonNode fetchManifest(
      @Nonnull final ManifestJob manifestJob,
      @Nonnull final List<FileRequestHeader> fileRequestHeaders,
      @Nullable final String accessToken
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

    // Add Authorization header if access token is available.
    if (accessToken != null) {
      requestBuilder.header("Authorization", "Bearer " + accessToken);
      log.debug("Using OAuth2 access token for manifest fetch");
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
   * @throws InvalidUserInputError If manifest entries are missing required fields.
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

    final List<String> errors = new ArrayList<>();
    int entryIndex = 0;

    for (final JsonNode fileNode : outputNode) {
      final String type = fileNode.has("type")
                          ? fileNode.get("type").asText()
                          : null;
      final String url = fileNode.has("url")
                         ? fileNode.get("url").asText()
                         : null;

      if (type == null && url == null) {
        errors.add("Output entry " + entryIndex + " is missing both 'type' and 'url' fields");
      } else if (type == null) {
        errors.add("Output entry " + entryIndex + " (url: " + url + ") is missing 'type' field");
      } else if (url == null) {
        errors.add("Output entry " + entryIndex + " (type: " + type + ") is missing 'url' field");
      } else {
        result.computeIfAbsent(type, k -> new ArrayList<>()).add(url);
      }

      entryIndex++;
    }

    // If there were errors, throw with all error details.
    if (!errors.isEmpty()) {
      throw new InvalidUserInputError("Invalid manifest entries: " + String.join("; ", errors));
    }

    log.info("Extracted URLs for resource types: {}", result.keySet());
    return result;
  }

  /**
   * Creates the persistent download directory for a submission. Files are stored in the same
   * location as export results: {databasePath}/jobs/{submissionId}/.
   *
   * @param submissionId The submission ID.
   * @return The path to the download directory.
   * @throws IOException If the directory cannot be created.
   */
  @Nonnull
  private Path createDownloadDirectory(@Nonnull final String submissionId) throws IOException {
    final Path downloadDir = Path.of(URI.create(databasePath).getPath(), "jobs", submissionId);
    Files.createDirectories(downloadDir);
    log.info("Created download directory: {}", downloadDir);
    return downloadDir;
  }

  /**
   * Downloads files to persistent storage and returns metadata about the downloaded files. Files
   * are named using the pattern {ResourceType}.{manifestJobId}-{index}.ndjson to comply with the
   * resourceNameWithQualifierMapper pattern used by FileSource.
   *
   * @param fileUrls Map of resource type to collection of URLs.
   * @param downloadDir The directory to download files to.
   * @param manifestJobId The manifest job ID (used in file naming).
   * @param manifestUrl The URL of the manifest from which these files are being downloaded.
   * @param fileRequestHeaders Custom HTTP headers to include when downloading files.
   * @param accessToken OAuth2 access token for authenticated downloads (may be null).
   * @return List of downloaded file metadata.
   * @throws IOException If a file cannot be downloaded.
   */
  @Nonnull
  private List<DownloadedFile> downloadFilesToPersistentStorage(
      @Nonnull final Map<String, Collection<String>> fileUrls,
      @Nonnull final Path downloadDir,
      @Nonnull final String manifestJobId,
      @Nonnull final String manifestUrl,
      @Nonnull final List<FileRequestHeader> fileRequestHeaders,
      @Nullable final String accessToken
  ) throws IOException {
    final List<DownloadedFile> downloadedFiles = new ArrayList<>();
    final AtomicInteger fileCounter = new AtomicInteger(0);

    for (final Map.Entry<String, Collection<String>> entry : fileUrls.entrySet()) {
      final String resourceType = entry.getKey();
      final Collection<String> urls = entry.getValue();

      for (final String url : urls) {
        final int fileNum = fileCounter.incrementAndGet();
        // Use dot-separator format: {ResourceType}.{qualifier}.ndjson
        final String fileName = resourceType + "." + manifestJobId + "-" + fileNum + ".ndjson";
        final Path localPath = downloadDir.resolve(fileName);

        downloadFile(url, localPath, fileRequestHeaders, accessToken);

        downloadedFiles.add(new DownloadedFile(
            resourceType,
            fileName,
            localPath.toUri().toString(),
            manifestUrl
        ));
      }
    }

    return downloadedFiles;
  }

  private void downloadFile(
      @Nonnull final String url,
      @Nonnull final Path destPath,
      @Nonnull final List<FileRequestHeader> fileRequestHeaders,
      @Nullable final String accessToken
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

    // Add Authorization header if access token is available.
    if (accessToken != null) {
      requestBuilder.header("Authorization", "Bearer " + accessToken);
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

  /**
   * Writes an error OperationOutcome to an NDJSON file.
   *
   * @param submissionId The submission ID.
   * @param manifestJobId The manifest job ID.
   * @param errorMessage The error message to include in the OperationOutcome.
   * @return The file name of the written error file.
   * @throws IOException If the file cannot be written.
   */
  @Nonnull
  private String writeErrorFile(
      @Nonnull final String submissionId,
      @Nonnull final String manifestJobId,
      @Nonnull final String errorMessage
  ) throws IOException {
    // Create the download directory if it doesn't exist.
    final Path downloadDir = createDownloadDirectory(submissionId);

    // Build the OperationOutcome resource.
    final OperationOutcome outcome = new OperationOutcome();
    outcome.addIssue()
        .setCode(IssueType.EXCEPTION)
        .setSeverity(IssueSeverity.ERROR)
        .setDiagnostics(errorMessage);

    // Serialise to JSON.
    final String json = fhirContext.newJsonParser().encodeResourceToString(outcome);

    // Write to NDJSON file (single line per resource).
    final String fileName = "OperationOutcome." + manifestJobId + "-error.ndjson";
    final Path filePath = downloadDir.resolve(fileName);
    try (final BufferedWriter writer = Files.newBufferedWriter(filePath, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
      writer.write(json);
      writer.newLine();
    }

    log.info("Wrote error file {} for manifest job {}", fileName, manifestJobId);
    return fileName;
  }

  /**
   * Acquires an OAuth2 access token for the given submitter if credentials are configured.
   *
   * @param submitter The submitter identifier.
   * @param fhirBaseUrl The FHIR base URL for SMART discovery.
   * @param oauthMetadataUrl Optional explicit URL to OAuth 2.0 metadata.
   * @return The access token, or null if no credentials are configured.
   */
  @Nullable
  private String acquireAccessToken(
      @Nonnull final SubmitterIdentifier submitter,
      @Nullable final String fhirBaseUrl,
      @Nullable final String oauthMetadataUrl
  ) {
    if (fhirBaseUrl == null) {
      log.debug("No fhirBaseUrl provided - skipping OAuth token acquisition");
      return null;
    }

    try {
      final Optional<String> token = authProvider.acquireToken(
          submitter,
          fhirBaseUrl,
          oauthMetadataUrl
      );
      if (token.isPresent()) {
        log.debug("Successfully acquired OAuth2 access token for submitter: {}", submitter.toKey());
      } else {
        log.debug("No OAuth credentials configured for submitter: {}", submitter.toKey());
      }
      return token.orElse(null);
    } catch (final IOException e) {
      log.warn("Failed to acquire OAuth2 access token for submitter {}: {}",
          submitter.toKey(), e.getMessage());
      // Return null to allow the download to proceed without authentication.
      // This supports scenarios where the manifest and files are publicly accessible.
      return null;
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
