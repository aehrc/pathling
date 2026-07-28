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

package au.csiro.pathling.async;

import static au.csiro.pathling.security.SecurityAspect.checkHasAuthority;
import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.ErrorHandlingInterceptor;
import au.csiro.pathling.errors.ErrorReportingInterceptor;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.io.JobDirectoryFileSystem;
import au.csiro.pathling.security.PathlingAuthority;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Provides operations for querying and managing asynchronous jobs.
 *
 * @author John Grimes
 */
@Component
@ConditionalOnProperty(prefix = "pathling", name = "async.enabled", havingValue = "true")
@Slf4j
public class JobProvider {

  // regex for UUID
  private static final Pattern ID_PATTERN =
      Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$");
  private static final String PROGRESS_HEADER = "X-Progress";

  @Nonnull private final ServerConfiguration configuration;

  @Nonnull private final JobRegistry jobRegistry;
  @Nonnull private final JobDirectoryFileSystem jobDirectoryFileSystem;
  @Nonnull private final SparkSession spark;

  /**
   * Creates a new JobProvider.
   *
   * @param configuration a {@link ServerConfiguration} for determining if authorisation is enabled
   * @param jobRegistry the {@link JobRegistry} used to keep track of running jobs
   * @param jobDirectoryFileSystem the {@link JobDirectoryFileSystem} used to resolve and delete
   *     per-job directories on the warehouse file system
   * @param spark the {@link SparkSession} used to cancel the Spark work belonging to a deleted job
   */
  public JobProvider(
      @Nonnull final ServerConfiguration configuration,
      @Nonnull final JobRegistry jobRegistry,
      @Nonnull final JobDirectoryFileSystem jobDirectoryFileSystem,
      @Nonnull final SparkSession spark) {
    this.configuration = configuration;
    this.jobRegistry = jobRegistry;
    this.jobDirectoryFileSystem = jobDirectoryFileSystem;
    this.spark = spark;
  }

  /**
   * Deletes a job and its associated resources.
   *
   * @param jobId the ID of the job to delete
   */
  public void deleteJob(final String jobId) {
    final Job<?> job = getJob(jobId);

    if (configuration.getAuth().isEnabled()) {
      // Mirror the ownership checks on the GET path: the caller must hold the authority for the
      // operation that initiated the job, and must be the job's owner.
      checkHasAuthority(PathlingAuthority.operationAccess(job.getOperation()));
      final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
      final Optional<String> currentUserId = getCurrentUserId(authentication);
      if (!job.getOwnerId().equals(currentUserId)) {
        throw new AccessDeniedError("The requested job is not owned by the current user");
      }
    }

    handleJobDeleteRequest(job);
  }

  /**
   * Sets cache headers for async endpoint responses. These endpoints use TTL-based caching instead
   * of ETag-based revalidation to ensure fresh responses for job status polling.
   *
   * @param response the HTTP response to set headers on
   */
  private void setAsyncCacheHeaders(@Nonnull final HttpServletResponse response) {
    final int maxAge = configuration.getAsync().getCacheMaxAge();
    response.setHeader("Cache-Control", "max-age=" + maxAge);
  }

  /**
   * Queries a running job for its progress, completion status and final result.
   *
   * @param id the ID of the running job
   * @param request the {@link HttpServletRequest} for checking its cacheability
   * @param response the {@link HttpServletResponse} for updating the response
   * @return the final result of the job, as a {@link Parameters} resource
   */
  @SuppressWarnings({"unused", "TypeMayBeWeakened"})
  @Operation(name = "$job", idempotent = true)
  public IBaseResource job(
      @Nullable @OperationParam(name = "id") final String id,
      @jakarta.validation.constraints.NotNull final HttpServletRequest request,
      @Nullable final HttpServletResponse response) {
    log.debug("Received $job request with id: {}", id);

    final Job<?> job = getJob(id);

    if (configuration.getAuth().isEnabled()) {
      // Check for the required authority associated with the operation that initiated the job.
      checkHasAuthority(PathlingAuthority.operationAccess(job.getOperation()));
      // Check that the user requesting the job status is the same user that started the job.
      final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
      final Optional<String> currentUserId = getCurrentUserId(authentication);
      if (!job.getOwnerId().equals(currentUserId)) {
        throw new AccessDeniedError("The requested job is not owned by the current user");
      }
    }
    return handleJobGetRequest(request, response, job);
  }

  private @NotNull Job<?> getJob(@Nullable final String id) {
    // Validate that the ID looks reasonable.
    if (id == null || !ID_PATTERN.matcher(id).matches()) {
      throw new ResourceNotFoundError("Job ID not found");
    }

    log.debug("Received request to check job status: {}", id);
    @Nullable final Job<?> job = jobRegistry.get(id);
    // Check that the job exists.
    if (job == null) {
      throw new ResourceNotFoundError("Job ID not found");
    }
    return job;
  }

  private void handleJobDeleteRequest(final Job<?> job) {
    /*
    Two possible situations:
      - The initial kick-off request is still ongoing -> cancel it and let the job's own thread
        remove the partial files as it exits
      - The initial kick-off request is complete (and the client may have already downloaded the
        files)
        -> interpret delete request from client as "do no longer need them". Depending on the
           caching setup, these files may or may not be deleted. Either way return a success status
           code

      handle if a delete request was initiated and another delete request is being called before the
      old one finishes
      -> just return success as well but don't schedule a new deletion internally OR return a 404
     */
    if (job.isMarkedAsDeleted()) {
      throw new ResourceNotFoundException("Already deleted this job.");
    }
    // Whichever party is last owns the removal of the job's output directory: this request if the
    // work has already stopped, otherwise the job's own thread as it exits. Removing it here while
    // tasks are still writing into it would leave output behind that nothing ever cleans up.
    final boolean removeFilesNow = job.markDeletedAndClaim();
    if (!job.getResult().isDone()) {
      job.getResult().cancel(false);
      // Signal Spark directly. Cancelling the future does not interrupt the thread running the job,
      // and the stage-event checks in SparkJobListener only reach Spark at the next stage boundary,
      // which for a job inside a single long write stage is not until that stage finishes on its
      // own. Those checks remain as a backstop.
      spark.sparkContext().cancelJobGroup(job.getId());
    }
    final boolean removalFailed = removeFilesNow && !tryDeleteJobFiles(job.getId());
    final boolean removed = jobRegistry.remove(job);
    if (removed) {
      log.debug("Removed job {} from registry.", job.getId());
    } else {
      log.warn(
          "Failed to remove job {} from registry. This might in wrong caching results.",
          job.getId());
    }
    throw new ProcessingNotCompletedException(
        "The job and its resources will be deleted.", buildDeletionOutcome(removalFailed));
  }

  /**
   * Deletes the files associated with a job from the file system. Deleting a directory that does
   * not exist is a normal outcome and is not reported as a failure.
   *
   * @param jobId the ID of the job whose files should be deleted
   * @throws IOException if the directory exists but could not be deleted
   */
  public void deleteJobFiles(final String jobId) throws IOException {
    log.debug("Deleting job directory for job {}", jobId);
    jobDirectoryFileSystem.deleteJobDirectory(jobId);
    log.debug("Deleted job directory for job {}", jobId);
  }

  /**
   * Removes the files associated with a job, reporting a failure rather than propagating it. By the
   * time this runs the job has been cancelled, so there is nothing the client can usefully retry.
   *
   * @param jobId the ID of the job whose files should be removed
   * @return true if the removal succeeded, false if it failed
   */
  private boolean tryDeleteJobFiles(@Nonnull final String jobId) {
    try {
      deleteJobFiles(jobId);
      return true;
    } catch (final IOException e) {
      reportFileRemovalFailure(jobId, e);
      return false;
    }
  }

  /**
   * Records a failure to remove a job's output directory, in the server log and in error reporting.
   * The operator is the only party who can act on it: the files are orphaned in the warehouse and
   * need manual attention.
   *
   * <p>Shared with {@link AsyncAspect}, which performs the same removal from the job's own thread
   * and has no response left to report the failure on.
   *
   * @param jobId the ID of the job whose files could not be removed
   * @param cause the failure encountered while removing them
   */
  public static void reportFileRemovalFailure(
      @Nonnull final String jobId, @Nonnull final Throwable cause) {
    log.error("Failed to remove the output directory of job {}.", jobId, cause);
    ErrorReportingInterceptor.reportExceptionToSentry(
        new InternalErrorException(
            "Failed to remove the output directory of job %s.".formatted(jobId), cause));
  }

  private IBaseResource handleJobGetRequest(
      @NotNull final HttpServletRequest request,
      @Nullable final HttpServletResponse response,
      @NotNull final Job<?> job) {
    if (job.getResult().isCancelled()) {
      throw handleCancelledJob();
    }
    if (job.getResult().isDone()) {
      return handleCompletedJob(job, request, response);
    }
    return handleInProgressJob(request, response, job);
  }

  /**
   * Handles a cancelled job by throwing a ResourceNotFoundException.
   *
   * @return Never returns, always throws.
   * @throws ResourceNotFoundException Always thrown.
   */
  @Nonnull
  private static ResourceNotFoundException handleCancelledJob() {
    // A DELETE request was initiated before the job completed. Depending on the async task, it may
    // periodically check the isCancelled state and abort. Otherwise, the job finishes but the user
    // will not see the result unless they initiate a new request and the cache layer reuses it.
    return new ResourceNotFoundException(
        "A DELETE request cancelled this job or deleted all files associated with this job.");
  }

  /**
   * Handles a completed job by returning its result or redirecting to the result endpoint.
   *
   * <p>If the job follows the {@link AsyncPattern#STANDARD_ASYNC_PATTERN} (the HL7 Asynchronous
   * Interaction Request Pattern, <a
   * href="https://build.fhir.org/ig/HL7/api-incubator-ig/branches/simplified-async-interaction/async-interaction.html">spec</a>),
   * returns 303 See Other with a Location header pointing to the result endpoint. Otherwise,
   * returns the result inline.
   *
   * @param job The completed job.
   * @param request The HTTP request for building the result URL.
   * @param response The HTTP response for applying response modifications.
   * @return The job result (if not redirecting) or an empty Parameters resource (if redirecting).
   * @throws InternalErrorException If the job was interrupted.
   */
  @Nonnull
  private IBaseResource handleCompletedJob(
      @Nonnull final Job<?> job,
      @Nonnull final HttpServletRequest request,
      @Nullable final HttpServletResponse response) {
    try {
      // Completed responses use TTL-based caching with configured max-age.
      if (response != null) {
        setAsyncCacheHeaders(response);
      }

      // Under the HL7 Asynchronous Interaction Request Pattern, return 303 See Other with a
      // Location header pointing to the result endpoint.
      if (job.getPattern() == AsyncPattern.STANDARD_ASYNC_PATTERN && response != null) {
        final String resultUrl = buildResultUrl(request, job.getId());
        response.setStatus(HttpServletResponse.SC_SEE_OTHER);
        response.setHeader("Location", resultUrl);
        return new Parameters();
      }

      // Otherwise return the result inline (legacy behaviour).
      job.getResponseModification().accept(response);
      return job.getResult().get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InternalErrorException("Job was interrupted", e);
    } catch (final ExecutionException e) {
      throw ErrorHandlingInterceptor.convertError(unwrapExecutionException(e));
    }
  }

  /**
   * Builds the URL for the job result endpoint. Uses the servlet context path to ensure the URL is
   * correctly prefixed (e.g., /fhir/$job-result).
   *
   * @param request The HTTP request to extract the context path from.
   * @param jobId The job ID.
   * @return The result URL.
   */
  @Nonnull
  private static String buildResultUrl(
      @Nonnull final HttpServletRequest request, @Nonnull final String jobId) {
    // Use the servlet path to get the FHIR server mount point (e.g., "/fhir").
    final String servletPath = request.getServletPath();
    return servletPath + "/$job-result?id=" + jobId;
  }

  /**
   * Unwraps the cause chain from an ExecutionException. The Future wraps exceptions in
   * ExecutionException, and AsyncAspect may wrap them in IllegalStateException.
   *
   * @param e The ExecutionException to unwrap.
   * @return The root cause or the original exception.
   */
  @Nonnull
  private static Throwable unwrapExecutionException(@Nonnull final ExecutionException e) {
    Throwable cause = e.getCause();
    if (cause != null && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause != null ? cause : e;
  }

  /**
   * Handles an in-progress job by setting headers and throwing ProcessingNotCompletedException.
   *
   * @param request The HTTP request for checking cacheability.
   * @param response The HTTP response for setting headers.
   * @param job The in-progress job.
   * @return Never returns, always throws.
   * @throws ProcessingNotCompletedException Always thrown with 202 status.
   */
  @Nonnull
  private IBaseResource handleInProgressJob(
      @Nonnull final HttpServletRequest request,
      @Nullable final HttpServletResponse response,
      @Nonnull final Job<?> job) {
    requireNonNull(response);

    // In-progress responses use no-cache to prevent caching of transient status.
    response.setHeader("Cache-Control", "no-cache");
    setProgressHeader(response, job);

    throw new ProcessingNotCompletedException("Processing", buildProcessingOutcome());
  }

  /**
   * Sets the X-Progress header based on job progress.
   *
   * @param response The HTTP response.
   * @param job The job to get progress from.
   */
  private static void setProgressHeader(
      @Nonnull final HttpServletResponse response, @Nonnull final Job<?> job) {
    if (job.getTotalStages() <= 0) {
      return;
    }

    final int progress = job.getProgressPercentage();
    if (progress != 100) {
      // We don't show 100% as it usually means outstanding stages have not yet been submitted.
      response.setHeader(PROGRESS_HEADER, progress + "%");
      job.setLastProgress(progress);
    } else {
      // Show the last recorded percentage instead.
      response.setHeader(PROGRESS_HEADER, job.getLastProgress() + "%");
    }
  }

  /**
   * Builds the outcome returned when a job is deleted. The informational issue comes first and is
   * unchanged, so a client reading only the first issue sees what it always has.
   *
   * @param removalFailed whether the job's output directory could not be removed
   * @return the outcome to attach to the acceptance
   */
  @Nonnull
  private static IBaseOperationOutcome buildDeletionOutcome(final boolean removalFailed) {
    final OperationOutcome operationOutcome = new OperationOutcome();
    operationOutcome
        .addIssue()
        .setCode(IssueType.INFORMATIONAL)
        .setSeverity(IssueSeverity.INFORMATION)
        .setDiagnostics("The job and its resources will be deleted.");
    if (removalFailed) {
      operationOutcome
          .addIssue()
          .setCode(IssueType.INCOMPLETE)
          .setSeverity(IssueSeverity.WARNING)
          .setDiagnostics(
              "The job's stored files could not be removed and may require manual clean-up.");
    }
    return operationOutcome;
  }

  @Nonnull
  private static OperationOutcome buildProcessingOutcome() {
    final OperationOutcome opOutcome = new OperationOutcome();
    final OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setCode(IssueType.INFORMATIONAL);
    issue.setSeverity(IssueSeverity.INFORMATION);
    issue.setDiagnostics("Job currently processing");
    opOutcome.addIssue(issue);
    return opOutcome;
  }
}
