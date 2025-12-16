/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.cache.EntityTagInterceptor;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.ErrorHandlingInterceptor;
import au.csiro.pathling.errors.ResourceNotFoundError;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Component
@ConditionalOnProperty(prefix = "pathling", name = "async.enabled", havingValue = "true")
@Slf4j
public class JobProvider {


  // regex for UUID
  private static final Pattern ID_PATTERN = Pattern.compile(
      "^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$");
  private static final String PROGRESS_HEADER = "X-Progress";

  @Nonnull
  private final ServerConfiguration configuration;

  @Nonnull
  private final JobRegistry jobRegistry;
  private final SparkSession sparkSession;
  private final String databasePath;

  /**
   * @param configuration a {@link ServerConfiguration} for determining if authorization is enabled
   * @param jobRegistry the {@link JobRegistry} used to keep track of running jobs
   */
  public JobProvider(@Nonnull final ServerConfiguration configuration,
      @Nonnull final JobRegistry jobRegistry, SparkSession sparkSession,
      @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
      String databasePath) {
    this.configuration = configuration;
    this.jobRegistry = jobRegistry;
    this.sparkSession = sparkSession;
    this.databasePath = new Path(databasePath, "jobs").toString();
  }


  public void deleteJob(String jobId) {
    final Job<?> job = getJob(jobId);
    handleJobDeleteRequest(job);
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
  public IBaseResource job(@Nullable @OperationParam(name = "id") final String id,
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

  private @NotNull Job<?> getJob(@Nullable String id) {
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

  private void handleJobDeleteRequest(Job<?> job) {
    /*
    Two possible situations:
      - The initial kick-off request is still ongoing -> cancel it and delete the partial files
      - The initial kick-off request is complete (and the client may have already downloaded the files) 
        -> interpret delete request from client as "do no longer need them". Depending on the caching setup,
        these files may or may not be deleted. Either way return a success status code
        
      handle if a delete request was initiated and another delete request is being called before the old one finishes
      -> just return success as well but don't schedule a new deletion internally OR return a 404
     */
    if (job.isMarkedAsDeleted()) {
      throw new ResourceNotFoundException("Already deleted this job.");
    }
    job.setMarkedAsDeleted(true);
    if (!job.getResult().isDone()) {
      job.getResult().cancel(false);
      // Currently, only the files up until "now" will be deleted. Anything that is created between
      // the cancel request and the job actually being cancelled by spark will remain on the disk
    }
    try {
      deleteJobFiles(job.getId());
    } catch (IOException e) {
      throw new InternalErrorException("Failed to delete files associated with the job.", e);
    } finally {
      boolean removed = jobRegistry.remove(job);
      if (removed) {
        log.debug("Removed job {} from registry.", job.getId());
      } else {
        log.warn("Failed to remove job {} from registry. This might in wrong caching results.",
            job.getId());
      }
    }
    throw new ProcessingNotCompletedException("The job and its resources will be deleted.",
        buildDeletionOutcome());
  }

  public void deleteJobFiles(String jobId) throws IOException {
    Configuration hadoopConfig = sparkSession.sparkContext().hadoopConfiguration();
    FileSystem fs = FileSystem.get(hadoopConfig);
    Path jobDirToDel = new Path(databasePath, jobId);
    log.debug("Deleting dir {}", jobDirToDel);
    boolean deleted = fs.delete(jobDirToDel, true);
    if (!deleted) {
      log.warn("Failed to delete dir {}", jobDirToDel);
    }
    log.debug("Deleted dir {}", jobDirToDel);
  }

  private IBaseResource handleJobGetRequest(@NotNull HttpServletRequest request,
      @Nullable HttpServletResponse response, @NotNull Job<?> job) {
    if (job.getResult().isCancelled()) {
      // a DELETE request was initiated before the job completed
      // Depending on the async task is running, the task may periodically check the isCancelled state and abort.
      // Otherwise, the job actually finishes but the user will never see the result (unless they 
      // initiate a new request and the cache-layer determined that is can reuse the result)
      throw new ResourceNotFoundException(
          "A DELETE request cancelled this job or deleted all files associated with this job.");
    }
    if (job.getResult().isDone()) {
      // If the job is done, we return the Parameters resource.
      try {
        job.getResponseModification().accept(response);
        return job.getResult().get();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InternalErrorException("Job was interrupted", e);
      } catch (final ExecutionException e) {
        // Unwrap the cause chain safely. The Future wraps exceptions in ExecutionException,
        // and AsyncAspect wraps them in IllegalStateException.
        Throwable cause = e.getCause();
        if (cause != null && cause.getCause() != null) {
          cause = cause.getCause();
        }
        throw ErrorHandlingInterceptor.convertError(cause != null
                                                    ? cause
                                                    : e);
      }
    } else {
      // If the job is not done, we return a 202 along with an OperationOutcome and progress header.
      requireNonNull(response);
      // We need to set the caching headers such that the incomplete response is never cached.
      if (EntityTagInterceptor.requestIsCacheable(request)) {
        EntityTagInterceptor.makeRequestNonCacheable(response, configuration);
      }
      // Add progress information to the response.
      if (job.getTotalStages() > 0) {
        final int progress = job.getProgressPercentage();
        if (progress != 100) {
          // We don't bother showing 100%, this usually means that there are outstanding stages
          // which have not yet been submitted.
          response.setHeader(PROGRESS_HEADER, progress + "%");
          job.setLastProgress(progress);
        } else {
          // instead show last percentage again
          response.setHeader(PROGRESS_HEADER, job.getLastProgress() + "%");
        }
      }
      throw new ProcessingNotCompletedException("Processing", buildProcessingOutcome());
    }
  }

  private static IBaseOperationOutcome buildDeletionOutcome() {
    OperationOutcome operationOutcome = new OperationOutcome();
    operationOutcome.addIssue()
        .setCode(IssueType.INFORMATIONAL)
        .setSeverity(IssueSeverity.INFORMATION)
        .setDiagnostics("The job and its resources will be deleted.");
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
