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

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.ErrorHandlingInterceptor;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.security.PathlingAuthority;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Provides the $job-result operation for retrieving the result of a completed async job. This
 * endpoint is used when operations are configured with {@code redirectOnComplete=true}, following
 * the SQL on FHIR unify-async specification.
 *
 * <p>The flow is: 1. Client polls $job endpoint until job completes 2. $job returns 303 See Other
 * with Location header pointing to $job-result 3. Client fetches result from $job-result endpoint
 *
 * @author John Grimes
 */
@Component
@ConditionalOnProperty(prefix = "pathling", name = "async.enabled", havingValue = "true")
@Slf4j
public class JobResultProvider {

  private static final Pattern ID_PATTERN =
      Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$");

  @Nonnull private final ServerConfiguration configuration;

  @Nonnull private final JobRegistry jobRegistry;

  /**
   * Creates a new JobResultProvider.
   *
   * @param configuration the server configuration
   * @param jobRegistry the job registry
   */
  public JobResultProvider(
      @Nonnull final ServerConfiguration configuration, @Nonnull final JobRegistry jobRegistry) {
    this.configuration = configuration;
    this.jobRegistry = jobRegistry;
  }

  /**
   * Retrieves the result of a completed async job.
   *
   * @param id the job ID
   * @param request the HTTP request
   * @param response the HTTP response
   * @return the job result as a Parameters resource
   */
  @SuppressWarnings("unused")
  @Operation(name = "$job-result", idempotent = true)
  public IBaseResource jobResult(
      @Nullable @OperationParam(name = "id") final String id,
      @Nonnull final HttpServletRequest request,
      @Nullable final HttpServletResponse response) {
    log.debug("Received $job-result request with id: {}", id);

    final Job<?> job = getJob(id);

    if (configuration.getAuth().isEnabled()) {
      // Check for the required authority associated with the operation that initiated the job.
      checkHasAuthority(PathlingAuthority.operationAccess(job.getOperation()));
      // Check that the user requesting the job result is the same user that started the job.
      final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
      final Optional<String> currentUserId = getCurrentUserId(authentication);
      if (!job.getOwnerId().equals(currentUserId)) {
        throw new AccessDeniedError("The requested job is not owned by the current user");
      }
    }

    return handleJobResultRequest(job, response);
  }

  @Nonnull
  private Job<?> getJob(@Nullable final String id) {
    // Validate that the ID looks reasonable.
    if (id == null || !ID_PATTERN.matcher(id).matches()) {
      throw new ResourceNotFoundError("Job ID not found");
    }

    log.debug("Received request for job result: {}", id);
    @Nullable final Job<?> job = jobRegistry.get(id);
    // Check that the job exists.
    if (job == null) {
      throw new ResourceNotFoundError("Job ID not found");
    }
    return job;
  }

  @Nonnull
  private IBaseResource handleJobResultRequest(
      @Nonnull final Job<?> job, @Nullable final HttpServletResponse response) {
    // Handle cancelled jobs.
    if (job.getResult().isCancelled()) {
      throw new ResourceNotFoundException(
          "A DELETE request cancelled this job or deleted all files associated with this job.");
    }

    // Verify the job is complete.
    if (!job.getResult().isDone()) {
      throw new InvalidRequestException(
          "Job is not yet complete. Poll the $job endpoint to check status.");
    }

    // Set cache headers.
    if (response != null) {
      final int maxAge = configuration.getAsync().getCacheMaxAge();
      response.setHeader("Cache-Control", "max-age=" + maxAge);
    }

    // Apply any response modifications set by the operation (e.g., Expires header).
    job.getResponseModification().accept(response);

    // Return the result.
    try {
      return job.getResult().get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InternalErrorException("Job was interrupted", e);
    } catch (final ExecutionException e) {
      throw ErrorHandlingInterceptor.convertError(unwrapExecutionException(e));
    }
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
}
