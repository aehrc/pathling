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
import au.csiro.pathling.security.PathlingAuthority;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Comparator;
import java.util.Date;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Provides the system-level {@code $jobs} operation, which lists the asynchronous jobs held in the
 * in-memory {@link JobRegistry}. The list is owner-scoped when authorisation is enabled and
 * contains every registered job when it is disabled.
 *
 * @author John Grimes
 */
@Component
@ConditionalOnProperty(prefix = "pathling", name = "async.enabled", havingValue = "true")
@Slf4j
public class JobListProvider {

  /** The authority and operation name required to list jobs. */
  private static final String JOBS_OPERATION = "jobs";

  @Nonnull private final ServerConfiguration configuration;
  @Nonnull private final JobRegistry jobRegistry;

  /**
   * Creates a new JobListProvider.
   *
   * @param configuration the server configuration, for determining if authorisation is enabled
   * @param jobRegistry the registry to enumerate jobs from
   */
  public JobListProvider(
      @Nonnull final ServerConfiguration configuration, @Nonnull final JobRegistry jobRegistry) {
    this.configuration = configuration;
    this.jobRegistry = jobRegistry;
  }

  /**
   * Lists the jobs owned by the caller as a {@link Parameters} resource, newest first. When
   * authorisation is enabled the caller must hold the {@code operation:jobs} authority and only
   * their own jobs are returned; when it is disabled every registered job is returned.
   *
   * @param requestDetails the request details, used to build absolute job status URLs
   * @param response the HTTP response, used to mark the list as non-cacheable
   * @return a {@link Parameters} resource with one repeating {@code job} parameter per job
   */
  @Operation(name = "$jobs", idempotent = true)
  public Parameters jobs(
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final HttpServletResponse response) {
    log.debug("Received $jobs request");

    final boolean authEnabled = configuration.getAuth().isEnabled();
    final Optional<String> currentUserId;
    if (authEnabled) {
      // The caller must hold the authority for the list operation itself.
      checkHasAuthority(PathlingAuthority.operationAccess(JOBS_OPERATION));
      final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
      currentUserId = getCurrentUserId(authentication);
    } else {
      currentUserId = Optional.empty();
    }

    // The list is a live snapshot of transient state, so it must not be cached.
    if (response != null) {
      response.setHeader("Cache-Control", "no-cache");
    }

    final String fhirServerBase = requestDetails.getFhirServerBase();
    final Parameters result = new Parameters();
    jobRegistry.allJobs().stream()
        .filter(job -> isVisibleToCaller(job, authEnabled, currentUserId))
        .sorted(Comparator.comparing((Job<?> job) -> job.getStartTime()).reversed())
        .forEach(job -> addJobParameter(result, job, fhirServerBase));
    return result;
  }

  /**
   * Determines whether a job should appear in the caller's list. All jobs are visible when
   * authorisation is disabled; otherwise only jobs owned by the caller's subject are, and a caller
   * without a subject sees none.
   *
   * @param job the job to test
   * @param authEnabled whether authorisation is enabled
   * @param currentUserId the caller's subject, when known
   * @return true if the job should be listed for this caller
   */
  private static boolean isVisibleToCaller(
      @Nonnull final Job<?> job,
      final boolean authEnabled,
      @Nonnull final Optional<String> currentUserId) {
    if (!authEnabled) {
      return true;
    }
    return currentUserId.isPresent() && job.getOwnerId().equals(currentUserId);
  }

  /**
   * Appends a single {@code job} parameter, with its parts, to the response.
   *
   * @param result the Parameters resource being built
   * @param job the job to project
   * @param fhirServerBase the absolute FHIR server base, for the status URL
   */
  private static void addJobParameter(
      @Nonnull final Parameters result,
      @Nonnull final Job<?> job,
      @Nonnull final String fhirServerBase) {
    final JobStatus status = JobStatus.fromResult(job.getResult());
    final ParametersParameterComponent jobParam = result.addParameter().setName("job");
    jobParam.addPart().setName("id").setValue(new StringType(job.getId()));
    jobParam.addPart().setName("operation").setValue(new CodeType(job.getOperation()));
    jobParam.addPart().setName("status").setValue(new CodeType(status.getCode()));
    // Progress is only meaningful, and only free of a divide-by-zero, for in-progress jobs whose
    // total stage count is known.
    if (status == JobStatus.IN_PROGRESS && job.getTotalStages() > 0) {
      jobParam.addPart().setName("progress").setValue(new IntegerType(job.getProgressPercentage()));
    }
    final InstantType startTime = new InstantType(Date.from(job.getStartTime()));
    startTime.setTimeZoneZulu(true);
    jobParam.addPart().setName("startTime").setValue(startTime);
    jobParam
        .addPart()
        .setName("url")
        .setValue(new UriType(fhirServerBase + "/$job?id=" + job.getId()));
  }
}
