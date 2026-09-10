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

package au.csiro.pathling.operations.sql;

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;

import au.csiro.pathling.async.AsyncJobContext;
import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.async.RequestTag;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.operations.bulkexport.ExportResult;
import au.csiro.pathling.operations.bulkexport.ExportResultRegistry;
import au.csiro.pathling.operations.export.ExportFileWriter;
import au.csiro.pathling.operations.export.ExportManifest;
import au.csiro.pathling.operations.export.ExportManifestOutput;
import au.csiro.pathling.operations.sqlquery.PreparedSqlQuery;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * The asynchronous-job machinery behind {@code $sql-export}: owning-job resolution, background
 * execution, completion-manifest construction, and the deterministic cache key that lets an
 * identical kick-off deduplicate onto an existing job.
 *
 * @author John Grimes
 */
@Component
public class SqlExportSupport {

  @Nonnull private final SqlExportExecutor executor;

  @Nonnull private final JobRegistry jobRegistry;

  @Nonnull private final RequestTagFactory requestTagFactory;

  @Nonnull private final ExportResultRegistry exportResultRegistry;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final ExportFileWriter fileWriter;

  /**
   * Constructs a new SqlExportSupport.
   *
   * @param executor runs the subjects and writes the output files
   * @param jobRegistry the async job registry
   * @param requestTagFactory the request tag factory used for job deduplication
   * @param exportResultRegistry the export result registry backing the result endpoint
   * @param serverConfiguration the server configuration, consulted for the result expiry
   * @param fileWriter the shared export file writer, used to clean up partial outputs on failure
   */
  @SuppressWarnings("java:S107")
  @Autowired
  public SqlExportSupport(
      @Nonnull final SqlExportExecutor executor,
      @Nonnull final JobRegistry jobRegistry,
      @Nonnull final RequestTagFactory requestTagFactory,
      @Nonnull final ExportResultRegistry exportResultRegistry,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final ExportFileWriter fileWriter) {
    this.executor = executor;
    this.jobRegistry = jobRegistry;
    this.requestTagFactory = requestTagFactory;
    this.exportResultRegistry = exportResultRegistry;
    this.serverConfiguration = serverConfiguration;
    this.fileWriter = fileWriter;
  }

  /**
   * Resolves the owning job, runs the export, and builds the completion manifest.
   *
   * @param requestDetails the request details
   * @param validation the provider's pre-async validation, used for the fallback job lookup
   * @return the completion manifest, or null when the job was cancelled
   */
  @Nullable
  public Parameters runExport(
      @Nonnull final ServletRequestDetails requestDetails,
      @Nonnull final PreAsyncValidation<SqlExportRequest> validation) {
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

    final Job<SqlExportRequest> ownJob = resolveOwnJob(requestDetails, authentication, validation);
    if (ownJob == null) {
      throw new InvalidRequestException("Missing 'Prefer: respond-async' header value.");
    }

    // The result belongs to the user who started the job, and to nobody else.
    final Optional<String> currentUserId = getCurrentUserId(authentication);
    if (currentUserId.isPresent() && !ownJob.getOwnerId().equals(currentUserId)) {
      throw new AccessDeniedError(
          "The requested result is not owned by the current user '%s'."
              .formatted(currentUserId.orElse("null")));
    }

    final SqlExportRequest exportRequest = ownJob.getPreAsyncValidationResult();
    if (ownJob.isCancelled()) {
      return null;
    }

    exportResultRegistry.put(ownJob.getId(), new ExportResult(ownJob.getOwnerId()));

    final List<ExportManifestOutput> outputs;
    try {
      outputs = executor.execute(exportRequest, ownJob.getId());
    } catch (final RuntimeException e) {
      // All-or-nothing: a failed subject fails the whole export. Remove the result registration and
      // delete any partial outputs so none are offered for download, then surface the failure.
      exportResultRegistry.remove(ownJob.getId());
      fileWriter.deleteJobDirectory(ownJob.getId());
      throw e;
    }

    ownJob.setResponseModification(
        httpServletResponse -> {
          final String expiresValue =
              ZonedDateTime.now(ZoneOffset.UTC)
                  .plusSeconds(serverConfiguration.getExport().getResultExpiry())
                  .format(DateTimeFormatter.RFC_1123_DATE_TIME);
          httpServletResponse.addHeader("Expires", expiresValue);
        });

    return new ExportManifest(
            exportRequest.serverBaseUrl(),
            ownJob.getId(),
            exportRequest.clientTrackingId(),
            exportRequest.format().getCode(),
            ownJob.getStartTime(),
            Instant.now(),
            outputs)
        .toParameters();
  }

  /**
   * Resolves the job owning this request: the one set by the async aspect when running
   * asynchronously, or - as a fallback when the async context is unavailable - the one looked up by
   * recomputing the request tag.
   */
  @Nullable
  private Job<SqlExportRequest> resolveOwnJob(
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final Authentication authentication,
      @Nonnull final PreAsyncValidation<SqlExportRequest> validation) {
    @SuppressWarnings("unchecked")
    final Optional<Job<SqlExportRequest>> contextJob =
        AsyncJobContext.getCurrentJob().map(job -> (Job<SqlExportRequest>) job);
    if (contextJob.isPresent()) {
      return contextJob.get();
    }

    final PreAsyncValidationResult<SqlExportRequest> validationResult =
        validation.preAsyncValidate(requestDetails, new Object[] {});
    final String operationCacheKey =
        validation.computeCacheKeyComponent(
            Objects.requireNonNull(
                validationResult.result(), "A valid request must produce a validation result"));
    final RequestTag ownTag =
        requestTagFactory.createTag(requestDetails, authentication, operationCacheKey);
    return jobRegistry.get(ownTag);
  }

  /**
   * Computes the deterministic cache key component from the parsed request, so that identical
   * kick-offs deduplicate onto the same job while differing ones do not.
   *
   * @param request the parsed request
   * @return the cache key component
   */
  @Nonnull
  public String computeCacheKeyComponent(@Nonnull final SqlExportRequest request) {
    final StringBuilder key = new StringBuilder();

    final String subjects =
        request.subjects().stream()
            .map(SqlExportSupport::describe)
            .collect(Collectors.joining(","));
    key.append("subjects=[").append(subjects).append("]");

    if (request.clientTrackingId() != null) {
      key.append("|clientTrackingId=").append(request.clientTrackingId());
    }
    key.append("|format=").append(request.format());
    key.append("|header=").append(request.includeHeader());

    if (!request.patientIds().isEmpty()) {
      key.append("|patientIds=[")
          .append(request.patientIds().stream().sorted().collect(Collectors.joining(",")))
          .append("]");
    }
    if (request.since() != null) {
      key.append("|since=").append(request.since().getValueAsString());
    }
    return key.toString();
  }

  /**
   * Renders one subject as a deterministic description for the cache key. A SQL subject is
   * described by its resolved SQL and bindings, and a view subject by its parsed projection, so two
   * kick-offs that would produce different data never share a job.
   */
  @Nonnull
  private static String describe(@Nonnull final SubjectInput subject) {
    final StringBuilder description = new StringBuilder(subject.name()).append(':');
    description.append(subject.kind()).append(':');
    final PreparedSqlQuery prepared = subject.preparedQuery();
    if (prepared != null) {
      description
          .append(prepared.getRequest().getParsedQuery().getSql())
          .append(':')
          .append(prepared.getRequest().getParameterBindings())
          .append(':')
          .append(prepared.getDependencyGraph().getTopLevelKeysByLabel());
    } else {
      description.append(Objects.requireNonNull(subject.view()).toString());
    }
    return description.toString();
  }
}
