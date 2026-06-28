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

package au.csiro.pathling.operations.sqlquery;

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
import au.csiro.pathling.operations.compartment.GroupMemberService;
import au.csiro.pathling.operations.export.ExportFileWriter;
import au.csiro.pathling.operations.export.ExportManifest;
import au.csiro.pathling.operations.export.ExportManifestOutput;
import au.csiro.pathling.views.ViewDefinitionGson;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Shared machinery for the {@code $sqlquery-export} providers across the system, type, and instance
 * levels: owning-job resolution, background execution, completion-manifest construction, the
 * deterministic cache key, and extraction of the simple kick-off parameters from the request body.
 * Both {@link SqlQueryExportProvider} (system) and {@link SqlQueryInstanceExportProvider}
 * (type/instance) delegate here so the level-agnostic logic lives in one place.
 *
 * @author John Grimes
 */
@Component
public class SqlQueryExportSupport {

  @Nonnull private final SqlQueryExportExecutor executor;

  @Nonnull private final JobRegistry jobRegistry;

  @Nonnull private final RequestTagFactory requestTagFactory;

  @Nonnull private final ExportResultRegistry exportResultRegistry;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final GroupMemberService groupMemberService;

  @Nonnull private final ExportFileWriter fileWriter;

  @Nonnull private final Gson gson;

  /**
   * Constructs a new SqlQueryExportSupport.
   *
   * @param executor runs the queries and writes the output files
   * @param jobRegistry the async job registry
   * @param requestTagFactory the request tag factory used for job deduplication
   * @param exportResultRegistry the export result registry backing the {@code $result} endpoint
   * @param serverConfiguration the server configuration
   * @param groupMemberService resolves {@code group} references to member patient ids
   * @param fileWriter the shared export file writer, used to clean up partial outputs on failure
   */
  @SuppressWarnings("java:S107")
  @Autowired
  public SqlQueryExportSupport(
      @Nonnull final SqlQueryExportExecutor executor,
      @Nonnull final JobRegistry jobRegistry,
      @Nonnull final RequestTagFactory requestTagFactory,
      @Nonnull final ExportResultRegistry exportResultRegistry,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final GroupMemberService groupMemberService,
      @Nonnull final ExportFileWriter fileWriter) {
    this.executor = executor;
    this.jobRegistry = jobRegistry;
    this.requestTagFactory = requestTagFactory;
    this.exportResultRegistry = exportResultRegistry;
    this.serverConfiguration = serverConfiguration;
    this.groupMemberService = groupMemberService;
    this.fileWriter = fileWriter;
    this.gson = ViewDefinitionGson.create();
  }

  /**
   * Resolves the owning job, runs the export in the background, and builds the completion manifest.
   *
   * @param requestDetails the request details
   * @param validation the provider's pre-async validation (used for the fallback job lookup)
   * @return the completion manifest, or null if the job was cancelled
   */
  @Nullable
  public Parameters runExport(
      @Nonnull final ServletRequestDetails requestDetails,
      @Nonnull final PreAsyncValidation<SqlQueryExportRequest> validation) {
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

    final Job<SqlQueryExportRequest> ownJob =
        resolveOwnJob(requestDetails, authentication, validation);
    if (ownJob == null) {
      throw new InvalidRequestException("Missing 'Prefer: respond-async' header value.");
    }

    // Check that the user requesting the result is the same user that started the job.
    final Optional<String> currentUserId = getCurrentUserId(authentication);
    if (currentUserId.isPresent() && !ownJob.getOwnerId().equals(currentUserId)) {
      throw new AccessDeniedError(
          "The requested result is not owned by the current user '%s'."
              .formatted(currentUserId.orElse("null")));
    }

    final SqlQueryExportRequest exportRequest = ownJob.getPreAsyncValidationResult();
    if (ownJob.isCancelled()) {
      return null;
    }

    exportResultRegistry.put(ownJob.getId(), new ExportResult(ownJob.getOwnerId()));

    final List<ExportManifestOutput> outputs;
    try {
      outputs = executor.execute(exportRequest, ownJob.getId());
    } catch (final RuntimeException e) {
      // All-or-nothing: a failed query fails the whole export. Remove the result registration and
      // delete any partial outputs so none are offered for download, then surface the failure.
      exportResultRegistry.remove(ownJob.getId());
      fileWriter.deleteJobDirectory(ownJob.getId());
      throw e;
    }

    // Set the Expires header on the completion response.
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
  private Job<SqlQueryExportRequest> resolveOwnJob(
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final Authentication authentication,
      @Nonnull final PreAsyncValidation<SqlQueryExportRequest> validation) {
    @SuppressWarnings("unchecked")
    final Optional<Job<SqlQueryExportRequest>> contextJob =
        AsyncJobContext.getCurrentJob().map(job -> (Job<SqlQueryExportRequest>) job);
    if (contextJob.isPresent()) {
      return contextJob.get();
    }

    final PreAsyncValidationResult<SqlQueryExportRequest> validationResult =
        validation.preAsyncValidate(requestDetails, new Object[] {});
    final String operationCacheKey =
        validation.computeCacheKeyComponent(
            Objects.requireNonNull(
                validationResult.result(),
                "Validation result should not be null for a valid request"));
    final RequestTag ownTag =
        requestTagFactory.createTag(requestDetails, authentication, operationCacheKey);
    return jobRegistry.get(ownTag);
  }

  /**
   * Computes the deterministic cache key component from the parsed request, so that identical
   * kick-offs deduplicate to the same job.
   *
   * @param request the parsed request
   * @return the cache key component
   */
  @Nonnull
  public String computeCacheKeyComponent(@Nonnull final SqlQueryExportRequest request) {
    final StringBuilder key = new StringBuilder();

    final String queriesJson =
        request.queries().stream()
            .map(
                q ->
                    (q.name() != null ? q.name() : "")
                        + ":"
                        + q.preparedQuery().getRequest().getParsedQuery().getSql()
                        + ":"
                        + gson.toJson(
                            describeDependencyGraph(q.preparedQuery().getDependencyGraph()))
                        + ":"
                        + gson.toJson(q.preparedQuery().getRequest().getParameterBindings()))
            .collect(Collectors.joining(","));
    key.append("queries=[").append(queriesJson).append("]");

    if (request.clientTrackingId() != null) {
      key.append("|clientTrackingId=").append(request.clientTrackingId());
    }
    key.append("|format=").append(request.format());
    key.append("|header=").append(request.includeHeader());

    if (!request.patientIds().isEmpty()) {
      final String sortedPatientIds =
          request.patientIds().stream().sorted().collect(Collectors.joining(","));
      key.append("|patientIds=[").append(sortedPatientIds).append("]");
    }
    if (request.since() != null) {
      key.append("|since=").append(request.since().getValueAsString());
    }
    return key.toString();
  }

  /**
   * Renders a resolved dependency graph as a deterministic, serialisable description for the cache
   * key, so two kick-offs whose composed queries differ deduplicate to distinct jobs. Captures the
   * top-level label-to-node mapping and, for each node, its canonical key and (for a SQLView) its
   * SQL and child label mapping.
   */
  @Nonnull
  private static List<String> describeDependencyGraph(
      @Nonnull final ResolvedDependencyGraph graph) {
    final List<String> parts = new java.util.ArrayList<>();
    parts.add("top=" + graph.getTopLevelKeysByLabel());
    for (final ResolvedDependency node : graph.getOrderedNodes()) {
      if (node instanceof final ResolvedSqlView sqlView) {
        parts.add(
            "view:"
                + sqlView.getCanonicalKey()
                + ":"
                + sqlView.getSql()
                + ":"
                + sqlView.getChildKeysByLabel());
      } else {
        parts.add("vd:" + node.getCanonicalKey());
      }
    }
    return parts;
  }

  /** Collects patient ids from both the {@code patient} and {@code group} parameters. */
  @Nonnull
  public Set<String> collectPatientIds(@Nonnull final ServletRequestDetails requestDetails) {
    final Set<String> allPatientIds = new HashSet<>();
    for (final Parameters.ParametersParameterComponent param :
        parametersOf(requestDetails).getParameter()) {
      if ("patient".equals(param.getName())) {
        final String id = stripResourcePrefix(referenceOrPrimitive(param.getValue()));
        if (id != null && !id.isBlank()) {
          allPatientIds.add(id);
        }
      } else if ("group".equals(param.getName())) {
        final String groupId = stripResourcePrefix(referenceOrPrimitive(param.getValue()));
        if (groupId != null && !groupId.isBlank()) {
          allPatientIds.addAll(groupMemberService.extractPatientIdsFromGroup(groupId));
        }
      }
    }
    return allPatientIds;
  }

  /**
   * Extracts a simple string-valued parameter from the request body.
   *
   * @param requestDetails the request details
   * @param name the parameter name
   * @return the primitive value, or null when absent
   */
  @Nullable
  public String stringParam(
      @Nonnull final ServletRequestDetails requestDetails, @Nonnull final String name) {
    for (final Parameters.ParametersParameterComponent param :
        parametersOf(requestDetails).getParameter()) {
      if (name.equals(param.getName()) && param.getValue() != null) {
        return param.getValue().primitiveValue();
      }
    }
    return null;
  }

  /**
   * Extracts the {@code header} boolean parameter from the request body.
   *
   * @param requestDetails the request details
   * @return the header flag, or null when absent
   */
  @Nullable
  public BooleanType headerParam(@Nonnull final ServletRequestDetails requestDetails) {
    final String value = stringParam(requestDetails, "header");
    return value == null ? null : new BooleanType(value);
  }

  /**
   * Extracts the {@code _since} instant parameter from the request body.
   *
   * @param requestDetails the request details
   * @return the since instant, or null when absent
   */
  @Nullable
  public InstantType sinceParam(@Nonnull final ServletRequestDetails requestDetails) {
    final String value = stringParam(requestDetails, "_since");
    return value == null ? null : new InstantType(value);
  }

  @Nullable
  private static String referenceOrPrimitive(@Nullable final Type value) {
    if (value == null) {
      return null;
    }
    if (value instanceof final Reference reference) {
      return reference.getReference();
    }
    return value.primitiveValue();
  }

  @Nullable
  private static String stripResourcePrefix(@Nullable final String reference) {
    if (reference == null) {
      return null;
    }
    final int slash = reference.lastIndexOf('/');
    return slash >= 0 ? reference.substring(slash + 1) : reference;
  }

  @Nonnull
  private static Parameters parametersOf(@Nonnull final ServletRequestDetails requestDetails) {
    return requestDetails.getResource() instanceof final Parameters parameters
        ? parameters
        : new Parameters();
  }
}
