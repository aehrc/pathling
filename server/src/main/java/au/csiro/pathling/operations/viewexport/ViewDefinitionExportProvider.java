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

package au.csiro.pathling.operations.viewexport;

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.async.RequestTag;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.bulkexport.ExportResult;
import au.csiro.pathling.operations.bulkexport.ExportResultRegistry;
import au.csiro.pathling.security.OperationAccess;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Provider for the $viewdefinition-export operation from the SQL on FHIR specification. This
 * operation executes one or more ViewDefinitions and exports the results to files.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/OperationDefinition-ViewDefinitionExport.html">ViewDefinitionExport</a>
 */
@Slf4j
@Component
public class ViewDefinitionExportProvider
    implements PreAsyncValidation<ViewDefinitionExportRequest> {

  private static final String PATIENT_REFERENCE_PREFIX = "Patient/";

  @Nonnull
  private final ViewDefinitionExportExecutor executor;

  @Nonnull
  private final JobRegistry jobRegistry;

  @Nonnull
  private final RequestTagFactory requestTagFactory;

  @Nonnull
  private final ExportResultRegistry exportResultRegistry;

  @Nonnull
  private final ServerConfiguration serverConfiguration;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final QueryableDataSource deltaLake;

  @Nonnull
  private final Gson gson;

  /**
   * Constructs a new ViewDefinitionExportProvider.
   *
   * @param executor the export executor
   * @param jobRegistry the job registry
   * @param requestTagFactory the request tag factory
   * @param exportResultRegistry the export result registry
   * @param serverConfiguration the server configuration
   * @param fhirContext the FHIR context
   * @param deltaLake the queryable data source
   */
  @Autowired
  public ViewDefinitionExportProvider(
      @Nonnull final ViewDefinitionExportExecutor executor,
      @Nonnull final JobRegistry jobRegistry,
      @Nonnull final RequestTagFactory requestTagFactory,
      @Nonnull final ExportResultRegistry exportResultRegistry,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final QueryableDataSource deltaLake) {
    this.executor = executor;
    this.jobRegistry = jobRegistry;
    this.requestTagFactory = requestTagFactory;
    this.exportResultRegistry = exportResultRegistry;
    this.serverConfiguration = serverConfiguration;
    this.fhirContext = fhirContext;
    this.deltaLake = deltaLake;
    this.gson = new GsonBuilder().create();
  }

  /**
   * Handles the $viewdefinition-export operation at the system level.
   *
   * @param viewNames the names for the exported views (parallel array with viewResources)
   * @param viewResources the ViewDefinition resources to export
   * @param clientTrackingId optional client-provided tracking identifier
   * @param format the output format (ndjson, csv, parquet)
   * @param includeHeader whether to include headers in CSV output
   * @param patientIds patient IDs to filter by
   * @param groupIds group IDs to filter by
   * @param since filter resources modified after this timestamp
   * @param requestDetails the request details
   * @return the binary result containing the export manifest, or null if cancelled
   */
  @SuppressWarnings({"unused", "java:S107"})
  @Operation(name = "viewdefinition-export", idempotent = true)
  @OperationAccess("view-export")
  @AsyncSupported
  @Nullable
  public Binary export(
      @Nullable @OperationParam(name = "view.name") final List<String> viewNames,
      @Nullable @OperationParam(name = "view.viewResource") final List<IBaseResource> viewResources,
      @Nullable @OperationParam(name = "clientTrackingId") final String clientTrackingId,
      @Nullable @OperationParam(name = "_format") final String format,
      @Nullable @OperationParam(name = "header") final BooleanType includeHeader,
      @Nullable @OperationParam(name = "patient") final List<String> patientIds,
      @Nullable @OperationParam(name = "group") final List<IdType> groupIds,
      @Nullable @OperationParam(name = "_since") final InstantType since,
      @Nonnull final ServletRequestDetails requestDetails
  ) {
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

    // Compute the operation cache key to match how AsyncAspect stores the job.
    final PreAsyncValidationResult<ViewDefinitionExportRequest> validationResult = preAsyncValidate(
        requestDetails, new Object[]{viewNames, viewResources, clientTrackingId, format,
            includeHeader, patientIds, groupIds, since});
    final String operationCacheKey = computeCacheKeyComponent(
        Objects.requireNonNull(validationResult.result(),
            "Validation result should not be null for a valid request"));
    final RequestTag ownTag = requestTagFactory.createTag(requestDetails, authentication,
        operationCacheKey);

    final Job<ViewDefinitionExportRequest> ownJob = jobRegistry.get(ownTag);
    if (ownJob == null) {
      throw new InvalidRequestException("Missing 'Prefer: respond-async' header value.");
    }

    // Check that the user requesting the result is the same user that started the job.
    final Optional<String> currentUserId = getCurrentUserId(authentication);
    if (currentUserId.isPresent() && !ownJob.getOwnerId().equals(currentUserId)) {
      throw new AccessDeniedError(
          "The requested result is not owned by the current user '%s'.".formatted(
              currentUserId.orElse("null")));
    }

    final ViewDefinitionExportRequest exportRequest = ownJob.getPreAsyncValidationResult();
    if (ownJob.isCancelled()) {
      return null;
    }

    exportResultRegistry.put(ownJob.getId(), new ExportResult(ownJob.getOwnerId()));

    final List<ViewExportOutput> outputs = executor.execute(exportRequest, ownJob.getId());

    // Set the Expires header.
    ownJob.setResponseModification(httpServletResponse -> {
      final String expiresValue = ZonedDateTime.now(ZoneOffset.UTC)
          .plusSeconds(serverConfiguration.getExport().getResultExpiry())
          .format(DateTimeFormatter.RFC_1123_DATE_TIME);
      httpServletResponse.addHeader("Expires", expiresValue);
    });

    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        exportRequest.originalRequest(),
        exportRequest.serverBaseUrl(),
        outputs,
        serverConfiguration.getAuth().isEnabled()
    );

    return response.toOutput();
  }

  @Override
  @Nonnull
  public PreAsyncValidationResult<ViewDefinitionExportRequest> preAsyncValidate(
      @Nonnull final ServletRequestDetails servletRequestDetails,
      @Nonnull final Object[] params) throws InvalidRequestException {

    @SuppressWarnings("unchecked") final List<String> viewNames =
        params[0] != null
        ? (List<String>) params[0]
        : Collections.emptyList();

    @SuppressWarnings("unchecked") final List<IBaseResource> viewResources =
        params[1] != null
        ? (List<IBaseResource>) params[1]
        : Collections.emptyList();

    final String clientTrackingId = (String) params[2];
    final String format = (String) params[3];
    final BooleanType includeHeader = (BooleanType) params[4];

    @SuppressWarnings("unchecked") final List<String> patientIds =
        params[5] != null
        ? (List<String>) params[5]
        : Collections.emptyList();

    @SuppressWarnings("unchecked") final List<IdType> groupIds =
        params[6] != null
        ? (List<IdType>) params[6]
        : Collections.emptyList();

    final InstantType since = (InstantType) params[7];

    // Validate that at least one view is provided.
    if (viewResources.isEmpty()) {
      throw new InvalidRequestException(
          "At least one view.viewResource parameter is required.");
    }

    // Parse ViewDefinitions.
    final List<ViewInput> views = parseViewInputs(viewNames, viewResources);

    // Collect patient IDs from both patient and group parameters.
    final Set<String> allPatientIds = collectPatientIds(patientIds, groupIds);

    // Determine header setting (default true).
    final boolean header = includeHeader == null || includeHeader.booleanValue();

    final ViewDefinitionExportRequest request = new ViewDefinitionExportRequest(
        servletRequestDetails.getCompleteUrl(),
        servletRequestDetails.getFhirServerBase(),
        views,
        clientTrackingId,
        ViewExportFormat.fromString(format),
        header,
        allPatientIds,
        since
    );

    return new PreAsyncValidationResult<>(request, Collections.emptyList());
  }

  @Override
  @Nonnull
  public String computeCacheKeyComponent(
      @Nonnull final ViewDefinitionExportRequest request) {
    // Build a deterministic cache key from request parameters.
    // Exclude originalRequest and serverBaseUrl as they're infrastructure details.
    final StringBuilder key = new StringBuilder();

    // Serialize views deterministically using JSON.
    final String viewsJson = request.views().stream()
        .map(v -> (v.name() != null
                   ? v.name()
                   : "") + ":" + gson.toJson(v.view()))
        .sorted()
        .collect(Collectors.joining(","));
    key.append("views=[").append(viewsJson).append("]");

    if (request.clientTrackingId() != null) {
      key.append("|clientTrackingId=").append(request.clientTrackingId());
    }

    key.append("|format=").append(request.format());
    key.append("|header=").append(request.includeHeader());

    // Sort patient IDs for determinism.
    if (!request.patientIds().isEmpty()) {
      final String sortedPatientIds = request.patientIds().stream()
          .sorted()
          .collect(Collectors.joining(","));
      key.append("|patientIds=[").append(sortedPatientIds).append("]");
    }

    if (request.since() != null) {
      key.append("|since=").append(request.since().getValueAsString());
    }

    return key.toString();
  }

  /**
   * Parses view inputs from the parameter arrays.
   */
  @Nonnull
  private List<ViewInput> parseViewInputs(
      @Nonnull final List<String> viewNames,
      @Nonnull final List<IBaseResource> viewResources) {

    final List<ViewInput> views = new ArrayList<>();

    for (int i = 0; i < viewResources.size(); i++) {
      final String name = i < viewNames.size()
                          ? viewNames.get(i)
                          : null;
      final IBaseResource resource = viewResources.get(i);
      final FhirView view = parseViewDefinition(resource, i);
      views.add(new ViewInput(name, view));
    }

    return views;
  }

  /**
   * Parses a ViewDefinition resource into a FhirView object.
   */
  @Nonnull
  private FhirView parseViewDefinition(@Nonnull final IBaseResource viewResource, final int index) {
    try {
      final String viewJson = fhirContext.newJsonParser().encodeResourceToString(viewResource);
      return gson.fromJson(viewJson, FhirView.class);
    } catch (final JsonSyntaxException e) {
      throw new InvalidRequestException(
          "Invalid ViewDefinition at index %d: %s".formatted(index, e.getMessage()));
    }
  }

  /**
   * Collects patient IDs from both patient and group parameters.
   */
  @Nonnull
  private Set<String> collectPatientIds(
      @Nonnull final List<String> patientIds,
      @Nonnull final List<IdType> groupIds) {

    final Set<String> allPatientIds = new HashSet<>(patientIds);

    for (final IdType groupId : groupIds) {
      allPatientIds.addAll(extractPatientIdsFromGroup(groupId.getIdPart()));
    }

    return allPatientIds;
  }

  /**
   * Extracts patient IDs from a Group resource.
   */
  @Nonnull
  private Set<String> extractPatientIdsFromGroup(@Nonnull final String groupId) {
    final Dataset<Row> groupDataset = deltaLake.read("Group");
    final Dataset<Row> filtered = groupDataset.filter(col("id").equalTo(groupId));

    if (filtered.isEmpty()) {
      throw new ResourceNotFoundException("Group/" + groupId);
    }

    final Dataset<Row> memberRefs = filtered
        .select(explode(col("member")).as("member"))
        .select(col("member.entity.reference").as("reference"));

    final Set<String> patientIdsFromGroup = new HashSet<>();
    for (final Row row : memberRefs.collectAsList()) {
      final String reference = row.getString(0);
      if (reference != null && reference.startsWith(PATIENT_REFERENCE_PREFIX)) {
        patientIdsFromGroup.add(reference.substring(PATIENT_REFERENCE_PREFIX.length()));
      }
    }

    log.debug("Extracted {} patient IDs from Group/{}", patientIdsFromGroup.size(), groupId);
    return patientIdsFromGroup;
  }

}
