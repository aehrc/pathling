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

package au.csiro.pathling.operations.view;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.GroupMemberService;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import au.csiro.pathling.views.ViewDefinitionGson;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.ConstraintViolationException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Helper class for executing ViewDefinition queries. Contains shared logic used by both {@link
 * ViewDefinitionRunProvider} and {@link ViewDefinitionInstanceRunProvider}.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class ViewExecutionHelper {

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final FhirEncoders fhirEncoders;

  @Nonnull private final PatientCompartmentService patientCompartmentService;

  @Nonnull private final GroupMemberService groupMemberService;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final Gson gson;

  @Nonnull private final ResultStreamingHelper streamingHelper;

  @Nonnull private final ReadExecutor readExecutor;

  /**
   * Constructs a new ViewExecutionHelper.
   *
   * @param sparkSession the Spark session
   * @param deltaLake the queryable data source
   * @param fhirContext the FHIR context
   * @param fhirEncoders the FHIR encoders
   * @param patientCompartmentService the patient compartment service
   * @param groupMemberService the group member service
   * @param serverConfiguration the server configuration
   * @param readExecutor the read executor for resolving stored ViewDefinitions by id
   */
  @SuppressWarnings("java:S107")
  @Autowired
  public ViewExecutionHelper(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final PatientCompartmentService patientCompartmentService,
      @Nonnull final GroupMemberService groupMemberService,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final ReadExecutor readExecutor) {
    this.sparkSession = sparkSession;
    this.deltaLake = deltaLake;
    this.fhirContext = fhirContext;
    this.fhirEncoders = fhirEncoders;
    this.patientCompartmentService = patientCompartmentService;
    this.groupMemberService = groupMemberService;
    this.serverConfiguration = serverConfiguration;
    this.readExecutor = readExecutor;
    this.gson = ViewDefinitionGson.create();
    this.streamingHelper = new ResultStreamingHelper(gson);
  }

  /**
   * Rejects the unsupported {@code source} parameter (external data source). Pathling does not
   * implement external data sources, so a supplied {@code source} value is rejected rather than
   * silently ignored, which would mislead the client about the data that was queried.
   *
   * @param source the {@code source} parameter value, if supplied
   * @throws InvalidRequestException if {@code source} is present and non-blank
   */
  public void rejectSourceParameter(@Nullable final String source) {
    if (source != null && !source.isBlank()) {
      throw new InvalidRequestException(
          "The 'source' parameter (external data source) is not supported by this server.");
    }
  }

  /**
   * Resolves the ViewDefinition to run from the mutually exclusive {@code viewResource} and {@code
   * viewReference} parameters, for the system and type levels. Exactly one must be supplied.
   *
   * @param viewResource the inline ViewDefinition resource, if supplied
   * @param viewReference a literal relative reference to a stored ViewDefinition, if supplied
   * @return the ViewDefinition resource to execute
   * @throws InvalidRequestException (400) if both or neither parameter is supplied
   * @throws ResourceNotFoundException (404) if the reference does not resolve to a stored
   *     ViewDefinition
   */
  @Nonnull
  public IBaseResource resolveViewInput(
      @Nullable final IBaseResource viewResource, @Nullable final Reference viewReference) {
    final boolean hasResource = viewResource != null;
    final boolean hasReference = viewReference != null && !viewReference.isEmpty();

    if (hasResource && hasReference) {
      throw new InvalidRequestException(
          "Provide exactly one of 'viewResource' or 'viewReference', not both.");
    }
    if (!hasResource && !hasReference) {
      throw new InvalidRequestException(
          "Either 'viewResource' or 'viewReference' must be provided.");
    }
    if (hasResource) {
      return viewResource;
    }

    final String id = ReferenceParameters.extractId(viewReference);
    if (id == null) {
      throw new ResourceNotFoundException(
          "The viewReference does not resolve to a stored ViewDefinition.");
    }
    return readStoredViewDefinition(id);
  }

  /**
   * Reads a stored ViewDefinition by its logical id, mapping a missing resource to a 404.
   *
   * @param id the logical id of the stored ViewDefinition
   * @return the stored ViewDefinition resource
   * @throws ResourceNotFoundException (404) if no ViewDefinition with the id exists
   */
  @Nonnull
  public IBaseResource readStoredViewDefinition(@Nonnull final String id) {
    try {
      return readExecutor.read("ViewDefinition", id);
    } catch (final ResourceNotFoundError e) {
      throw new ResourceNotFoundException("ViewDefinition with ID '" + id + "' not found");
    } catch (final IllegalArgumentException e) {
      // Handle the case where no ViewDefinition data exists in the data source at all.
      if (e.getMessage() != null && e.getMessage().contains("No data found for resource type")) {
        throw new ResourceNotFoundException("ViewDefinition with ID '" + id + "' not found");
      }
      throw e;
    }
  }

  /**
   * Reduces the {@code patient} parameter (a {@code Reference} capped at one) to the logical id
   * used for compartment filtering.
   *
   * @param patients the supplied {@code patient} references, if any
   * @return a list containing the single patient logical id, or an empty list
   * @throws InvalidRequestException (400) if more than one {@code patient} is supplied
   */
  @Nonnull
  public List<String> toPatientIds(@Nullable final List<Reference> patients) {
    if (patients == null || patients.isEmpty()) {
      return List.of();
    }
    if (patients.size() > 1) {
      throw new InvalidRequestException("Only a single 'patient' parameter is supported.");
    }
    final String id = ReferenceParameters.extractId(patients.get(0));
    return id == null ? List.of() : List.of(id);
  }

  /**
   * Reduces the repeatable {@code group} parameter ({@code Reference} values) to the logical ids
   * used for compartment filtering.
   *
   * @param groups the supplied {@code group} references, if any
   * @return the list of group logical ids, in order
   */
  @Nonnull
  public List<IdType> toGroupIds(@Nullable final List<Reference> groups) {
    if (groups == null || groups.isEmpty()) {
      return List.of();
    }
    final List<IdType> ids = new ArrayList<>();
    for (final Reference group : groups) {
      final String id = ReferenceParameters.extractId(group);
      if (id != null) {
        ids.add(new IdType(id));
      }
    }
    return ids;
  }

  /**
   * Executes a ViewDefinition and streams the results to the HTTP response.
   *
   * @param viewResource the ViewDefinition resource to execute
   * @param format the output format (ndjson or csv), overrides Accept header if provided
   * @param acceptHeader the HTTP Accept header value, used as fallback if format is not provided
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param patientIds patient IDs to filter by
   * @param groupIds group IDs to filter by
   * @param since filter by meta.lastUpdated >= value
   * @param inlineResources FHIR resources to use instead of the main data source
   * @param response the HTTP response for streaming output
   */
  @SuppressWarnings("java:S107")
  public void executeView(
      @Nonnull final IBaseResource viewResource,
      @Nullable final String format,
      @Nullable final String acceptHeader,
      @Nullable final BooleanType includeHeader,
      @Nullable final IntegerType limit,
      @Nullable final List<String> patientIds,
      @Nullable final List<IdType> groupIds,
      @Nullable final InstantType since,
      @Nullable final List<String> inlineResources,
      @Nullable final HttpServletResponse response) {

    if (response == null) {
      throw new InvalidRequestException("HTTP response is required for this operation");
    }

    // Parse the ViewDefinition.
    final FhirView view = parseViewDefinition(viewResource);

    // Check resource-level authorization if authentication is enabled.
    if (serverConfiguration.getAuth().isEnabled()) {
      SecurityAspect.checkHasAuthority(
          PathlingAuthority.resourceAccess(AccessType.READ, view.getResource()));
    }

    // Determine output format: an explicit _format parameter is parsed strictly (an unsupported
    // value is rejected), while Accept-header negotiation remains lenient and falls back to NDJSON.
    final ViewOutputFormat outputFormat =
        (format != null && !format.isBlank())
            ? ViewOutputFormat.fromStringStrict(format)
            : ViewOutputFormat.fromAcceptHeader(acceptHeader);
    final boolean shouldIncludeHeader = includeHeader == null || includeHeader.booleanValue();

    // Build the data source.
    final DataSource dataSource = buildDataSource(inlineResources, patientIds, groupIds, since);

    // Get column names from the view for CSV header.
    final List<String> columnNames = view.getAllColumns().map(Column::getName).toList();

    // Set response headers for streaming.
    response.setContentType(outputFormat.getContentType());
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setStatus(HttpServletResponse.SC_OK);

    try {
      final OutputStream outputStream = response.getOutputStream();

      // For CSV with header, write header immediately to minimise TTFB.
      if (outputFormat == ViewOutputFormat.CSV && shouldIncludeHeader) {
        streamingHelper.writeCsvHeader(outputStream, columnNames);
        outputStream.flush();
      }

      // Execute the view query and stream results.
      executeAndStreamResults(view, dataSource, limit, outputFormat, outputStream);

      outputStream.flush();
    } catch (final IOException e) {
      log.error("Error streaming view results", e);
      throw new InvalidRequestException("Error streaming results: " + e.getMessage());
    }
  }

  /** Executes the view query and streams results to the output stream. */
  private void executeAndStreamResults(
      @Nonnull final FhirView view,
      @Nonnull final DataSource dataSource,
      @Nullable final IntegerType limit,
      @Nonnull final ViewOutputFormat outputFormat,
      @Nonnull final OutputStream outputStream)
      throws IOException {

    final FhirViewExecutor executor =
        new FhirViewExecutor(fhirContext, dataSource, serverConfiguration.getQuery());
    Dataset<Row> result;
    try {
      result = executor.buildQuery(view);
    } catch (final ConstraintViolationException e) {
      // A well-formed request carrying a semantically invalid ViewDefinition maps to 422,
      // distinct from the 400 used for malformed requests and invalid parameters.
      throw new UnprocessableEntityException("Invalid ViewDefinition: " + e.getMessage());
    } catch (final UnsupportedOperationException | UnsupportedFhirPathFeatureError e) {
      // Thrown when a FHIRPath expression is not supported, such as accessing a choice element
      // without specifying the type via ofType().
      throw new InvalidRequestException("Unsupported expression: " + e.getMessage());
    }

    // Apply limit if specified.
    if (limit != null && limit.getValue() != null) {
      result = result.limit(limit.getValue());
    }

    // Stream results.
    final StructType schema = result.schema();
    final Iterator<Row> iterator = result.toLocalIterator();

    switch (outputFormat) {
      case NDJSON -> streamingHelper.streamNdjson(outputStream, iterator, schema);
      case JSON -> streamingHelper.writeJson(outputStream, iterator, schema);
      default -> streamingHelper.streamCsv(outputStream, iterator, schema);
    }
  }

  /**
   * Parses the ViewDefinition resource into a FhirView object.
   *
   * <p>This method serialises the HAPI resource back to JSON, then parses it with Gson into the
   * FhirView class. This approach avoids duplicating the FhirView class hierarchy as HAPI resource
   * components.
   */
  @Nonnull
  private FhirView parseViewDefinition(@Nonnull final IBaseResource viewResource) {
    try {
      // Serialise the HAPI resource back to JSON.
      final String viewJson = fhirContext.newJsonParser().encodeResourceToString(viewResource);
      // Parse the JSON into the FhirView class.
      return gson.fromJson(viewJson, FhirView.class);
    } catch (final JsonSyntaxException e) {
      throw new InvalidRequestException("Invalid ViewDefinition: " + e.getMessage());
    }
  }

  /** Builds the data source, either from inline resources or the Delta Lake. */
  @Nonnull
  private DataSource buildDataSource(
      @Nullable final List<String> inlineResources,
      @Nullable final List<String> patientIds,
      @Nullable final List<IdType> groupIds,
      @Nullable final InstantType since) {

    // If inline resources are provided, use them instead of Delta Lake.
    if (inlineResources != null && !inlineResources.isEmpty()) {
      final List<IBaseResource> resources = parseInlineResources(inlineResources);
      return new ObjectDataSource(sparkSession, fhirEncoders, resources);
    }

    // Otherwise use Delta Lake with filters.
    QueryableDataSource dataSource = deltaLake;

    // Apply _since filter.
    if (since != null) {
      dataSource =
          dataSource.map(
              rowDataset ->
                  rowDataset.filter(
                      "meta.lastUpdated IS NULL OR meta.lastUpdated >= '"
                          + since.getValueAsString()
                          + "'"));
    }

    // Collect all patient IDs from both patient and group parameters.
    final Set<String> allPatientIds = new HashSet<>();
    if (patientIds != null) {
      allPatientIds.addAll(patientIds);
    }
    if (groupIds != null) {
      for (final IdType groupId : groupIds) {
        allPatientIds.addAll(groupMemberService.extractPatientIdsFromGroup(groupId.getIdPart()));
      }
    }

    // Apply patient compartment filter if patient IDs were specified.
    if (!allPatientIds.isEmpty()) {
      dataSource = applyPatientCompartmentFilter(dataSource, allPatientIds);
    }

    return dataSource;
  }

  /**
   * Parses inline FHIR resources from JSON strings, unwrapping any {@code Bundle} value into its
   * entry resources. A {@code Bundle} contributes its {@code entry[*].resource} members (one level
   * of unwrapping); standalone resources are used directly. The data source is the union of all
   * standalone resources and all unwrapped Bundle entry resources; an empty Bundle contributes
   * nothing.
   */
  @Nonnull
  private List<IBaseResource> parseInlineResources(@Nonnull final List<String> inlineResources) {
    final IParser jsonParser = fhirContext.newJsonParser();
    final List<IBaseResource> resources = new ArrayList<>();
    for (final String resourceJson : inlineResources) {
      final IBaseResource parsed;
      try {
        parsed = jsonParser.parseResource(resourceJson);
      } catch (final Exception e) {
        throw new InvalidRequestException("Invalid inline resource: " + e.getMessage());
      }
      if (parsed instanceof final Bundle bundle) {
        for (final Bundle.BundleEntryComponent entry : bundle.getEntry()) {
          if (entry.hasResource()) {
            resources.add(entry.getResource());
          }
        }
      } else {
        resources.add(parsed);
      }
    }
    return resources;
  }

  /** Applies patient compartment filter to the data source. */
  @Nonnull
  private QueryableDataSource applyPatientCompartmentFilter(
      @Nonnull final QueryableDataSource dataSource, @Nonnull final Set<String> patientIds) {

    // Filter out resource types that are not in the Patient compartment.
    final QueryableDataSource filtered =
        dataSource.filterByResourceType(patientCompartmentService::isInPatientCompartment);

    // Apply row-level filtering based on patient compartment membership.
    return filtered.map(
        (resourceType, rowDataset) -> {
          final org.apache.spark.sql.Column patientFilter =
              patientCompartmentService.buildPatientFilter(resourceType, patientIds);
          log.debug(
              "Applying patient compartment filter for resource type {}: {}",
              resourceType,
              patientFilter);
          return rowDataset.filter(patientFilter);
        });
  }

  /**
   * Returns the result streaming helper for use by other components.
   *
   * @return the result streaming helper
   */
  @Nonnull
  public ResultStreamingHelper getStreamingHelper() {
    return streamingHelper;
  }
}
