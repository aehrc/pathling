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

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.view.ViewExecutionHelper;
import au.csiro.pathling.operations.view.ViewExportFormat;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import au.csiro.pathling.views.ViewDefinitionGson;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Parses the raw {@code $sqlquery-export} kick-off inputs into a validated {@link
 * SqlQueryExportRequest}. Performs every check that does not require executing the queries: source
 * rejection, strict {@code _format} parsing, per-{@code query} and per-{@code view} exclusivity,
 * query Library resolution, request-supplied view resolution and semantic validation, parameter
 * binding, and static SQL validation. Each query is prepared (parsed, bound, view-resolved) via the
 * shared {@link SqlQueryPipeline} so that the export and run operations share identical semantics.
 *
 * @author John Grimes
 */
@Component
public class SqlQueryExportRequestParser {

  @Nonnull private final SqlQueryPipeline pipeline;

  @Nonnull private final LibraryReferenceResolver libraryReferenceResolver;

  @Nonnull private final ViewExecutionHelper viewExecutionHelper;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final Gson gson;

  /**
   * Constructs a new SqlQueryExportRequestParser.
   *
   * @param pipeline the shared SQL query pipeline (prepare and static validation)
   * @param libraryReferenceResolver resolves a {@code queryReference} to a stored SQLQuery Library
   * @param viewExecutionHelper resolves a {@code view.viewReference} to a stored ViewDefinition,
   *     reusing the per-part exclusivity and reference-resolution semantics of the view operations
   * @param fhirContext the FHIR context, used to serialise supplied ViewDefinitions for parsing
   * @param serverConfiguration the server configuration (auth toggle and query config)
   * @param deltaLake the data source used to semantically validate supplied ViewDefinitions
   */
  @SuppressWarnings("java:S107")
  @Autowired
  public SqlQueryExportRequestParser(
      @Nonnull final SqlQueryPipeline pipeline,
      @Nonnull final LibraryReferenceResolver libraryReferenceResolver,
      @Nonnull final ViewExecutionHelper viewExecutionHelper,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final QueryableDataSource deltaLake) {
    this.pipeline = pipeline;
    this.libraryReferenceResolver = libraryReferenceResolver;
    this.viewExecutionHelper = viewExecutionHelper;
    this.fhirContext = fhirContext;
    this.serverConfiguration = serverConfiguration;
    this.deltaLake = deltaLake;
    this.gson = ViewDefinitionGson.create();
  }

  /**
   * Parses and validates the kick-off request.
   *
   * @param requestDetails the servlet request details (for the raw Parameters body and URLs)
   * @param boundLibrary the bound Library at instance level, or null at system/type level
   * @param format the explicit {@code _format} parameter, if any
   * @param includeHeader whether to include a CSV header row; {@code null} defaults to {@code true}
   * @param clientTrackingId optional client-provided tracking identifier
   * @param patientIds patient ids to filter by, resolved from {@code patient} and {@code group}
   * @param since the {@code _since} filter, if any
   * @param source the unsupported {@code source} parameter, rejected when supplied
   * @return the validated request
   * @throws InvalidRequestException (400) for statically detectable structural failures
   */
  @Nonnull
  @SuppressWarnings("java:S107")
  public SqlQueryExportRequest parse(
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final IBaseResource boundLibrary,
      @Nullable final String format,
      @Nullable final BooleanType includeHeader,
      @Nullable final String clientTrackingId,
      @Nonnull final Set<String> patientIds,
      @Nullable final InstantType since,
      @Nullable final String source) {

    // Reject the unsupported source parameter synchronously, before any other work.
    if (source != null && !source.isBlank()) {
      throw new InvalidRequestException(
          "The 'source' parameter (external data source) is not supported by this server.");
    }

    // Parse the explicit _format strictly, so an unsupported value (e.g. json, fhir) is rejected at
    // kick-off regardless of the query parameters.
    final ViewExportFormat exportFormat = ViewExportFormat.fromString(format);

    final Parameters parameters = extractParameters(requestDetails);

    // Resolve request-supplied views (system/type level only); the bound-Library instance level
    // resolves its views from server storage.
    final Map<String, FhirView> suppliedViews =
        boundLibrary == null ? resolveSuppliedViews(parameters) : Map.of();

    final List<QueryInput> queries = new ArrayList<>();
    if (boundLibrary != null) {
      // Instance level: the bound Library is the single query source; the query parameter does not
      // apply and per-query parameter binding is not offered.
      queries.add(prepareQuery(null, boundLibrary, null, suppliedViews));
    } else {
      for (final RawQuery rawQuery : extractQueries(parameters)) {
        final IBaseResource library =
            selectLibrary(rawQuery.queryResource(), rawQuery.queryReference());
        queries.add(prepareQuery(rawQuery.name(), library, rawQuery.parameters(), suppliedViews));
      }
      if (queries.isEmpty()) {
        throw new InvalidRequestException(
            "At least one 'query' parameter is required at the system and type levels.");
      }
    }

    final boolean header = includeHeader == null || includeHeader.booleanValue();

    return new SqlQueryExportRequest(
        requestDetails.getCompleteUrl(),
        requestDetails.getFhirServerBase(),
        queries,
        clientTrackingId,
        exportFormat,
        header,
        patientIds,
        since);
  }

  /**
   * Prepares a single query: resolves its Library name fallback, then parses, binds parameters, and
   * resolves views via the shared pipeline, and statically validates the SQL.
   */
  @Nonnull
  private QueryInput prepareQuery(
      @Nullable final String name,
      @Nonnull final IBaseResource library,
      @Nullable final Parameters parameters,
      @Nonnull final Map<String, FhirView> suppliedViews) {
    final PreparedSqlQuery prepared =
        pipeline.prepare(library, null, null, null, null, parameters, suppliedViews);
    pipeline.validateStatically(prepared);
    return new QueryInput(name, libraryName(library), prepared);
  }

  /** Returns the SQLQuery Library's {@code name} element, or null when not a named Library. */
  @Nullable
  private static String libraryName(@Nonnull final IBaseResource library) {
    return library instanceof final Library lib && lib.hasName() ? lib.getName() : null;
  }

  /**
   * Enforces the "exactly one of queryResource / queryReference" contract and resolves the Library.
   */
  @Nonnull
  private IBaseResource selectLibrary(
      @Nullable final IBaseResource queryResource, @Nullable final Reference queryReference) {
    final boolean hasResource = queryResource != null;
    final boolean hasReference = queryReference != null && !queryReference.isEmpty();

    if (hasResource && hasReference) {
      throw new InvalidRequestException(
          "Each 'query' must supply exactly one of 'queryResource' and 'queryReference', not"
              + " both.");
    }
    if (!hasResource && !hasReference) {
      throw new InvalidRequestException(
          "Each 'query' must supply one of 'queryResource' or 'queryReference'.");
    }
    return hasResource ? queryResource : libraryReferenceResolver.resolve(queryReference);
  }

  /**
   * Resolves the {@code view} parts into a map keyed by the ViewDefinition id they satisfy, parsing
   * inline views, reading referenced views, applying the per-resource READ check to stored views,
   * and semantically validating each supplied view (a malformed view is a 400; a semantically
   * invalid one a 422).
   */
  @Nonnull
  private Map<String, FhirView> resolveSuppliedViews(@Nonnull final Parameters parameters) {
    final Map<String, FhirView> resolved = new LinkedHashMap<>();
    for (final ParametersParameterComponent param : parameters.getParameter()) {
      if (!"view".equals(param.getName())) {
        continue;
      }
      IBaseResource viewResource = null;
      Reference viewReference = null;
      for (final ParametersParameterComponent part : param.getPart()) {
        if ("viewResource".equals(part.getName()) && part.getResource() != null) {
          viewResource = part.getResource();
        } else if ("viewReference".equals(part.getName())
            && part.getValue() instanceof final Reference reference) {
          viewReference = reference;
        }
      }

      // resolveViewInput enforces per-part exclusivity (400), presence (400), and reference
      // resolution (404), and returns the resolved ViewDefinition resource.
      final boolean inline = viewResource != null;
      final IBaseResource resolvedResource =
          viewExecutionHelper.resolveViewInput(viewResource, viewReference);
      final FhirView view = parseViewDefinition(resolvedResource);

      // A stored ViewDefinition is subject to the per-resource READ check; an inline view carries
      // its own content and is authorised as the request payload.
      if (!inline && serverConfiguration.getAuth().isEnabled()) {
        SecurityAspect.checkHasAuthority(
            PathlingAuthority.resourceAccess(AccessType.READ, view.getResource()));
      }

      validateViewSemantically(view);

      final String matchKey = resolvedResource.getIdElement().getIdPart();
      if (matchKey != null && !matchKey.isBlank()) {
        resolved.put(matchKey, view);
      }
    }
    return resolved;
  }

  /** Parses a ViewDefinition resource into a FhirView via JSON round-tripping. */
  @Nonnull
  private FhirView parseViewDefinition(@Nonnull final IBaseResource viewResource) {
    try {
      final String viewJson = fhirContext.newJsonParser().encodeResourceToString(viewResource);
      return gson.fromJson(viewJson, FhirView.class);
    } catch (final JsonSyntaxException e) {
      throw new InvalidRequestException("Invalid ViewDefinition: " + e.getMessage());
    }
  }

  /**
   * Semantically validates a supplied ViewDefinition by building its query plan, consistent with
   * the view operations: a semantically invalid view is a 422, an unsupported expression a 400.
   */
  private void validateViewSemantically(@Nonnull final FhirView view) {
    try {
      new FhirViewExecutor(fhirContext, deltaLake, serverConfiguration.getQuery()).buildQuery(view);
    } catch (final ConstraintViolationException e) {
      throw new UnprocessableEntityException("Invalid ViewDefinition: " + e.getMessage());
    } catch (final UnsupportedOperationException | UnsupportedFhirPathFeatureError e) {
      throw new InvalidRequestException("Unsupported expression: " + e.getMessage());
    }
  }

  /** Extracts the raw {@code query} parts from the request Parameters, preserving order. */
  @Nonnull
  private List<RawQuery> extractQueries(@Nonnull final Parameters parameters) {
    final List<RawQuery> queries = new ArrayList<>();
    for (final ParametersParameterComponent param : parameters.getParameter()) {
      if (!"query".equals(param.getName())) {
        continue;
      }
      String name = null;
      IBaseResource queryResource = null;
      Reference queryReference = null;
      Parameters queryParameters = null;
      for (final ParametersParameterComponent part : param.getPart()) {
        switch (part.getName()) {
          case "name" -> name = part.getValue() != null ? part.getValue().primitiveValue() : null;
          case "queryResource" -> queryResource = part.getResource();
          case "queryReference" -> {
            if (part.getValue() instanceof final Reference reference) {
              queryReference = reference;
            }
          }
          case "parameters" -> {
            if (part.getResource() instanceof final Parameters params) {
              queryParameters = params;
            }
          }
          default -> {
            // Ignore unrecognised parts.
          }
        }
      }
      queries.add(new RawQuery(name, queryResource, queryReference, queryParameters));
    }
    return queries;
  }

  /** Extracts the Parameters resource from the request body, or an empty one when absent. */
  @Nonnull
  private static Parameters extractParameters(@Nonnull final ServletRequestDetails requestDetails) {
    return requestDetails.getResource() instanceof final Parameters parameters
        ? parameters
        : new Parameters();
  }

  /** A raw, unresolved {@code query} part extracted from the request Parameters. */
  private record RawQuery(
      @Nullable String name,
      @Nullable IBaseResource queryResource,
      @Nullable Reference queryReference,
      @Nullable Parameters parameters) {}
}
