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

import static au.csiro.pathling.operations.sql.SubjectResolver.SUBJECT_EXPRESSION;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.export.ExportDataSourceBuilder;
import au.csiro.pathling.operations.sqlquery.PreparedSqlQuery;
import au.csiro.pathling.operations.sqlquery.SqlQueryOutputFormat;
import au.csiro.pathling.operations.sqlquery.SqlQueryPipeline;
import au.csiro.pathling.operations.sqlquery.SqlQueryResultStreamer;
import au.csiro.pathling.operations.view.ViewExecutionHelper;
import au.csiro.pathling.operations.view.ViewOutputFormat;
import au.csiro.pathling.security.OperationAccess;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.api.RequestTypeEnum;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provider for the system-level {@code $sql-run} operation from the SQL on FHIR specification: the
 * synchronous execution of a single subject, which may be a {@code ViewDefinition}, a {@code
 * SQLQuery} {@code Library} or a {@code SQLView} {@code Library}.
 *
 * <p>The subject decides how the request is processed. A ViewDefinition goes to the FhirView
 * evaluation engine and may be run over data supplied inline with the request; a SQL Library goes
 * to the SQL pipeline and may carry parameter bindings. The conditional parameter rules, the
 * available output formats, and the streaming path all follow from the resolved kind.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhir/uv/sql-on-fhir/OperationDefinition/SQLRun">SQLRun</a>
 */
@Slf4j
@Component
public class SqlRunProvider {

  /** Parameter names that carry a resource, and so cannot be supplied over GET. */
  private static final Set<String> RESOURCE_CARRYING_PARAMETERS =
      Set.of("subjectResource", "parameters", "context", "resource");

  @Nonnull private final SubjectResolver subjectResolver;

  @Nonnull private final SqlFilterResolver filterResolver;

  @Nonnull private final ContextArtefactParser contextParser;

  @Nonnull private final ViewExecutionHelper viewExecutionHelper;

  @Nonnull private final SqlQueryPipeline pipeline;

  @Nonnull private final SqlQueryResultStreamer streamer;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final ExportDataSourceBuilder dataSourceBuilder;

  /**
   * Constructs a new SqlRunProvider.
   *
   * @param subjectResolver resolves the subject and detects its kind
   * @param filterResolver resolves the patient, group and _since filters
   * @param contextParser parses the inline supporting artefacts
   * @param viewExecutionHelper the ViewDefinition evaluation engine
   * @param pipeline the SQL evaluation engine
   * @param streamer streams a SQL result in the requested format
   * @param deltaLake the server's data source
   * @param dataSourceBuilder applies the request's filters to the data source
   */
  @SuppressWarnings("java:S107")
  @Autowired
  public SqlRunProvider(
      @Nonnull final SubjectResolver subjectResolver,
      @Nonnull final SqlFilterResolver filterResolver,
      @Nonnull final ContextArtefactParser contextParser,
      @Nonnull final ViewExecutionHelper viewExecutionHelper,
      @Nonnull final SqlQueryPipeline pipeline,
      @Nonnull final SqlQueryResultStreamer streamer,
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final ExportDataSourceBuilder dataSourceBuilder) {
    this.subjectResolver = subjectResolver;
    this.filterResolver = filterResolver;
    this.contextParser = contextParser;
    this.viewExecutionHelper = viewExecutionHelper;
    this.pipeline = pipeline;
    this.streamer = streamer;
    this.deltaLake = deltaLake;
    this.dataSourceBuilder = dataSourceBuilder;
  }

  /**
   * Executes a single subject and streams the result in the negotiated format.
   *
   * <p>Declared idempotent so that HAPI accepts both GET and POST. A GET carrying a resource-valued
   * parameter is rejected explicitly, since the parameter cannot be expressed in a query string and
   * silently ignoring it would run a different request than the client asked for.
   *
   * @param subjectCanonical the subject's canonical URL, honouring a {@code |version} pin
   * @param subjectReference a relative reference to a stored subject
   * @param subjectResource an inline subject
   * @param parameters runtime parameter bindings, for SQL subjects only
   * @param context inline supporting artefacts for dependencies the server cannot resolve
   * @param inlineResources FHIR resources to project instead of server data, for ViewDefinition
   *     subjects only
   * @param format the output format, taking precedence over the Accept header
   * @param includeHeader whether to include a header row in CSV output
   * @param patient patient references restricting the resources fed to the subject
   * @param group group references restricting the resources fed to the subject
   * @param since restricts to resources updated at or after this instant
   * @param source the unsupported external data source parameter, rejected when supplied
   * @param limit the maximum number of rows to return, applied after evaluation
   * @param requestDetails the servlet request details
   * @param response the HTTP response to stream to
   */
  @SuppressWarnings("java:S107")
  @Operation(name = "sql-run", idempotent = true, manualResponse = true)
  @OperationAccess("sql-run")
  public void run(
      @Nullable @OperationParam(name = "subjectCanonical") final String subjectCanonical,
      @Nullable @OperationParam(name = "subjectReference") final Reference subjectReference,
      @Nullable @OperationParam(name = "subjectResource") final IBaseResource subjectResource,
      @Nullable @OperationParam(name = "parameters") final Parameters parameters,
      @Nullable @OperationParam(name = "context", max = OperationParam.MAX_UNLIMITED)
          final List<IBaseResource> context,
      @Nullable @OperationParam(name = "resource", max = OperationParam.MAX_UNLIMITED)
          final List<String> inlineResources,
      @Nullable @OperationParam(name = "_format") final String format,
      @Nullable @OperationParam(name = "header") final BooleanType includeHeader,
      @Nullable @OperationParam(name = "patient", max = OperationParam.MAX_UNLIMITED)
          final List<Reference> patient,
      @Nullable @OperationParam(name = "group", max = OperationParam.MAX_UNLIMITED)
          final List<Reference> group,
      @Nullable @OperationParam(name = "_since") final InstantType since,
      @Nullable @OperationParam(name = "source") final String source,
      @Nullable @OperationParam(name = "_limit") final IntegerType limit,
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final HttpServletResponse response) {

    if (response == null) {
      throw new InvalidRequestException("An HTTP response is required for this operation.");
    }

    rejectResourceParametersOverGet(requestDetails);
    rejectSource(source);

    // Filters are resolved first and their issues carried, so that a request with both an
    // unresolvable subject and an unresolvable filter reports both problems in one outcome.
    final ResolvedFilters filters = filterResolver.resolve(patient, group, since);
    final ResolvedSubject subject =
        resolveSubject(subjectCanonical, subjectReference, subjectResource, filters);
    if (filters.hasIssues()) {
      throw SqlOperationError.of(HttpServletResponse.SC_BAD_REQUEST, filters.issues());
    }

    rejectInapplicableParameters(subject, parameters, inlineResources);

    final String acceptHeader = requestDetails.getServletRequest().getHeader("Accept");
    final SqlRunFormat outputFormat = SqlRunFormat.select(format, acceptHeader, subject.getKind());
    final boolean header = includeHeader == null || includeHeader.booleanValue();
    final SuppliedArtefacts supplied = contextParser.parse(context);

    if (subject.getKind() == SubjectKind.VIEW_DEFINITION) {
      runViewDefinition(
          subject, supplied, inlineResources, filters, outputFormat, header, limit, response);
    } else {
      runSqlLibrary(
          subject,
          supplied,
          parameters,
          filters,
          outputFormat,
          header,
          limit,
          requestDetails,
          response);
    }
  }

  /** Executes a ViewDefinition subject through the FhirView evaluation engine. */
  @SuppressWarnings("java:S107")
  private void runViewDefinition(
      @Nonnull final ResolvedSubject subject,
      @Nonnull final SuppliedArtefacts supplied,
      @Nullable final List<String> inlineResources,
      @Nonnull final ResolvedFilters filters,
      @Nonnull final SqlRunFormat outputFormat,
      final boolean header,
      @Nullable final IntegerType limit,
      @Nonnull final HttpServletResponse response) {

    // A ViewDefinition declares no dependencies, so any supplied context entry matches nothing.
    supplied.checkAllMatched();

    final FhirView view = viewExecutionHelper.parseViewDefinition(subject.getResource());
    final DataSource dataSource =
        inlineResources == null || inlineResources.isEmpty()
            ? filteredSource(filters)
            : viewExecutionHelper.inlineDataSource(inlineResources);

    viewExecutionHelper.streamView(
        view, dataSource, toViewOutputFormat(outputFormat), header, limit, response);
  }

  /** Executes a SQLQuery or SQLView subject through the SQL evaluation engine. */
  @SuppressWarnings("java:S107")
  private void runSqlLibrary(
      @Nonnull final ResolvedSubject subject,
      @Nonnull final SuppliedArtefacts supplied,
      @Nullable final Parameters parameters,
      @Nonnull final ResolvedFilters filters,
      @Nonnull final SqlRunFormat outputFormat,
      final boolean header,
      @Nullable final IntegerType limit,
      @Nonnull final ServletRequestDetails requestDetails,
      @Nonnull final HttpServletResponse response) {

    final PreparedSqlQuery prepared =
        pipeline.prepare(
            subject.asLibrary(),
            outputFormat.getCode(),
            null,
            new BooleanType(header),
            limit,
            parameters,
            supplied);
    // Unmatched entries are detected only once the whole graph has been traversed, since an entry
    // may be reached through another supplied entry.
    supplied.checkAllMatched();

    pipeline.execute(
        prepared,
        filteredSource(filters),
        requestDetails.getRequestId(),
        result -> streamer.stream(result, toSqlQueryOutputFormat(outputFormat), header, response));
  }

  /** Applies the request's filters to the server's data source. */
  @Nonnull
  private QueryableDataSource filteredSource(@Nonnull final ResolvedFilters filters) {
    return dataSourceBuilder.build(deltaLake, filters.since(), filters.patientIds());
  }

  /**
   * Resolves the subject, folding any filter issues into the failure so that a request carrying
   * both an unresolvable subject and an unresolvable filter reports them together. The subject
   * failure is the more fundamental of the two, so it decides the status code.
   */
  @Nonnull
  private ResolvedSubject resolveSubject(
      @Nullable final String subjectCanonical,
      @Nullable final Reference subjectReference,
      @Nullable final IBaseResource subjectResource,
      @Nonnull final ResolvedFilters filters) {
    try {
      return subjectResolver.resolve(subjectCanonical, subjectReference, subjectResource, null);
    } catch (final BaseServerResponseException e) {
      if (!filters.hasIssues()) {
        throw e;
      }
      final List<OperationOutcomeIssueComponent> combined =
          new ArrayList<>(SqlOperationOutcomes.issuesOf(e, SUBJECT_EXPRESSION));
      combined.addAll(filters.issues());
      throw SqlOperationError.of(e.getStatusCode(), combined);
    }
  }

  /**
   * Enforces the conditional parameter rules, which depend on what the subject resolved to: {@code
   * parameters} binds values declared by a SQL Library and means nothing for a view, while {@code
   * resource} supplies data for a view to project and means nothing for a SQL subject.
   */
  private static void rejectInapplicableParameters(
      @Nonnull final ResolvedSubject subject,
      @Nullable final Parameters parameters,
      @Nullable final List<String> inlineResources) {
    if (subject.getKind() == SubjectKind.VIEW_DEFINITION && parameters != null) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          "parameters",
          "The 'parameters' parameter applies only to a SQLQuery or SQLView subject; a"
              + " ViewDefinition declares no parameters.");
    }
    if (subject.getKind().isSql() && inlineResources != null && !inlineResources.isEmpty()) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          "resource",
          "The 'resource' parameter applies only to a ViewDefinition subject; a SQL subject reads"
              + " through its declared dependencies.");
    }
  }

  /**
   * Rejects a resource-carrying parameter supplied over GET. HAPI cannot bind such a parameter from
   * a query string, so without this check it would be silently dropped.
   */
  private static void rejectResourceParametersOverGet(
      @Nonnull final ServletRequestDetails requestDetails) {
    if (requestDetails.getRequestType() != RequestTypeEnum.GET) {
      return;
    }
    for (final String name : requestDetails.getParameters().keySet()) {
      if (RESOURCE_CARRYING_PARAMETERS.contains(name)) {
        throw SqlOperationError.badRequest(
            IssueType.INVALID,
            name,
            "The '%s' parameter carries a resource and cannot be supplied over GET; use POST."
                .formatted(name));
      }
    }
  }

  /**
   * Rejects the unsupported {@code source} parameter. Pathling does not implement external data
   * sources, so a supplied value is rejected rather than silently ignored, which would mislead the
   * client about the data that was queried.
   */
  private static void rejectSource(@Nullable final String source) {
    if (source != null && !source.isBlank()) {
      throw SqlOperationError.badRequest(
          IssueType.NOTSUPPORTED,
          "source",
          "The 'source' parameter (external data source) is not supported by this server.");
    }
  }

  /** Maps a run format to the equivalent format on the ViewDefinition streaming path. */
  @Nonnull
  private static ViewOutputFormat toViewOutputFormat(@Nonnull final SqlRunFormat format) {
    return ViewOutputFormat.fromStringStrict(format.getCode());
  }

  /** Maps a run format to the equivalent format on the SQL streaming path. */
  @Nonnull
  private static SqlQueryOutputFormat toSqlQueryOutputFormat(@Nonnull final SqlRunFormat format) {
    return SqlQueryOutputFormat.fromStringStrict(format.getCode());
  }
}
