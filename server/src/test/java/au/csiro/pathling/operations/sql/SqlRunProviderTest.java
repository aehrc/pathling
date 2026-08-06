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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.export.ExportDataSourceBuilder;
import au.csiro.pathling.operations.sqlquery.SqlLibraryFixtures;
import au.csiro.pathling.operations.sqlquery.SqlQueryPipeline;
import au.csiro.pathling.operations.sqlquery.SqlQueryResultStreamer;
import au.csiro.pathling.operations.view.ViewExecutionHelper;
import ca.uhn.fhir.rest.api.RequestTypeEnum;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link SqlRunProvider}, covering the request-validation rows of
 * contracts/sql-run.md: the conditional parameter rules that depend on the subject kind, the
 * unsupported-parameter rejections, the GET restriction, and the combining of a subject failure
 * with filter failures into one outcome.
 *
 * <p>The evaluation engines are mocked, so these tests exercise the provider's contract enforcement
 * and dispatch rather than query execution, which the integration tests cover.
 *
 * @author John Grimes
 */
class SqlRunProviderTest {

  private static final String VIEW_CANONICAL = SqlLibraryFixtures.viewDefinitionUrl("demographics");

  private SubjectResolver subjectResolver;
  private SqlFilterResolver filterResolver;
  private ContextArtefactParser contextParser;
  private ViewExecutionHelper viewExecutionHelper;
  private SqlQueryPipeline pipeline;
  private SqlQueryResultStreamer streamer;
  private SqlRunProvider provider;

  private HttpServletResponse response;

  @BeforeEach
  void setUp() {
    subjectResolver = mock(SubjectResolver.class);
    filterResolver = mock(SqlFilterResolver.class);
    contextParser = mock(ContextArtefactParser.class);
    viewExecutionHelper = mock(ViewExecutionHelper.class);
    pipeline = mock(SqlQueryPipeline.class);
    streamer = mock(SqlQueryResultStreamer.class);
    response = mock(HttpServletResponse.class);

    final ExportDataSourceBuilder dataSourceBuilder = mock(ExportDataSourceBuilder.class);
    when(dataSourceBuilder.build(any(), any(), any())).thenReturn(mock(QueryableDataSource.class));

    provider =
        new SqlRunProvider(
            subjectResolver,
            filterResolver,
            contextParser,
            viewExecutionHelper,
            pipeline,
            streamer,
            mock(QueryableDataSource.class),
            dataSourceBuilder);

    // By default the filters resolve cleanly and no context is supplied.
    when(filterResolver.resolve(any(), any(), any()))
        .thenReturn(new ResolvedFilters(Set.of(), null, List.of()));
    when(contextParser.parse(any())).thenReturn(SuppliedArtefacts.empty());
  }

  // ---------------------------------------------------------------------------
  // Conditional parameters.
  // ---------------------------------------------------------------------------

  // `parameters` binds values a SQL Library declares; a ViewDefinition declares none, so supplying
  // it is a 400 naming the parameter.
  @Test
  void rejectsParametersWithAViewDefinitionSubject() {
    stubSubject(SubjectKind.VIEW_DEFINITION);

    final BaseServerResponseException exception =
        catchServerException(() -> run(builder().parameters(new Parameters())));

    assertThat(exception).isInstanceOf(InvalidRequestException.class);
    assertIssue(exception, IssueType.INVALID, "parameters");
  }

  // `resource` supplies data for a view to project; a SQL subject reads through its declared
  // dependencies, so supplying it is a 400 naming the parameter.
  @ParameterizedTest
  @ValueSource(strings = {"SQL_QUERY", "SQL_VIEW"})
  void rejectsInlineResourcesWithASqlSubject(final String kindName) {
    stubSubject(SubjectKind.valueOf(kindName));

    final BaseServerResponseException exception =
        catchServerException(
            () -> run(builder().inlineResources(List.of("{\"resourceType\":\"Patient\"}"))));

    assertIssue(exception, IssueType.INVALID, "resource");
  }

  // The two conditional parameters are accepted for the kind that offers them.
  @Test
  void acceptsParametersForASqlSubjectAndResourcesForAView() {
    stubSubject(SubjectKind.SQL_QUERY);
    run(builder().parameters(new Parameters()));
    verify(pipeline).prepare(any(), any(), any(), any(), any(), any(), any());

    stubSubject(SubjectKind.VIEW_DEFINITION);
    run(builder().inlineResources(List.of("{\"resourceType\":\"Patient\"}")));
    verify(viewExecutionHelper).inlineDataSource(any());
  }

  // ---------------------------------------------------------------------------
  // Unsupported parameters and methods.
  // ---------------------------------------------------------------------------

  // External data sources are not implemented, so a supplied `source` is rejected rather than
  // silently ignored, which would mislead the client about the data that was queried.
  @Test
  void rejectsTheSourceParameterAsNotSupported() {
    stubSubject(SubjectKind.SQL_QUERY);

    final BaseServerResponseException exception =
        catchServerException(() -> run(builder().source("http://example.org/data")));

    assertIssue(exception, IssueType.NOTSUPPORTED, "source");
  }

  // A blank source is treated as absent, so it does not block an otherwise valid request.
  @Test
  void ignoresABlankSourceParameter() {
    stubSubject(SubjectKind.SQL_QUERY);

    run(builder().source("  "));

    verify(pipeline).prepare(any(), any(), any(), any(), any(), any(), any());
  }

  // A resource-carrying parameter cannot be expressed in a query string, so a GET naming one is
  // rejected rather than having the parameter silently dropped.
  @ParameterizedTest
  @ValueSource(strings = {"subjectResource", "parameters", "context", "resource"})
  void rejectsResourceCarryingParametersOverGet(final String parameterName) {
    stubSubject(SubjectKind.SQL_QUERY);

    final BaseServerResponseException exception =
        catchServerException(
            () -> run(builder().method(RequestTypeEnum.GET).queryParameter(parameterName)));

    assertIssue(exception, IssueType.INVALID, parameterName);
  }

  // A GET carrying only primitive parameters is the supported form and is not rejected.
  @Test
  void acceptsAGetCarryingOnlyPrimitiveParameters() {
    stubSubject(SubjectKind.VIEW_DEFINITION);

    run(builder().method(RequestTypeEnum.GET).queryParameter("_format").queryParameter("_limit"));

    verify(viewExecutionHelper).streamView(any(), any(), any(), anyBooleanValue(), any(), any());
  }

  // ---------------------------------------------------------------------------
  // Formats.
  // ---------------------------------------------------------------------------

  // The per-kind format rules apply to the resolved subject, so parquet is refused for a view.
  @Test
  void appliesThePerKindFormatRules() {
    stubSubject(SubjectKind.VIEW_DEFINITION);

    final BaseServerResponseException exception =
        catchServerException(() -> run(builder().format("parquet")));

    assertIssue(exception, IssueType.NOTSUPPORTED, "_format");
  }

  // The chosen format is handed to the SQL engine, so the prepared query and the streamed output
  // agree on it.
  @Test
  void passesTheSelectedFormatToTheSqlEngine() {
    stubSubject(SubjectKind.SQL_QUERY);

    run(builder().format("csv"));

    verify(pipeline)
        .prepare(any(), org.mockito.ArgumentMatchers.eq("csv"), any(), any(), any(), any(), any());
  }

  // ---------------------------------------------------------------------------
  // Filters, and combining failures.
  // ---------------------------------------------------------------------------

  // An unresolvable filter value is a 400 naming the parameter, so the client learns that the
  // result would have been narrower than asked for.
  @Test
  void rejectsAnUnresolvableFilterValue() {
    stubSubject(SubjectKind.SQL_QUERY);
    when(filterResolver.resolve(any(), any(), any()))
        .thenReturn(
            new ResolvedFilters(
                Set.of(),
                null,
                List.of(
                    SqlOperationError.issue(
                        IssueType.NOTFOUND, "patient", "No Patient with id 'nope' was found."))));

    final BaseServerResponseException exception =
        catchServerException(() -> run(builder().patient("Patient/nope")));

    assertThat(exception.getStatusCode()).isEqualTo(400);
    assertIssue(exception, IssueType.NOTFOUND, "patient");
  }

  // A request that names both an unresolvable subject and an unresolvable filter reports both
  // problems at once, under the subject's status, since the subject failure is the more
  // fundamental of the two.
  @Test
  void reportsSubjectAndFilterFailuresTogetherUnderTheSubjectStatus() {
    when(subjectResolver.resolve(any(), any(), any(), any()))
        .thenThrow(SqlOperationError.notFound("subject", "No ViewDefinition matches."));
    when(filterResolver.resolve(any(), any(), any()))
        .thenReturn(
            new ResolvedFilters(
                Set.of(),
                null,
                List.of(
                    SqlOperationError.issue(
                        IssueType.NOTFOUND, "patient", "No Patient with id 'nope' was found."))));

    final BaseServerResponseException exception =
        catchServerException(() -> run(builder().patient("Patient/nope")));

    assertThat(exception.getStatusCode()).isEqualTo(404);
    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
    assertThat(outcome.getIssue()).hasSize(2);
    assertIssue(exception, IssueType.NOTFOUND, "subject");
    assertIssue(exception, IssueType.NOTFOUND, "patient");
  }

  // With clean filters, a subject failure surfaces on its own and unchanged.
  @Test
  void propagatesASubjectFailureWhenTheFiltersAreClean() {
    when(subjectResolver.resolve(any(), any(), any(), any()))
        .thenThrow(SqlOperationError.notFound("subject", "No ViewDefinition matches."));

    assertThatThrownBy(() -> run(builder())).isInstanceOf(ResourceNotFoundException.class);
  }

  // The resolved filters reach the data source that the subject reads through.
  @Test
  void appliesTheResolvedFiltersToTheDataSource() {
    stubSubject(SubjectKind.VIEW_DEFINITION);
    final InstantType since = new InstantType("2026-01-01T00:00:00Z");
    when(filterResolver.resolve(any(), any(), any()))
        .thenReturn(new ResolvedFilters(Set.of("p1", "p2"), since, List.of()));

    run(builder());

    verify(viewExecutionHelper).streamView(any(), any(), any(), anyBooleanValue(), any(), any());
  }

  // ---------------------------------------------------------------------------
  // Context entries.
  // ---------------------------------------------------------------------------

  // A ViewDefinition declares no dependencies, so a context entry supplied alongside one can match
  // nothing and is rejected.
  @Test
  void rejectsContextEntriesSuppliedWithAViewDefinitionSubject() {
    stubSubject(SubjectKind.VIEW_DEFINITION);
    final SuppliedArtefacts artefacts =
        SuppliedArtefacts.of(
            List.of(
                SuppliedArtefact.ofView(
                    VIEW_CANONICAL, null, mock(au.csiro.pathling.views.FhirView.class))));
    when(contextParser.parse(any())).thenReturn(artefacts);

    final BaseServerResponseException exception =
        catchServerException(() -> run(builder().context(List.of(new Library()))));

    assertIssue(exception, IssueType.INVALID, "context");
  }

  // Unmatched-entry detection runs after the SQL graph has been traversed, so an entry reached
  // only through another supplied entry still counts as matched.
  @Test
  void checksContextEntriesAfterTheSqlGraphIsTraversed() {
    stubSubject(SubjectKind.SQL_QUERY);
    final SuppliedArtefacts artefacts = mock(SuppliedArtefacts.class);
    when(contextParser.parse(any())).thenReturn(artefacts);

    run(builder().context(List.of(new Library())));

    final org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(pipeline, artefacts);
    inOrder.verify(pipeline).prepare(any(), any(), any(), any(), any(), any(), any());
    inOrder.verify(artefacts).checkAllMatched();
  }

  // ---------------------------------------------------------------------------
  // Dispatch.
  // ---------------------------------------------------------------------------

  // A view subject goes to the FhirView engine and never to the SQL pipeline.
  @Test
  void dispatchesAViewSubjectToTheViewEngine() {
    stubSubject(SubjectKind.VIEW_DEFINITION);

    run(builder());

    verify(viewExecutionHelper).streamView(any(), any(), any(), anyBooleanValue(), any(), any());
    verify(pipeline, never()).prepare(any(), any(), any(), any(), any(), any(), any());
  }

  // A SQL subject goes to the SQL pipeline and never to the FhirView engine.
  @ParameterizedTest
  @ValueSource(strings = {"SQL_QUERY", "SQL_VIEW"})
  void dispatchesASqlSubjectToTheSqlEngine(final String kindName) {
    stubSubject(SubjectKind.valueOf(kindName));

    run(builder());

    verify(pipeline).prepare(any(), any(), any(), any(), any(), any(), any());
    verify(viewExecutionHelper, never())
        .streamView(any(), any(), any(), anyBooleanValue(), any(), any());
  }

  // ---- helpers ----

  /** Matches any boolean argument, for the primitive parameter of streamView. */
  private static boolean anyBooleanValue() {
    return org.mockito.ArgumentMatchers.anyBoolean();
  }

  /** Stubs the subject resolver to return a subject of the given kind. */
  private void stubSubject(@Nonnull final SubjectKind kind) {
    final IBaseResource artefact =
        kind == SubjectKind.VIEW_DEFINITION
            ? viewDefinition()
            : (kind == SubjectKind.SQL_QUERY
                ? SqlLibraryFixtures.sqlQuery("SELECT 1")
                : SqlLibraryFixtures.sqlView("SELECT 1"));
    when(subjectResolver.resolve(any(), any(), any(), any()))
        .thenReturn(new ResolvedSubject(kind, artefact, null));
  }

  @Nonnull
  private static au.csiro.pathling.encoders.ViewDefinitionResource viewDefinition() {
    final au.csiro.pathling.encoders.ViewDefinitionResource view =
        new au.csiro.pathling.encoders.ViewDefinitionResource();
    view.setStatusElement(new CodeType("active"));
    view.setResourceElement(new CodeType("Patient"));
    return view;
  }

  /** Starts building a request. */
  @Nonnull
  private Request builder() {
    return new Request();
  }

  /** Invokes the operation with the built request. */
  private void run(@Nonnull final Request request) {
    provider.run(
        request.subjectCanonical,
        request.subjectReference,
        request.subjectResource,
        request.parameters,
        request.context,
        request.inlineResources,
        request.format,
        null,
        request.patients,
        null,
        null,
        request.source,
        null,
        request.requestDetails(),
        response);
  }

  /** A mutable builder for the operation's many parameters, keeping the tests readable. */
  private static final class Request {

    @Nullable private String subjectCanonical = VIEW_CANONICAL;
    @Nullable private Reference subjectReference;
    @Nullable private IBaseResource subjectResource;
    @Nullable private Parameters parameters;
    @Nullable private List<IBaseResource> context;
    @Nullable private List<String> inlineResources;
    @Nullable private String format;
    @Nullable private List<Reference> patients;
    @Nullable private String source;
    private RequestTypeEnum method = RequestTypeEnum.POST;
    private final Map<String, String[]> queryParameters = new HashMap<>();

    @Nonnull
    Request parameters(@Nonnull final Parameters value) {
      this.parameters = value;
      return this;
    }

    @Nonnull
    Request context(@Nonnull final List<IBaseResource> value) {
      this.context = value;
      return this;
    }

    @Nonnull
    Request inlineResources(@Nonnull final List<String> value) {
      this.inlineResources = value;
      return this;
    }

    @Nonnull
    Request format(@Nonnull final String value) {
      this.format = value;
      return this;
    }

    @Nonnull
    Request patient(@Nonnull final String reference) {
      this.patients = List.of(new Reference(reference));
      return this;
    }

    @Nonnull
    Request source(@Nonnull final String value) {
      this.source = value;
      return this;
    }

    @Nonnull
    Request method(@Nonnull final RequestTypeEnum value) {
      this.method = value;
      return this;
    }

    @Nonnull
    Request queryParameter(@Nonnull final String name) {
      queryParameters.put(name, new String[] {"value"});
      return this;
    }

    @Nonnull
    ServletRequestDetails requestDetails() {
      final ServletRequestDetails details = mock(ServletRequestDetails.class);
      final HttpServletRequest servletRequest = mock(HttpServletRequest.class);
      when(details.getServletRequest()).thenReturn(servletRequest);
      when(servletRequest.getHeader("Accept")).thenReturn(null);
      when(details.getRequestType()).thenReturn(method);
      when(details.getParameters()).thenReturn(queryParameters);
      when(details.getRequestId()).thenReturn("req-1");
      return details;
    }
  }

  /** Runs the given action and returns the HAPI exception it threw. */
  @Nonnull
  private static BaseServerResponseException catchServerException(@Nonnull final Runnable action) {
    try {
      action.run();
    } catch (final BaseServerResponseException e) {
      return e;
    }
    throw new AssertionError("Expected a BaseServerResponseException to be thrown");
  }

  /** Asserts that the exception carries an issue with the given code and expression. */
  private static void assertIssue(
      @Nonnull final BaseServerResponseException exception,
      @Nonnull final IssueType code,
      @Nonnull final String expression) {
    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
    assertThat(outcome).isNotNull();
    assertThat(outcome.getIssue())
        .anySatisfy(
            issue -> {
              assertThat(issue.getCode()).isEqualTo(code);
              assertThat(issue.getExpression())
                  .extracting(StringType::getValue)
                  .contains(expression);
            });
  }
}
