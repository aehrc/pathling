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
import static org.mockito.Mockito.doAnswer;
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
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.AnalysisException;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
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

  // A binding the subject does not declare is a client mistake about the bindings, so the outcome
  // names the part at fault rather than reaching the client as a bare message.
  @Test
  void reportsAnUndeclaredBindingAgainstTheParametersPart() {
    stubSubject(SubjectKind.SQL_QUERY);
    when(pipeline.prepare(any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(
            new InvalidRequestException(
                "Parameter 'nosuch' is not declared in the SQLQuery Library's parameter list"));

    final BaseServerResponseException exception =
        catchServerException(() -> run(builder().parameters(new Parameters())));

    assertThat(exception.getStatusCode()).isEqualTo(400);
    assertIssue(exception, IssueType.INVALID, "parameters");
  }

  // A value that cannot be coerced to its declared type is the same kind of mistake.
  @Test
  void reportsAMistypedBindingAgainstTheParametersPart() {
    stubSubject(SubjectKind.SQL_QUERY);
    when(pipeline.prepare(any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(new InvalidRequestException("Parameter 'count' expects an integer"));

    final BaseServerResponseException exception =
        catchServerException(() -> run(builder().parameters(new Parameters())));

    assertIssue(exception, IssueType.INVALID, "parameters");
  }

  // Relabelling is scoped to a request that actually supplied bindings; a failure raised with no
  // bindings in play is about something else and must reach the client unchanged.
  @Test
  void leavesAFailureUnlabelledWhenNoBindingsWereSupplied() {
    stubSubject(SubjectKind.SQL_QUERY);
    when(pipeline.prepare(any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(new InvalidRequestException("Cycle detected in the dependency graph"));

    final BaseServerResponseException exception = catchServerException(() -> run(builder()));

    assertThat(exception.getOperationOutcome()).isNull();
    assertThat(exception).hasMessageContaining("Cycle detected");
  }

  // A failure that already carries its own outcome keeps it, so a dependency 404 raised during
  // preparation is not mislabelled as a binding problem.
  @Test
  void preservesAnOutcomeAFailureAlreadyCarries() {
    stubSubject(SubjectKind.SQL_QUERY);
    when(pipeline.prepare(any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(
            SqlOperationError.badRequest(
                IssueType.NOTSUPPORTED, "subject", "The SQL uses an unsupported construct."));

    final BaseServerResponseException exception =
        catchServerException(() -> run(builder().parameters(new Parameters())));

    assertIssue(exception, IssueType.NOTSUPPORTED, "subject");
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

  // ---------------------------------------------------------------------------
  // Analysis failures.
  // ---------------------------------------------------------------------------

  // Spark's analyser is what catches an unresolved column, an unknown function or a missing GROUP
  // BY, and those are faults in the subject's own SQL rather than in the server. The dataset is
  // analysed before the streaming consumer writes a byte, so the failure is reported as a 422
  // naming the subject, carrying Spark's own message so the caller can see what is wrong.
  @Test
  void reportsAnAnalysisFailureAgainstTheSubject() {
    stubSubject(SubjectKind.SQL_QUERY);
    stubExecuteToThrow(analysisException(UNRESOLVED_COLUMN_MESSAGE));

    final BaseServerResponseException exception = catchServerException(() -> run(builder()));

    assertThat(exception).isInstanceOf(UnprocessableEntityException.class);
    assertThat(exception.getStatusCode()).isEqualTo(422);
    assertIssue(exception, IssueType.INVALID, "subject");
    assertThat(diagnosticsOf(exception))
        .contains("UNRESOLVED_COLUMN")
        .contains("no_such_col")
        .contains("Did you mean");
  }

  // The translation is confined to analysis failures. Anything else raised by the query engine is
  // an infrastructure fault, and must keep propagating untouched so that it still renders as a 500
  // rather than being mislabelled as a fault in the caller's SQL.
  @Test
  void leavesANonAnalysisFailureUntranslated() {
    stubSubject(SubjectKind.SQL_QUERY);
    stubExecuteToThrow(new IllegalStateException("The warehouse is unreachable"));

    assertThatThrownBy(() -> run(builder()))
        .isInstanceOf(IllegalStateException.class)
        .isNotInstanceOf(BaseServerResponseException.class)
        .hasMessage("The warehouse is unreachable");
  }

  // getMessage appends the whole unresolved logical plan, which is unbounded, names the internal
  // request-scoped views the dependency graph was materialised under, and tells the caller nothing
  // they can act on. The analyser's own simple message is what is returned.
  @Test
  void omitsTheLogicalPlanFromTheReportedMessage() {
    stubSubject(SubjectKind.SQL_QUERY);
    final AnalysisException failure = analysisException(UNRESOLVED_COLUMN_MESSAGE);
    when(failure.getMessage())
        .thenReturn(
            UNRESOLVED_COLUMN_MESSAGE
                + "\n'Project ['no_such_col]\n+- SubqueryAlias sqlquery_rKo3ryQTTa2xoDQx_internal");
    stubExecuteToThrow(failure);

    final BaseServerResponseException exception = catchServerException(() -> run(builder()));

    assertThat(diagnosticsOf(exception))
        .isEqualTo(UNRESOLVED_COLUMN_MESSAGE)
        .doesNotContain("SubqueryAlias")
        .doesNotContain("sqlquery_rKo3ryQTTa2xoDQx_internal");
  }

  // The suggestion list is drawn from the subject's own columns, so a wide dependency can make even
  // the simple message long. A response body is not the place for an unbounded string.
  @Test
  void boundsTheLengthOfTheReportedMessage() {
    stubSubject(SubjectKind.SQL_QUERY);
    stubExecuteToThrow(analysisException("[UNRESOLVED_COLUMN] ".concat("x".repeat(5000))));

    final BaseServerResponseException exception = catchServerException(() -> run(builder()));

    final String diagnostics = diagnosticsOf(exception);
    assertThat(diagnostics).hasSize(1027).startsWith("[UNRESOLVED_COLUMN] ").endsWith("...");
  }

  // ---- helpers ----

  /** A representative Spark analyser message, of the shape the caller is meant to receive. */
  private static final String UNRESOLVED_COLUMN_MESSAGE =
      "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name "
          + "`no_such_col` cannot be resolved. Did you mean one of the following? [`id`, `gender`]."
          + " SQLSTATE: 42703; line 1 pos 7;";

  /**
   * Builds a stand-in for Spark's analyser failure. {@code AnalysisException} is a Scala-declared
   * checked exception whose constructors take Scala collections, so it is stubbed rather than
   * constructed; the real thing is exercised end-to-end by {@link SqlRunProviderIT}.
   */
  @Nonnull
  private static AnalysisException analysisException(@Nonnull final String message) {
    final AnalysisException exception = mock(AnalysisException.class);
    when(exception.getSimpleMessage()).thenReturn(message);
    return exception;
  }

  /**
   * Stubs the pipeline to fail during execution. An {@code Answer} is used rather than {@code
   * doThrow}, since the failure is not declared on the method's signature.
   */
  private void stubExecuteToThrow(@Nonnull final Throwable failure) {
    doAnswer(
            invocation -> {
              throw failure;
            })
        .when(pipeline)
        .execute(any(), any(), any(), any());
  }

  /** Extracts the diagnostics of the exception's first outcome issue. */
  @Nonnull
  private static String diagnosticsOf(@Nonnull final BaseServerResponseException exception) {
    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
    assertThat(outcome).isNotNull();
    return outcome.getIssueFirstRep().getDiagnostics();
  }

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

    /**
     * Builds the request details, carrying the resource-valued parameters in the body as the
     * provider now reads them.
     */
    @Nonnull
    ServletRequestDetails requestDetails() {
      final ServletRequestDetails details = mock(ServletRequestDetails.class);
      final HttpServletRequest servletRequest = mock(HttpServletRequest.class);
      when(details.getServletRequest()).thenReturn(servletRequest);
      when(servletRequest.getHeader("Accept")).thenReturn(null);
      when(details.getRequestType()).thenReturn(method);
      when(details.getParameters()).thenReturn(queryParameters);
      when(details.getRequestId()).thenReturn("req-1");
      when(details.getResource()).thenReturn(body());
      return details;
    }

    /** Builds the Parameters body carrying whichever resource-valued parameters were set. */
    @Nonnull
    private Parameters body() {
      final Parameters body = new Parameters();
      if (subjectResource != null) {
        body.addParameter().setName("subjectResource").setResource((Resource) subjectResource);
      }
      if (parameters != null) {
        body.addParameter().setName("parameters").setResource(parameters);
      }
      if (context != null) {
        for (final IBaseResource entry : context) {
          body.addParameter().setName("context").setResource((Resource) entry);
        }
      }
      return body;
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
