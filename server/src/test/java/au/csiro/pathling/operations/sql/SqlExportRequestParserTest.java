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
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.operations.sqlquery.PreparedSqlQuery;
import au.csiro.pathling.operations.sqlquery.SqlLibraryFixtures;
import au.csiro.pathling.operations.sqlquery.SqlQueryPipeline;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.rest.api.RequestTypeEnum;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link SqlExportRequestParser}, covering the kick-off validation rows of
 * contracts/sql-export.md: the three subject naming forms and their exclusivity, per-subject
 * parameter binding, output-name derivation and uniqueness, the request-level rejections, and the
 * rule that every problem in one body is reported in one outcome.
 *
 * <p>Subject resolution, view validation and the SQL pipeline are mocked, so these tests exercise
 * the parser's own contract enforcement rather than resolution or query planning.
 *
 * @author John Grimes
 */
class SqlExportRequestParserTest {

  private SubjectResolver subjectResolver;
  private SqlFilterResolver filterResolver;
  private ContextArtefactParser contextParser;
  private FhirViewValidator viewValidator;
  private SqlQueryPipeline pipeline;
  private SqlExportRequestParser parser;

  @BeforeEach
  void setUp() {
    subjectResolver = mock(SubjectResolver.class);
    filterResolver = mock(SqlFilterResolver.class);
    contextParser = mock(ContextArtefactParser.class);
    viewValidator = mock(FhirViewValidator.class);
    pipeline = mock(SqlQueryPipeline.class);
    parser =
        new SqlExportRequestParser(
            subjectResolver, filterResolver, contextParser, viewValidator, pipeline);

    when(filterResolver.resolve(any(), any(), any()))
        .thenReturn(new ResolvedFilters(Set.of(), null, List.of()));
    when(contextParser.parse(any())).thenReturn(SuppliedArtefacts.empty());
    when(viewValidator.parse(any(), any())).thenReturn(mock(FhirView.class));
    when(pipeline.prepare(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(mock(PreparedSqlQuery.class));
  }

  // ---------------------------------------------------------------------------
  // Subject naming forms.
  // ---------------------------------------------------------------------------

  // Each naming form is passed to the resolver in its own slot, so the resolver sees exactly what
  // the client wrote and can apply the exactly-one rule itself.
  @Test
  void passesEachNamingFormThroughToTheResolver() {
    stubSubject(SubjectKind.SQL_QUERY);

    parse(
        body(
            subjectPart(null, canonicalPart("https://example.org/q")),
            subjectPart(null, referencePart("subjectReference", "Library/q2")),
            subjectPart(
                null, resourcePart("subjectResource", SqlLibraryFixtures.sqlQuery("SELECT 1")))));

    verify(subjectResolver)
        .resolve(org.mockito.ArgumentMatchers.eq("https://example.org/q"), any(), any(), any());
    verify(subjectResolver)
        .resolve(
            any(),
            org.mockito.ArgumentMatchers.argThat(
                r -> r != null && "Library/q2".equals(r.getReference())),
            any(),
            any());
    verify(subjectResolver)
        .resolve(
            any(), any(), org.mockito.ArgumentMatchers.argThat(java.util.Objects::nonNull), any());
  }

  // A body with no subject has nothing to export, so it is rejected as a missing requirement.
  @Test
  void rejectsABodyWithNoSubject() {
    final BaseServerResponseException exception = catchException(() -> parse(body()));

    assertThat(exception.getStatusCode()).isEqualTo(400);
    assertIssue(exception, IssueType.REQUIRED, "subject");
  }

  // A repetition naming zero or several forms is the resolver's rule; its failure reaches the
  // caller unchanged.
  @Test
  void surfacesTheResolversExactlyOneRule() {
    when(subjectResolver.resolve(any(), any(), any(), any()))
        .thenThrow(
            SqlOperationError.badRequest(
                IssueType.INVALID, "subject", "More than one naming form was supplied."));

    final BaseServerResponseException exception =
        catchException(
            () -> parse(body(subjectPart(null, canonicalPart("https://example.org/q")))));

    assertIssue(exception, IssueType.INVALID, "subject");
  }

  // ---------------------------------------------------------------------------
  // Per-subject parameters.
  // ---------------------------------------------------------------------------

  // A ViewDefinition declares no parameters, so binding values to one is a 400 naming the part.
  @Test
  void rejectsParametersOnAViewDefinitionSubject() {
    stubSubject(SubjectKind.VIEW_DEFINITION);

    final BaseServerResponseException exception =
        catchException(
            () ->
                parse(
                    body(
                        subjectPart(
                            null,
                            canonicalPart("https://example.org/v"),
                            resourcePart("parameters", new Parameters())))));

    assertIssue(exception, IssueType.INVALID, "parameters");
  }

  // Bindings supplied for a SQL subject reach the pipeline, which type checks them.
  @Test
  void passesParametersOfASqlSubjectToThePipeline() {
    stubSubject(SubjectKind.SQL_QUERY);
    final Parameters bindings = new Parameters();
    bindings.addParameter().setName("family").setValue(new StringType("Smith"));

    parse(
        body(
            subjectPart(
                null,
                canonicalPart("https://example.org/q"),
                resourcePart("parameters", bindings))));

    verify(pipeline)
        .prepare(
            any(),
            any(),
            any(),
            any(),
            any(),
            org.mockito.ArgumentMatchers.eq(bindings),
            any(),
            any());
  }

  // A binding failure reported by the pipeline without an outcome of its own is relabelled onto
  // the 'parameters' part, so the client is told which part to correct.
  @Test
  void reportsABindingFailureAgainstTheParametersPart() {
    stubSubject(SubjectKind.SQL_QUERY);
    when(pipeline.prepare(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(new InvalidRequestException("Unknown parameter 'nope'"));

    final BaseServerResponseException exception =
        catchException(
            () ->
                parse(
                    body(
                        subjectPart(
                            null,
                            canonicalPart("https://example.org/q"),
                            resourcePart("parameters", new Parameters())))));

    assertIssue(exception, IssueType.INVALID, "parameters");
  }

  // ---------------------------------------------------------------------------
  // Output names.
  // ---------------------------------------------------------------------------

  // The name fallback chain runs supplied name, then the artefact's own name, then a generated one.
  @Test
  void derivesOutputNamesThroughTheFallbackChain() {
    final Library named = SqlLibraryFixtures.sqlQuery("SELECT 1");
    named.setName("library_name");
    final Library anonymous = SqlLibraryFixtures.sqlQuery("SELECT 2");

    when(subjectResolver.resolve(any(), any(), any(), any()))
        .thenReturn(new ResolvedSubject(SubjectKind.SQL_QUERY, named, "supplied"))
        .thenReturn(new ResolvedSubject(SubjectKind.SQL_QUERY, named, null))
        .thenReturn(new ResolvedSubject(SubjectKind.SQL_QUERY, anonymous, null));

    final SqlExportRequest request =
        parse(
            body(
                subjectPart("supplied", canonicalPart("https://example.org/a")),
                subjectPart(null, canonicalPart("https://example.org/b")),
                subjectPart(null, canonicalPart("https://example.org/c"))));

    assertThat(request.subjects())
        .extracting(SubjectInput::name)
        .containsExactly("supplied", "library_name", "subject_2");
  }

  // Two subjects resolving to the same output name would leave the manifest ambiguous, so the
  // collision is rejected rather than silently renamed.
  @Test
  void rejectsCollidingOutputNames() {
    stubSubject(SubjectKind.SQL_QUERY);

    final BaseServerResponseException exception =
        catchException(
            () ->
                parse(
                    body(
                        subjectPart("same", canonicalPart("https://example.org/a")),
                        subjectPart("same", canonicalPart("https://example.org/b")))));

    assertIssue(exception, IssueType.INVALID, "subject");
  }

  // ---------------------------------------------------------------------------
  // Context.
  // ---------------------------------------------------------------------------

  // Context entries are job-wide: they are parsed once and offered to every subject.
  @Test
  void parsesContextOnceForTheWholeJob() {
    stubSubject(SubjectKind.SQL_QUERY);
    final SuppliedArtefacts artefacts = mock(SuppliedArtefacts.class);
    when(contextParser.parse(any())).thenReturn(artefacts);

    parse(
        body(
            subjectPart(null, canonicalPart("https://example.org/a")),
            subjectPart("b", canonicalPart("https://example.org/b")),
            contextEntry(SqlLibraryFixtures.sqlView("SELECT 1"))));

    verify(contextParser).parse(any());
    verify(artefacts).checkAllMatched();
    verify(pipeline, org.mockito.Mockito.times(2))
        .prepare(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            org.mockito.ArgumentMatchers.eq(artefacts),
            any());
  }

  // Dependency resolution is memoised across the job, so every subject shares one node map and a
  // canonical URL reached from several subjects resolves once.
  @Test
  void sharesOneMemoisationMapAcrossSubjects() {
    stubSubject(SubjectKind.SQL_QUERY);

    parse(
        body(
            subjectPart("a", canonicalPart("https://example.org/a")),
            subjectPart("b", canonicalPart("https://example.org/b"))));

    final var captor = org.mockito.ArgumentCaptor.forClass(Map.class);
    verify(pipeline, org.mockito.Mockito.times(2))
        .prepare(any(), any(), any(), any(), any(), any(), any(), captor.capture());
    assertThat(captor.getAllValues().get(0)).isSameAs(captor.getAllValues().get(1));
  }

  // ---------------------------------------------------------------------------
  // Request-level rejections.
  // ---------------------------------------------------------------------------

  // An export's subjects and context cannot be expressed in a query string, so GET is refused.
  @Test
  void rejectsAGet() {
    final BaseServerResponseException exception =
        catchException(() -> parse(body(), RequestTypeEnum.GET, "respond-async"));

    assertThat(exception.getStatusCode()).isEqualTo(400);
    assertIssue(exception, IssueType.REQUIRED, null);
  }

  // The operation is asynchronous by contract, so a client that has not asked for that pattern is
  // told so rather than being given a synchronous response it did not expect.
  @Test
  void rejectsAMissingRespondAsyncPreference() {
    final BaseServerResponseException exception =
        catchException(() -> parse(body(), RequestTypeEnum.POST, null));

    assertIssue(exception, IssueType.REQUIRED, null);
  }

  // External data sources are not implemented, so a supplied source is rejected rather than
  // silently ignored.
  @Test
  void rejectsTheSourceParameter() {
    final BaseServerResponseException exception =
        catchException(() -> parse(body(simplePart("source", "http://example.org"))));

    assertIssue(exception, IssueType.NOTSUPPORTED, "source");
  }

  // An export writes the whole result set, so a row cap is not offered.
  @Test
  void rejectsTheLimitParameter() {
    final BaseServerResponseException exception =
        catchException(() -> parse(body(simplePart("_limit", "10"))));

    assertIssue(exception, IssueType.INVALID, "_limit");
  }

  // ---------------------------------------------------------------------------
  // Formats.
  // ---------------------------------------------------------------------------

  // The three bulk formats are accepted by code.
  @ParameterizedTest
  @ValueSource(strings = {"ndjson", "csv", "parquet"})
  void acceptsTheBulkFormats(final String code) {
    stubSubject(SubjectKind.SQL_QUERY);

    final SqlExportRequest request =
        parse(
            body(
                subjectPart(null, canonicalPart("https://example.org/q")),
                simplePart("_format", code)));

    assertThat(request.format().getCode()).isEqualTo(code);
  }

  // Omitting the format yields ndjson, irrespective of any Accept header: an export has no content
  // negotiation, because its result is a file set rather than a response body.
  @Test
  void defaultsToNdjson() {
    stubSubject(SubjectKind.SQL_QUERY);

    final SqlExportRequest request =
        parse(body(subjectPart(null, canonicalPart("https://example.org/q"))));

    assertThat(request.format()).isEqualTo(SqlExportFormat.NDJSON);
  }

  // json is a format this server has not implemented for export, which is not the same as the
  // client having made a mistake.
  @Test
  void rejectsJsonAsNotSupported() {
    final BaseServerResponseException exception =
        catchException(() -> parse(body(simplePart("_format", "json"))));

    assertIssue(exception, IssueType.NOTSUPPORTED, "_format");
  }

  // fhir is meaningless for a bulk file set, so asking for it is a client mistake.
  @Test
  void rejectsFhirAsInvalid() {
    final BaseServerResponseException exception =
        catchException(() -> parse(body(simplePart("_format", "fhir"))));

    assertIssue(exception, IssueType.INVALID, "_format");
  }

  // ---------------------------------------------------------------------------
  // Multi-issue collection.
  // ---------------------------------------------------------------------------

  // Two bad subjects are reported together, so the client can correct both in one round trip
  // rather than discovering them one at a time.
  @Test
  void reportsTwoBadSubjectsInOneOutcome() {
    when(subjectResolver.resolve(any(), any(), any(), any()))
        .thenThrow(SqlOperationError.notFound("subject", "No artefact matches 'a'."))
        .thenThrow(SqlOperationError.notFound("subject", "No artefact matches 'b'."));

    final BaseServerResponseException exception =
        catchException(
            () ->
                parse(
                    body(
                        subjectPart(null, canonicalPart("https://example.org/a")),
                        subjectPart(null, canonicalPart("https://example.org/b")))));

    assertThat(exception.getStatusCode()).isEqualTo(404);
    assertThat(((OperationOutcome) exception.getOperationOutcome()).getIssue()).hasSize(2);
  }

  // A subject failure and a filter failure are reported together, under the subject's status: the
  // subject failure is the more fundamental of the two.
  @Test
  void reportsSubjectAndFilterFailuresTogetherUnderTheSubjectStatus() {
    when(subjectResolver.resolve(any(), any(), any(), any()))
        .thenThrow(SqlOperationError.notFound("subject", "No artefact matches."));
    when(filterResolver.resolve(any(), any(), any()))
        .thenReturn(
            new ResolvedFilters(
                Set.of(),
                null,
                List.of(
                    SqlOperationError.issue(
                        IssueType.NOTFOUND, "patient", "No Patient with id 'nope'."))));

    final BaseServerResponseException exception =
        catchException(
            () ->
                parse(
                    body(
                        subjectPart(null, canonicalPart("https://example.org/a")),
                        referencePart("patient", "Patient/nope"))));

    assertThat(exception.getStatusCode()).isEqualTo(404);
    assertIssue(exception, IssueType.NOTFOUND, "subject");
    assertIssue(exception, IssueType.NOTFOUND, "patient");
  }

  // A filter failure on its own is a 400, since the subjects themselves are sound.
  @Test
  void reportsAFilterFailureAloneAsABadRequest() {
    stubSubject(SubjectKind.SQL_QUERY);
    when(filterResolver.resolve(any(), any(), any()))
        .thenReturn(
            new ResolvedFilters(
                Set.of(),
                null,
                List.of(
                    SqlOperationError.issue(
                        IssueType.NOTFOUND, "group", "No Group with id 'nope'."))));

    final BaseServerResponseException exception =
        catchException(
            () ->
                parse(
                    body(
                        subjectPart(null, canonicalPart("https://example.org/a")),
                        referencePart("group", "Group/nope"))));

    assertThat(exception.getStatusCode()).isEqualTo(400);
    assertIssue(exception, IssueType.NOTFOUND, "group");
  }

  // ---------------------------------------------------------------------------
  // Successful parse.
  // ---------------------------------------------------------------------------

  // A mixed job carries one input per subject, of the kind each resolved to, and the job-wide
  // settings taken from the body.
  @Test
  void producesOneInputPerSubjectForAMixedJob() {
    final ViewDefinitionResource view = viewDefinition();
    when(subjectResolver.resolve(any(), any(), any(), any()))
        .thenReturn(new ResolvedSubject(SubjectKind.VIEW_DEFINITION, view, "the_view"))
        .thenReturn(
            new ResolvedSubject(
                SubjectKind.SQL_QUERY, SqlLibraryFixtures.sqlQuery("SELECT 1"), "the_query"));

    final SqlExportRequest request =
        parse(
            body(
                subjectPart("the_view", canonicalPart("https://example.org/v")),
                subjectPart("the_query", canonicalPart("https://example.org/q")),
                simplePart("clientTrackingId", "tracking-1"),
                simplePart("header", "false")));

    assertThat(request.subjects()).hasSize(2);
    assertThat(request.subjects().get(0).kind()).isEqualTo(SubjectKind.VIEW_DEFINITION);
    assertThat(request.subjects().get(0).view()).isNotNull();
    assertThat(request.subjects().get(1).kind()).isEqualTo(SubjectKind.SQL_QUERY);
    assertThat(request.subjects().get(1).preparedQuery()).isNotNull();
    assertThat(request.clientTrackingId()).isEqualTo("tracking-1");
    assertThat(request.includeHeader()).isFalse();
  }

  // A ViewDefinition subject is validated at kick-off, so an unexecutable view fails the request
  // rather than the job.
  @Test
  void validatesAViewSubjectAtKickOff() {
    stubSubject(SubjectKind.VIEW_DEFINITION);

    parse(body(subjectPart(null, canonicalPart("https://example.org/v"))));

    verify(viewValidator).validateSemantically(any(), org.mockito.ArgumentMatchers.eq("subject"));
  }

  // ---- helpers ----

  /** Stubs the subject resolver to return a subject of the given kind, carrying the given name. */
  private void stubSubject(@Nonnull final SubjectKind kind) {
    when(subjectResolver.resolve(any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                new ResolvedSubject(
                    kind, artefactOf(kind), invocation.getArgument(3, String.class)));
  }

  @Nonnull
  private static IBaseResource artefactOf(@Nonnull final SubjectKind kind) {
    return switch (kind) {
      case VIEW_DEFINITION -> viewDefinition();
      case SQL_QUERY -> SqlLibraryFixtures.sqlQuery("SELECT 1");
      case SQL_VIEW -> SqlLibraryFixtures.sqlView("SELECT 1");
    };
  }

  @Nonnull
  private static ViewDefinitionResource viewDefinition() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setStatusElement(new CodeType("active"));
    view.setResourceElement(new CodeType("Patient"));
    return view;
  }

  @Nonnull
  private SqlExportRequest parse(@Nonnull final Parameters body) {
    return parse(body, RequestTypeEnum.POST, "respond-async");
  }

  @Nonnull
  private SqlExportRequest parse(
      @Nonnull final Parameters body,
      @Nonnull final RequestTypeEnum method,
      @Nullable final String prefer) {
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    when(requestDetails.getRequestType()).thenReturn(method);
    when(requestDetails.getHeader("Prefer")).thenReturn(prefer);
    when(requestDetails.getResource()).thenReturn(body);
    when(requestDetails.getCompleteUrl()).thenReturn("http://localhost/fhir/$sql-export");
    when(requestDetails.getFhirServerBase()).thenReturn("http://localhost/fhir");
    return parser.parse(requestDetails);
  }

  @Nonnull
  private static Parameters body(@Nonnull final ParametersParameterComponent... parts) {
    final Parameters parameters = new Parameters();
    for (final ParametersParameterComponent part : parts) {
      parameters.addParameter(part);
    }
    return parameters;
  }

  @Nonnull
  private static ParametersParameterComponent subjectPart(
      @Nullable final String name, @Nonnull final ParametersParameterComponent... parts) {
    final ParametersParameterComponent subject = new ParametersParameterComponent();
    subject.setName("subject");
    if (name != null) {
      subject.addPart(simplePart("name", name));
    }
    for (final ParametersParameterComponent part : parts) {
      subject.addPart(part);
    }
    return subject;
  }

  @Nonnull
  private static ParametersParameterComponent canonicalPart(@Nonnull final String url) {
    return simplePart("subjectCanonical", url);
  }

  @Nonnull
  private static ParametersParameterComponent simplePart(
      @Nonnull final String name, @Nonnull final String value) {
    final ParametersParameterComponent part = new ParametersParameterComponent();
    part.setName(name);
    part.setValue(new StringType(value));
    return part;
  }

  @Nonnull
  private static ParametersParameterComponent referencePart(
      @Nonnull final String name, @Nonnull final String reference) {
    final ParametersParameterComponent part = new ParametersParameterComponent();
    part.setName(name);
    part.setValue(new Reference(reference));
    return part;
  }

  @Nonnull
  private static ParametersParameterComponent resourcePart(
      @Nonnull final String name, @Nonnull final org.hl7.fhir.r4.model.Resource resource) {
    final ParametersParameterComponent part = new ParametersParameterComponent();
    part.setName(name);
    part.setResource(resource);
    return part;
  }

  @Nonnull
  private static ParametersParameterComponent contextEntry(
      @Nonnull final org.hl7.fhir.r4.model.Resource resource) {
    return resourcePart("context", resource);
  }

  @Nonnull
  private static BaseServerResponseException catchException(@Nonnull final Runnable action) {
    final BaseServerResponseException exception =
        catchThrowableOfType(action::run, BaseServerResponseException.class);
    assertThat(exception).as("Expected the parse to be rejected").isNotNull();
    return exception;
  }

  /** Asserts that the outcome carries an issue with the given code and expression. */
  private static void assertIssue(
      @Nonnull final BaseServerResponseException exception,
      @Nonnull final IssueType code,
      @Nullable final String expression) {
    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
    assertThat(outcome).as("Expected the exception to carry an OperationOutcome").isNotNull();
    assertThat(outcome.getIssue())
        .anySatisfy(
            issue -> {
              assertThat(issue.getCode()).isEqualTo(code);
              if (expression == null) {
                assertThat(issue.getExpression()).isEmpty();
              } else {
                assertThat(issue.getExpression())
                    .extracting(StringType::getValue)
                    .contains(expression);
              }
            });
  }
}
