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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.operations.sqlquery.SqlLibraryFixtures;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Unit tests for {@link SubjectResolver}, covering the naming-form exclusivity rule, canonical and
 * reference resolution across both artefact families, kind detection, and effective-name derivation
 * as specified in contracts/sql-run.md and contracts/sql-export.md.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
class SubjectResolverTest {

  private static final String VIEW_URL = SqlLibraryFixtures.viewDefinitionUrl("demographics");
  private static final String LIBRARY_URL = SqlLibraryFixtures.sqlViewUrl("bp-summary");

  @Autowired private SparkSession spark;

  @Autowired private FhirEncoders fhirEncoders;

  private ReadExecutor readExecutor;
  private DataSource dataSource;
  private SubjectResolver resolver;

  @BeforeEach
  void setUp() {
    readExecutor = mock(ReadExecutor.class);
    dataSource = mock(DataSource.class);
    resolver = newResolver(false);
  }

  // ---------------------------------------------------------------------------
  // Exactly-one naming form.
  // ---------------------------------------------------------------------------

  // The contract requires exactly one naming form per subject; supplying none is a 400 with
  // issue.code = required naming `subject`.
  @Test
  void rejectsNoNamingFormWithRequired() {
    assertThatThrownBy(() -> resolver.resolve(null, null, null, null))
        .isInstanceOf(InvalidRequestException.class);
    assertIssue(
        catchServerException(() -> resolver.resolve(null, null, null, null)),
        OperationOutcome.IssueType.REQUIRED,
        "subject");
  }

  // Supplying more than one naming form is a 400 with issue.code = invalid naming `subject`.
  @Test
  void rejectsTwoNamingFormsWithInvalid() {
    final Reference reference = new Reference("ViewDefinition/abc");
    assertIssue(
        catchServerException(() -> resolver.resolve(VIEW_URL, reference, null, null)),
        OperationOutcome.IssueType.INVALID,
        "subject");
  }

  // All three forms supplied at once is likewise a 400 invalid.
  @Test
  void rejectsThreeNamingFormsWithInvalid() {
    final Reference reference = new Reference("ViewDefinition/abc");
    final ViewDefinitionResource inline = viewDefinition(null, null, null);
    assertIssue(
        catchServerException(() -> resolver.resolve(VIEW_URL, reference, inline, null)),
        OperationOutcome.IssueType.INVALID,
        "subject");
  }

  // ---------------------------------------------------------------------------
  // Inline subjects (subjectResource).
  // ---------------------------------------------------------------------------

  // An inline ViewDefinition resolves to the VIEW_DEFINITION kind without touching storage.
  @Test
  void resolvesInlineViewDefinition() {
    final ViewDefinitionResource inline = viewDefinition(null, null, "inline_view");

    final ResolvedSubject resolved = resolver.resolve(null, null, inline, null);

    assertThat(resolved.getKind()).isEqualTo(SubjectKind.VIEW_DEFINITION);
    assertThat(resolved.getResource()).isSameAs(inline);
  }

  // An inline SQLQuery Library resolves to the SQL_QUERY kind.
  @Test
  void resolvesInlineSqlQueryLibrary() {
    final Library inline = SqlLibraryFixtures.sqlQuery("SELECT 1");

    final ResolvedSubject resolved = resolver.resolve(null, null, inline, null);

    assertThat(resolved.getKind()).isEqualTo(SubjectKind.SQL_QUERY);
  }

  // An inline SQLView Library resolves to the SQL_VIEW kind, so a SQLView can be run or exported
  // directly as a subject rather than only as a dependency.
  @Test
  void resolvesInlineSqlViewLibrary() {
    final Library inline = SqlLibraryFixtures.sqlView("SELECT 1");

    final ResolvedSubject resolved = resolver.resolve(null, null, inline, null);

    assertThat(resolved.getKind()).isEqualTo(SubjectKind.SQL_VIEW);
  }

  // An artefact conforming to none of the three profiles is a 422 naming `subject`.
  @Test
  void rejectsUnadmittedResourceKindWith422() {
    final Patient patient = new Patient();

    final BaseServerResponseException exception =
        catchServerException(() -> resolver.resolve(null, null, patient, null));

    assertThat(exception).isInstanceOf(UnprocessableEntityException.class);
    assertIssue(exception, OperationOutcome.IssueType.INVALID, "subject");
  }

  // A Library carrying no SQL on FHIR type coding conforms to neither SQL profile, so it is a 422
  // rather than a parse failure surfaced later.
  @Test
  void rejectsLibraryWithoutSqlOnFhirTypeWith422() {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);

    assertThat(catchServerException(() -> resolver.resolve(null, null, library, null)))
        .isInstanceOf(UnprocessableEntityException.class);
  }

  // ---------------------------------------------------------------------------
  // Canonical resolution.
  // ---------------------------------------------------------------------------

  // A canonical URL matching a stored ViewDefinition resolves it and reports the view kind.
  @Test
  void resolvesCanonicalToStoredViewDefinition() {
    stubViewDefinitions(viewDefinition(VIEW_URL, "1.0", "demographics"));
    stubLibraries();

    final ResolvedSubject resolved = resolver.resolve(VIEW_URL, null, null, null);

    assertThat(resolved.getKind()).isEqualTo(SubjectKind.VIEW_DEFINITION);
    assertThat(resolved.artefactName()).isEqualTo("demographics");
  }

  // A canonical URL matching a stored SQLQuery Library resolves it and reports the query kind.
  @Test
  void resolvesCanonicalToStoredSqlQueryLibrary() {
    stubViewDefinitions();
    final Library library = SqlLibraryFixtures.sqlQuery("SELECT 1");
    library.setUrl(LIBRARY_URL);
    library.setName("bp_summary");
    stubLibraries(library);

    final ResolvedSubject resolved = resolver.resolve(LIBRARY_URL, null, null, null);

    assertThat(resolved.getKind()).isEqualTo(SubjectKind.SQL_QUERY);
    assertThat(resolved.artefactName()).isEqualTo("bp_summary");
  }

  // A version pin selects that version only, so the pinned artefact is the one resolved.
  @Test
  void honoursVersionPinOnCanonical() {
    stubViewDefinitions(
        viewDefinition(VIEW_URL, "1.0", "v1"), viewDefinition(VIEW_URL, "2.0", "v2"));
    stubLibraries();

    final ResolvedSubject resolved = resolver.resolve(VIEW_URL + "|1.0", null, null, null);

    assertThat(resolved.artefactName()).isEqualTo("v1");
  }

  // An unpinned canonical follows the shared candidate-selection rule: the latest active version.
  @Test
  void selectsLatestActiveVersionForUnpinnedCanonical() {
    stubViewDefinitions(
        viewDefinition(VIEW_URL, "1.0", "v1"), viewDefinition(VIEW_URL, "2.0", "v2"));
    stubLibraries();

    final ResolvedSubject resolved = resolver.resolve(VIEW_URL, null, null, null);

    assertThat(resolved.artefactName()).isEqualTo("v2");
  }

  // A canonical matching nothing is a 404 naming `subject`.
  @Test
  void rejectsUnresolvableCanonicalWith404() {
    stubViewDefinitions();
    stubLibraries();

    final BaseServerResponseException exception =
        catchServerException(() -> resolver.resolve(VIEW_URL, null, null, null));

    assertThat(exception).isInstanceOf(ResourceNotFoundException.class);
    assertIssue(exception, OperationOutcome.IssueType.NOTFOUND, "subject");
  }

  // A version pin that matches no stored version is a 404, even though the bare url does match.
  @Test
  void rejectsUnresolvableVersionPinWith404() {
    stubViewDefinitions(viewDefinition(VIEW_URL, "1.0", "v1"));
    stubLibraries();

    assertThat(catchServerException(() -> resolver.resolve(VIEW_URL + "|9.0", null, null, null)))
        .isInstanceOf(ResourceNotFoundException.class);
  }

  // A canonical matching both a ViewDefinition and a Library cannot be resolved unambiguously, so
  // it is rejected rather than one arm being silently preferred.
  @Test
  void rejectsAmbiguousCanonicalMatchingBothFamilies() {
    stubViewDefinitions(viewDefinition(VIEW_URL, null, "view"));
    final Library library = SqlLibraryFixtures.sqlQuery("SELECT 1");
    library.setUrl(VIEW_URL);
    stubLibraries(library);

    assertThatThrownBy(() -> resolver.resolve(VIEW_URL, null, null, null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("ambiguous");
  }

  // ---------------------------------------------------------------------------
  // Reference resolution.
  // ---------------------------------------------------------------------------

  // A relative ViewDefinition reference reads the stored resource through the shared read path.
  @Test
  void resolvesViewDefinitionReference() {
    final ViewDefinitionResource stored = viewDefinition(VIEW_URL, null, "demographics");
    when(readExecutor.read("ViewDefinition", "abc")).thenReturn(stored);

    final ResolvedSubject resolved =
        resolver.resolve(null, new Reference("ViewDefinition/abc"), null, null);

    assertThat(resolved.getKind()).isEqualTo(SubjectKind.VIEW_DEFINITION);
    assertThat(resolved.getResource()).isSameAs(stored);
  }

  // A relative Library reference resolves a SQL Library, admitting the second artefact family that
  // the old view-only run operation could not reach.
  @Test
  void resolvesLibraryReference() {
    final Library stored = SqlLibraryFixtures.sqlQuery("SELECT 1");
    when(readExecutor.read("Library", "lib1")).thenReturn(stored);

    final ResolvedSubject resolved =
        resolver.resolve(null, new Reference("Library/lib1"), null, null);

    assertThat(resolved.getKind()).isEqualTo(SubjectKind.SQL_QUERY);
  }

  // An unresolvable reference is a 404 naming `subject`.
  @Test
  void rejectsUnresolvableReferenceWith404() {
    when(readExecutor.read("ViewDefinition", "missing"))
        .thenThrow(new ResourceNotFoundError("not there"));
    final Reference reference = new Reference("ViewDefinition/missing");

    final BaseServerResponseException exception =
        catchServerException(() -> resolver.resolve(null, reference, null, null));

    assertThat(exception).isInstanceOf(ResourceNotFoundException.class);
    assertIssue(exception, OperationOutcome.IssueType.NOTFOUND, "subject");
  }

  // A reference naming a resource type that is neither family cannot identify a subject.
  @Test
  void rejectsReferenceToUnadmittedType() {
    final Reference reference = new Reference("Patient/abc");

    assertThatThrownBy(() -> resolver.resolve(null, reference, null, null))
        .isInstanceOf(InvalidRequestException.class);
  }

  // A reference with no type prefix cannot say which artefact family to read, so it is rejected
  // rather than guessed at.
  @Test
  void rejectsUntypedReference() {
    final Reference reference = new Reference("abc");

    assertThatThrownBy(() -> resolver.resolve(null, reference, null, null))
        .isInstanceOf(InvalidRequestException.class);
  }

  // A blank reference value carries no target at all.
  @Test
  void rejectsBlankReference() {
    final Reference reference = new Reference("");

    assertThatThrownBy(() -> resolver.resolve(null, reference, null, null))
        .isInstanceOf(InvalidRequestException.class);
  }

  // ---------------------------------------------------------------------------
  // Read authority.
  // ---------------------------------------------------------------------------

  // A stored subject is subject to the resource-level READ check, preserved from the operations
  // this feature replaces. With no authentication in the context the check denies the read.
  @Test
  void enforcesReadAuthorityForStoredViewDefinition() {
    final SubjectResolver secured = newResolver(true);
    final Reference reference = new Reference("ViewDefinition/abc");

    assertThatThrownBy(() -> secured.resolve(null, reference, null, null))
        .isInstanceOf(AccessDeniedError.class);
    verifyNoInteractions(readExecutor);
  }

  // An inline subject carries its own content as the request payload, so no resource READ check
  // applies to it.
  @Test
  void doesNotEnforceReadAuthorityForInlineSubject() {
    final SubjectResolver secured = newResolver(true);
    final Library inline = SqlLibraryFixtures.sqlQuery("SELECT 1");

    assertThat(secured.resolve(null, null, inline, null).getKind())
        .isEqualTo(SubjectKind.SQL_QUERY);
  }

  // ---------------------------------------------------------------------------
  // Effective-name derivation.
  // ---------------------------------------------------------------------------

  // The supplied name wins over the artefact's own name element.
  @Test
  void suppliedNameTakesPrecedence() {
    final Library inline = SqlLibraryFixtures.sqlQuery("SELECT 1");
    inline.setName("library_name");

    final ResolvedSubject resolved = resolver.resolve(null, null, inline, "supplied");

    assertThat(resolved.getEffectiveName(0)).isEqualTo("supplied");
  }

  // With no supplied name, the artefact's own name element is used.
  @Test
  void fallsBackToArtefactName() {
    final Library inline = SqlLibraryFixtures.sqlQuery("SELECT 1");
    inline.setName("library_name");

    assertThat(resolver.resolve(null, null, inline, null).getEffectiveName(0))
        .isEqualTo("library_name");
  }

  // With neither a supplied name nor a name element, a unique identifier is generated from the
  // subject's position in the request.
  @Test
  void generatesNameWhenArtefactIsUnnamed() {
    final Library inline = SqlLibraryFixtures.sqlQuery("SELECT 1");

    assertThat(resolver.resolve(null, null, inline, null).getEffectiveName(2))
        .isEqualTo("subject_2");
  }

  // A blank supplied name is treated as absent, so the fallback chain still applies.
  @Test
  void treatsBlankSuppliedNameAsAbsent() {
    final ViewDefinitionResource inline = viewDefinition(null, null, "view_name");

    assertThat(resolver.resolve(null, null, inline, "  ").getEffectiveName(0))
        .isEqualTo("view_name");
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  @Nonnull
  private SubjectResolver newResolver(final boolean authEnabled) {
    final ServerConfiguration configuration = new ServerConfiguration();
    final AuthorizationConfiguration auth = new AuthorizationConfiguration();
    auth.setEnabled(authEnabled);
    configuration.setAuth(auth);
    return new SubjectResolver(readExecutor, dataSource, fhirEncoders, configuration);
  }

  private void stubViewDefinitions(@Nonnull final ViewDefinitionResource... viewDefinitions) {
    when(dataSource.read("ViewDefinition"))
        .thenReturn(
            spark
                .createDataset(List.of(viewDefinitions), fhirEncoders.of("ViewDefinition"))
                .toDF());
  }

  private void stubLibraries(@Nonnull final Library... libraries) {
    final Dataset<Row> dataset =
        spark.createDataset(List.of(libraries), fhirEncoders.of("Library")).toDF();
    when(dataSource.read("Library")).thenReturn(dataset);
  }

  @Nonnull
  private static ViewDefinitionResource viewDefinition(
      @Nullable final String url, @Nullable final String version, @Nullable final String name) {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setStatusElement(new CodeType("active"));
    view.setResourceElement(new CodeType("Patient"));
    if (url != null) {
      view.setUrl(url);
    }
    if (version != null) {
      view.setVersion(version);
    }
    if (name != null) {
      view.setNameElement(new StringType(name));
    }
    return view;
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

  /** Asserts that the exception carries an OperationOutcome issue with the code and expression. */
  private static void assertIssue(
      @Nonnull final BaseServerResponseException exception,
      @Nonnull final OperationOutcome.IssueType code,
      @Nonnull final String expression) {
    assertThat(exception.getOperationOutcome()).isInstanceOf(OperationOutcome.class);
    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
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
