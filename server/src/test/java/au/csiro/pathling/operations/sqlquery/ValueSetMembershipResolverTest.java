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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.operations.sql.SubjectResolver;
import au.csiro.pathling.operations.sql.SuppliedArtefact;
import au.csiro.pathling.operations.sql.SuppliedArtefacts;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.terminology.expand.ExpansionLimitExceededException;
import au.csiro.pathling.terminology.expand.ValueSetExpansion;
import au.csiro.pathling.terminology.expand.ValueSetExpansionException;
import au.csiro.pathling.terminology.expand.ValueSetMember;
import au.csiro.pathling.util.LogCapture;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import ch.qos.logback.classic.Level;
import jakarta.annotation.Nonnull;
import java.net.ConnectException;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ValueSetMembershipResolver} over a mocked {@link TerminologyService}: the
 * arguments passed to {@code expand}, the node built from an expansion, the not-found and disabled
 * short circuits, the translation of the two expansion exceptions into a 422 that names the label,
 * the canonical URL and the reason, and the handling of a supplied {@code context} ValueSet, whose
 * expansion is used as-is and whose compose is expanded by the service, and the provenance line
 * logged once for every membership resolved, naming its source.
 *
 * @author John Grimes
 */
class ValueSetMembershipResolverTest {

  private static final String URL = "http://example.org/ValueSet/cardiovascular-disease";

  private static final String LABEL = "cvd_codes";

  private static final int MAX_MEMBERS = 250;

  private static final String SERVER_URL = "http://tx.example.org/fhir";

  private static final String EXPANSION_IDENTIFIER =
      "urn:uuid:5b7c1a1e-2f34-4d6a-9c3e-8e2f6a1b0c9d";

  private static final String EXPANSION_TIMESTAMP = "2026-01-31T10:15:30+10:00";

  private static final String SNOMED_VERSION =
      "http://snomed.info/sct|http://snomed.info/sct/32506021000036107/version/20260131";

  private static final String LOINC_VERSION = "http://loinc.org|2.80";

  private static final ValueSetMember MEMBER =
      new ValueSetMember(
          "http://snomed.info/sct", "20260131", "22298006", "Myocardial infarction", null);

  private TerminologyService terminologyService;

  private ServerConfiguration serverConfiguration;

  private PathlingContext pathlingContext;

  @BeforeEach
  void setUp() {
    terminologyService = mock(TerminologyService.class);
    final TerminologyServiceFactory factory = mock(TerminologyServiceFactory.class);
    when(factory.build()).thenReturn(terminologyService);
    pathlingContext = mock(PathlingContext.class);
    when(pathlingContext.getTerminologyServiceFactory()).thenReturn(factory);
    serverConfiguration = new ServerConfiguration();
    serverConfiguration.getSqlQuery().setValueSetMaxMembers(MAX_MEMBERS);
  }

  @Test
  void passesTheUrlThePinnedVersionAndTheCapToTheService() {
    when(terminologyService.expand(URL, "2026", MAX_MEMBERS))
        .thenReturn(Optional.of(expansion("2026")));

    final Optional<ResolvedValueSet> resolved =
        resolver()
            .resolveCanonical(reference(URL + "|2026"), CanonicalReference.parse(URL + "|2026"));

    assertThat(resolved).isPresent();
    assertThat(resolved.get().getExpansion().getMembers()).containsExactly(MEMBER);
  }

  @Test
  void passesANullVersionForAnUnpinnedReference() {
    when(terminologyService.expand(URL, null, MAX_MEMBERS))
        .thenReturn(Optional.of(expansion(null)));

    final Optional<ResolvedValueSet> resolved =
        resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL));

    assertThat(resolved).isPresent();
  }

  @Test
  void keysTheNodeByTheReferenceCanonicalAsWritten() {
    // The terminology layer may report a different version than the one pinned, or none; the node
    // is keyed by the reference so a second reference to the same string reuses it.
    when(terminologyService.expand(URL, "2026", MAX_MEMBERS))
        .thenReturn(Optional.of(expansion("2026.1")));

    final ResolvedValueSet resolved =
        resolver()
            .resolveCanonical(reference(URL + "|2026"), CanonicalReference.parse(URL + "|2026"))
            .orElseThrow();

    assertThat(resolved.getCanonicalKey()).isEqualTo(URL + "|2026");
  }

  @Test
  void returnsEmptyWhenTheServiceCannotResolveTheCanonical() {
    when(terminologyService.expand(anyString(), isNull(), anyInt())).thenReturn(Optional.empty());

    assertThat(resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isEmpty();
  }

  @Test
  void translatesAnExpansionFailureIntoA422NamingTheLabelUrlAndReason() {
    when(terminologyService.expand(URL, null, MAX_MEMBERS))
        .thenThrow(new ValueSetExpansionException("the terminology server returned HTTP 500"));

    assertThatThrownBy(
            () -> resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContainingAll("'" + LABEL + "'", URL, "the terminology server returned HTTP 500")
        .satisfies(ValueSetMembershipResolverTest::assertSubjectInvalidIssue);
  }

  @Test
  void translatesALimitBreachIntoA422NamingTheCap() {
    when(terminologyService.expand(URL, null, MAX_MEMBERS))
        .thenThrow(new ExpansionLimitExceededException(MAX_MEMBERS));

    assertThatThrownBy(
            () -> resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContainingAll("'" + LABEL + "'", URL, String.valueOf(MAX_MEMBERS))
        .satisfies(ValueSetMembershipResolverTest::assertSubjectInvalidIssue);
  }

  @Test
  void returnsEmptyWithoutCallingTheServiceWhenTerminologyIsDisabled() {
    serverConfiguration.setTerminology(TerminologyConfiguration.builder().enabled(false).build());

    assertThat(resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isEmpty();
    verifyNoInteractions(terminologyService);
  }

  // ---------------------------------------------------------------------------
  // Supplied ValueSets (US2).
  // ---------------------------------------------------------------------------

  @Test
  void usesASuppliedExpansionWithoutCallingTheService() {
    final ValueSet supplied = suppliedValueSet("2026");
    supplied.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("22298006");
    supplied
        .getExpansion()
        .addContains()
        .setSystem("http://hl7.org/fhir/sid/icd-10")
        .setCode("I21");

    final ResolvedValueSet resolved =
        resolver().resolveSupplied(reference(URL + "|2026"), artefact(supplied));

    assertThat(resolved.getCanonicalKey()).isEqualTo(URL + "|2026");
    assertThat(resolved.getExpansion().getMembers())
        .extracting(ValueSetMember::getCode)
        .containsExactly("22298006", "I21");
    verifyNoInteractions(terminologyService);
  }

  @Test
  void expandsASuppliedComposeThroughTheServiceWithTheCap() {
    final ValueSet supplied = suppliedValueSet(null);
    supplied
        .getCompose()
        .addInclude()
        .setSystem("http://snomed.info/sct")
        .addConcept()
        .setCode("1");
    when(terminologyService.expand(supplied, MAX_MEMBERS)).thenReturn(expansion(null));

    final ResolvedValueSet resolved =
        resolver().resolveSupplied(reference(URL), artefact(supplied));

    assertThat(resolved.getCanonicalKey()).isEqualTo(URL);
    assertThat(resolved.getExpansion().getMembers()).containsExactly(MEMBER);
  }

  @Test
  void rejectsASuppliedValueSetWithNeitherExpansionNorCompose() {
    final ValueSet supplied = suppliedValueSet(null);

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(supplied)))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContainingAll("'" + LABEL + "'", URL, "defines no membership")
        .satisfies(ValueSetMembershipResolverTest::assertContextInvalidIssue);
    verifyNoInteractions(terminologyService);
  }

  @Test
  void rejectsAnIncompleteSuppliedExpansionNamingTheContextParameter() {
    final ValueSet supplied = suppliedValueSet(null);
    supplied.getExpansion().setOffset(0);
    supplied.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("22298006");

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(supplied)))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContainingAll("'" + LABEL + "'", URL, "incomplete")
        .satisfies(ValueSetMembershipResolverTest::assertContextInvalidIssue);
  }

  @Test
  void rejectsASuppliedExpansionWhoseEntryDoesNotIdentifyAMember() {
    final ValueSet supplied = suppliedValueSet(null);
    supplied.getExpansion().addContains().setCode("22298006");

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(supplied)))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContainingAll("'" + LABEL + "'", URL, "does not identify a member")
        .satisfies(ValueSetMembershipResolverTest::assertContextInvalidIssue);
  }

  @Test
  void rejectsASuppliedExpansionOverTheCapNamingTheContextParameter() {
    serverConfiguration.getSqlQuery().setValueSetMaxMembers(1);
    final ValueSet supplied = suppliedValueSet(null);
    supplied.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("1");
    supplied.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("2");

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(supplied)))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContainingAll("'" + LABEL + "'", URL, "maximum of 1")
        .satisfies(ValueSetMembershipResolverTest::assertContextInvalidIssue);
  }

  @Test
  void rejectsASuppliedComposeWhenTerminologyIsDisabled() {
    serverConfiguration.setTerminology(TerminologyConfiguration.builder().enabled(false).build());
    final ValueSet supplied = suppliedValueSet(null);
    supplied
        .getCompose()
        .addInclude()
        .setSystem("http://snomed.info/sct")
        .addConcept()
        .setCode("1");

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(supplied)))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContainingAll("'" + LABEL + "'", URL, "disabled")
        .satisfies(ValueSetMembershipResolverTest::assertContextInvalidIssue);
    verifyNoInteractions(terminologyService);
  }

  @Test
  void translatesASuppliedComposeFailureIntoA422NamingTheContextParameter() {
    final ValueSet supplied = suppliedValueSet(null);
    supplied.getCompose().addInclude().setSystem("http://loinc.org").addConcept().setCode("1");
    when(terminologyService.expand(supplied, MAX_MEMBERS))
        .thenThrow(
            new ValueSetExpansionException("code system http://loinc.org is not in the store"));

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(supplied)))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContainingAll("'" + LABEL + "'", URL, "http://loinc.org is not in the store")
        .satisfies(ValueSetMembershipResolverTest::assertContextInvalidIssue);
  }

  // ---------------------------------------------------------------------------
  // Provenance (US5): one INFO line per resolution, naming the source.
  // ---------------------------------------------------------------------------

  @Test
  void logsTheTerminologyServerUrlAsTheSourceOfACanonicalInServerMode() {
    serverConfiguration.setTerminology(
        TerminologyConfiguration.builder().serverUrl(SERVER_URL).build());
    when(terminologyService.expand(URL, "2026", MAX_MEMBERS))
        .thenReturn(
            Optional.of(
                new ValueSetExpansion(
                    URL,
                    "2026.1",
                    EXPANSION_IDENTIFIER,
                    EXPANSION_TIMESTAMP,
                    List.of(SNOMED_VERSION, LOINC_VERSION),
                    List.of(MEMBER))));

    try (LogCapture capture = LogCapture.forClass(ValueSetMembershipResolver.class)) {
      resolver()
          .resolveCanonical(reference(URL + "|2026"), CanonicalReference.parse(URL + "|2026"));

      assertSingleProvenanceLine(
          capture,
          "Resolved value set '"
              + URL
              + "' (version 2026.1) from "
              + SERVER_URL
              + ": 1 members; expansion "
              + EXPANSION_IDENTIFIER
              + " at "
              + EXPANSION_TIMESTAMP
              + "; code systems ["
              + SNOMED_VERSION
              + ", "
              + LOINC_VERSION
              + "]");
    }
  }

  @Test
  void logsTheLocalStoreAsTheSourceOfACanonicalInLocalModeWithNoneForAbsentValues() {
    serverConfiguration.setTerminology(
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(LocalTerminologyConfiguration.builder().storagePath("/data/terminology").build())
            .build());
    when(terminologyService.expand(URL, null, MAX_MEMBERS))
        .thenReturn(
            Optional.of(new ValueSetExpansion(URL, null, null, null, List.of(), List.of())));

    try (LogCapture capture = LogCapture.forClass(ValueSetMembershipResolver.class)) {
      resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL));

      assertSingleProvenanceLine(
          capture,
          "Resolved value set '"
              + URL
              + "' (version none) from local store: 0 members; expansion none at none; code"
              + " systems []");
    }
  }

  @Test
  void logsTheContextParameterAsTheSourceOfASuppliedValueSet() {
    final ValueSet supplied = suppliedValueSet("2026");
    supplied.getExpansion().setIdentifier(EXPANSION_IDENTIFIER);
    supplied.getExpansion().setTimestampElement(new DateTimeType(EXPANSION_TIMESTAMP));
    supplied.getExpansion().addParameter().setName("version").setValue(new UriType(SNOMED_VERSION));
    supplied.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("22298006");
    supplied.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("57054005");

    try (LogCapture capture = LogCapture.forClass(ValueSetMembershipResolver.class)) {
      resolver().resolveSupplied(reference(URL + "|2026"), artefact(supplied));

      assertSingleProvenanceLine(
          capture,
          "Resolved value set '"
              + URL
              + "' (version 2026) from "
              + SuppliedArtefacts.CONTEXT_EXPRESSION
              + ": 2 members; expansion "
              + EXPANSION_IDENTIFIER
              + " at "
              + EXPANSION_TIMESTAMP
              + "; code systems ["
              + SNOMED_VERSION
              + "]");
    }
    verifyNoInteractions(terminologyService);
  }

  // ---------------------------------------------------------------------------
  // Fault contract (US4): the exact issue text and expression of every fault.
  // ---------------------------------------------------------------------------

  @Test
  void notFoundIsAnEmptyResultAfterExactlyOneExpansionAttempt() {
    // The 404 text belongs to SqlDependencyResolver, which reports an empty result as not found;
    // this collaborator only guarantees that it asked the terminology layer once and said nothing.
    when(terminologyService.expand(URL, null, MAX_MEMBERS)).thenReturn(Optional.empty());

    assertThat(resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isEmpty();
    verify(terminologyService, times(1)).expand(URL, null, MAX_MEMBERS);
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void serverErrorIssueCarriesTheLabelUrlAndDiagnosticsVerbatim() {
    when(terminologyService.expand(URL, null, MAX_MEMBERS))
        .thenThrow(
            new ValueSetExpansionException(
                "the terminology server returned HTTP 422: Unable to expand: too many codes"));

    assertThatThrownBy(
            () -> resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SubjectResolver.SUBJECT_EXPRESSION,
                    "The membership of the value set for label 'cvd_codes' (canonical URL"
                        + " 'http://example.org/ValueSet/cardiovascular-disease') could not be"
                        + " determined: the terminology server returned HTTP 422: Unable to expand:"
                        + " too many codes"));
  }

  @Test
  void unreachableServerIssueNamesOnlyTheConfiguredUrl() {
    // The core reports an unreachable server with its configured URL only; the resolver relays that
    // reason and must not append the cause, which names the host, port and socket failure.
    when(terminologyService.expand(URL, null, MAX_MEMBERS))
        .thenThrow(
            new ValueSetExpansionException(
                "terminology server http://tx.example.org/fhir could not be reached",
                new FhirClientConnectionException(
                    new ConnectException("Connection refused: tx.example.org/10.0.0.7:8080"))));

    assertThatThrownBy(
            () -> resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SubjectResolver.SUBJECT_EXPRESSION,
                    "The membership of the value set for label 'cvd_codes' (canonical URL"
                        + " 'http://example.org/ValueSet/cardiovascular-disease') could not be"
                        + " determined: terminology server http://tx.example.org/fhir could not be"
                        + " reached"))
        .satisfies(
            thrown ->
                assertThat(diagnosticsOf(thrown))
                    .doesNotContain("Connection refused", "10.0.0.7", "8080", "ConnectException"));
  }

  @Test
  void capIssueNamesTheLabelUrlAndTheConfiguredMaximum() {
    when(terminologyService.expand(URL, null, MAX_MEMBERS))
        .thenThrow(new ExpansionLimitExceededException(MAX_MEMBERS));

    assertThatThrownBy(
            () -> resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SubjectResolver.SUBJECT_EXPRESSION,
                    "The value set for label 'cvd_codes' (canonical URL"
                        + " 'http://example.org/ValueSet/cardiovascular-disease') has more than the"
                        + " maximum of 250 members permitted by"
                        + " pathling.sqlQuery.valueSetMaxMembers"));
  }

  @Test
  void suppliedExpansionWithAnOffsetIsReportedAsIncompleteAgainstTheContext() {
    final ValueSet supplied = suppliedValueSet(null);
    supplied.getExpansion().setOffset(0);
    supplied.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("22298006");

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(supplied)))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SuppliedArtefacts.CONTEXT_EXPRESSION,
                    "The membership of the supplied value set for label 'cvd_codes' (canonical URL"
                        + " 'http://example.org/ValueSet/cardiovascular-disease') could not be"
                        + " determined: the expansion is incomplete: it carries an offset"));
    verifyNoInteractions(terminologyService);
  }

  @Test
  void suppliedExpansionWithTotalOverItsEntriesIsReportedAsIncomplete() {
    final ValueSet supplied = suppliedValueSet(null);
    supplied.getExpansion().setTotal(5);
    supplied.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("1");
    supplied.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("2");

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(supplied)))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SuppliedArtefacts.CONTEXT_EXPRESSION,
                    "The membership of the supplied value set for label 'cvd_codes' (canonical URL"
                        + " 'http://example.org/ValueSet/cardiovascular-disease') could not be"
                        + " determined: the expansion is incomplete: total 5 exceeds 2 entries"));
  }

  @Test
  void suppliedEntryWithoutCodeIsReportedAsNotIdentifyingAMember() {
    final ValueSet supplied = suppliedValueSet(null);
    supplied.getExpansion().addContains().setSystem("http://snomed.info/sct");

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(supplied)))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SuppliedArtefacts.CONTEXT_EXPRESSION,
                    "The membership of the supplied value set for label 'cvd_codes' (canonical URL"
                        + " 'http://example.org/ValueSet/cardiovascular-disease') could not be"
                        + " determined: an entry does not identify a member: no code"));
  }

  @Test
  void suppliedEntryWithoutSystemIsReportedNamingTheCode() {
    final ValueSet supplied = suppliedValueSet(null);
    supplied.getExpansion().addContains().setCode("22298006");

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(supplied)))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SuppliedArtefacts.CONTEXT_EXPRESSION,
                    "The membership of the supplied value set for label 'cvd_codes' (canonical URL"
                        + " 'http://example.org/ValueSet/cardiovascular-disease') could not be"
                        + " determined: an entry does not identify a member: no system for code"
                        + " '22298006'"));
  }

  @Test
  void suppliedValueSetWithNeitherExpansionNorComposeDefinesNoMembership() {
    final ValueSet supplied = suppliedValueSet(null);

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(supplied)))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SuppliedArtefacts.CONTEXT_EXPRESSION,
                    "The supplied value set for label 'cvd_codes' (canonical URL"
                        + " 'http://example.org/ValueSet/cardiovascular-disease') defines no"
                        + " membership: it carries neither an expansion nor a compose"));
  }

  @Test
  void suppliedExpansionOverTheCapNamesTheSuppliedValueSetAndTheMaximum() {
    serverConfiguration.getSqlQuery().setValueSetMaxMembers(1);
    final ValueSet supplied = suppliedValueSet(null);
    supplied.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("1");
    supplied.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("2");

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(supplied)))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SuppliedArtefacts.CONTEXT_EXPRESSION,
                    "The supplied value set for label 'cvd_codes' (canonical URL"
                        + " 'http://example.org/ValueSet/cardiovascular-disease') has more than the"
                        + " maximum of 1 members permitted by"
                        + " pathling.sqlQuery.valueSetMaxMembers"));
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  @Nonnull
  private ValueSetMembershipResolver resolver() {
    return new ValueSetMembershipResolver(pathlingContext, serverConfiguration);
  }

  @Nonnull
  private static ViewArtifactReference reference(@Nonnull final String canonical) {
    return new ViewArtifactReference(LABEL, canonical);
  }

  @Nonnull
  private static ValueSetExpansion expansion(final String version) {
    return new ValueSetExpansion(URL, version, null, null, List.of(), List.of(MEMBER));
  }

  /**
   * Builds a supplied ValueSet resource carrying the URL and the given version, with no content.
   */
  @Nonnull
  private static ValueSet suppliedValueSet(final String version) {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(URL);
    valueSet.setVersion(version);
    return valueSet;
  }

  /** Wraps a ValueSet resource as the context entry the parser would produce for it. */
  @Nonnull
  private static SuppliedArtefact artefact(@Nonnull final ValueSet valueSet) {
    return SuppliedArtefact.ofValueSet(valueSet.getUrl(), valueSet.getVersion(), valueSet);
  }

  /** Asserts that the exception carries one invalid issue whose expression is the subject. */
  private static void assertSubjectInvalidIssue(@Nonnull final Throwable thrown) {
    final UnprocessableEntityException exception = (UnprocessableEntityException) thrown;
    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
    assertThat(outcome.getIssue()).hasSize(1);
    assertThat(outcome.getIssueFirstRep().getCode()).isEqualTo(IssueType.INVALID);
    assertThat(outcome.getIssueFirstRep().getExpression())
        .extracting(expression -> expression.getValue())
        .containsExactly(SubjectResolver.SUBJECT_EXPRESSION);
  }

  /** Asserts that the exception carries one invalid issue whose expression is the context. */
  private static void assertContextInvalidIssue(@Nonnull final Throwable thrown) {
    final UnprocessableEntityException exception = (UnprocessableEntityException) thrown;
    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
    assertThat(outcome.getIssue()).hasSize(1);
    assertThat(outcome.getIssueFirstRep().getCode()).isEqualTo(IssueType.INVALID);
    assertThat(outcome.getIssueFirstRep().getExpression())
        .extracting(expression -> expression.getValue())
        .containsExactly(SuppliedArtefacts.CONTEXT_EXPRESSION);
  }

  /**
   * Asserts that the exception carries exactly one invalid issue with the given expression, whose
   * diagnostics and exception message both equal the given text verbatim.
   */
  private static void assertIssue(
      @Nonnull final Throwable thrown,
      @Nonnull final String expression,
      @Nonnull final String diagnostics) {
    final UnprocessableEntityException exception = (UnprocessableEntityException) thrown;
    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
    assertThat(outcome.getIssue()).hasSize(1);
    final OperationOutcomeIssueComponent issue = outcome.getIssueFirstRep();
    assertThat(issue.getCode()).isEqualTo(IssueType.INVALID);
    assertThat(issue.getExpression())
        .extracting(value -> value.getValue())
        .containsExactly(expression);
    assertThat(issue.getDiagnostics()).isEqualTo(diagnostics);
    assertThat(exception.getMessage()).isEqualTo(diagnostics);
  }

  /** The diagnostics of the exception's single issue. */
  @Nonnull
  private static String diagnosticsOf(@Nonnull final Throwable thrown) {
    final UnprocessableEntityException exception = (UnprocessableEntityException) thrown;
    return ((OperationOutcome) exception.getOperationOutcome()).getIssueFirstRep().getDiagnostics();
  }

  /**
   * Asserts that the capture holds exactly one event, at INFO, whose formatted message is the given
   * provenance line verbatim.
   */
  private static void assertSingleProvenanceLine(
      @Nonnull final LogCapture capture, @Nonnull final String line) {
    assertThat(capture.events())
        .singleElement()
        .satisfies(
            event -> {
              assertThat(event.getLevel()).isEqualTo(Level.INFO);
              assertThat(event.getFormattedMessage()).isEqualTo(line);
            });
  }
}
