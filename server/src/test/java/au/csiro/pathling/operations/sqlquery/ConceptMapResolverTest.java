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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.operations.sql.SubjectResolver;
import au.csiro.pathling.operations.sql.SuppliedArtefact;
import au.csiro.pathling.operations.sql.SuppliedArtefacts;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContentException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapLimitExceededException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapLookupException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapRelationship;
import au.csiro.pathling.terminology.conceptmap.ConceptMapVersionException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import au.csiro.pathling.util.LogCapture;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import ch.qos.logback.classic.Level;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.net.ConnectException;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConceptMapResolver} over a mocked {@link TerminologyService}: the arguments
 * passed to {@code readConceptMap}, the node built from the content it returns, the not-found and
 * disabled short circuits, a supplied concept map converted without the terminology layer, and the
 * exact status, issue text and expression of every fault the terminology layer reports.
 *
 * @author John Grimes
 */
class ConceptMapResolverTest {

  private static final String URL = "http://example.org/ConceptMap/sct-to-icd10";

  private static final String LABEL = "sct_to_icd10";

  private static final int MAX_MAPPINGS = 250;

  private static final String IMPLICIT_URL = "http://snomed.info/sct?fhir_cm=900000000000526001";

  private static final String IMPLICIT_LABEL = "replaced_by";

  /** The shared not-found sentence for {@link #IMPLICIT_URL} under {@link #IMPLICIT_LABEL}. */
  private static final String IMPLICIT_NOT_FOUND =
      "Failed to resolve the dependency for label '"
          + IMPLICIT_LABEL
          + "' with reference '"
          + IMPLICIT_URL
          + "': no ViewDefinition, SQLView, external table, concept map or value set matches that"
          + " canonical URL";

  private static final ConceptMapping MAPPING =
      new ConceptMapping(
          "http://snomed.info/sct",
          null,
          "22298006",
          "Myocardial infarction",
          "http://hl7.org/fhir/sid/icd-10",
          "2019",
          "I21",
          "Acute myocardial infarction",
          ConceptMapRelationship.EQUIVALENT);

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
    serverConfiguration.getSqlQuery().setConceptMapMaxMappings(MAX_MAPPINGS);
  }

  @Test
  void passesTheUrlThePinnedVersionAndTheCapToTheService() {
    when(terminologyService.readConceptMap(URL, "2026", MAX_MAPPINGS))
        .thenReturn(Optional.of(content("2026")));

    final Optional<ResolvedConceptMap> resolved =
        resolver()
            .resolveCanonical(reference(URL + "|2026"), CanonicalReference.parse(URL + "|2026"));

    assertThat(resolved).isPresent();
    assertThat(resolved.get().getContent().getMappings()).containsExactly(MAPPING);
    verify(terminologyService).readConceptMap(URL, "2026", MAX_MAPPINGS);
  }

  @Test
  void passesANullVersionForAnUnpinnedReference() {
    when(terminologyService.readConceptMap(URL, null, MAX_MAPPINGS))
        .thenReturn(Optional.of(content(null)));

    final Optional<ResolvedConceptMap> resolved =
        resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL));

    assertThat(resolved).isPresent();
    verify(terminologyService).readConceptMap(URL, null, MAX_MAPPINGS);
  }

  @Test
  void keysTheNodeByTheReferenceCanonicalAsWritten() {
    // The terminology layer may report a different version than the one pinned, or none; the node
    // is keyed by the reference so a second reference to the same string reuses it.
    when(terminologyService.readConceptMap(URL, "2026", MAX_MAPPINGS))
        .thenReturn(Optional.of(content("2026.1")));

    final ResolvedConceptMap resolved =
        resolver()
            .resolveCanonical(reference(URL + "|2026"), CanonicalReference.parse(URL + "|2026"))
            .orElseThrow();

    assertThat(resolved.getCanonicalKey()).isEqualTo(URL + "|2026");
    assertThat(resolved.getContent().getVersion()).isEqualTo("2026.1");
  }

  @Test
  void returnsEmptyWhenTheServiceCannotResolveTheCanonical() {
    when(terminologyService.readConceptMap(anyString(), isNull(), anyInt()))
        .thenReturn(Optional.empty());

    assertThat(resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isEmpty();
  }

  @Test
  void returnsEmptyWithoutCallingTheServiceWhenTerminologyIsDisabled() {
    serverConfiguration.setTerminology(TerminologyConfiguration.builder().enabled(false).build());

    assertThat(resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isEmpty();
    verifyNoInteractions(terminologyService);
  }

  // ---------------------------------------------------------------------------
  // A SNOMED CT implicit concept map URL.
  // ---------------------------------------------------------------------------

  @Test
  void implicitConceptMapNotFoundInLocalModeIsA404WithTheSharedSentence() {
    serverConfiguration.setTerminology(
        TerminologyConfiguration.builder().mode(TerminologyMode.LOCAL).build());
    when(terminologyService.readConceptMap(IMPLICIT_URL, null, MAX_MAPPINGS))
        .thenReturn(Optional.empty());

    assertThatThrownBy(
            () ->
                resolver()
                    .resolveCanonical(implicitReference(), CanonicalReference.parse(IMPLICIT_URL)))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessage(IMPLICIT_NOT_FOUND);
    verify(terminologyService).readConceptMap(IMPLICIT_URL, null, MAX_MAPPINGS);
  }

  @Test
  void implicitConceptMapNotFoundInServerModeIsA404NamingLocalMode() {
    serverConfiguration.setTerminology(
        TerminologyConfiguration.builder().mode(TerminologyMode.SERVER).build());
    when(terminologyService.readConceptMap(IMPLICIT_URL, null, MAX_MAPPINGS))
        .thenReturn(Optional.empty());

    assertThatThrownBy(
            () ->
                resolver()
                    .resolveCanonical(implicitReference(), CanonicalReference.parse(IMPLICIT_URL)))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessage(
            IMPLICIT_NOT_FOUND
                + "; SNOMED CT implicit concept maps are resolved only in local terminology mode");
  }

  @Test
  void implicitConceptMapWithTerminologyDisabledIsA404WithTheSharedSentence() {
    serverConfiguration.setTerminology(TerminologyConfiguration.builder().enabled(false).build());

    assertThatThrownBy(
            () ->
                resolver()
                    .resolveCanonical(implicitReference(), CanonicalReference.parse(IMPLICIT_URL)))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessage(IMPLICIT_NOT_FOUND);
    verifyNoInteractions(terminologyService);
  }

  @Test
  void resolvesAnImplicitConceptMapTheServiceReturns() {
    when(terminologyService.readConceptMap(IMPLICIT_URL, null, MAX_MAPPINGS))
        .thenReturn(Optional.of(new ConceptMapContent(IMPLICIT_URL, null, List.of(MAPPING))));

    final Optional<ResolvedConceptMap> resolved =
        resolver().resolveCanonical(implicitReference(), CanonicalReference.parse(IMPLICIT_URL));

    assertThat(resolved).isPresent();
    assertThat(resolved.get().getCanonicalKey()).isEqualTo(IMPLICIT_URL);
  }

  // ---------------------------------------------------------------------------
  // A supplied concept map.
  // ---------------------------------------------------------------------------

  @Test
  void resolvesASuppliedConceptMapFromTheResourceWithoutCallingTheService() {
    final ConceptMap conceptMap = suppliedConceptMap("2026");

    final ResolvedConceptMap resolved =
        resolver().resolveSupplied(reference(URL + "|2026"), artefact(conceptMap));

    assertThat(resolved.getContent())
        .isEqualTo(ConceptMapContent.fromResource(conceptMap, MAX_MAPPINGS));
    assertThat(resolved.getContent().getMappings()).hasSize(2);
    verifyNoInteractions(terminologyService);
  }

  @Test
  void keysASuppliedConceptMapByItsUrlAndVersion() {
    final ResolvedConceptMap resolved =
        resolver().resolveSupplied(reference(URL), artefact(suppliedConceptMap("2026")));

    assertThat(resolved.getCanonicalKey()).isEqualTo(URL + "|2026");
  }

  @Test
  void keysAnUnversionedSuppliedConceptMapByItsUrl() {
    final ResolvedConceptMap resolved =
        resolver().resolveSupplied(reference(URL), artefact(suppliedConceptMap(null)));

    assertThat(resolved.getCanonicalKey()).isEqualTo(URL);
  }

  @Test
  void resolvesASuppliedConceptMapWhenTerminologyIsDisabled() {
    serverConfiguration.setTerminology(TerminologyConfiguration.builder().enabled(false).build());
    final ConceptMap conceptMap = suppliedConceptMap("2026");

    final ResolvedConceptMap resolved =
        resolver().resolveSupplied(reference(URL), artefact(conceptMap));

    assertThat(resolved.getContent())
        .isEqualTo(ConceptMapContent.fromResource(conceptMap, MAX_MAPPINGS));
    verifyNoInteractions(terminologyService);
  }

  @Test
  void logsTheProvenanceOfASuppliedConceptMapAsTheContext() {
    try (LogCapture capture = LogCapture.forClass(ConceptMapResolver.class)) {
      resolver().resolveSupplied(reference(URL), artefact(suppliedConceptMap("2026")));

      assertThat(capture.events())
          .singleElement()
          .satisfies(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.INFO);
                assertThat(event.getFormattedMessage())
                    .isEqualTo(
                        "Resolved concept map '"
                            + URL
                            + "' (version 2026) from context: 2 mappings");
              });
    }
  }

  @Test
  void logsTheProvenanceOfACanonicalResolvedInServerModeAsTheConfiguredServerUrl() {
    // The version logged is the one the terminology layer resolved, not the one pinned.
    serverConfiguration.setTerminology(
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.SERVER)
            .serverUrl("http://tx.example.org/fhir")
            .build());
    when(terminologyService.readConceptMap(URL, "2026", MAX_MAPPINGS))
        .thenReturn(Optional.of(content("2026.1")));

    try (LogCapture capture = LogCapture.forClass(ConceptMapResolver.class)) {
      resolver()
          .resolveCanonical(reference(URL + "|2026"), CanonicalReference.parse(URL + "|2026"));

      assertThat(capture.events())
          .singleElement()
          .satisfies(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.INFO);
                assertThat(event.getFormattedMessage())
                    .isEqualTo(
                        "Resolved concept map '"
                            + URL
                            + "' (version 2026.1) from http://tx.example.org/fhir: 1 mappings");
              });
    }
  }

  @Test
  void logsTheProvenanceOfAnUnversionedCanonicalResolvedInLocalModeAsTheLocalStore() {
    serverConfiguration.setTerminology(
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .serverUrl("http://tx.example.org/fhir")
            .build());
    when(terminologyService.readConceptMap(URL, null, MAX_MAPPINGS))
        .thenReturn(Optional.of(content(null)));

    try (LogCapture capture = LogCapture.forClass(ConceptMapResolver.class)) {
      resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL));

      assertThat(capture.events())
          .singleElement()
          .satisfies(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.INFO);
                assertThat(event.getFormattedMessage())
                    .isEqualTo(
                        "Resolved concept map '"
                            + URL
                            + "' (version none) from local store: 1 mappings");
              });
    }
  }

  @Test
  void logsNothingWhenNoConceptMapIsResolved() {
    when(terminologyService.readConceptMap(URL, null, MAX_MAPPINGS)).thenReturn(Optional.empty());

    try (LogCapture capture = LogCapture.forClass(ConceptMapResolver.class)) {
      resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL));

      assertThat(capture.events()).isEmpty();
    }
  }

  @Test
  void reportsUnrepresentableSuppliedContentAsA422AtTheContext() {
    final ConceptMap conceptMap = suppliedConceptMap("2026");
    conceptMap.getGroupFirstRep().setSource(null);

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(conceptMap)))
        .isInstanceOf(UnprocessableEntityException.class)
        .isNotInstanceOf(IndeterminateLookupException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SuppliedArtefacts.CONTEXT_EXPRESSION,
                    "The mappings of the supplied concept map for label 'sct_to_icd10' (canonical"
                        + " URL 'http://example.org/ConceptMap/sct-to-icd10') could not be"
                        + " determined: a group has no source system"));
  }

  @Test
  void reportsASuppliedConceptMapOverTheCapAsA422AtTheContext() {
    serverConfiguration.getSqlQuery().setConceptMapMaxMappings(1);

    assertThatThrownBy(
            () -> resolver().resolveSupplied(reference(URL), artefact(suppliedConceptMap("2026"))))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SuppliedArtefacts.CONTEXT_EXPRESSION,
                    "The supplied concept map for label 'sct_to_icd10' (canonical URL"
                        + " 'http://example.org/ConceptMap/sct-to-icd10') has more than the maximum"
                        + " of 1 mappings permitted by pathling.sqlQuery.conceptMapMaxMappings"));
  }

  // ---------------------------------------------------------------------------
  // Fault contract (US5): the exact issue text and expression of every fault.
  // ---------------------------------------------------------------------------

  @Test
  void unrepresentableContentFromTheTerminologyLayerIsA422AtTheSubject() {
    when(terminologyService.readConceptMap(URL, "2026", MAX_MAPPINGS))
        .thenThrow(
            new ConceptMapContentException(
                "the mapping for source code '73211009' depends on other elements (dependsOn)"));

    assertThatThrownBy(
            () ->
                resolver()
                    .resolveCanonical(
                        reference(URL + "|2026"), CanonicalReference.parse(URL + "|2026")))
        .isInstanceOf(UnprocessableEntityException.class)
        .isNotInstanceOf(IndeterminateLookupException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SubjectResolver.SUBJECT_EXPRESSION,
                    "The mappings of the concept map for label 'sct_to_icd10' (canonical URL"
                        + " 'http://example.org/ConceptMap/sct-to-icd10') could not be determined:"
                        + " the mapping for source code '73211009' depends on other elements"
                        + " (dependsOn)"));
  }

  @Test
  void aChosenConceptMapThatCannotBeReadIsA422CarryingTheServersReasonOnly() {
    when(terminologyService.readConceptMap(URL, null, MAX_MAPPINGS))
        .thenThrow(
            new ConceptMapContentException(
                "terminology server http://tx.example.org/fhir could not be reached",
                new FhirClientConnectionException(
                    new ConnectException("Connection refused: tx.example.org/10.0.0.7:8080"))));

    assertThatThrownBy(
            () -> resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isInstanceOf(UnprocessableEntityException.class)
        .isNotInstanceOf(IndeterminateLookupException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SubjectResolver.SUBJECT_EXPRESSION,
                    "The mappings of the concept map for label 'sct_to_icd10' (canonical URL"
                        + " 'http://example.org/ConceptMap/sct-to-icd10') could not be determined:"
                        + " terminology server http://tx.example.org/fhir could not be reached"))
        .satisfies(
            thrown ->
                assertThat(diagnosticsOf(thrown))
                    .doesNotContain("Connection refused", "10.0.0.7", "8080", "ConnectException"));
  }

  @Test
  void aConceptMapOverTheCapFromTheTerminologyLayerIsA422AtTheSubject() {
    when(terminologyService.readConceptMap(URL, null, MAX_MAPPINGS))
        .thenThrow(new ConceptMapLimitExceededException(MAX_MAPPINGS));

    assertThatThrownBy(
            () -> resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isInstanceOf(UnprocessableEntityException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SubjectResolver.SUBJECT_EXPRESSION,
                    "The concept map for label 'sct_to_icd10' (canonical URL"
                        + " 'http://example.org/ConceptMap/sct-to-icd10') has more than the maximum"
                        + " of 250 mappings permitted by pathling.sqlQuery.conceptMapMaxMappings"));
  }

  @Test
  void anUndeterminableVersionIsA404NamingTheLabelTheReferenceAndTheReason() {
    when(terminologyService.readConceptMap(URL, null, MAX_MAPPINGS))
        .thenThrow(
            new ConceptMapVersionException(
                "unable to determine the latest version of the ConceptMaps with the URL " + URL));

    assertThatThrownBy(
            () -> resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessage(
            "Failed to resolve the dependency for label 'sct_to_icd10' with reference"
                + " 'http://example.org/ConceptMap/sct-to-icd10': the version to use cannot be"
                + " determined: unable to determine the latest version of the ConceptMaps with the"
                + " URL http://example.org/ConceptMap/sct-to-icd10");
  }

  @Test
  void aFailedSearchIsAnIndeterminateLookupNamingTheOperationThatFailed() {
    when(terminologyService.readConceptMap(URL, "2026", MAX_MAPPINGS))
        .thenThrow(new ConceptMapLookupException("the terminology server returned HTTP 500: boom"));

    assertThatThrownBy(
            () ->
                resolver()
                    .resolveCanonical(
                        reference(URL + "|2026"), CanonicalReference.parse(URL + "|2026")))
        .isInstanceOf(IndeterminateLookupException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SubjectResolver.SUBJECT_EXPRESSION,
                    "Failed to resolve the dependency for label 'sct_to_icd10' with reference"
                        + " 'http://example.org/ConceptMap/sct-to-icd10|2026': searching for it as"
                        + " a concept map failed: the terminology server returned HTTP 500: boom"))
        .satisfies(
            thrown ->
                assertThat(((IndeterminateLookupException) thrown).getIssue().getDiagnostics())
                    .isEqualTo(diagnosticsOf(thrown)));
  }

  @Test
  void anUnreachableServerDuringTheSearchNamesOnlyTheConfiguredUrl() {
    when(terminologyService.readConceptMap(URL, null, MAX_MAPPINGS))
        .thenThrow(
            new ConceptMapLookupException(
                "terminology server http://tx.example.org/fhir could not be reached",
                new FhirClientConnectionException(
                    new ConnectException("Connection refused: tx.example.org/10.0.0.7:8080"))));

    assertThatThrownBy(
            () -> resolver().resolveCanonical(reference(URL), CanonicalReference.parse(URL)))
        .isInstanceOf(IndeterminateLookupException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SubjectResolver.SUBJECT_EXPRESSION,
                    "Failed to resolve the dependency for label 'sct_to_icd10' with reference"
                        + " 'http://example.org/ConceptMap/sct-to-icd10': searching for it as a"
                        + " concept map failed: terminology server http://tx.example.org/fhir could"
                        + " not be reached"))
        .satisfies(
            thrown ->
                assertThat(diagnosticsOf(thrown))
                    .doesNotContain("Connection refused", "10.0.0.7", "8080", "ConnectException"));
  }

  @Test
  void aFailedSearchForAnImplicitConceptMapIsIndeterminateRatherThanNotFound() {
    when(terminologyService.readConceptMap(IMPLICIT_URL, null, MAX_MAPPINGS))
        .thenThrow(new ConceptMapLookupException("the terminology server returned HTTP 503: down"));

    assertThatThrownBy(
            () ->
                resolver()
                    .resolveCanonical(implicitReference(), CanonicalReference.parse(IMPLICIT_URL)))
        .isInstanceOf(IndeterminateLookupException.class)
        .hasMessage(
            "Failed to resolve the dependency for label 'replaced_by' with reference '"
                + IMPLICIT_URL
                + "': searching for it as a concept map failed: the terminology server returned"
                + " HTTP 503: down");
  }

  @Test
  void aPinnedImplicitConceptMapInLocalModeIsA422AtTheSubject() {
    serverConfiguration.setTerminology(
        TerminologyConfiguration.builder().mode(TerminologyMode.LOCAL).build());
    when(terminologyService.readConceptMap(IMPLICIT_URL, "20260131", MAX_MAPPINGS))
        .thenThrow(
            new ConceptMapContentException(
                "cannot determine which version to use: an implicit concept map URL carries its"
                    + " version in its base"));

    assertThatThrownBy(
            () ->
                resolver()
                    .resolveCanonical(
                        new ViewArtifactReference(IMPLICIT_LABEL, IMPLICIT_URL + "|20260131"),
                        CanonicalReference.parse(IMPLICIT_URL + "|20260131")))
        .isInstanceOf(UnprocessableEntityException.class)
        .isNotInstanceOf(IndeterminateLookupException.class)
        .satisfies(
            thrown ->
                assertIssue(
                    thrown,
                    SubjectResolver.SUBJECT_EXPRESSION,
                    "The mappings of the concept map for label 'replaced_by' (canonical URL '"
                        + IMPLICIT_URL
                        + "') could not be determined: cannot determine which version to use: an"
                        + " implicit concept map URL carries its version in its base"));
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  @Nonnull
  private ConceptMapResolver resolver() {
    return new ConceptMapResolver(pathlingContext, serverConfiguration);
  }

  @Nonnull
  private static ViewArtifactReference reference(@Nonnull final String canonical) {
    return new ViewArtifactReference(LABEL, canonical);
  }

  @Nonnull
  private static ViewArtifactReference implicitReference() {
    return new ViewArtifactReference(IMPLICIT_LABEL, IMPLICIT_URL);
  }

  @Nonnull
  private static ConceptMapContent content(@Nullable final String version) {
    return new ConceptMapContent(URL, version, List.of(MAPPING));
  }

  /** A supplied ConceptMap with one mapped target and one unmatched target. */
  @Nonnull
  private static ConceptMap suppliedConceptMap(@Nullable final String version) {
    final ConceptMap conceptMap = new ConceptMap();
    conceptMap.setUrl(URL);
    conceptMap.setVersion(version);
    final ConceptMapGroupComponent group = conceptMap.addGroup();
    group.setSource("http://snomed.info/sct");
    group.setTarget("http://hl7.org/fhir/sid/icd-10");
    group
        .addElement()
        .setCode("22298006")
        .setDisplay("Myocardial infarction")
        .addTarget()
        .setCode("I21.9")
        .setDisplay("Acute myocardial infarction, unspecified")
        .setEquivalence(ConceptMapEquivalence.EQUIVALENT);
    group
        .addElement()
        .setCode("38341003")
        .addTarget()
        .setEquivalence(ConceptMapEquivalence.UNMATCHED);
    return conceptMap;
  }

  @Nonnull
  private static SuppliedArtefact artefact(@Nonnull final ConceptMap conceptMap) {
    return SuppliedArtefact.ofConceptMap(conceptMap.getUrl(), conceptMap.getVersion(), conceptMap);
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
}
