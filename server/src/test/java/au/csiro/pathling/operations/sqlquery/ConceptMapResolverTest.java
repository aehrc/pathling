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
import au.csiro.pathling.operations.sql.SuppliedArtefact;
import au.csiro.pathling.operations.sql.SuppliedArtefacts;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapRelationship;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import au.csiro.pathling.util.LogCapture;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import ch.qos.logback.classic.Level;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConceptMapResolver} over a mocked {@link TerminologyService}: the arguments
 * passed to {@code readConceptMap}, the node built from the content it returns, the not-found and
 * disabled short circuits, and a supplied concept map converted without the terminology layer.
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
  void reportsUnrepresentableSuppliedContentAsA422AtTheContext() {
    final ConceptMap conceptMap = suppliedConceptMap("2026");
    conceptMap.getGroupFirstRep().setSource(null);

    assertThatThrownBy(() -> resolver().resolveSupplied(reference(URL), artefact(conceptMap)))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContainingAll(LABEL, URL, "a group has no source system")
        .satisfies(ConceptMapResolverTest::assertContextInvalidIssue);
  }

  @Test
  void reportsASuppliedConceptMapOverTheCapAsA422AtTheContext() {
    serverConfiguration.getSqlQuery().setConceptMapMaxMappings(1);

    assertThatThrownBy(
            () -> resolver().resolveSupplied(reference(URL), artefact(suppliedConceptMap("2026"))))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessageContainingAll(LABEL, URL, "pathling.sqlQuery.conceptMapMaxMappings", "1")
        .satisfies(ConceptMapResolverTest::assertContextInvalidIssue);
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
}
