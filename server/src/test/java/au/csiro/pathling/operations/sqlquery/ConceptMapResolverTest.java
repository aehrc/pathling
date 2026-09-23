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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapRelationship;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConceptMapResolver} over a mocked {@link TerminologyService}: the arguments
 * passed to {@code readConceptMap}, the node built from the content it returns, and the not-found
 * and disabled short circuits.
 *
 * @author John Grimes
 */
class ConceptMapResolverTest {

  private static final String URL = "http://example.org/ConceptMap/sct-to-icd10";

  private static final String LABEL = "sct_to_icd10";

  private static final int MAX_MAPPINGS = 250;

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
  private static ConceptMapContent content(@Nullable final String version) {
    return new ConceptMapContent(URL, version, List.of(MAPPING));
  }
}
