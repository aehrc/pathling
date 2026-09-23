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

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ContextArtefactParser} covering the {@code ValueSet} and {@code ConceptMap}
 * kinds of {@code context} entry: each parses to its own kind of artefact and must carry a {@code
 * url}; neither is inspected for its content at parse time; both are named among the admissible
 * kinds when an entry of another kind is rejected.
 *
 * @author John Grimes
 */
class ContextArtefactParserTest {

  private static final String URL = "http://example.org/ValueSet/cardiovascular-disease";

  private static final String CONCEPT_MAP_URL = "http://example.org/ConceptMap/sct-to-icd10";

  private ContextArtefactParser parser;

  @BeforeEach
  void setUp() {
    parser = new ContextArtefactParser(mock(FhirViewValidator.class));
  }

  @Test
  void parsesAValueSetWithAUrlAsAValueSetArtefact() {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(URL);
    valueSet.setVersion("2026");
    valueSet.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("22298006");

    final SuppliedArtefacts artefacts = parser.parse(List.of(valueSet));

    final Optional<SuppliedArtefact> matched = artefacts.match(URL, "2026");
    assertThat(matched).isPresent();
    assertThat(matched.get().isValueSet()).isTrue();
    assertThat(matched.get().getVersion()).isEqualTo("2026");
    assertThat(matched.get().getValueSet()).isSameAs(valueSet);
  }

  @Test
  void rejectsAValueSetWithoutAUrl() {
    final ValueSet valueSet = new ValueSet();
    valueSet.getExpansion().addContains().setSystem("http://snomed.info/sct").setCode("22298006");

    assertThatThrownBy(() -> parser.parse(List.of(valueSet)))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("ValueSet", "must carry a url")
        .satisfies(ContextArtefactParserTest::assertContextExpression);
  }

  @Test
  void doesNotInspectAValueSetForAnExpansionOrAComposeAtParseTime() {
    // Whether the resource defines a membership is decided at resolution, where the label is
    // known, so a bare ValueSet passes the parser.
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(URL);

    final SuppliedArtefacts artefacts = parser.parse(List.of(valueSet));

    assertThat(artefacts.match(URL, null)).isPresent();
    assertThat(artefacts.match(URL, null).get().isValueSet()).isTrue();
  }

  @Test
  void namesTheFourAdmissibleKindsWhenRejectingAnEntryOfAnotherKind() {
    assertThatThrownBy(() -> parser.parse(List.of(new Patient())))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("ViewDefinition", "SQLView", "ValueSet", "ConceptMap", "Patient")
        .satisfies(ContextArtefactParserTest::assertContextExpression);
  }

  @Test
  void parsesAConceptMapWithAUrlAsAConceptMapArtefact() {
    final ConceptMap conceptMap = new ConceptMap();
    conceptMap.setUrl(CONCEPT_MAP_URL);
    conceptMap.setVersion("2026");

    final SuppliedArtefacts artefacts = parser.parse(List.of(conceptMap));

    final Optional<SuppliedArtefact> matched = artefacts.match(CONCEPT_MAP_URL, "2026");
    assertThat(matched).isPresent();
    assertThat(matched.get().isConceptMap()).isTrue();
    assertThat(matched.get().isValueSet()).isFalse();
    assertThat(matched.get().getVersion()).isEqualTo("2026");
    assertThat(matched.get().getConceptMap()).isSameAs(conceptMap);
  }

  @Test
  void rejectsAConceptMapWithoutAUrl() {
    final ConceptMap conceptMap = new ConceptMap();
    conceptMap.addGroup().setSource("http://snomed.info/sct");

    assertThatThrownBy(() -> parser.parse(List.of(conceptMap)))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContainingAll("ConceptMap", "must carry a url")
        .satisfies(ContextArtefactParserTest::assertContextExpression);
  }

  @Test
  void doesNotRejectAConceptMapCarryingDependsOnAtParseTime() {
    // Whether the content can be represented is decided at resolution, where the label is known.
    final ConceptMap conceptMap = new ConceptMap();
    conceptMap.setUrl(CONCEPT_MAP_URL);
    final ConceptMapGroupComponent group = conceptMap.addGroup();
    group.setSource("http://snomed.info/sct");
    group.setTarget("http://hl7.org/fhir/sid/icd-10");
    final TargetElementComponent target = group.addElement().setCode("22298006").addTarget();
    target.setCode("I21.9").setEquivalence(ConceptMapEquivalence.EQUIVALENT);
    target.addDependsOn().setProperty("http://example.org/property").setValue("x");

    final SuppliedArtefacts artefacts = parser.parse(List.of(conceptMap));

    assertThat(artefacts.match(CONCEPT_MAP_URL, null)).isPresent();
    assertThat(artefacts.match(CONCEPT_MAP_URL, null).get().isConceptMap()).isTrue();
  }

  /** Asserts that the exception carries one issue whose expression names the context parameter. */
  private static void assertContextExpression(@Nonnull final Throwable thrown) {
    final InvalidRequestException exception = (InvalidRequestException) thrown;
    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
    assertThat(outcome.getIssue()).hasSize(1);
    assertThat(outcome.getIssueFirstRep().getExpression())
        .extracting(expression -> expression.getValue())
        .containsExactly(SuppliedArtefacts.CONTEXT_EXPRESSION);
  }
}
