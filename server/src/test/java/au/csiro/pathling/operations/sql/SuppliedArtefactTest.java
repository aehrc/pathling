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

import au.csiro.pathling.views.FhirView;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SuppliedArtefact}, covering the four backing kinds a {@code context} entry
 * may take and the guards on their accessors.
 *
 * @author John Grimes
 */
class SuppliedArtefactTest {

  private static final String URL = "http://example.org/ValueSet/cvd";

  private static final String CONCEPT_MAP_URL = "http://example.org/ConceptMap/sct-to-icd10";

  @Test
  void ofValueSetReportsOnlyIsValueSet() {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(URL);

    final SuppliedArtefact artefact = SuppliedArtefact.ofValueSet(URL, "2026", valueSet);

    assertThat(artefact.isValueSet()).isTrue();
    assertThat(artefact.isView()).isFalse();
    assertThat(artefact.isSqlView()).isFalse();
    assertThat(artefact.getUrl()).isEqualTo(URL);
    assertThat(artefact.getVersion()).isEqualTo("2026");
    assertThat(artefact.getValueSet()).isSameAs(valueSet);
  }

  @Test
  void ofViewReportsOnlyIsView() {
    final SuppliedArtefact artefact = SuppliedArtefact.ofView(URL, null, mock(FhirView.class));

    assertThat(artefact.isView()).isTrue();
    assertThat(artefact.isSqlView()).isFalse();
    assertThat(artefact.isValueSet()).isFalse();
    assertThat(artefact.isConceptMap()).isFalse();
  }

  @Test
  void ofSqlViewReportsOnlyIsSqlView() {
    final SuppliedArtefact artefact = SuppliedArtefact.ofSqlView(URL, null, new Library());

    assertThat(artefact.isSqlView()).isTrue();
    assertThat(artefact.isView()).isFalse();
    assertThat(artefact.isValueSet()).isFalse();
    assertThat(artefact.isConceptMap()).isFalse();
  }

  @Test
  void getValueSetOnAViewThrows() {
    final SuppliedArtefact artefact = SuppliedArtefact.ofView(URL, null, mock(FhirView.class));

    assertThatThrownBy(artefact::getValueSet)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(URL)
        .hasMessageContaining("ValueSet");
  }

  @Test
  void getViewOnAValueSetThrows() {
    final SuppliedArtefact artefact = SuppliedArtefact.ofValueSet(URL, null, new ValueSet());

    assertThatThrownBy(artefact::getView)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(URL)
        .hasMessageContaining("ViewDefinition");
  }

  @Test
  void getSqlViewOnAValueSetThrows() {
    final SuppliedArtefact artefact = SuppliedArtefact.ofValueSet(URL, null, new ValueSet());

    assertThatThrownBy(artefact::getSqlView)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(URL)
        .hasMessageContaining("SQLView");
  }

  @Test
  void ofConceptMapReportsOnlyIsConceptMap() {
    final ConceptMap conceptMap = new ConceptMap();
    conceptMap.setUrl(CONCEPT_MAP_URL);

    final SuppliedArtefact artefact =
        SuppliedArtefact.ofConceptMap(CONCEPT_MAP_URL, "2026", conceptMap);

    assertThat(artefact.isConceptMap()).isTrue();
    assertThat(artefact.isValueSet()).isFalse();
    assertThat(artefact.isView()).isFalse();
    assertThat(artefact.isSqlView()).isFalse();
    assertThat(artefact.getUrl()).isEqualTo(CONCEPT_MAP_URL);
    assertThat(artefact.getVersion()).isEqualTo("2026");
    assertThat(artefact.getConceptMap()).isSameAs(conceptMap);
  }

  @Test
  void ofValueSetIsNotAConceptMap() {
    assertThat(SuppliedArtefact.ofValueSet(URL, null, new ValueSet()).isConceptMap()).isFalse();
  }

  @Test
  void getConceptMapOnAValueSetThrows() {
    final SuppliedArtefact artefact = SuppliedArtefact.ofValueSet(URL, null, new ValueSet());

    assertThatThrownBy(artefact::getConceptMap)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(URL)
        .hasMessageContaining("ConceptMap");
  }

  @Test
  void getConceptMapOnAViewThrows() {
    final SuppliedArtefact artefact = SuppliedArtefact.ofView(URL, null, mock(FhirView.class));

    assertThatThrownBy(artefact::getConceptMap).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void getConceptMapOnASqlViewThrows() {
    final SuppliedArtefact artefact = SuppliedArtefact.ofSqlView(URL, null, new Library());

    assertThatThrownBy(artefact::getConceptMap).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void otherGettersOnAConceptMapThrow() {
    final SuppliedArtefact artefact =
        SuppliedArtefact.ofConceptMap(CONCEPT_MAP_URL, null, new ConceptMap());

    assertThatThrownBy(artefact::getValueSet)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(CONCEPT_MAP_URL)
        .hasMessageContaining("ValueSet");
    assertThatThrownBy(artefact::getView)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("ViewDefinition");
    assertThatThrownBy(artefact::getSqlView)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("SQLView");
  }
}
