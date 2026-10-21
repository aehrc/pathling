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

import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for {@link ResolvedConceptMap#describeContent()}, which keys export jobs on the
 * mappings a concept map actually resolved to, so it must be stable across mapping order and
 * sensitive to every column of every mapping.
 *
 * @author John Grimes
 */
class ResolvedConceptMapTest {

  private static final String URL = "http://example.org/ConceptMap/sct-to-icd10";

  private static final String KEY = URL + "|2026";

  private static final String SNOMED = "http://snomed.info/sct";

  private static final String ICD10 = "http://hl7.org/fhir/sid/icd-10";

  private static final ConceptMapping MI =
      new ConceptMapping(
          SNOMED,
          "20260131",
          "22298006",
          "Myocardial infarction",
          ICD10,
          "2019",
          "I21",
          "Acute myocardial infarction",
          "equivalent");

  private static final ConceptMapping ASTHMA =
      new ConceptMapping(SNOMED, null, "195967001", "Asthma", ICD10, null, null, null, null);

  @Test
  void describeContentStartsWithTheConceptMapPrefix() {
    // The prefix separates a concept map from the other leaf kinds in a graph description, so two
    // nodes of different kinds cannot collide on a hash.
    assertThat(resolved(MI, ASTHMA).describeContent()).startsWith("concept-map:");
  }

  @Test
  void describeContentIsIdenticalForTheSameMappingsInADifferentOrder() {
    // Two sources may list the same mappings in a different order; the rows are the same, so an
    // export job for one must be reusable for the other.
    assertThat(resolved(MI, ASTHMA).describeContent())
        .isEqualTo(resolved(ASTHMA, MI).describeContent());
  }

  @Test
  void describeContentIsIdenticalForIdenticalMappings() {
    assertThat(resolved(MI, ASTHMA).describeContent())
        .isEqualTo(resolved(MI, ASTHMA).describeContent());
  }

  @Test
  void describeContentDiffersWhenAMappingIsMissing() {
    assertThat(resolved(MI, ASTHMA).describeContent()).isNotEqualTo(resolved(MI).describeContent());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("variants")
  void describeContentDiffersWhenAnyColumnDiffers(
      @Nonnull final String column, @Nonnull final ConceptMapping variant) {
    // Every one of the nine columns is part of the rows the relation produces, displays included,
    // so
    // a job that ran with one value cannot serve a request that inlined another.
    assertThat(resolved(MI, ASTHMA).describeContent())
        .isNotEqualTo(resolved(variant, ASTHMA).describeContent());
  }

  @Test
  void describeContentDoesNotDependOnTheMapVersionOrKey() {
    // The map's own version is provenance: it does not alter the rows.
    final ResolvedConceptMap first =
        new ResolvedConceptMap(KEY, new ConceptMapContent(URL, "2025", List.of(MI)));
    final ResolvedConceptMap second =
        new ResolvedConceptMap(URL, new ConceptMapContent(URL, "2026", List.of(MI)));

    assertThat(first.describeContent()).isEqualTo(second.describeContent());
  }

  @Nonnull
  static Stream<Arguments> variants() {
    return Stream.of(
        Arguments.of(
            "sourceSystem",
            new ConceptMapping(
                "http://example.org/other",
                "20260131",
                "22298006",
                "Myocardial infarction",
                ICD10,
                "2019",
                "I21",
                "Acute myocardial infarction",
                "equivalent")),
        Arguments.of(
            "sourceVersion",
            new ConceptMapping(
                SNOMED,
                null,
                "22298006",
                "Myocardial infarction",
                ICD10,
                "2019",
                "I21",
                "Acute myocardial infarction",
                "equivalent")),
        Arguments.of(
            "sourceCode",
            new ConceptMapping(
                SNOMED,
                "20260131",
                "73211009",
                "Myocardial infarction",
                ICD10,
                "2019",
                "I21",
                "Acute myocardial infarction",
                "equivalent")),
        Arguments.of(
            "sourceDisplay",
            new ConceptMapping(
                SNOMED,
                "20260131",
                "22298006",
                "Heart attack",
                ICD10,
                "2019",
                "I21",
                "Acute myocardial infarction",
                "equivalent")),
        Arguments.of(
            "targetSystem",
            new ConceptMapping(
                SNOMED,
                "20260131",
                "22298006",
                "Myocardial infarction",
                null,
                "2019",
                "I21",
                "Acute myocardial infarction",
                "equivalent")),
        Arguments.of(
            "targetVersion",
            new ConceptMapping(
                SNOMED,
                "20260131",
                "22298006",
                "Myocardial infarction",
                ICD10,
                "2016",
                "I21",
                "Acute myocardial infarction",
                "equivalent")),
        Arguments.of(
            "targetCode",
            new ConceptMapping(
                SNOMED,
                "20260131",
                "22298006",
                "Myocardial infarction",
                ICD10,
                "2019",
                "I22",
                "Acute myocardial infarction",
                "equivalent")),
        Arguments.of(
            "targetDisplay",
            new ConceptMapping(
                SNOMED,
                "20260131",
                "22298006",
                "Myocardial infarction",
                ICD10,
                "2019",
                "I21",
                null,
                "equivalent")),
        Arguments.of(
            "relationship",
            new ConceptMapping(
                SNOMED,
                "20260131",
                "22298006",
                "Myocardial infarction",
                ICD10,
                "2019",
                "I21",
                "Acute myocardial infarction",
                "source-is-narrower-than-target")));
  }

  @Nonnull
  private static ResolvedConceptMap resolved(@Nonnull final ConceptMapping... mappings) {
    return new ResolvedConceptMap(KEY, new ConceptMapContent(URL, "2026", List.of(mappings)));
  }
}
