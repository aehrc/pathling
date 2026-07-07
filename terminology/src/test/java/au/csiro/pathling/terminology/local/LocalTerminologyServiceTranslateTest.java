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

package au.csiro.pathling.terminology.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.test.FhirFixtures;
import au.csiro.pathling.test.NoNetworkExtension;
import au.csiro.pathling.test.Rf2Mini;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests local {@code translate}: explicit imported ConceptMaps (forward, reverse, target-scoped,
 * with equivalences preserved), the unknown-content fallback, and SNOMED implicit concept maps
 * derived from association reference sets.
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class LocalTerminologyServiceTranslateTest {

  private static TerminologyService fhirService;
  private static TerminologyService snomedService;

  @BeforeAll
  static void setUp() {
    fhirService = FhirTerminologyFixture.service();
    final TerminologyConfiguration snomedConfig =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(
                LocalTerminologyConfiguration.builder()
                    .storagePath(LocalTerminologyFixture.storagePath())
                    .build())
            .build();
    snomedService = new LocalTerminologyService(snomedConfig, Map.of());
  }

  private static Coding species(final String code) {
    return new Coding().setSystem(FhirFixtures.ANIMAL_SPECIES).setCode(code);
  }

  private static Coding category(final String code) {
    return new Coding().setSystem(FhirFixtures.ANIMAL_CATEGORY).setCode(code);
  }

  private static Coding snomed(final String code) {
    return new Coding().setSystem(Rf2Mini.SNOMED_URI).setCode(code);
  }

  private static Set<String> targetCodes(final List<Translation> translations) {
    return translations.stream().map(t -> t.getConcept().getCode()).collect(Collectors.toSet());
  }

  @Test
  void translatesForward() {
    final List<Translation> result =
        fhirService.translate(species(FhirFixtures.DOG), FhirFixtures.CONCEPT_MAP, false, null);
    assertEquals(1, result.size());
    assertEquals("pet", result.get(0).getConcept().getCode());
    assertEquals(FhirFixtures.ANIMAL_CATEGORY, result.get(0).getConcept().getSystem());
    assertEquals(ConceptMapEquivalence.EQUIVALENT, result.get(0).getEquivalence());
  }

  @Test
  void translatesReverse() {
    // Everything that maps to the "pet" category: dog, cat, and sparrow.
    final List<Translation> result =
        fhirService.translate(category("pet"), FhirFixtures.CONCEPT_MAP, true, null);
    assertEquals(
        Set.of(FhirFixtures.DOG, FhirFixtures.CAT, FhirFixtures.SPARROW), targetCodes(result));
  }

  @Test
  void reverseTranslationInvertsEquivalence() {
    // Sparrow maps to "pet" with a "wider" equivalence; the reverse mapping is "narrower".
    final List<Translation> result =
        fhirService.translate(category("pet"), FhirFixtures.CONCEPT_MAP, true, null);
    final ConceptMapEquivalence sparrowEquivalence =
        result.stream()
            .filter(t -> FhirFixtures.SPARROW.equals(t.getConcept().getCode()))
            .map(Translation::getEquivalence)
            .findFirst()
            .orElseThrow();
    assertEquals(ConceptMapEquivalence.NARROWER, sparrowEquivalence);
  }

  @Test
  void translatesWithTargetValueSetFilter() {
    // The target names a value set: dog maps to "pet", which is a member of the pets value set.
    final List<Translation> matching =
        fhirService.translate(
            species(FhirFixtures.DOG), FhirFixtures.CONCEPT_MAP, false, FhirFixtures.VS_PETS);
    assertEquals(Set.of("pet"), targetCodes(matching));

    // A target value set that does not contain the mapped concept excludes the translation.
    final List<Translation> nonMatching =
        fhirService.translate(
            species(FhirFixtures.DOG),
            FhirFixtures.CONCEPT_MAP,
            false,
            FhirFixtures.VS_MAMMALS_ENUMERATED);
    assertTrue(nonMatching.isEmpty());
  }

  @Test
  void unknownConceptMapReturnsEmpty() {
    assertTrue(
        fhirService
            .translate(
                species(FhirFixtures.DOG),
                "http://example.org/fhir/ConceptMap/does-not-exist",
                false,
                null)
            .isEmpty());
  }

  @Test
  void translatesSnomedAssociationRefsetForward() {
    // The inactive concept has a SAME AS association to its active replacement.
    final String conceptMap = Rf2Mini.SNOMED_URI + "?fhir_cm=" + Rf2Mini.SAME_AS_REFSET;
    final List<Translation> result =
        snomedService.translate(snomed(Rf2Mini.DIABETES_INACTIVE), conceptMap, false, null);
    assertEquals(Set.of(Rf2Mini.TYPE2_DIABETES), targetCodes(result));
    assertEquals(Rf2Mini.SNOMED_URI, result.get(0).getConcept().getSystem());
  }

  @Test
  void translatesSnomedAssociationRefsetReverse() {
    final String conceptMap = Rf2Mini.SNOMED_URI + "?fhir_cm=" + Rf2Mini.SAME_AS_REFSET;
    final List<Translation> result =
        snomedService.translate(snomed(Rf2Mini.TYPE2_DIABETES), conceptMap, true, null);
    assertEquals(Set.of(Rf2Mini.DIABETES_INACTIVE), targetCodes(result));
  }
}
