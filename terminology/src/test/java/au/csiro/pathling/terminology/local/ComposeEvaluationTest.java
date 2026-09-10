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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.FhirFixtures;
import au.csiro.pathling.test.NoNetworkExtension;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests {@code member_of} evaluation of explicit imported FHIR value sets: enumerated concepts,
 * hierarchy and property filters, excludes, nested value set references, expansion-only value sets,
 * and version-qualified references, over the animal-species fixture.
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class ComposeEvaluationTest {

  private static TerminologyService service;

  @BeforeAll
  static void setUp() {
    service = FhirTerminologyFixture.service();
  }

  private static Coding species(final String code) {
    return new Coding().setSystem(FhirFixtures.ANIMAL_SPECIES).setCode(code);
  }

  @Test
  void enumeratedValueSet() {
    assertTrue(service.validateCode(FhirFixtures.VS_MAMMALS_ENUMERATED, species(FhirFixtures.DOG)));
    assertTrue(
        service.validateCode(FhirFixtures.VS_MAMMALS_ENUMERATED, species(FhirFixtures.WHALE)));
    assertFalse(
        service.validateCode(FhirFixtures.VS_MAMMALS_ENUMERATED, species(FhirFixtures.SPARROW)));
  }

  @Test
  void isaFilterValueSet() {
    // is-a mammal includes mammal itself and its descendants.
    assertTrue(service.validateCode(FhirFixtures.VS_MAMMALS_ISA, species(FhirFixtures.MAMMAL)));
    assertTrue(service.validateCode(FhirFixtures.VS_MAMMALS_ISA, species(FhirFixtures.DOG)));
    assertTrue(service.validateCode(FhirFixtures.VS_MAMMALS_ISA, species(FhirFixtures.WHALE)));
    assertFalse(service.validateCode(FhirFixtures.VS_MAMMALS_ISA, species(FhirFixtures.SPARROW)));
  }

  @Test
  void includeWithExclude() {
    assertTrue(
        service.validateCode(FhirFixtures.VS_ANIMALS_EXCEPT_WHALE, species(FhirFixtures.DOG)));
    assertTrue(
        service.validateCode(FhirFixtures.VS_ANIMALS_EXCEPT_WHALE, species(FhirFixtures.SPARROW)));
    // Whale is excluded even though it is-a animal.
    assertFalse(
        service.validateCode(FhirFixtures.VS_ANIMALS_EXCEPT_WHALE, species(FhirFixtures.WHALE)));
  }

  @Test
  void propertyFilterValueSet() {
    assertTrue(service.validateCode(FhirFixtures.VS_LAND_DWELLERS, species(FhirFixtures.DOG)));
    assertTrue(service.validateCode(FhirFixtures.VS_LAND_DWELLERS, species(FhirFixtures.SPARROW)));
    assertFalse(service.validateCode(FhirFixtures.VS_LAND_DWELLERS, species(FhirFixtures.WHALE)));
    assertFalse(service.validateCode(FhirFixtures.VS_LAND_DWELLERS, species(FhirFixtures.PENGUIN)));
  }

  @Test
  void nestedValueSetReference() {
    assertTrue(service.validateCode(FhirFixtures.VS_NESTED_MAMMALS, species(FhirFixtures.DOG)));
    assertFalse(
        service.validateCode(FhirFixtures.VS_NESTED_MAMMALS, species(FhirFixtures.SPARROW)));
  }

  @Test
  void expansionOnlyValueSet() {
    assertTrue(service.validateCode(FhirFixtures.VS_EXPANSION_ONLY, species(FhirFixtures.DOG)));
    assertTrue(service.validateCode(FhirFixtures.VS_EXPANSION_ONLY, species(FhirFixtures.SPARROW)));
    assertFalse(service.validateCode(FhirFixtures.VS_EXPANSION_ONLY, species(FhirFixtures.CAT)));
  }

  @Test
  void versionQualifiedReference() {
    assertTrue(
        service.validateCode(
            FhirFixtures.VS_MAMMALS_ENUMERATED + "|" + FhirFixtures.VERSION,
            species(FhirFixtures.DOG)));
    // A version that was not imported is unknown content.
    assertFalse(
        service.validateCode(
            FhirFixtures.VS_MAMMALS_ENUMERATED + "|9.9.9", species(FhirFixtures.DOG)));
  }

  @Test
  void unknownValueSetIsUnknownContent() {
    assertFalse(
        service.validateCode(
            "http://example.org/fhir/ValueSet/does-not-exist", species(FhirFixtures.DOG)));
  }
}
