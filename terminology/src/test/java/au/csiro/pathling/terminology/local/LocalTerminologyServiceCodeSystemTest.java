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

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Designation;
import au.csiro.pathling.terminology.TerminologyService.Property;
import au.csiro.pathling.test.FhirFixtures;
import au.csiro.pathling.test.NoNetworkExtension;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests the lookup-backed functions and subsumption over an imported FHIR CodeSystem: display,
 * declared scalar properties with their FHIR types, designations, hierarchy subsumption from the
 * CodeSystem's nesting, and the exclusion of SNOMED-specific properties.
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class LocalTerminologyServiceCodeSystemTest {

  private static TerminologyService service;

  @BeforeAll
  static void setUp() {
    service = FhirTerminologyFixture.service();
  }

  private static Coding species(final String code) {
    return new Coding().setSystem(FhirFixtures.ANIMAL_SPECIES).setCode(code);
  }

  @Nonnull
  private static List<Type> propertyValues(
      @Nonnull final Coding coding, @Nonnull final String propertyCode) {
    return service.lookup(coding, propertyCode, null).stream()
        .filter(Property.class::isInstance)
        .map(Property.class::cast)
        .filter(p -> propertyCode.equals(p.getCode()))
        .map(Property::getValue)
        .toList();
  }

  @Test
  void returnsDisplay() {
    final List<Type> display = propertyValues(species(FhirFixtures.DOG), "display");
    assertEquals("Dog", display.get(0).primitiveValue());
  }

  @Test
  void returnsDeclaredScalarPropertiesWithTypes() {
    final List<Type> legs = propertyValues(species(FhirFixtures.DOG), "legs");
    assertEquals(1, legs.size());
    assertEquals("integer", legs.get(0).fhirType());
    assertEquals("4", legs.get(0).primitiveValue());

    final List<Type> habitat = propertyValues(species(FhirFixtures.DOG), "habitat");
    assertEquals("code", habitat.get(0).fhirType());
    assertEquals("land", habitat.get(0).primitiveValue());

    final List<Type> endangered = propertyValues(species(FhirFixtures.WHALE), "endangered");
    assertEquals("boolean", endangered.get(0).fhirType());
    assertEquals("true", endangered.get(0).primitiveValue());
  }

  @Test
  void returnsDesignations() {
    final List<Designation> designations =
        service.lookup(species(FhirFixtures.DOG), Designation.PROPERTY_CODE, null).stream()
            .filter(Designation.class::isInstance)
            .map(Designation.class::cast)
            .toList();
    assertTrue(designations.stream().anyMatch(d -> "Canine".equals(d.getValue())));
  }

  @Test
  void returnsParentAndChildFromNesting() {
    final List<Type> parents = propertyValues(species(FhirFixtures.DOG), "parent");
    assertEquals(1, parents.size());
    assertEquals(FhirFixtures.MAMMAL, parents.get(0).primitiveValue());
  }

  @Test
  void doesNotReturnSnomedSpecificProperties() {
    // sufficientlyDefined is a SNOMED property and must not appear for a FHIR CodeSystem concept.
    assertTrue(propertyValues(species(FhirFixtures.DOG), "sufficientlyDefined").isEmpty());
  }

  @Test
  void computesSubsumptionFromHierarchy() {
    assertEquals(
        ConceptSubsumptionOutcome.SUBSUMES,
        service.subsumes(species(FhirFixtures.MAMMAL), species(FhirFixtures.DOG)));
    assertEquals(
        ConceptSubsumptionOutcome.SUBSUMEDBY,
        service.subsumes(species(FhirFixtures.DOG), species(FhirFixtures.MAMMAL)));
    // Transitive subsumption from the nested hierarchy.
    assertEquals(
        ConceptSubsumptionOutcome.SUBSUMES,
        service.subsumes(species(FhirFixtures.ORGANISM), species(FhirFixtures.DOG)));
    assertEquals(
        ConceptSubsumptionOutcome.NOTSUBSUMED,
        service.subsumes(species(FhirFixtures.DOG), species(FhirFixtures.CAT)));
  }
}
