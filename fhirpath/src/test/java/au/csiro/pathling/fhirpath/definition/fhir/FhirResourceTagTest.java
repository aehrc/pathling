/*
 * Copyright 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.definition.fhir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FhirResourceTag}.
 *
 * @author John Grimes
 */
class FhirResourceTagTest {

  @Test
  void testOfResourceType() {
    // Test creation from a standard FHIR ResourceType.
    final FhirResourceTag tag = FhirResourceTag.of(ResourceType.PATIENT);

    assertEquals("Patient", tag.getResourceCode());
    assertTrue(tag.getResourceType().isPresent());
    assertEquals(ResourceType.PATIENT, tag.getResourceType().get());
  }

  @Test
  void testOfResourceCodeStandard() {
    // Test creation from a valid FHIR resource code string that maps to ResourceType.
    final FhirResourceTag tag = FhirResourceTag.of("Observation");

    assertEquals("Observation", tag.getResourceCode());
    assertTrue(tag.getResourceType().isPresent());
    assertEquals(ResourceType.OBSERVATION, tag.getResourceType().get());
  }

  @Test
  void testOfResourceCodeCustom() {
    // Test creation from a custom resource code that is not in the FHIR specification.
    final FhirResourceTag tag = FhirResourceTag.of("ViewDefinition");

    assertEquals("ViewDefinition", tag.getResourceCode());
    assertFalse(tag.getResourceType().isPresent());
  }

  @Test
  void testOfResourceCodeAndOptionalResourceType() {
    // Test creation with explicit code and optional ResourceType.
    final FhirResourceTag tagWithType =
        FhirResourceTag.of("Patient", Optional.of(ResourceType.PATIENT));
    assertEquals("Patient", tagWithType.getResourceCode());
    assertTrue(tagWithType.getResourceType().isPresent());

    // Test creation with code and empty optional.
    final FhirResourceTag tagWithoutType = FhirResourceTag.of("CustomResource", Optional.empty());
    assertEquals("CustomResource", tagWithoutType.getResourceCode());
    assertFalse(tagWithoutType.getResourceType().isPresent());
  }

  @Test
  void testToString() {
    // Test that toString returns the resource code.
    final FhirResourceTag standardTag = FhirResourceTag.of(ResourceType.ENCOUNTER);
    assertEquals("Encounter", standardTag.toString());

    final FhirResourceTag customTag = FhirResourceTag.of("ViewDefinition");
    assertEquals("ViewDefinition", customTag.toString());
  }

  @Test
  void testToCode() {
    // Test that toCode returns the resource code.
    final FhirResourceTag standardTag = FhirResourceTag.of(ResourceType.CONDITION);
    assertEquals("Condition", standardTag.toCode());

    final FhirResourceTag customTag = FhirResourceTag.of("CustomResource");
    assertEquals("CustomResource", customTag.toCode());
  }
}
