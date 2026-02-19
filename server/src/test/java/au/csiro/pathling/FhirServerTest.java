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

package au.csiro.pathling;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FhirServer} covering supported resource types, unsupported resource types, and
 * custom resource type detection.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class FhirServerTest {

  // -- supportedResourceTypes --

  @Test
  void supportedResourceTypesIsNotEmpty() {
    // The set of supported resource types should not be empty.
    final Set<ResourceType> supported = FhirServer.supportedResourceTypes();
    assertNotNull(supported);
    assertFalse(supported.isEmpty());
  }

  @Test
  void supportedResourceTypesIncludesPatient() {
    // Patient should be a supported resource type.
    final Set<ResourceType> supported = FhirServer.supportedResourceTypes();
    assertTrue(supported.contains(ResourceType.PATIENT));
  }

  @Test
  void supportedResourceTypesExcludesNull() {
    // The NULL enum value should not be in the supported set.
    final Set<ResourceType> supported = FhirServer.supportedResourceTypes();
    assertFalse(supported.contains(ResourceType.NULL));
  }

  @Test
  void supportedResourceTypesExcludesDomainResource() {
    // DomainResource should not be in the supported set.
    final Set<ResourceType> supported = FhirServer.supportedResourceTypes();
    assertFalse(supported.contains(ResourceType.DOMAINRESOURCE));
  }

  @Test
  void supportedResourceTypesExcludesResource() {
    // Resource should not be in the supported set.
    final Set<ResourceType> supported = FhirServer.supportedResourceTypes();
    assertFalse(supported.contains(ResourceType.RESOURCE));
  }

  // -- unsupportedResourceTypes --

  @Test
  void unsupportedResourceTypesContainsNull() {
    // NULL should be in the unsupported set.
    final Set<ResourceType> unsupported = FhirServer.unsupportedResourceTypes();
    assertTrue(unsupported.contains(ResourceType.NULL));
  }

  @Test
  void unsupportedResourceTypesContainsDomainResource() {
    // DomainResource should be in the unsupported set.
    final Set<ResourceType> unsupported = FhirServer.unsupportedResourceTypes();
    assertTrue(unsupported.contains(ResourceType.DOMAINRESOURCE));
  }

  @Test
  void supportedAndUnsupportedAreDisjoint() {
    // Supported and unsupported resource types should not overlap.
    final Set<ResourceType> supported = FhirServer.supportedResourceTypes();
    final Set<ResourceType> unsupported = FhirServer.unsupportedResourceTypes();
    for (final ResourceType type : supported) {
      assertFalse(
          unsupported.contains(type), "Supported type " + type + " should not be unsupported");
    }
  }

  // -- isCustomResourceType --

  @Test
  void viewDefinitionIsCustomResourceType() {
    // ViewDefinition should be recognised as a custom resource type.
    assertTrue(FhirServer.isCustomResourceType("ViewDefinition"));
  }

  @Test
  void patientIsNotCustomResourceType() {
    // Standard FHIR types should not be custom resource types.
    assertFalse(FhirServer.isCustomResourceType("Patient"));
  }

  // -- Header constants --

  @Test
  void headerConstantsAreDefined() {
    // The static header constants should be defined.
    assertNotNull(FhirServer.ACCEPT_HEADER);
    assertNotNull(FhirServer.PREFER_RESPOND_TYPE_HEADER);
    assertNotNull(FhirServer.PREFER_LENIENT_HEADER);
    assertNotNull(FhirServer.OUTPUT_FORMAT);
  }
}
