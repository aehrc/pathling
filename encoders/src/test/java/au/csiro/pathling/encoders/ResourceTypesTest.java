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
 *
 * @author John Grimes
 */

package au.csiro.pathling.encoders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for the ResourceTypes utility class. */
class ResourceTypesTest {

  // Tests for constants.

  @Test
  void viewDefinitionConstantHasCorrectValue() {
    assertEquals("ViewDefinition", ResourceTypes.VIEW_DEFINITION);
  }

  @Test
  void customResourceTypesContainsViewDefinition() {
    assertTrue(ResourceTypes.CUSTOM_RESOURCE_TYPES.contains("ViewDefinition"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"Parameters", "StructureDefinition", "StructureMap", "Bundle"})
  void unsupportedResourcesContainsExpectedTypes(final String resourceType) {
    final Set<String> unsupported = ResourceTypes.UNSUPPORTED_RESOURCES;
    assertTrue(unsupported.contains(resourceType));
  }

  // Tests for isCustomResourceType().

  @ParameterizedTest(name = "isCustomResourceType(\"{0}\") should return {1}")
  @CsvSource({
    // Custom resource types return true.
    "ViewDefinition, true",
    // Standard FHIR types return false.
    "Patient, false",
    "Observation, false",
    // Unknown types return false.
    "UnknownType, false"
  })
  void isCustomResourceType(final String code, final boolean expected) {
    assertEquals(expected, ResourceTypes.isCustomResourceType(code));
  }

  // Tests for isSupported().

  @ParameterizedTest(name = "isSupported(\"{0}\") should return {1}")
  @CsvSource({
    // Standard FHIR types are supported.
    "Patient, true",
    "Observation, true",
    // Custom resource types are supported.
    "ViewDefinition, true",
    // Unsupported types return false.
    "Parameters, false",
    "Bundle, false",
    "StructureDefinition, false",
    "StructureMap, false",
    // Unknown types are not supported.
    "UnknownType, false"
  })
  void isSupported(final String code, final boolean expected) {
    assertEquals(expected, ResourceTypes.isSupported(code));
  }

  // Tests for matchSupportedResourceType().

  static Stream<Arguments> matchSupportedResourceTypeProvider() {
    return Stream.of(
        // Exact matches for standard FHIR types.
        Arguments.of("Patient", Optional.of("Patient")),
        Arguments.of("Observation", Optional.of("Observation")),
        // Case-insensitive matches for standard FHIR types.
        Arguments.of("patient", Optional.of("Patient")),
        Arguments.of("PATIENT", Optional.of("Patient")),
        Arguments.of("observation", Optional.of("Observation")),
        // Exact match for custom resource types.
        Arguments.of("ViewDefinition", Optional.of("ViewDefinition")),
        // Case-insensitive match for custom resource types.
        Arguments.of("viewdefinition", Optional.of("ViewDefinition")),
        Arguments.of("VIEWDEFINITION", Optional.of("ViewDefinition")),
        // Unsupported types return empty.
        Arguments.of("Parameters", Optional.empty()),
        Arguments.of("Bundle", Optional.empty()),
        // Unknown types return empty.
        Arguments.of("UnknownType", Optional.empty()));
  }

  @ParameterizedTest(name = "matchSupportedResourceType(\"{0}\") should return {1}")
  @MethodSource("matchSupportedResourceTypeProvider")
  void matchSupportedResourceType(final String input, final Optional<String> expected) {
    assertEquals(expected, ResourceTypes.matchSupportedResourceType(input));
  }
}
