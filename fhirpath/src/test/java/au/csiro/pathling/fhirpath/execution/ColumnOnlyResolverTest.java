/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import ca.uhn.fhir.context.FhirContext;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ColumnOnlyResolver}.
 */
class ColumnOnlyResolverTest {

  private FhirContext fhirContext;

  @BeforeEach
  void setUp() {
    fhirContext = FhirContext.forR4();
  }

  @Test
  void resolveSubjectResource_returnsResourceCollection() {
    final ColumnOnlyResolver resolver = new ColumnOnlyResolver(
        ResourceType.PATIENT, fhirContext);

    final ResourceCollection result = resolver.resolveSubjectResource();

    assertNotNull(result);
    assertEquals("Patient", result.getFhirType().orElseThrow().toCode());
  }

  @Test
  void resolveSubjectResource_columnReferencesResourceTypeCode() {
    final ColumnOnlyResolver resolver = new ColumnOnlyResolver(
        ResourceType.OBSERVATION, fhirContext);

    final ResourceCollection result = resolver.resolveSubjectResource();

    // The column should reference the resource type code
    // This is used when the filter is applied to a wrapped dataset
    assertNotNull(result.getColumn());
  }

  @Test
  void resolveResource_matchingCode_returnsResource() {
    final ColumnOnlyResolver resolver = new ColumnOnlyResolver(
        ResourceType.PATIENT, fhirContext);

    final Optional<ResourceCollection> result = resolver.resolveResource("Patient");

    assertTrue(result.isPresent());
    assertEquals("Patient", result.get().getFhirType().orElseThrow().toCode());
  }

  @Test
  void resolveResource_nonMatchingCode_throwsException() {
    final ColumnOnlyResolver resolver = new ColumnOnlyResolver(
        ResourceType.PATIENT, fhirContext);

    assertThrows(IllegalArgumentException.class,
        () -> resolver.resolveResource("Observation"));
  }

  @Test
  void createView_throwsUnsupportedOperationException() {
    final ColumnOnlyResolver resolver = new ColumnOnlyResolver(
        ResourceType.PATIENT, fhirContext);

    assertThrows(UnsupportedOperationException.class, resolver::createView);
  }

  @Test
  void getSubjectResource_returnsConfiguredResourceType() {
    final ColumnOnlyResolver resolver = new ColumnOnlyResolver(
        ResourceType.CONDITION, fhirContext);

    assertEquals(ResourceType.CONDITION, resolver.getSubjectResource());
  }

  @Test
  void getFhirContext_returnsConfiguredContext() {
    final ColumnOnlyResolver resolver = new ColumnOnlyResolver(
        ResourceType.PATIENT, fhirContext);

    assertEquals(fhirContext, resolver.getFhirContext());
  }
}
