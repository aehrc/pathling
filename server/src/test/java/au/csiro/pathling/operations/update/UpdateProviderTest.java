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

package au.csiro.pathling.operations.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import au.csiro.pathling.errors.InvalidUserInputError;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.MethodOutcome;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link UpdateProvider} covering delegation to UpdateExecutor and input validation.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class UpdateProviderTest {

  @Mock private UpdateExecutor updateExecutor;

  private UpdateProvider provider;

  @BeforeEach
  void setUp() {
    final FhirContext fhirContext = FhirContext.forR4();
    provider = new UpdateProvider(updateExecutor, fhirContext, Patient.class);
  }

  @Test
  void getResourceTypeReturnsConfiguredClass() {
    // The provider should return the resource class it was configured with.
    assertEquals(Patient.class, provider.getResourceType());
  }

  @Test
  void updateDelegatesToExecutor() {
    // A valid update should delegate to the UpdateExecutor with the correct resource type code.
    final Patient patient = new Patient();
    patient.setId("patient-1");

    final MethodOutcome outcome = provider.update(new IdType("Patient/patient-1"), patient);

    verify(updateExecutor).merge(eq("Patient"), eq(patient));
    assertNotNull(outcome);
    assertNotNull(outcome.getResource());
  }

  @Test
  void updateReturnsOutcomeWithResourceId() {
    // The MethodOutcome should contain the resource ID.
    final Patient patient = new Patient();
    patient.setId("patient-1");

    final MethodOutcome outcome = provider.update(new IdType("Patient/patient-1"), patient);

    assertNotNull(outcome.getId());
    assertEquals("patient-1", outcome.getId().getIdPart());
  }

  @Test
  void updateThrowsWhenIdIsNull() {
    // A null ID should trigger an InvalidUserInputError.
    final Patient patient = new Patient();
    patient.setId("patient-1");

    assertThrows(InvalidUserInputError.class, () -> provider.update(null, patient));
  }

  @Test
  void updateThrowsWhenIdIsEmpty() {
    // An empty ID should trigger an InvalidUserInputError.
    final Patient patient = new Patient();
    patient.setId("patient-1");

    assertThrows(InvalidUserInputError.class, () -> provider.update(new IdType(), patient));
  }

  @Test
  void updateThrowsWhenResourceIsNull() {
    // A null resource should trigger an InvalidUserInputError.
    assertThrows(
        InvalidUserInputError.class, () -> provider.update(new IdType("Patient/patient-1"), null));
  }
}
