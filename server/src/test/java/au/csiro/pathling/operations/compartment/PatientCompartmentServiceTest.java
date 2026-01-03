/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.compartment;

import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for PatientCompartmentService.
 *
 * @author John Grimes
 */
class PatientCompartmentServiceTest {

  private PatientCompartmentService service;

  @BeforeEach
  void setUp() {
    final FhirContext fhirContext = FhirContext.forR4();
    service = new PatientCompartmentService(fhirContext);
  }

  // Tests for getPatientCompartmentPaths.

  @Test
  void observationHasSubjectPath() {
    // Observation references Patient via subject.
    final List<String> paths = service.getPatientCompartmentPaths("Observation");
    assertThat(paths).contains("subject");
  }

  @Test
  void conditionHasSubjectPath() {
    // Condition references Patient via subject.
    final List<String> paths = service.getPatientCompartmentPaths("Condition");
    assertThat(paths).contains("subject");
  }

  @Test
  void encounterHasSubjectPath() {
    // Encounter references Patient via subject.
    final List<String> paths = service.getPatientCompartmentPaths("Encounter");
    assertThat(paths).contains("subject");
  }

  @Test
  void medicationRequestHasSubjectPath() {
    // MedicationRequest references Patient via subject.
    final List<String> paths = service.getPatientCompartmentPaths("MedicationRequest");
    assertThat(paths).contains("subject");
  }

  @Test
  void organizationHasNoPaths() {
    // Organization is not in Patient compartment.
    final List<String> paths = service.getPatientCompartmentPaths("Organization");
    assertThat(paths).isEmpty();
  }

  @Test
  void locationHasNoPaths() {
    // Location is not in Patient compartment.
    final List<String> paths = service.getPatientCompartmentPaths("Location");
    assertThat(paths).isEmpty();
  }

  @Test
  void patientHasLinkOtherPath() {
    // Patient has link.other which references other Patients in the compartment.
    final List<String> paths = service.getPatientCompartmentPaths("Patient");
    assertThat(paths).contains("link.other");
  }

  @Test
  void pathsCached() {
    // Call twice to verify caching works.
    final List<String> paths1 = service.getPatientCompartmentPaths("Observation");
    final List<String> paths2 = service.getPatientCompartmentPaths("Observation");
    assertThat(paths1).isSameAs(paths2);
  }

  @Test
  void unknownResourceTypeReturnsEmptyPaths() {
    // Unknown resource type should return empty list without throwing.
    final List<String> paths = service.getPatientCompartmentPaths("UnknownResourceType");
    assertThat(paths).isEmpty();
  }

  // Tests for isInPatientCompartment.

  @Test
  void patientIsInOwnCompartment() {
    // Patient is always in its own compartment.
    assertThat(service.isInPatientCompartment("Patient")).isTrue();
  }

  @Test
  void observationIsInPatientCompartment() {
    // Observation references Patient and is in compartment.
    assertThat(service.isInPatientCompartment("Observation")).isTrue();
  }

  @Test
  void conditionIsInPatientCompartment() {
    // Condition references Patient and is in compartment.
    assertThat(service.isInPatientCompartment("Condition")).isTrue();
  }

  @Test
  void organizationIsNotInPatientCompartment() {
    // Organization does not reference Patient.
    assertThat(service.isInPatientCompartment("Organization")).isFalse();
  }

  @Test
  void locationIsNotInPatientCompartment() {
    // Location does not reference Patient.
    assertThat(service.isInPatientCompartment("Location")).isFalse();
  }

  @Test
  void practitionerIsNotInPatientCompartment() {
    // Practitioner does not reference Patient.
    assertThat(service.isInPatientCompartment("Practitioner")).isFalse();
  }

  // Tests for buildPatientFilter.

  @Test
  void patientFilterWithEmptyIds() {
    // Empty patient IDs means all patients - returns true literal.
    final Column filter = service.buildPatientFilter("Patient", Set.of());
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("true");
  }

  @Test
  void patientFilterWithSpecificIds() {
    // Specific patient IDs filter by id column.
    final Column filter = service.buildPatientFilter("Patient", Set.of("123", "456"));
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("id");
  }

  @Test
  void observationFilterWithEmptyIds() {
    // Empty IDs for non-Patient resource matches any Patient reference.
    final Column filter = service.buildPatientFilter("Observation", Set.of());
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("Patient/");
  }

  @Test
  void observationFilterWithSpecificIds() {
    // Specific IDs for non-Patient resource matches exact references.
    final Column filter = service.buildPatientFilter("Observation", Set.of("123"));
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("Patient/123");
  }

  @Test
  void nonCompartmentResourceReturnsFilterFalse() {
    // Resources not in compartment return false literal.
    final Column filter = service.buildPatientFilter("Organization", Set.of());
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("false");
  }
}
