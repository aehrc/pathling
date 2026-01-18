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

package au.csiro.pathling.operations.compartment;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.CustomObjectDataSource;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Appointment;
import org.hl7.fhir.r4.model.Appointment.AppointmentParticipantComponent;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for PatientCompartmentService.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
@Import({PatientCompartmentService.class, FhirServerTestConfiguration.class})
class PatientCompartmentServiceTest {

  @Autowired private PatientCompartmentService service;

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  // ==================== Tests for getPatientCompartmentPaths ====================

  /**
   * Provides test cases for resource types that have a "subject" compartment path.
   *
   * @return stream of resource type names
   */
  static Stream<String> resourceTypesWithSubjectPath() {
    return Stream.of("Observation", "Condition", "Encounter", "MedicationRequest");
  }

  @ParameterizedTest(name = "{0} has subject path")
  @MethodSource("resourceTypesWithSubjectPath")
  void resourceTypeHasSubjectPath(final String resourceType) {
    final List<String> paths = service.getPatientCompartmentPaths(resourceType);
    assertThat(paths).contains("subject");
  }

  /**
   * Provides test cases for resource types that are not in the Patient compartment.
   *
   * @return stream of resource type names
   */
  static Stream<String> resourceTypesNotInCompartment() {
    return Stream.of("Organization", "Location", "UnknownResourceType");
  }

  @ParameterizedTest(name = "{0} has no compartment paths")
  @MethodSource("resourceTypesNotInCompartment")
  void resourceTypeHasNoCompartmentPaths(final String resourceType) {
    final List<String> paths = service.getPatientCompartmentPaths(resourceType);
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

  // ==================== Tests for isInPatientCompartment ====================

  @ParameterizedTest(name = "{0} is in Patient compartment: {1}")
  @CsvSource({
    "Patient, true",
    "Observation, true",
    "Condition, true",
    "Appointment, true",
    "Organization, false",
    "Location, false",
    "Practitioner, false"
  })
  void isInPatientCompartment(final String resourceType, final boolean expected) {
    assertThat(service.isInPatientCompartment(resourceType)).isEqualTo(expected);
  }

  // ==================== Tests for filterByPatientCompartment ====================

  /**
   * Provides test cases for Patient filtering scenarios.
   *
   * @return stream of arguments: patientIds to filter, expected count
   */
  static Stream<Arguments> patientFilterScenarios() {
    return Stream.of(
        // Empty IDs means all patients - should match all rows.
        Arguments.of(Set.of(), 2),
        // Specific patient IDs should match only those patients.
        Arguments.of(Set.of("patient-1"), 1),
        Arguments.of(Set.of("patient-1", "patient-2"), 2));
  }

  @ParameterizedTest(name = "Patient filter with IDs {0} matches {1} patients")
  @MethodSource("patientFilterScenarios")
  void patientFilterMatchesExpectedCount(final Set<String> patientIds, final int expectedCount) {
    final Patient patient1 = new Patient();
    patient1.setId("patient-1");
    final Patient patient2 = new Patient();
    patient2.setId("patient-2");

    final CustomObjectDataSource dataSource =
        new CustomObjectDataSource(
            sparkSession, pathlingContext, fhirEncoders, List.of(patient1, patient2));

    final Dataset<Row> dataset = dataSource.read("Patient");
    final Dataset<Row> filtered =
        service.filterByPatientCompartment("Patient", patientIds, dataset, dataSource);

    assertThat(filtered.count()).isEqualTo(expectedCount);
  }

  /**
   * Provides test cases for Observation filtering scenarios.
   *
   * @return stream of arguments: patientIds to filter, expected IDs in result
   */
  static Stream<Arguments> observationFilterScenarios() {
    return Stream.of(
        // Empty IDs means all patients - matches rows with Patient references.
        Arguments.of(Set.of(), List.of("obs-1", "obs-2")),
        // Specific patient ID matches only that patient's observations.
        Arguments.of(Set.of("patient-1"), List.of("obs-1")),
        Arguments.of(Set.of("patient-2"), List.of("obs-2")),
        Arguments.of(Set.of("patient-1", "patient-2"), List.of("obs-1", "obs-2")));
  }

  @ParameterizedTest(name = "Observation filter with patient IDs {0} matches {1}")
  @MethodSource("observationFilterScenarios")
  void observationFilterMatchesExpectedIds(
      final Set<String> patientIds, final List<String> expectedIds) {
    // Create observations with patient references.
    final Observation obs1 = new Observation();
    obs1.setId("obs-1");
    obs1.setSubject(new Reference("Patient/patient-1"));

    final Observation obs2 = new Observation();
    obs2.setId("obs-2");
    obs2.setSubject(new Reference("Patient/patient-2"));

    final Observation obs3 = new Observation();
    obs3.setId("obs-3");
    obs3.setSubject(new Reference("Device/device-1")); // Not a patient reference.

    final CustomObjectDataSource dataSource =
        new CustomObjectDataSource(
            sparkSession, pathlingContext, fhirEncoders, List.of(obs1, obs2, obs3));

    final Dataset<Row> dataset = dataSource.read("Observation");
    final Dataset<Row> filtered =
        service.filterByPatientCompartment("Observation", patientIds, dataset, dataSource);

    assertThat(filtered.count()).isEqualTo(expectedIds.size());
    final List<String> actualIds = filtered.select("id").as(Encoders.STRING()).collectAsList();
    assertThat(actualIds).containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  /**
   * Provides test cases for Appointment filtering scenarios with complex FHIRPath expressions.
   *
   * @return stream of arguments: patientIds to filter, expected IDs in result
   */
  static Stream<Arguments> appointmentFilterScenarios() {
    return Stream.of(
        // Empty IDs means all patients - matches appointments with Patient references.
        Arguments.of(Set.of(), List.of("appt-1", "appt-3")),
        // Specific patient ID matches only that patient's appointments.
        Arguments.of(Set.of("patient-1"), List.of("appt-1")),
        Arguments.of(Set.of("patient-2"), List.of("appt-3")),
        Arguments.of(Set.of("patient-1", "patient-2"), List.of("appt-1", "appt-3")));
  }

  @ParameterizedTest(name = "Appointment filter with patient IDs {0} matches {1}")
  @MethodSource("appointmentFilterScenarios")
  void appointmentFilterWithComplexFhirPathMatchesExpectedIds(
      final Set<String> patientIds, final List<String> expectedIds) {
    // Appointment has a complex compartment path: participant.actor.where(resolve() is Patient).
    final Appointment appt1 = new Appointment();
    appt1.setId("appt-1");
    appt1.setStatus(Appointment.AppointmentStatus.BOOKED);
    final AppointmentParticipantComponent participant1 = new AppointmentParticipantComponent();
    participant1.setActor(new Reference("Patient/patient-1"));
    appt1.addParticipant(participant1);

    final Appointment appt2 = new Appointment();
    appt2.setId("appt-2");
    appt2.setStatus(Appointment.AppointmentStatus.BOOKED);
    final AppointmentParticipantComponent participant2 = new AppointmentParticipantComponent();
    participant2.setActor(new Reference("Practitioner/practitioner-1")); // Not a patient.
    appt2.addParticipant(participant2);

    final Appointment appt3 = new Appointment();
    appt3.setId("appt-3");
    appt3.setStatus(Appointment.AppointmentStatus.BOOKED);
    final AppointmentParticipantComponent participant3 = new AppointmentParticipantComponent();
    participant3.setActor(new Reference("Patient/patient-2"));
    appt3.addParticipant(participant3);

    final CustomObjectDataSource dataSource =
        new CustomObjectDataSource(
            sparkSession, pathlingContext, fhirEncoders, List.of(appt1, appt2, appt3));

    final Dataset<Row> dataset = dataSource.read("Appointment");
    final Dataset<Row> filtered =
        service.filterByPatientCompartment("Appointment", patientIds, dataset, dataSource);

    assertThat(filtered.count()).isEqualTo(expectedIds.size());
    final List<String> actualIds = filtered.select("id").as(Encoders.STRING()).collectAsList();
    assertThat(actualIds).containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  @Test
  void nonCompartmentResourceReturnsEmptyResult() {
    // Organization is not in Patient compartment - filter should return no rows.
    final Organization org1 = new Organization();
    org1.setId("org-1");

    final CustomObjectDataSource dataSource =
        new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, List.of(org1));

    final Dataset<Row> dataset = dataSource.read("Organization");
    final Dataset<Row> filtered =
        service.filterByPatientCompartment("Organization", Set.of(), dataset, dataSource);

    assertThat(filtered.count()).isZero();
  }

  // ==================== Tests for buildPatientFilter (backward compatibility) ====================

  @ParameterizedTest(name = "buildPatientFilter for Patient with IDs {0}")
  @MethodSource("buildPatientFilterScenarios")
  void buildPatientFilterForPatientWithoutDataSource(
      final Set<String> patientIds, final String expectedContent) {
    final Column filter = service.buildPatientFilter("Patient", patientIds);
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains(expectedContent);
  }

  static Stream<Arguments> buildPatientFilterScenarios() {
    return Stream.of(
        // Empty IDs returns a filter that matches all (contains "true").
        Arguments.of(Set.of(), "true"),
        // Specific IDs returns a filter on id column.
        Arguments.of(Set.of("123", "456"), "id"));
  }
}
