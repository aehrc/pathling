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

package au.csiro.pathling.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.CustomObjectDataSource;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests for {@link ReadExecutor}.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class ReadExecutorTest {

  @Autowired
  private SparkSession sparkSession;

  @Autowired
  private PathlingContext pathlingContext;

  @Autowired
  private FhirEncoders fhirEncoders;

  private CustomObjectDataSource dataSource;
  private ReadExecutor readExecutor;

  @BeforeEach
  void setUp() {
    // Create test data with various resource types.
    final List<IBaseResource> resources = new ArrayList<>();

    // Add Patient resources.
    resources.add(createPatient("patient-1", "Smith", "John", AdministrativeGender.MALE));
    resources.add(createPatient("patient-2", "Jones", "Jane", AdministrativeGender.FEMALE));
    resources.add(createPatient("patient-3", "Brown", "Bob", AdministrativeGender.MALE));

    // Add Observation resources.
    resources.add(createObservation("obs-1", "patient-1"));
    resources.add(createObservation("obs-2", "patient-2"));

    // Add ViewDefinition resources.
    resources.add(createViewDefinition("view-1", "patient_view", "Patient"));
    resources.add(createViewDefinition("view-2", "observation_view", "Observation"));

    dataSource = new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders,
        resources);
    readExecutor = new ReadExecutor(dataSource, fhirEncoders);
  }

  // -------------------------------------------------------------------------
  // Test reading Patient resources
  // -------------------------------------------------------------------------

  @Test
  void readExistingPatient() {
    // When: reading an existing Patient by ID.
    final IBaseResource result = readExecutor.read("Patient", "patient-1");

    // Then: the correct Patient should be returned.
    assertThat(result).isInstanceOf(Patient.class);
    final Patient patient = (Patient) result;
    assertThat(patient.getIdElement().getIdPart()).isEqualTo("patient-1");
    assertThat(patient.getNameFirstRep().getFamily()).isEqualTo("Smith");
    assertThat(patient.getGender()).isEqualTo(AdministrativeGender.MALE);
  }

  @Test
  void readDifferentPatient() {
    // When: reading a different Patient by ID.
    final IBaseResource result = readExecutor.read("Patient", "patient-2");

    // Then: the correct Patient should be returned.
    assertThat(result).isInstanceOf(Patient.class);
    final Patient patient = (Patient) result;
    assertThat(patient.getIdElement().getIdPart()).isEqualTo("patient-2");
    assertThat(patient.getNameFirstRep().getFamily()).isEqualTo("Jones");
  }

  @Test
  void readNonExistentPatientThrowsError() {
    // When/Then: reading a non-existent Patient should throw ResourceNotFoundError.
    assertThatThrownBy(() -> readExecutor.read("Patient", "non-existent"))
        .isInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("Patient")
        .hasMessageContaining("non-existent");
  }

  // -------------------------------------------------------------------------
  // Test reading Observation resources
  // -------------------------------------------------------------------------

  @Test
  void readExistingObservation() {
    // When: reading an existing Observation by ID.
    final IBaseResource result = readExecutor.read("Observation", "obs-1");

    // Then: the correct Observation should be returned.
    assertThat(result).isInstanceOf(Observation.class);
    final Observation observation = (Observation) result;
    assertThat(observation.getIdElement().getIdPart()).isEqualTo("obs-1");
  }

  @Test
  void readNonExistentObservationThrowsError() {
    // When/Then: reading a non-existent Observation should throw ResourceNotFoundError.
    assertThatThrownBy(() -> readExecutor.read("Observation", "non-existent"))
        .isInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("Observation");
  }

  // -------------------------------------------------------------------------
  // Test reading ViewDefinition resources (custom resource type)
  // -------------------------------------------------------------------------

  @Test
  void readExistingViewDefinition() {
    // When: reading an existing ViewDefinition by ID.
    final IBaseResource result = readExecutor.read("ViewDefinition", "view-1");

    // Then: the correct ViewDefinition should be returned.
    assertThat(result).isInstanceOf(ViewDefinitionResource.class);
    final ViewDefinitionResource view = (ViewDefinitionResource) result;
    assertThat(view.getIdElement().getIdPart()).isEqualTo("view-1");
    assertThat(view.getName().getValue()).isEqualTo("patient_view");
    assertThat(view.getResource().getValue()).isEqualTo("Patient");
  }

  @Test
  void readDifferentViewDefinition() {
    // When: reading a different ViewDefinition by ID.
    final IBaseResource result = readExecutor.read("ViewDefinition", "view-2");

    // Then: the correct ViewDefinition should be returned.
    assertThat(result).isInstanceOf(ViewDefinitionResource.class);
    final ViewDefinitionResource view = (ViewDefinitionResource) result;
    assertThat(view.getIdElement().getIdPart()).isEqualTo("view-2");
    assertThat(view.getName().getValue()).isEqualTo("observation_view");
  }

  @Test
  void readNonExistentViewDefinitionThrowsError() {
    // When/Then: reading a non-existent ViewDefinition should throw ResourceNotFoundError.
    assertThatThrownBy(() -> readExecutor.read("ViewDefinition", "non-existent"))
        .isInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("ViewDefinition");
  }

  // -------------------------------------------------------------------------
  // Test error cases
  // -------------------------------------------------------------------------

  @Test
  void readWithNullResourceIdThrowsError() {
    // When/Then: reading with null ID should throw an error.
    assertThatThrownBy(() -> readExecutor.read("Patient", null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readWithEmptyResourceIdThrowsError() {
    // When/Then: reading with empty ID should throw an error.
    assertThatThrownBy(() -> readExecutor.read("Patient", ""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readWithBlankResourceIdThrowsError() {
    // When/Then: reading with blank ID should throw an error.
    assertThatThrownBy(() -> readExecutor.read("Patient", "   "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  @Nonnull
  private Patient createPatient(@Nonnull final String id, @Nonnull final String family,
      @Nonnull final String given, @Nonnull final AdministrativeGender gender) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName(new HumanName().setFamily(family).addGiven(given));
    patient.setGender(gender);
    return patient;
  }

  @Nonnull
  private Observation createObservation(@Nonnull final String id,
      @Nonnull final String subjectId) {
    final Observation observation = new Observation();
    observation.setId(id);
    observation.getSubject().setReference("Patient/" + subjectId);
    return observation;
  }

  @Nonnull
  private ViewDefinitionResource createViewDefinition(@Nonnull final String id,
      @Nonnull final String name, @Nonnull final String resource) {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(id);
    view.setName(new StringType(name));
    view.setResource(new CodeType(resource));
    view.setStatus(new CodeType("active"));

    // Add minimal select clause.
    final SelectComponent select = new SelectComponent();
    final ColumnComponent column = new ColumnComponent();
    column.setName(new StringType("id"));
    column.setPath(new StringType("id"));
    select.getColumn().add(column);
    view.getSelect().add(select);

    return view;
  }

}
