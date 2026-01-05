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

package au.csiro.pathling.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.security.OperationAccess;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.CustomObjectDataSource;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Read;
import jakarta.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests for {@link ReadProvider}.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class ReadProviderTest {

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  @Autowired private FhirContext fhirContext;

  private CustomObjectDataSource dataSource;
  private ReadProvider readProvider;

  @BeforeEach
  void setUp() {
    // Create test data.
    final List<IBaseResource> resources = new ArrayList<>();
    resources.add(createPatient("patient-1", "Smith", "John", AdministrativeGender.MALE));
    resources.add(createPatient("patient-2", "Jones", "Jane", AdministrativeGender.FEMALE));

    dataSource = new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, resources);

    final ReadExecutor readExecutor = new ReadExecutor(dataSource, fhirEncoders);
    readProvider = new ReadProvider(readExecutor, fhirContext, Patient.class);
  }

  // -------------------------------------------------------------------------
  // Test successful read operations
  // -------------------------------------------------------------------------

  @Test
  void readReturnsExistingResource() {
    // Given: an ID for an existing Patient.
    final IdType id = new IdType("Patient", "patient-1");

    // When: reading the Patient.
    final IBaseResource result = readProvider.read(id);

    // Then: the correct Patient should be returned.
    assertThat(result).isInstanceOf(Patient.class);
    final Patient patient = (Patient) result;
    assertThat(patient.getIdElement().getIdPart()).isEqualTo("patient-1");
    assertThat(patient.getNameFirstRep().getFamily()).isEqualTo("Smith");
  }

  @Test
  void readReturnsDifferentResource() {
    // Given: an ID for a different Patient.
    final IdType id = new IdType("Patient", "patient-2");

    // When: reading the Patient.
    final IBaseResource result = readProvider.read(id);

    // Then: the correct Patient should be returned.
    assertThat(result).isInstanceOf(Patient.class);
    final Patient patient = (Patient) result;
    assertThat(patient.getIdElement().getIdPart()).isEqualTo("patient-2");
    assertThat(patient.getNameFirstRep().getFamily()).isEqualTo("Jones");
  }

  // -------------------------------------------------------------------------
  // Test error cases
  // -------------------------------------------------------------------------

  @Test
  void readWithNullIdThrowsError() {
    // When/Then: reading with null ID should throw InvalidUserInputError.
    assertThatThrownBy(() -> readProvider.read(null))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("ID");
  }

  @Test
  void readWithEmptyIdThrowsError() {
    // Given: an empty ID.
    final IdType emptyId = new IdType();

    // When/Then: reading with empty ID should throw InvalidUserInputError.
    assertThatThrownBy(() -> readProvider.read(emptyId))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("ID");
  }

  @Test
  void readNonExistentResourceThrowsNotFound() {
    // Given: an ID for a non-existent Patient.
    final IdType id = new IdType("Patient", "non-existent");

    // When/Then: reading a non-existent resource should throw ResourceNotFoundError.
    assertThatThrownBy(() -> readProvider.read(id))
        .isInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("Patient")
        .hasMessageContaining("non-existent");
  }

  // -------------------------------------------------------------------------
  // Test provider configuration
  // -------------------------------------------------------------------------

  @Test
  void getResourceTypeReturnsCorrectType() {
    // When: getting the resource type.
    final Class<? extends IBaseResource> resourceType = readProvider.getResourceType();

    // Then: it should return the Patient class.
    assertThat(resourceType).isEqualTo(Patient.class);
  }

  @Test
  void readMethodHasReadAnnotation() throws NoSuchMethodException {
    // When: checking for @Read annotation on the read method.
    final Method readMethod = ReadProvider.class.getMethod("read", IdType.class);
    final Read readAnnotation = readMethod.getAnnotation(Read.class);

    // Then: the annotation should be present.
    assertThat(readAnnotation).isNotNull();
  }

  @Test
  void readMethodHasOperationAccessAnnotation() throws NoSuchMethodException {
    // When: checking for @OperationAccess annotation on the read method.
    final Method readMethod = ReadProvider.class.getMethod("read", IdType.class);
    final OperationAccess operationAccess = readMethod.getAnnotation(OperationAccess.class);

    // Then: the annotation should be present with value "read".
    assertThat(operationAccess).isNotNull();
    assertThat(operationAccess.value()).containsExactly("read");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  @Nonnull
  private Patient createPatient(
      @Nonnull final String id,
      @Nonnull final String family,
      @Nonnull final String given,
      @Nonnull final AdministrativeGender gender) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName(new HumanName().setFamily(family).addGiven(given));
    patient.setGender(gender);
    return patient;
  }
}
