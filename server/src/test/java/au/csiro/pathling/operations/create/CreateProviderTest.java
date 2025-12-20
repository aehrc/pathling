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

package au.csiro.pathling.operations.create;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.operations.update.UpdateExecutor;
import au.csiro.pathling.security.OperationAccess;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.api.MethodOutcome;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests for {@link CreateProvider}.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class CreateProviderTest {

  @Autowired
  private SparkSession sparkSession;

  @Autowired
  private PathlingContext pathlingContext;

  @Autowired
  private FhirEncoders fhirEncoders;

  @Autowired
  private FhirContext fhirContext;

  private Path tempDatabasePath;
  private CreateProvider createProvider;
  private UpdateExecutor updateExecutor;

  @BeforeEach
  void setUp() throws IOException {
    // Create a temporary directory for the Delta Lake database.
    tempDatabasePath = Files.createTempDirectory("create-provider-test-");

    // Create UpdateExecutor with the temp database path.
    updateExecutor = new UpdateExecutor(pathlingContext, fhirEncoders,
        tempDatabasePath.toAbsolutePath().toString());

    // Create the CreateProvider.
    createProvider = new CreateProvider(updateExecutor, fhirContext, Patient.class);
  }

  @AfterEach
  void tearDown() throws IOException {
    // Clean up the temporary directory.
    if (tempDatabasePath != null && Files.exists(tempDatabasePath)) {
      Files.walk(tempDatabasePath)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  // -------------------------------------------------------------------------
  // Test successful create operations
  // -------------------------------------------------------------------------

  @Test
  void createGeneratesNewUuid() {
    // Given: a Patient resource without an ID.
    final Patient patient = createPatient(null, "Smith", "John", AdministrativeGender.MALE);

    // When: creating the resource.
    final MethodOutcome outcome = createProvider.create(patient);

    // Then: a new UUID should be generated.
    assertThat(outcome.getId()).isNotNull();
    assertThat(outcome.getId().getIdPart()).isNotEmpty();
    assertThat(isValidUuid(outcome.getId().getIdPart())).isTrue();
  }

  @Test
  void createIgnoresClientProvidedId() {
    // Given: a Patient resource with a client-provided ID.
    final String clientId = "client-provided-id";
    final Patient patient = createPatient(clientId, "Smith", "John", AdministrativeGender.MALE);

    // When: creating the resource.
    final MethodOutcome outcome = createProvider.create(patient);

    // Then: the client-provided ID should be ignored and a new UUID generated.
    assertThat(outcome.getId()).isNotNull();
    assertThat(outcome.getId().getIdPart()).isNotEqualTo(clientId);
    assertThat(isValidUuid(outcome.getId().getIdPart())).isTrue();
  }

  @Test
  void createSetsCreatedToTrue() {
    // Given: a Patient resource.
    final Patient patient = createPatient(null, "Smith", "John", AdministrativeGender.MALE);

    // When: creating the resource.
    final MethodOutcome outcome = createProvider.create(patient);

    // Then: the outcome should indicate the resource was created.
    assertThat(outcome.getCreated()).isTrue();
  }

  @Test
  void createReturnsResourceWithGeneratedId() {
    // Given: a Patient resource.
    final Patient patient = createPatient(null, "Smith", "John", AdministrativeGender.MALE);

    // When: creating the resource.
    final MethodOutcome outcome = createProvider.create(patient);

    // Then: the returned resource should have the generated ID.
    assertThat(outcome.getResource()).isNotNull();
    assertThat(outcome.getResource()).isInstanceOf(Patient.class);
    final Patient returnedPatient = (Patient) outcome.getResource();
    assertThat(returnedPatient.getIdElement().getIdPart())
        .isEqualTo(outcome.getId().getIdPart());
  }

  @Test
  void createPersistsResourceToDeltaLake() {
    // Given: a Patient resource.
    final Patient patient = createPatient(null, "Smith", "John", AdministrativeGender.MALE);

    // When: creating the resource.
    final MethodOutcome outcome = createProvider.create(patient);

    // Then: the resource should be persisted to Delta Lake.
    final String tablePath = tempDatabasePath.resolve("Patient.parquet").toString();
    assertThat(DeltaTable.isDeltaTable(sparkSession, tablePath)).isTrue();

    final Dataset<Row> dataset = sparkSession.read().format("delta").load(tablePath);
    assertThat(dataset.count()).isEqualTo(1);

    final Row row = dataset.first();
    assertThat(row.getAs("id").toString()).isEqualTo(outcome.getId().getIdPart());
  }

  @Test
  void createMultipleResourcesGeneratesUniqueIds() {
    // Given: multiple Patient resources.
    final Patient patient1 = createPatient(null, "Smith", "John", AdministrativeGender.MALE);
    final Patient patient2 = createPatient(null, "Jones", "Jane", AdministrativeGender.FEMALE);

    // When: creating both resources.
    final MethodOutcome outcome1 = createProvider.create(patient1);
    final MethodOutcome outcome2 = createProvider.create(patient2);

    // Then: each should have a unique UUID.
    assertThat(outcome1.getId().getIdPart()).isNotEqualTo(outcome2.getId().getIdPart());
    assertThat(isValidUuid(outcome1.getId().getIdPart())).isTrue();
    assertThat(isValidUuid(outcome2.getId().getIdPart())).isTrue();

    // And: both should be persisted.
    final String tablePath = tempDatabasePath.resolve("Patient.parquet").toString();
    final Dataset<Row> dataset = sparkSession.read().format("delta").load(tablePath);
    assertThat(dataset.count()).isEqualTo(2);
  }

  // -------------------------------------------------------------------------
  // Test error cases
  // -------------------------------------------------------------------------

  @Test
  void createWithNullResourceThrowsError() {
    // When/Then: creating with null resource should throw InvalidUserInputError.
    assertThatThrownBy(() -> createProvider.create(null))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("Resource");
  }

  // -------------------------------------------------------------------------
  // Test provider configuration
  // -------------------------------------------------------------------------

  @Test
  void getResourceTypeReturnsCorrectType() {
    // When: getting the resource type.
    final Class<? extends IBaseResource> resourceType = createProvider.getResourceType();

    // Then: it should return the Patient class.
    assertThat(resourceType).isEqualTo(Patient.class);
  }

  @Test
  void createMethodHasCreateAnnotation() throws NoSuchMethodException {
    // When: checking for @Create annotation on the create method.
    final Method createMethod = CreateProvider.class.getMethod("create", IBaseResource.class);
    final Create createAnnotation = createMethod.getAnnotation(Create.class);

    // Then: the annotation should be present.
    assertThat(createAnnotation).isNotNull();
  }

  @Test
  void createMethodHasOperationAccessAnnotation() throws NoSuchMethodException {
    // When: checking for @OperationAccess annotation on the create method.
    final Method createMethod = CreateProvider.class.getMethod("create", IBaseResource.class);
    final OperationAccess operationAccess = createMethod.getAnnotation(OperationAccess.class);

    // Then: the annotation should be present with value "create".
    assertThat(operationAccess).isNotNull();
    assertThat(operationAccess.value()).containsExactly("create");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  @Nonnull
  private Patient createPatient(final String id, @Nonnull final String family,
      @Nonnull final String given, @Nonnull final AdministrativeGender gender) {
    final Patient patient = new Patient();
    if (id != null) {
      patient.setId(id);
    }
    patient.addName(new HumanName().setFamily(family).addGiven(given));
    patient.setGender(gender);
    return patient;
  }

  private boolean isValidUuid(@Nonnull final String str) {
    try {
      UUID.fromString(str);
      return true;
    } catch (final IllegalArgumentException e) {
      return false;
    }
  }

}
