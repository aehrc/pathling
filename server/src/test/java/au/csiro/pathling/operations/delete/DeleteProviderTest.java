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

package au.csiro.pathling.operations.delete;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.operations.update.UpdateExecutor;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.MethodOutcome;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests for {@link DeleteProvider}.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class DeleteProviderTest {

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  @Autowired private FhirContext fhirContext;

  @Autowired private CacheableDatabase cacheableDatabase;

  private Path tempDatabasePath;
  private DeleteProvider deleteProvider;
  private DeleteExecutor deleteExecutor;
  private UpdateExecutor updateExecutor;

  @BeforeEach
  void setUp() throws IOException {
    // Create a temporary directory for the Delta Lake database.
    tempDatabasePath = Files.createTempDirectory("delete-provider-test-");

    // Create UpdateExecutor to set up test data.
    updateExecutor =
        new UpdateExecutor(
            pathlingContext,
            fhirEncoders,
            tempDatabasePath.toAbsolutePath().toString(),
            cacheableDatabase);

    // Create DeleteExecutor with the temp database path.
    deleteExecutor =
        new DeleteExecutor(
            pathlingContext, tempDatabasePath.toAbsolutePath().toString(), cacheableDatabase);

    // Create the DeleteProvider.
    deleteProvider = new DeleteProvider(deleteExecutor, fhirContext, Patient.class);
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
  // Test successful delete operations
  // -------------------------------------------------------------------------

  @Test
  void deleteExistingResource() {
    // Given: a Patient resource exists in the database.
    final Patient patient = createPatient("patient-1", "Smith", "John", AdministrativeGender.MALE);
    updateExecutor.merge("Patient", patient);

    // When: deleting the resource.
    final MethodOutcome outcome = deleteProvider.delete(new IdType("Patient", "patient-1"));

    // Then: the outcome should contain the ID.
    assertThat(outcome.getId()).isNotNull();
    assertThat(outcome.getId().getIdPart()).isEqualTo("patient-1");
  }

  @Test
  void deletePersistsToDatabase() {
    // Given: a Patient resource exists in the database.
    final Patient patient = createPatient("patient-1", "Smith", "John", AdministrativeGender.MALE);
    updateExecutor.merge("Patient", patient);

    // Verify patient exists before deletion.
    final String tablePath = tempDatabasePath.resolve("Patient.parquet").toString();
    assertThat(DeltaTable.isDeltaTable(sparkSession, tablePath)).isTrue();
    Dataset<Row> dataset = sparkSession.read().format("delta").load(tablePath);
    assertThat(dataset.count()).isEqualTo(1);

    // When: deleting the resource.
    deleteProvider.delete(new IdType("Patient", "patient-1"));

    // Then: the resource should no longer exist in Delta Lake.
    dataset = sparkSession.read().format("delta").load(tablePath);
    assertThat(dataset.count()).isEqualTo(0);
  }

  // -------------------------------------------------------------------------
  // Test error cases
  // -------------------------------------------------------------------------

  @Test
  void deleteNonExistentResourceThrowsError() {
    // Given: a Patient resource exists but we try to delete a different one.
    final Patient patient = createPatient("patient-1", "Smith", "John", AdministrativeGender.MALE);
    updateExecutor.merge("Patient", patient);

    // When/Then: deleting a non-existent resource should throw ResourceNotFoundError.
    assertThatThrownBy(() -> deleteProvider.delete(new IdType("Patient", "non-existent-id")))
        .isInstanceOf(ResourceNotFoundError.class);
  }

  @Test
  void deleteFromNonExistentTableThrowsError() {
    // Given: no Patient table exists (empty database).
    // When/Then: deleting should throw ResourceNotFoundError.
    assertThatThrownBy(() -> deleteProvider.delete(new IdType("Patient", "patient-1")))
        .isInstanceOf(ResourceNotFoundError.class);
  }

  @Test
  void deleteWithNullIdThrowsError() {
    // When/Then: deleting with null ID should throw InvalidUserInputError.
    assertThatThrownBy(() -> deleteProvider.delete(null))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("ID");
  }

  @Test
  void deleteWithEmptyIdThrowsError() {
    // When/Then: deleting with empty ID should throw InvalidUserInputError.
    assertThatThrownBy(() -> deleteProvider.delete(new IdType()))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("ID");
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
