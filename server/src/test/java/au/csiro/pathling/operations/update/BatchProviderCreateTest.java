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

package au.csiro.pathling.operations.update;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryRequestComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Bundle.HTTPVerb;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests for create operations within the batch provider.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class BatchProviderCreateTest {

  @Autowired
  private SparkSession sparkSession;

  @Autowired
  private PathlingContext pathlingContext;

  @Autowired
  private FhirEncoders fhirEncoders;

  @Autowired
  private ServerConfiguration configuration;

  private Path tempDatabasePath;
  private BatchProvider batchProvider;
  private UpdateExecutor updateExecutor;

  @BeforeEach
  void setUp() throws IOException {
    // Create a temporary directory for the Delta Lake database.
    tempDatabasePath = Files.createTempDirectory("batch-create-test-");

    // Create UpdateExecutor with the temp database path.
    updateExecutor = new UpdateExecutor(pathlingContext, fhirEncoders,
        tempDatabasePath.toAbsolutePath().toString());

    // Create the BatchProvider.
    batchProvider = new BatchProvider(updateExecutor, configuration);
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
  // Tests for POST (create) operations in batch
  // -------------------------------------------------------------------------

  @Test
  void batchCreateGeneratesUuidsForResources() {
    // Given: a batch bundle with a POST entry (no ID specified).
    final Bundle requestBundle = new Bundle();
    requestBundle.setType(BundleType.BATCH);

    final Patient patient = createPatient(null, "Smith", "John", AdministrativeGender.MALE);
    final BundleEntryComponent entry = requestBundle.addEntry();
    entry.setResource(patient);
    final BundleEntryRequestComponent request = new BundleEntryRequestComponent();
    request.setMethod(HTTPVerb.POST);
    request.setUrl("Patient");
    entry.setRequest(request);

    // When: executing the batch.
    final Bundle response = batchProvider.batch(requestBundle);

    // Then: the response should contain a 201 status and a generated UUID.
    assertThat(response.getEntry()).hasSize(1);
    assertThat(response.getEntry().get(0).getResponse().getStatus()).isEqualTo("201");
    assertThat(response.getEntry().get(0).getResource()).isInstanceOf(Patient.class);

    final Patient returnedPatient = (Patient) response.getEntry().get(0).getResource();
    assertThat(returnedPatient.getId()).isNotNull();
    assertThat(isValidUuid(returnedPatient.getIdElement().getIdPart())).isTrue();
  }

  @Test
  void batchCreateIgnoresClientProvidedId() {
    // Given: a batch bundle with a POST entry that includes a client-provided ID.
    final Bundle requestBundle = new Bundle();
    requestBundle.setType(BundleType.BATCH);

    final String clientId = "client-provided-id";
    final Patient patient = createPatient(clientId, "Smith", "John", AdministrativeGender.MALE);
    final BundleEntryComponent entry = requestBundle.addEntry();
    entry.setResource(patient);
    final BundleEntryRequestComponent request = new BundleEntryRequestComponent();
    request.setMethod(HTTPVerb.POST);
    request.setUrl("Patient");
    entry.setRequest(request);

    // When: executing the batch.
    final Bundle response = batchProvider.batch(requestBundle);

    // Then: the generated ID should be different from the client-provided ID.
    final Patient returnedPatient = (Patient) response.getEntry().get(0).getResource();
    assertThat(returnedPatient.getIdElement().getIdPart()).isNotEqualTo(clientId);
    assertThat(isValidUuid(returnedPatient.getIdElement().getIdPart())).isTrue();
  }

  @Test
  void batchCreatePersistsResourceToDeltaLake() {
    // Given: a batch bundle with a POST entry.
    final Bundle requestBundle = new Bundle();
    requestBundle.setType(BundleType.BATCH);

    final Patient patient = createPatient(null, "Persisted", "Test", AdministrativeGender.FEMALE);
    final BundleEntryComponent entry = requestBundle.addEntry();
    entry.setResource(patient);
    final BundleEntryRequestComponent request = new BundleEntryRequestComponent();
    request.setMethod(HTTPVerb.POST);
    request.setUrl("Patient");
    entry.setRequest(request);

    // When: executing the batch.
    final Bundle response = batchProvider.batch(requestBundle);

    // Then: the resource should be persisted to Delta Lake.
    final String tablePath = tempDatabasePath.resolve("Patient.parquet").toString();
    assertThat(DeltaTable.isDeltaTable(sparkSession, tablePath)).isTrue();

    final Dataset<Row> dataset = sparkSession.read().format("delta").load(tablePath);
    assertThat(dataset.count()).isEqualTo(1);

    final String generatedId = ((Patient) response.getEntry().get(0).getResource())
        .getIdElement().getIdPart();
    final Row row = dataset.first();
    assertThat(row.getAs("id").toString()).isEqualTo(generatedId);
  }

  @Test
  void batchCreateMultipleResourcesGeneratesUniqueIds() {
    // Given: a batch bundle with multiple POST entries.
    final Bundle requestBundle = new Bundle();
    requestBundle.setType(BundleType.BATCH);

    final Patient patient1 = createPatient(null, "First", "Patient", AdministrativeGender.MALE);
    final BundleEntryComponent entry1 = requestBundle.addEntry();
    entry1.setResource(patient1);
    final BundleEntryRequestComponent request1 = new BundleEntryRequestComponent();
    request1.setMethod(HTTPVerb.POST);
    request1.setUrl("Patient");
    entry1.setRequest(request1);

    final Patient patient2 = createPatient(null, "Second", "Patient", AdministrativeGender.FEMALE);
    final BundleEntryComponent entry2 = requestBundle.addEntry();
    entry2.setResource(patient2);
    final BundleEntryRequestComponent request2 = new BundleEntryRequestComponent();
    request2.setMethod(HTTPVerb.POST);
    request2.setUrl("Patient");
    entry2.setRequest(request2);

    // When: executing the batch.
    final Bundle response = batchProvider.batch(requestBundle);

    // Then: each resource should have a unique UUID.
    assertThat(response.getEntry()).hasSize(2);

    final String id1 = ((Patient) response.getEntry().get(0).getResource())
        .getIdElement().getIdPart();
    final String id2 = ((Patient) response.getEntry().get(1).getResource())
        .getIdElement().getIdPart();

    assertThat(id1).isNotEqualTo(id2);
    assertThat(isValidUuid(id1)).isTrue();
    assertThat(isValidUuid(id2)).isTrue();
  }

  @Test
  void batchMixedCreateAndUpdateOperations() {
    // Given: a batch bundle with both POST and PUT entries.
    final Bundle requestBundle = new Bundle();
    requestBundle.setType(BundleType.BATCH);

    // POST entry (create).
    final Patient newPatient = createPatient(null, "New", "Patient", AdministrativeGender.MALE);
    final BundleEntryComponent createEntry = requestBundle.addEntry();
    createEntry.setResource(newPatient);
    final BundleEntryRequestComponent createRequest = new BundleEntryRequestComponent();
    createRequest.setMethod(HTTPVerb.POST);
    createRequest.setUrl("Patient");
    createEntry.setRequest(createRequest);

    // PUT entry (update).
    final Patient existingPatient = createPatient("existing-id", "Existing", "Patient",
        AdministrativeGender.FEMALE);
    final BundleEntryComponent updateEntry = requestBundle.addEntry();
    updateEntry.setResource(existingPatient);
    final BundleEntryRequestComponent updateRequest = new BundleEntryRequestComponent();
    updateRequest.setMethod(HTTPVerb.PUT);
    updateRequest.setUrl("Patient/existing-id");
    updateEntry.setRequest(updateRequest);

    // When: executing the batch.
    final Bundle response = batchProvider.batch(requestBundle);

    // Then: both operations should succeed.
    assertThat(response.getEntry()).hasSize(2);

    // Create should return 201 with generated UUID.
    assertThat(response.getEntry().get(0).getResponse().getStatus()).isEqualTo("201");
    final String createdId = ((Patient) response.getEntry().get(0).getResource())
        .getIdElement().getIdPart();
    assertThat(isValidUuid(createdId)).isTrue();

    // Update should return 200 with provided ID.
    assertThat(response.getEntry().get(1).getResponse().getStatus()).isEqualTo("200");
    final String updatedId = ((Patient) response.getEntry().get(1).getResource())
        .getIdElement().getIdPart();
    assertThat(updatedId).isEqualTo("existing-id");
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
