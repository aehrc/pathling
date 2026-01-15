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

package au.csiro.pathling.operations.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.CustomObjectDataSource;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for {@link ViewDefinitionExportExecutor}.
 *
 * @author John Grimes
 */
@Import({FhirServerTestConfiguration.class, PatientCompartmentService.class})
@SpringBootUnitTest
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Execution(ExecutionMode.CONCURRENT)
class ViewDefinitionExportExecutorTest {

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirContext fhirContext;

  @Autowired private FhirEncoders fhirEncoders;

  @Autowired private ServerConfiguration serverConfiguration;

  @Autowired private PatientCompartmentService patientCompartmentService;

  @TempDir private Path tempDir;

  private Path uniqueTempDir;

  private ViewDefinitionExportExecutor executor;

  @BeforeEach
  void setUp() throws IOException {
    SharedMocks.resetAll();
    uniqueTempDir = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(uniqueTempDir);
  }

  // -------------------------------------------------------------------------
  // NDJSON output tests
  // -------------------------------------------------------------------------

  @Test
  void ndjsonOutputCreatesFiles() {
    final Patient patient = createPatient("test-1", "Smith");
    executor = createExecutor(patient);

    final FhirView view = createSimplePatientView();
    final ViewInput viewInput = new ViewInput("patients", view);
    final ViewDefinitionExportRequest request =
        new ViewDefinitionExportRequest(
            "http://example.org/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(viewInput),
            null,
            ViewExportFormat.NDJSON,
            true,
            Collections.emptySet(),
            null);

    final List<ViewExportOutput> outputs = executor.execute(request, UUID.randomUUID().toString());

    assertThat(outputs).hasSize(1);
    assertThat(outputs.get(0).name()).isEqualTo("patients");
    assertThat(outputs.get(0).fileUrls()).isNotEmpty();
    // Verify files have correct extension.
    assertThat(outputs.get(0).fileUrls().get(0)).endsWith(".ndjson");
  }

  // -------------------------------------------------------------------------
  // CSV output tests
  // -------------------------------------------------------------------------

  @Test
  void csvOutputCreatesFiles() {
    final Patient patient = createPatient("test-1", "Smith");
    executor = createExecutor(patient);

    final FhirView view = createSimplePatientView();
    final ViewInput viewInput = new ViewInput("patients", view);
    final ViewDefinitionExportRequest request =
        new ViewDefinitionExportRequest(
            "http://example.org/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(viewInput),
            null,
            ViewExportFormat.CSV,
            true,
            Collections.emptySet(),
            null);

    final List<ViewExportOutput> outputs = executor.execute(request, UUID.randomUUID().toString());

    assertThat(outputs).hasSize(1);
    assertThat(outputs.get(0).name()).isEqualTo("patients");
    assertThat(outputs.get(0).fileUrls()).isNotEmpty();
    // Verify files have correct extension.
    assertThat(outputs.get(0).fileUrls().get(0)).contains(".csv");
  }

  // -------------------------------------------------------------------------
  // Parquet output tests
  // -------------------------------------------------------------------------

  @Test
  void parquetOutputCreatesFiles() {
    final Patient patient = createPatient("test-1", "Smith");
    executor = createExecutor(patient);

    final FhirView view = createSimplePatientView();
    final ViewInput viewInput = new ViewInput("patients", view);
    final ViewDefinitionExportRequest request =
        new ViewDefinitionExportRequest(
            "http://example.org/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(viewInput),
            null,
            ViewExportFormat.PARQUET,
            true,
            Collections.emptySet(),
            null);

    final List<ViewExportOutput> outputs = executor.execute(request, UUID.randomUUID().toString());

    assertThat(outputs).hasSize(1);
    assertThat(outputs.get(0).name()).isEqualTo("patients");
    assertThat(outputs.get(0).fileUrls()).isNotEmpty();
    // Verify files have correct extension.
    assertThat(outputs.get(0).fileUrls().get(0)).endsWith(".parquet");
  }

  // -------------------------------------------------------------------------
  // Multiple views tests
  // -------------------------------------------------------------------------

  @Test
  void multipleViewsCreateSeparateOutputs() {
    final Patient patient = createPatient("test-1", "Smith");
    executor = createExecutor(patient);

    final FhirView view1 = createSimplePatientView();
    final FhirView view2 = createSimplePatientView();
    final ViewInput viewInput1 = new ViewInput("patients_a", view1);
    final ViewInput viewInput2 = new ViewInput("patients_b", view2);
    final ViewDefinitionExportRequest request =
        new ViewDefinitionExportRequest(
            "http://example.org/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(viewInput1, viewInput2),
            null,
            ViewExportFormat.NDJSON,
            true,
            Collections.emptySet(),
            null);

    final List<ViewExportOutput> outputs = executor.execute(request, UUID.randomUUID().toString());

    assertThat(outputs).hasSize(2);
    assertThat(outputs.get(0).name()).isEqualTo("patients_a");
    assertThat(outputs.get(1).name()).isEqualTo("patients_b");
  }

  // -------------------------------------------------------------------------
  // Name deduplication tests
  // -------------------------------------------------------------------------

  @Test
  void duplicateNamesAreDeduplicatedWithSuffix() {
    final Patient patient = createPatient("test-1", "Smith");
    executor = createExecutor(patient);

    final FhirView view1 = createSimplePatientView();
    final FhirView view2 = createSimplePatientView();
    // Both views have the same name.
    final ViewInput viewInput1 = new ViewInput("patients", view1);
    final ViewInput viewInput2 = new ViewInput("patients", view2);
    final ViewDefinitionExportRequest request =
        new ViewDefinitionExportRequest(
            "http://example.org/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(viewInput1, viewInput2),
            null,
            ViewExportFormat.NDJSON,
            true,
            Collections.emptySet(),
            null);

    final List<ViewExportOutput> outputs = executor.execute(request, UUID.randomUUID().toString());

    assertThat(outputs).hasSize(2);
    assertThat(outputs.get(0).name()).isEqualTo("patients");
    assertThat(outputs.get(1).name()).isEqualTo("patients_1");
  }

  // -------------------------------------------------------------------------
  // Invalid view definition tests
  // -------------------------------------------------------------------------

  @Test
  void invalidViewDefinitionThrowsException() {
    final Patient patient = createPatient("test-1", "Smith");
    executor = createExecutor(patient);

    // Create a view with invalid select (empty).
    final FhirView invalidView = new FhirView();
    invalidView.setResource("Patient");
    invalidView.setSelect(Collections.emptyList());

    final ViewInput viewInput = new ViewInput("invalid", invalidView);
    final ViewDefinitionExportRequest request =
        new ViewDefinitionExportRequest(
            "http://example.org/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(viewInput),
            null,
            ViewExportFormat.NDJSON,
            true,
            Collections.emptySet(),
            null);

    assertThatThrownBy(() -> executor.execute(request, UUID.randomUUID().toString()))
        .isInstanceOf(InvalidRequestException.class);
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  private ViewDefinitionExportExecutor createExecutor(final IBaseResource... resources) {
    final QueryableDataSource dataSource =
        new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, List.of(resources));
    return new ViewDefinitionExportExecutor(
        dataSource,
        fhirContext,
        sparkSession,
        "file://" + uniqueTempDir.toAbsolutePath(),
        serverConfiguration,
        patientCompartmentService);
  }

  private Patient createPatient(final String id, final String familyName) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName().setFamily(familyName);
    return patient;
  }

  private FhirView createSimplePatientView() {
    return FhirView.ofResource("Patient")
        .select(
            FhirView.columns(
                FhirView.column("id", "id"), FhirView.column("family_name", "name.first().family")))
        .build();
  }
}
