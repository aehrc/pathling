/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * Unit tests for {@link ViewDefinitionRunProvider}.
 *
 * @author John Grimes
 */
@Import({
    FhirServerTestConfiguration.class,
    PatientCompartmentService.class
})
@SpringBootUnitTest
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Execution(ExecutionMode.CONCURRENT)
class ViewDefinitionRunProviderTest {

  @Autowired
  private SparkSession sparkSession;

  @Autowired
  private PathlingContext pathlingContext;

  @Autowired
  private QueryableDataSource deltaLake;

  @Autowired
  private FhirContext fhirContext;

  @Autowired
  private FhirEncoders fhirEncoders;

  @Autowired
  private PatientCompartmentService patientCompartmentService;

  @Autowired
  private ServerConfiguration serverConfiguration;

  private ViewDefinitionRunProvider provider;

  private Gson gson;

  @BeforeEach
  void setUp() {
    provider = new ViewDefinitionRunProvider(
        sparkSession,
        deltaLake,
        fhirContext,
        fhirEncoders,
        patientCompartmentService,
        serverConfiguration
    );
    gson = new GsonBuilder().create();
  }

  /**
   * Parses a ViewDefinition JSON string into an IBaseResource.
   */
  @Nonnull
  private IBaseResource parseViewResource(@Nonnull final String viewJson) {
    return fhirContext.newJsonParser().parseResource(viewJson);
  }

  // -------------------------------------------------------------------------
  // Output format tests
  // -------------------------------------------------------------------------

  @Test
  void ndjsonOutputReturnsCorrectContentType() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(viewResource, "application/x-ndjson", null, null, null, null, null,
        List.of(inlinePatient), response);

    assertThat(response.getContentType()).startsWith("application/x-ndjson");
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void csvOutputReturnsCorrectContentType() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(viewResource, "text/csv", null, null, null, null, null,
        List.of(inlinePatient), response);

    assertThat(response.getContentType()).startsWith("text/csv");
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void csvOutputIncludesHeaderByDefault() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(viewResource, "text/csv", null, null, null, null, null,
        List.of(inlinePatient), response);

    final String content = response.getContentAsString();
    final String[] lines = content.split("\n");
    assertThat(lines.length).isGreaterThanOrEqualTo(2);
    // First line should be header.
    assertThat(lines[0]).contains("id");
    assertThat(lines[0]).contains("family_name");
  }

  @Test
  void csvOutputExcludesHeaderWhenFalse() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(viewResource, "text/csv", new BooleanType(false), null, null, null, null,
        List.of(inlinePatient), response);

    final String content = response.getContentAsString();
    final String[] lines = content.split("\n");
    // First line should be data, not header.
    assertThat(lines[0]).contains("test-1");
    assertThat(lines[0]).contains("Smith");
  }

  @Test
  void defaultFormatIsNdjson() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(viewResource, null, null, null, null, null, null,
        List.of(inlinePatient), response);

    assertThat(response.getContentType()).startsWith("application/x-ndjson");
  }

  // -------------------------------------------------------------------------
  // ViewDefinition parsing tests
  // -------------------------------------------------------------------------

  @Test
  void invalidViewDefinitionThrowsException() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    // Create a ViewDefinition missing required 'resource' field.
    final Map<String, Object> invalidView = new HashMap<>();
    invalidView.put("resourceType", "ViewDefinition");
    invalidView.put("name", "invalid_view");
    invalidView.put("status", "active");
    final IBaseResource viewResource = parseViewResource(gson.toJson(invalidView));

    assertThatThrownBy(() ->
        provider.run(viewResource, null, null, null, null, null, null, null, response))
        .isInstanceOf(Exception.class);
  }

  @Test
  void viewDefinitionWithMultipleColumnsProducesCorrectOutput() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createMultiColumnPatientView());
    final String inlinePatient = createPatientJsonWithGender("test-1", "Smith", "John", "male");

    provider.run(viewResource, "application/x-ndjson", null, null, null, null, null,
        List.of(inlinePatient), response);

    final String content = response.getContentAsString().trim();
    assertThat(content).contains("\"id\":\"test-1\"");
    assertThat(content).contains("\"family_name\":\"Smith\"");
    assertThat(content).contains("\"gender\":\"male\"");
  }

  // -------------------------------------------------------------------------
  // Limit parameter tests
  // -------------------------------------------------------------------------

  @Test
  void limitRestrictsRowCount() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final List<String> patients = List.of(
        createPatientJson("p1", "Smith"),
        createPatientJson("p2", "Jones"),
        createPatientJson("p3", "Brown")
    );

    provider.run(viewResource, "application/x-ndjson", null, new IntegerType(2),
        null, null, null, patients, response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    assertThat(lines.length).isEqualTo(2);
  }

  @Test
  void noLimitReturnsAllRows() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final List<String> patients = List.of(
        createPatientJson("p1", "Smith"),
        createPatientJson("p2", "Jones"),
        createPatientJson("p3", "Brown")
    );

    provider.run(viewResource, "application/x-ndjson", null, null,
        null, null, null, patients, response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    assertThat(lines.length).isEqualTo(3);
  }

  // -------------------------------------------------------------------------
  // Inline resources tests
  // -------------------------------------------------------------------------

  @Test
  void inlineResourcesUsedInsteadOfDeltaLake() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("inline-patient-123", "InlineFamily");

    provider.run(viewResource, "application/x-ndjson", null, null, null, null, null,
        List.of(inlinePatient), response);

    final String content = response.getContentAsString().trim();
    // Should contain the inline patient, not Delta Lake patients.
    assertThat(content).contains("inline-patient-123");
    assertThat(content).contains("InlineFamily");
  }

  @Test
  void multipleInlineResourcesOfSameType() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final List<String> patients = List.of(
        createPatientJson("inline-1", "Family1"),
        createPatientJson("inline-2", "Family2")
    );

    provider.run(viewResource, "application/x-ndjson", null, null, null, null, null,
        patients, response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    assertThat(lines.length).isEqualTo(2);
    assertThat(content).contains("inline-1");
    assertThat(content).contains("inline-2");
  }

  @Test
  void inlineResourcesFilteredByViewResourceType() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    // Include both Patient and Observation, but the view only queries Patient.
    final List<String> resources = List.of(
        createPatientJson("patient-1", "PatientFamily"),
        createObservationJson("obs-1", "patient-1")
    );

    provider.run(viewResource, "application/x-ndjson", null, null, null, null, null,
        resources, response);

    final String content = response.getContentAsString().trim();
    // Should only return the patient data.
    assertThat(content).contains("patient-1");
    assertThat(content).doesNotContain("obs-1");
  }

  @Test
  void invalidInlineResourceThrowsException() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());

    assertThatThrownBy(() ->
        provider.run(viewResource, null, null, null, null, null, null,
            List.of("{ not valid fhir }"), response))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid inline resource");
  }

  // -------------------------------------------------------------------------
  // Patient/group filtering tests
  // -------------------------------------------------------------------------

  // Note: Patient compartment filtering with inline resources is not supported because
  // ObjectDataSource does not implement QueryableDataSource. These tests would require
  // integration tests with a full Delta Lake setup.

  // -------------------------------------------------------------------------
  // Error handling tests
  // -------------------------------------------------------------------------

  @Test
  void nullResponseThrowsInvalidRequestException() {
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());

    assertThatThrownBy(() ->
        provider.run(viewResource, null, null, null, null, null, null, null, null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("HTTP response is required");
  }

  // -------------------------------------------------------------------------
  // Row conversion tests
  // -------------------------------------------------------------------------

  @Test
  void ndjsonRowContainsExpectedFields() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("row-test-id", "RowTestFamily");

    provider.run(viewResource, "application/x-ndjson", null, null, null, null, null,
        List.of(inlinePatient), response);

    final String content = response.getContentAsString().trim();
    // Parse the JSON line to verify structure.
    @SuppressWarnings("unchecked") final Map<String, Object> row = gson.fromJson(content,
        Map.class);
    assertThat(row).containsKey("id");
    assertThat(row).containsKey("family_name");
    assertThat(row.get("id")).isEqualTo("row-test-id");
    assertThat(row.get("family_name")).isEqualTo("RowTestFamily");
  }

  @Test
  void ndjsonHandlesNullValues() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    // Create patient without family name.
    final String patientJson = """
        {"resourceType":"Patient","id":"null-test"}
        """;

    provider.run(viewResource, "application/x-ndjson", null, null, null, null, null,
        List.of(patientJson), response);

    final String content = response.getContentAsString().trim();
    // Should still produce valid JSON.
    @SuppressWarnings("unchecked") final Map<String, Object> row = gson.fromJson(content,
        Map.class);
    assertThat(row).containsKey("id");
    // Null values should not be present in JSON output.
    assertThat(row.get("family_name")).isNull();
  }

  @Test
  void csvOutputProducesValidFormat() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("csv-test-id", "CsvTestFamily");

    provider.run(viewResource, "text/csv", null, null, null, null, null,
        List.of(inlinePatient), response);

    final String content = response.getContentAsString();
    final String[] lines = content.split("\n");
    assertThat(lines.length).isGreaterThanOrEqualTo(2);
    // Header line.
    assertThat(lines[0]).contains("id");
    assertThat(lines[0]).contains("family_name");
    // Data line.
    assertThat(lines[1]).contains("csv-test-id");
    assertThat(lines[1]).contains("CsvTestFamily");
  }

  // -------------------------------------------------------------------------
  // ViewOutputFormat tests
  // -------------------------------------------------------------------------

  @Test
  void viewOutputFormatParsesNdjsonCode() {
    assertThat(ViewOutputFormat.fromString("ndjson")).isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void viewOutputFormatParsesNdjsonContentType() {
    assertThat(ViewOutputFormat.fromString("application/x-ndjson"))
        .isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void viewOutputFormatParsesCsvCode() {
    assertThat(ViewOutputFormat.fromString("csv")).isEqualTo(ViewOutputFormat.CSV);
  }

  @Test
  void viewOutputFormatParsesCsvContentType() {
    assertThat(ViewOutputFormat.fromString("text/csv")).isEqualTo(ViewOutputFormat.CSV);
  }

  @Test
  void viewOutputFormatDefaultsToNdjsonForUnknown() {
    assertThat(ViewOutputFormat.fromString("unknown")).isEqualTo(ViewOutputFormat.NDJSON);
    assertThat(ViewOutputFormat.fromString("")).isEqualTo(ViewOutputFormat.NDJSON);
    assertThat(ViewOutputFormat.fromString(null)).isEqualTo(ViewOutputFormat.NDJSON);
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  @Nonnull
  private String createSimplePatientView() {
    final Map<String, Object> view = new HashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("name", "test_patient_view");
    view.put("resource", "Patient");
    view.put("status", "active");
    view.put("select", List.of(
        Map.of("column", List.of(Map.of("name", "id", "path", "id"))),
        Map.of("column", List.of(Map.of("name", "family_name", "path", "name.first().family")))
    ));
    return gson.toJson(view);
  }

  @Nonnull
  private String createMultiColumnPatientView() {
    final Map<String, Object> view = new HashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("name", "multi_column_patient_view");
    view.put("resource", "Patient");
    view.put("status", "active");
    view.put("select", List.of(
        Map.of("column", List.of(Map.of("name", "id", "path", "id"))),
        Map.of("column", List.of(Map.of("name", "family_name", "path", "name.first().family"))),
        Map.of("column",
            List.of(Map.of("name", "given_name", "path", "name.first().given.first()"))),
        Map.of("column", List.of(Map.of("name", "gender", "path", "gender")))
    ));
    return gson.toJson(view);
  }

  @Nonnull
  private String createPatientJson(@Nonnull final String id, @Nonnull final String familyName) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName().setFamily(familyName);
    return fhirContext.newJsonParser().encodeResourceToString(patient);
  }

  @Nonnull
  private String createPatientJsonWithGender(@Nonnull final String id,
      @Nonnull final String familyName, @Nonnull final String givenName,
      @Nonnull final String gender) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName().setFamily(familyName).addGiven(givenName);
    patient.setGender(AdministrativeGender.fromCode(gender));
    return fhirContext.newJsonParser().encodeResourceToString(patient);
  }

  @Nonnull
  private String createObservationJson(@Nonnull final String id,
      @Nonnull final String patientId) {
    final Observation observation = new Observation();
    observation.setId(id);
    observation.setSubject(new Reference("Patient/" + patientId));
    observation.setStatus(Observation.ObservationStatus.FINAL);
    return fhirContext.newJsonParser().encodeResourceToString(observation);
  }

}
