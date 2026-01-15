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

import au.csiro.pathling.operations.compartment.GroupMemberService;
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
  PatientCompartmentService.class,
  GroupMemberService.class,
  ViewExecutionHelper.class
})
@SpringBootUnitTest
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Execution(ExecutionMode.CONCURRENT)
class ViewDefinitionRunProviderTest {

  @Autowired private FhirContext fhirContext;

  @Autowired private ViewExecutionHelper viewExecutionHelper;

  private ViewDefinitionRunProvider provider;

  private Gson gson;

  @BeforeEach
  void setUp() {
    provider = new ViewDefinitionRunProvider(viewExecutionHelper);
    gson = new GsonBuilder().create();
  }

  /** Parses a ViewDefinition JSON string into an IBaseResource. */
  @Nonnull
  private IBaseResource parseViewResource(@Nonnull final String viewJson) {
    return fhirContext.newJsonParser().parseResource(viewJson);
  }

  // -------------------------------------------------------------------------
  // Output format tests
  // -------------------------------------------------------------------------

  // Verifies that each output format returns the correct content type.
  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "application/x-ndjson, application/x-ndjson",
    "text/csv, text/csv",
    "application/json, application/json"
  })
  void outputFormatReturnsCorrectContentType(
      final String formatParam, final String expectedContentType) {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(
        viewResource,
        formatParam,
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails(null),
        response);

    assertThat(response.getContentType()).startsWith(expectedContentType);
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void csvOutputIncludesHeaderByDefault() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(
        viewResource,
        "text/csv",
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString();
    final String[] lines = content.split("\n");
    // First line should be header.
    assertThat(lines)
        .hasSizeGreaterThanOrEqualTo(2)
        .satisfies(l -> assertThat(l[0]).contains("id").contains("family_name"));
  }

  @Test
  void csvOutputExcludesHeaderWhenFalse() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(
        viewResource,
        "text/csv",
        new BooleanType(false),
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString();
    final String[] lines = content.split("\n");
    // First line should be data, not header.
    assertThat(lines[0]).contains("test-1").contains("Smith");
  }

  @Test
  void defaultFormatIsNdjson() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(
        viewResource,
        null,
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails(null),
        response);

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

    assertThatThrownBy(
            () ->
                provider.run(
                    viewResource,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    mockRequestDetails(null),
                    response))
        .isInstanceOf(Exception.class);
  }

  @Test
  void viewDefinitionWithMultipleColumnsProducesCorrectOutput() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createMultiColumnPatientView());
    final String inlinePatient = createPatientJsonWithGender("test-1", "Smith", "John", "male");

    provider.run(
        viewResource,
        "application/x-ndjson",
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString().trim();
    assertThat(content)
        .contains("\"id\":\"test-1\"")
        .contains("\"family_name\":\"Smith\"")
        .contains("\"gender\":\"male\"");
  }

  // -------------------------------------------------------------------------
  // Limit parameter tests
  // -------------------------------------------------------------------------

  @Test
  void limitRestrictsRowCount() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final List<String> patients =
        List.of(
            createPatientJson("p1", "Smith"),
            createPatientJson("p2", "Jones"),
            createPatientJson("p3", "Brown"));

    provider.run(
        viewResource,
        "application/x-ndjson",
        null,
        new IntegerType(2),
        null,
        null,
        null,
        patients,
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    assertThat(lines).hasSize(2);
  }

  @Test
  void noLimitReturnsAllRows() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final List<String> patients =
        List.of(
            createPatientJson("p1", "Smith"),
            createPatientJson("p2", "Jones"),
            createPatientJson("p3", "Brown"));

    provider.run(
        viewResource,
        "application/x-ndjson",
        null,
        null,
        null,
        null,
        null,
        patients,
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    assertThat(lines).hasSize(3);
  }

  // -------------------------------------------------------------------------
  // Inline resources tests
  // -------------------------------------------------------------------------

  @Test
  void inlineResourcesUsedInsteadOfDeltaLake() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("inline-patient-123", "InlineFamily");

    provider.run(
        viewResource,
        "application/x-ndjson",
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString().trim();
    // Should contain the inline patient, not Delta Lake patients.
    assertThat(content).contains("inline-patient-123").contains("InlineFamily");
  }

  @Test
  void multipleInlineResourcesOfSameType() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final List<String> patients =
        List.of(createPatientJson("inline-1", "Family1"), createPatientJson("inline-2", "Family2"));

    provider.run(
        viewResource,
        "application/x-ndjson",
        null,
        null,
        null,
        null,
        null,
        patients,
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    assertThat(lines).hasSize(2);
    assertThat(content).contains("inline-1").contains("inline-2");
  }

  @Test
  void inlineResourcesFilteredByViewResourceType() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    // Include both Patient and Observation, but the view only queries Patient.
    final List<String> resources =
        List.of(
            createPatientJson("patient-1", "PatientFamily"),
            createObservationJson("obs-1", "patient-1"));

    provider.run(
        viewResource,
        "application/x-ndjson",
        null,
        null,
        null,
        null,
        null,
        resources,
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString().trim();
    // Should only return the patient data.
    assertThat(content).contains("patient-1").doesNotContain("obs-1");
  }

  @Test
  void invalidInlineResourceThrowsException() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final List<String> invalidResources = List.of("{ not valid fhir }");
    final ca.uhn.fhir.rest.server.servlet.ServletRequestDetails requestDetails =
        mockRequestDetails(null);

    assertThatThrownBy(
            () ->
                provider.run(
                    viewResource,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    invalidResources,
                    requestDetails,
                    response))
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
    final ca.uhn.fhir.rest.server.servlet.ServletRequestDetails requestDetails =
        mockRequestDetails(null);

    assertThatThrownBy(
            () ->
                provider.run(
                    viewResource, null, null, null, null, null, null, null, requestDetails, null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("HTTP response is required");
  }

  // Verifies that accessing a choice element without specifying the type throws a clear error.
  @Test
  void choiceElementWithoutTypeThrowsDescriptiveError() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    // Create a view that accesses 'deceased' which is a choice element (deceased[x]).
    final IBaseResource viewResource = parseViewResource(createPatientViewWithChoiceElement());
    final String inlinePatient = createPatientJsonWithDeceased("test-1", "Smith", true);

    assertThatThrownBy(
            () ->
                provider.run(
                    viewResource,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    List.of(inlinePatient),
                    mockRequestDetails(null),
                    response))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("deceased")
        .hasMessageContaining("ofType");
  }

  // -------------------------------------------------------------------------
  // Row conversion tests
  // -------------------------------------------------------------------------

  @Test
  void ndjsonRowContainsExpectedFields() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("row-test-id", "RowTestFamily");

    provider.run(
        viewResource,
        "application/x-ndjson",
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString().trim();
    // Parse the JSON line to verify structure.
    @SuppressWarnings("unchecked")
    final Map<String, Object> row = gson.fromJson(content, Map.class);
    assertThat(row)
        .containsKey("id")
        .containsKey("family_name")
        .containsEntry("id", "row-test-id")
        .containsEntry("family_name", "RowTestFamily");
  }

  @Test
  void ndjsonHandlesNullValues() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    // Create patient without family name.
    final String patientJson =
        """
        {"resourceType":"Patient","id":"null-test"}
        """;

    provider.run(
        viewResource,
        "application/x-ndjson",
        null,
        null,
        null,
        null,
        null,
        List.of(patientJson),
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString().trim();
    // Should still produce valid JSON.
    @SuppressWarnings("unchecked")
    final Map<String, Object> row = gson.fromJson(content, Map.class);
    assertThat(row).containsKey("id");
    // Null values should not be present in JSON output.
    assertThat(row.get("family_name")).isNull();
  }

  @Test
  void csvOutputProducesValidFormat() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("csv-test-id", "CsvTestFamily");

    provider.run(
        viewResource,
        "text/csv",
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString();
    final String[] lines = content.split("\n");
    assertThat(lines).hasSizeGreaterThanOrEqualTo(2);
    // Header line.
    assertThat(lines[0]).contains("id").contains("family_name");
    // Data line.
    assertThat(lines[1]).contains("csv-test-id").contains("CsvTestFamily");
  }

  // -------------------------------------------------------------------------
  // ViewOutputFormat tests
  // -------------------------------------------------------------------------

  // Verifies that ViewOutputFormat correctly parses format codes and content types.
  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "ndjson, NDJSON",
    "application/x-ndjson, NDJSON",
    "csv, CSV",
    "text/csv, CSV",
    "json, JSON",
    "application/json, JSON"
  })
  void viewOutputFormatParsesFormatString(final String input, final String expectedFormat) {
    assertThat(ViewOutputFormat.fromString(input))
        .isEqualTo(ViewOutputFormat.valueOf(expectedFormat));
  }

  @Test
  void viewOutputFormatDefaultsToNdjsonForUnknown() {
    assertThat(ViewOutputFormat.fromString("unknown")).isEqualTo(ViewOutputFormat.NDJSON);
    assertThat(ViewOutputFormat.fromString("")).isEqualTo(ViewOutputFormat.NDJSON);
    assertThat(ViewOutputFormat.fromString(null)).isEqualTo(ViewOutputFormat.NDJSON);
  }

  // -------------------------------------------------------------------------
  // JSON output format tests
  // -------------------------------------------------------------------------

  // Verifies that JSON output returns an array containing a single object.
  @Test
  void jsonOutputReturnsArrayWithSingleRow() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(
        viewResource,
        "json",
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString();
    // Parse as array and verify structure.
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> rows = gson.fromJson(content, List.class);
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).containsEntry("id", "test-1").containsEntry("family_name", "Smith");
  }

  // Verifies that JSON output returns an array containing multiple objects.
  @Test
  void jsonOutputReturnsArrayWithMultipleRows() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final List<String> patients =
        List.of(
            createPatientJson("p1", "Smith"),
            createPatientJson("p2", "Jones"),
            createPatientJson("p3", "Brown"));

    provider.run(
        viewResource,
        "json",
        null,
        null,
        null,
        null,
        null,
        patients,
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString();
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> rows = gson.fromJson(content, List.class);
    assertThat(rows).hasSize(3);
  }

  // Verifies that each row in the JSON array matches the format of an NDJSON row.
  @Test
  void jsonRowFormatMatchesNdjsonRowFormat() throws IOException {
    // Run with NDJSON format.
    final MockHttpServletResponse ndjsonResponse = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("format-test", "FormatFamily");

    provider.run(
        viewResource,
        "ndjson",
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails(null),
        ndjsonResponse);

    final String ndjsonContent = ndjsonResponse.getContentAsString().trim();
    @SuppressWarnings("unchecked")
    final Map<String, Object> ndjsonRow = gson.fromJson(ndjsonContent, Map.class);

    // Run with JSON format.
    final MockHttpServletResponse jsonResponse = new MockHttpServletResponse();
    provider.run(
        viewResource,
        "json",
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails(null),
        jsonResponse);

    final String jsonContent = jsonResponse.getContentAsString();
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> jsonRows = gson.fromJson(jsonContent, List.class);

    // The row structure should be identical.
    assertThat(jsonRows).hasSize(1);
    assertThat(jsonRows.getFirst()).isEqualTo(ndjsonRow);
  }

  // Verifies that the limit parameter works correctly with JSON output.
  @Test
  void jsonOutputRespectsLimit() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final List<String> patients =
        List.of(
            createPatientJson("p1", "Smith"),
            createPatientJson("p2", "Jones"),
            createPatientJson("p3", "Brown"));

    provider.run(
        viewResource,
        "json",
        null,
        new IntegerType(2),
        null,
        null,
        null,
        patients,
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString();
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> rows = gson.fromJson(content, List.class);
    assertThat(rows).hasSize(2);
  }

  // -------------------------------------------------------------------------
  // Accept header tests
  // -------------------------------------------------------------------------

  // Verifies that the Accept header determines output format when _format is not provided.
  @Test
  void acceptHeaderDeterminesOutputFormat() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(
        viewResource,
        null, // No _format parameter.
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails("text/csv"),
        response);

    assertThat(response.getContentType()).startsWith("text/csv");
  }

  // Verifies that _format parameter takes precedence over Accept header.
  @Test
  void formatParamOverridesAcceptHeader() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(
        viewResource,
        "ndjson", // Explicit _format parameter.
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails("text/csv"), // Different Accept header.
        response);

    // _format should take precedence.
    assertThat(response.getContentType()).startsWith("application/x-ndjson");
  }

  // Verifies that wildcard Accept header defaults to NDJSON.
  @Test
  void wildcardAcceptHeaderDefaultsToNdjson() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createSimplePatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    provider.run(
        viewResource,
        null,
        null,
        null,
        null,
        null,
        null,
        List.of(inlinePatient),
        mockRequestDetails("*/*"),
        response);

    assertThat(response.getContentType()).startsWith("application/x-ndjson");
  }

  // -------------------------------------------------------------------------
  // Constant parsing tests
  // -------------------------------------------------------------------------

  // Verifies that constants with valueCode are correctly parsed and used in FHIRPath expressions.
  @Test
  void constantsWithValueCodeAreParsed() throws IOException {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createPatientViewWithConstant());
    final String matchingPatient = createPatientJsonWithGender("p1", "Smith", "John", "male");
    final String nonMatchingPatient = createPatientJsonWithGender("p2", "Jones", "Jane", "female");

    provider.run(
        viewResource,
        "application/x-ndjson",
        null,
        null,
        null,
        null,
        null,
        List.of(matchingPatient, nonMatchingPatient),
        mockRequestDetails(null),
        response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    // Only the matching patient should be returned due to the where clause using the constant.
    assertThat(lines).hasSize(1);
    assertThat(content).contains("p1").doesNotContain("p2");
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
    view.put(
        "select",
        List.of(
            Map.of("column", List.of(Map.of("name", "id", "path", "id"))),
            Map.of(
                "column", List.of(Map.of("name", "family_name", "path", "name.first().family")))));
    return gson.toJson(view);
  }

  @Nonnull
  private String createMultiColumnPatientView() {
    final Map<String, Object> view = new HashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("name", "multi_column_patient_view");
    view.put("resource", "Patient");
    view.put("status", "active");
    view.put(
        "select",
        List.of(
            Map.of("column", List.of(Map.of("name", "id", "path", "id"))),
            Map.of("column", List.of(Map.of("name", "family_name", "path", "name.first().family"))),
            Map.of(
                "column",
                List.of(Map.of("name", "given_name", "path", "name.first().given.first()"))),
            Map.of("column", List.of(Map.of("name", "gender", "path", "gender")))));
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
  private String createPatientJsonWithGender(
      @Nonnull final String id,
      @Nonnull final String familyName,
      @Nonnull final String givenName,
      @Nonnull final String gender) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName().setFamily(familyName).addGiven(givenName);
    patient.setGender(AdministrativeGender.fromCode(gender));
    return fhirContext.newJsonParser().encodeResourceToString(patient);
  }

  @Nonnull
  private String createObservationJson(@Nonnull final String id, @Nonnull final String patientId) {
    final Observation observation = new Observation();
    observation.setId(id);
    observation.setSubject(new Reference("Patient/" + patientId));
    observation.setStatus(Observation.ObservationStatus.FINAL);
    return fhirContext.newJsonParser().encodeResourceToString(observation);
  }

  // Creates a ViewDefinition with a constant using valueCode in the where clause.
  @Nonnull
  private String createPatientViewWithConstant() {
    final Map<String, Object> view = new HashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("name", "patient_view_with_constant");
    view.put("resource", "Patient");
    view.put("status", "active");
    view.put("select", List.of(Map.of("column", List.of(Map.of("name", "id", "path", "id")))));
    view.put("where", List.of(Map.of("path", "gender = %target_gender")));
    view.put("constant", List.of(Map.of("name", "target_gender", "valueCode", "male")));
    return gson.toJson(view);
  }

  /** Creates a mock ServletRequestDetails with the specified Accept header. */
  @Nonnull
  private ca.uhn.fhir.rest.server.servlet.ServletRequestDetails mockRequestDetails(
      @jakarta.annotation.Nullable final String acceptHeader) {
    final jakarta.servlet.http.HttpServletRequest httpRequest =
        org.mockito.Mockito.mock(jakarta.servlet.http.HttpServletRequest.class);
    org.mockito.Mockito.when(httpRequest.getHeader("Accept")).thenReturn(acceptHeader);

    final ca.uhn.fhir.rest.server.servlet.ServletRequestDetails requestDetails =
        org.mockito.Mockito.mock(ca.uhn.fhir.rest.server.servlet.ServletRequestDetails.class);
    org.mockito.Mockito.when(requestDetails.getServletRequest()).thenReturn(httpRequest);

    return requestDetails;
  }

  // Creates a ViewDefinition that accesses the 'deceased' choice element directly.
  @Nonnull
  private String createPatientViewWithChoiceElement() {
    final Map<String, Object> view = new HashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("name", "patient_view_with_choice");
    view.put("resource", "Patient");
    view.put("status", "active");
    view.put(
        "select",
        List.of(
            Map.of("column", List.of(Map.of("name", "id", "path", "id"))),
            // This accesses deceased[x] directly without specifying the type.
            Map.of("column", List.of(Map.of("name", "deceased", "path", "deceased")))));
    return gson.toJson(view);
  }

  @Nonnull
  private String createPatientJsonWithDeceased(
      @Nonnull final String id, @Nonnull final String familyName, final boolean deceased) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName().setFamily(familyName);
    patient.setDeceased(new BooleanType(deceased));
    return fhirContext.newJsonParser().encodeResourceToString(patient);
  }
}
