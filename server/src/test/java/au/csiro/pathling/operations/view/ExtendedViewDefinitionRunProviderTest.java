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
import au.csiro.pathling.operations.compartment.GroupMemberService;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.CustomObjectDataSource;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * Unit tests for {@link ExtendedViewDefinitionRunProvider}, focusing on cross-resource join
 * functionality.
 *
 * @author jkiddo
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
class ExtendedViewDefinitionRunProviderTest {

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private ViewExecutionHelper viewExecutionHelper;

  @Autowired private ServerConfiguration serverConfiguration;

  private FhirEncoders fhirEncoders;

  private Gson gson;

  @BeforeEach
  void setUp() {
    fhirEncoders = pathlingContext.getFhirEncoders();
    gson = new GsonBuilder().create();
  }

  // -------------------------------------------------------------------------
  // Single view (no join) tests
  // -------------------------------------------------------------------------

  @Test
  void singleAnchorViewReturnsAllMatchingRows() throws IOException {
    final List<IBaseResource> resources =
        List.of(createPatient("p1", "Smith"), createPatient("p2", "Jones"));

    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters()
            .addParameter("anchorType", new CodeType("Patient"))
            .addParameter("_limit", new IntegerType(100));
    addViewPart(parameters, "Patient", List.of("id", "name.first().family"), List.of(), null, null);

    provider.run(parameters, mockRequestDetails(), response);

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getContentType()).startsWith("application/x-ndjson");

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    assertThat(lines).hasSize(2);
    assertThat(content).contains("p1").contains("p2");
  }

  @Test
  void singleAnchorViewWithWhereClauseFiltersRows() throws IOException {
    final List<IBaseResource> resources =
        List.of(
            createPatientWithGender("p1", "Smith", "male"),
            createPatientWithGender("p2", "Jones", "female"));

    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters().addParameter("anchorType", new CodeType("Patient"));
    addViewPart(
        parameters, "Patient", List.of("id", "gender"), List.of("gender = 'male'"), null, null);

    provider.run(parameters, mockRequestDetails(), response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    assertThat(lines).hasSize(1);
    assertThat(content).contains("p1").doesNotContain("p2");
  }

  // -------------------------------------------------------------------------
  // Join tests
  // -------------------------------------------------------------------------

  @Test
  void joinWithObservationsFiltersToMatchingPatients() throws IOException {
    // Patient p1 has an observation, p2 does not.
    final List<IBaseResource> resources =
        List.of(
            createPatient("p1", "Smith"),
            createPatient("p2", "Jones"),
            createObservation("obs1", "p1"));

    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters().addParameter("anchorType", new CodeType("Patient"));
    addViewPart(parameters, "Patient", List.of("id", "name.first().family"), List.of(), null, null);
    addViewPart(
        parameters, "Observation", List.of("id"), List.of(), "Patient", "subject.reference");

    provider.run(parameters, mockRequestDetails(), response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    assertThat(lines).hasSize(1);
    // Only p1 should appear since p2 has no observations.
    assertThat(content).contains("p1").doesNotContain("p2");
  }

  @Test
  void joinWithObservationWhereClauseFiltersToMatchingPatients() throws IOException {
    // Both patients have observations, but only p1's observation is "final".
    final Observation obs1 = createObservation("obs1", "p1");
    obs1.setStatus(Observation.ObservationStatus.FINAL);
    final Observation obs2 = createObservation("obs2", "p2");
    obs2.setStatus(Observation.ObservationStatus.PRELIMINARY);

    final List<IBaseResource> resources =
        List.of(createPatient("p1", "Smith"), createPatient("p2", "Jones"), obs1, obs2);

    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters().addParameter("anchorType", new CodeType("Patient"));
    addViewPart(parameters, "Patient", List.of("id"), List.of(), null, null);
    addViewPart(
        parameters,
        "Observation",
        List.of("id"),
        List.of("status = 'final'"),
        "Patient",
        "subject.reference");

    provider.run(parameters, mockRequestDetails(), response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    assertThat(lines).hasSize(1);
    assertThat(content).contains("p1").doesNotContain("p2");
  }

  @Test
  void joinWithMultipleViewsIntersectsResults() throws IOException {
    // p1: has observation AND condition
    // p2: has observation but NO condition
    // p3: has condition but NO observation
    final List<IBaseResource> resources =
        List.of(
            createPatient("p1", "Smith"),
            createPatient("p2", "Jones"),
            createPatient("p3", "Brown"),
            createObservation("obs1", "p1"),
            createObservation("obs2", "p2"),
            createCondition("cond1", "p1"),
            createCondition("cond2", "p3"));

    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters().addParameter("anchorType", new CodeType("Patient"));
    addViewPart(parameters, "Patient", List.of("id"), List.of(), null, null);
    addViewPart(
        parameters, "Observation", List.of("id"), List.of(), "Patient", "subject.reference");
    addViewPart(parameters, "Condition", List.of("id"), List.of(), "Patient", "subject.reference");

    provider.run(parameters, mockRequestDetails(), response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    // Only p1 has both an observation and a condition.
    assertThat(lines).hasSize(1);
    assertThat(content).contains("p1").doesNotContain("p2").doesNotContain("p3");
  }

  @Test
  void joinProducingEmptyResultReturnsNoRows() throws IOException {
    // Patient p1 exists but has no observations.
    final List<IBaseResource> resources =
        List.of(createPatient("p1", "Smith"), createObservation("obs1", "nonexistent"));

    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters().addParameter("anchorType", new CodeType("Patient"));
    addViewPart(parameters, "Patient", List.of("id"), List.of(), null, null);
    addViewPart(
        parameters, "Observation", List.of("id"), List.of(), "Patient", "subject.reference");

    provider.run(parameters, mockRequestDetails(), response);

    final String content = response.getContentAsString().trim();
    assertThat(content).isEmpty();
  }

  @Test
  void joinWithMultipleObservationsForSamePatientDeduplicates() throws IOException {
    // p1 has two observations - should appear only once in results.
    final List<IBaseResource> resources =
        List.of(
            createPatient("p1", "Smith"),
            createObservation("obs1", "p1"),
            createObservation("obs2", "p1"));

    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters().addParameter("anchorType", new CodeType("Patient"));
    addViewPart(parameters, "Patient", List.of("id"), List.of(), null, null);
    addViewPart(
        parameters, "Observation", List.of("id"), List.of(), "Patient", "subject.reference");

    provider.run(parameters, mockRequestDetails(), response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    assertThat(lines).hasSize(1);
    assertThat(content).contains("p1");
  }

  // -------------------------------------------------------------------------
  // Output format tests
  // -------------------------------------------------------------------------

  @Test
  void csvOutputFormatWithJoin() throws IOException {
    final List<IBaseResource> resources =
        List.of(createPatient("p1", "Smith"), createObservation("obs1", "p1"));

    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters()
            .addParameter("anchorType", new CodeType("Patient"))
            .addParameter("_format", new StringType("csv"));
    addViewPart(parameters, "Patient", List.of("id", "name.first().family"), List.of(), null, null);
    addViewPart(
        parameters, "Observation", List.of("id"), List.of(), "Patient", "subject.reference");

    provider.run(parameters, mockRequestDetails(), response);

    assertThat(response.getContentType()).startsWith("text/csv");
    final String content = response.getContentAsString();
    final String[] lines = content.split("\n");
    // Header + 1 data row.
    assertThat(lines).hasSize(2);
    assertThat(lines[0]).contains("id").contains("name_first_family");
    assertThat(lines[1]).contains("p1").contains("Smith");
  }

  @Test
  void jsonOutputFormatWithJoin() throws IOException {
    final List<IBaseResource> resources =
        List.of(createPatient("p1", "Smith"), createObservation("obs1", "p1"));

    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters()
            .addParameter("anchorType", new CodeType("Patient"))
            .addParameter("_format", new StringType("json"));
    addViewPart(parameters, "Patient", List.of("id", "name.first().family"), List.of(), null, null);
    addViewPart(
        parameters, "Observation", List.of("id"), List.of(), "Patient", "subject.reference");

    provider.run(parameters, mockRequestDetails(), response);

    assertThat(response.getContentType()).startsWith("application/json");
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> rows = gson.fromJson(response.getContentAsString(), List.class);
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).containsEntry("id", "p1");
  }

  @Test
  void csvHeaderCanBeExcluded() throws IOException {
    final List<IBaseResource> resources =
        List.of(createPatient("p1", "Smith"), createObservation("obs1", "p1"));

    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters()
            .addParameter("anchorType", new CodeType("Patient"))
            .addParameter("_format", new StringType("csv"))
            .addParameter("header", new BooleanType(false));
    addViewPart(parameters, "Patient", List.of("id"), List.of(), null, null);
    addViewPart(
        parameters, "Observation", List.of("id"), List.of(), "Patient", "subject.reference");

    provider.run(parameters, mockRequestDetails(), response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    // No header, just data.
    assertThat(lines).hasSize(1);
    assertThat(lines[0]).contains("p1");
  }

  // -------------------------------------------------------------------------
  // Limit tests
  // -------------------------------------------------------------------------

  @Test
  void limitRestrictsRowCount() throws IOException {
    final List<IBaseResource> resources =
        List.of(
            createPatient("p1", "Smith"),
            createPatient("p2", "Jones"),
            createPatient("p3", "Brown"),
            createObservation("obs1", "p1"),
            createObservation("obs2", "p2"),
            createObservation("obs3", "p3"));

    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters()
            .addParameter("anchorType", new CodeType("Patient"))
            .addParameter("_limit", new IntegerType(2));
    addViewPart(parameters, "Patient", List.of("id"), List.of(), null, null);
    addViewPart(
        parameters, "Observation", List.of("id"), List.of(), "Patient", "subject.reference");

    provider.run(parameters, mockRequestDetails(), response);

    final String content = response.getContentAsString().trim();
    final String[] lines = content.split("\n");
    assertThat(lines).hasSize(2);
  }

  // -------------------------------------------------------------------------
  // Error handling tests
  // -------------------------------------------------------------------------

  @Test
  void missingAnchorTypeThrowsException() {
    final List<IBaseResource> resources = List.of(createPatient("p1", "Smith"));
    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters = new Parameters();
    addViewPart(parameters, "Patient", List.of("id"), List.of(), null, null);

    assertThatThrownBy(() -> provider.run(parameters, mockRequestDetails(), response))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("anchorType");
  }

  @Test
  void emptyParametersThrowsException() {
    final List<IBaseResource> resources = List.of(createPatient("p1", "Smith"));
    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters = new Parameters();

    assertThatThrownBy(() -> provider.run(parameters, mockRequestDetails(), response))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Parameters must include");
  }

  @Test
  void noViewsThrowsException() {
    final List<IBaseResource> resources = List.of(createPatient("p1", "Smith"));
    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters().addParameter("anchorType", new CodeType("Patient"));

    assertThatThrownBy(() -> provider.run(parameters, mockRequestDetails(), response))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("At least one view");
  }

  @Test
  void noAnchorViewThrowsException() {
    final List<IBaseResource> resources =
        List.of(createPatient("p1", "Smith"), createObservation("obs1", "p1"));
    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    // anchorType is Patient, but only an Observation view is provided.
    final Parameters parameters =
        new Parameters().addParameter("anchorType", new CodeType("Patient"));
    addViewPart(
        parameters, "Observation", List.of("id"), List.of(), "Patient", "subject.reference");

    assertThatThrownBy(() -> provider.run(parameters, mockRequestDetails(), response))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("No view defined for anchor type");
  }

  @Test
  void joinViewWithoutJoinOnThrowsException() {
    final List<IBaseResource> resources =
        List.of(createPatient("p1", "Smith"), createObservation("obs1", "p1"));
    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);
    final MockHttpServletResponse response = new MockHttpServletResponse();

    final Parameters parameters =
        new Parameters().addParameter("anchorType", new CodeType("Patient"));
    addViewPart(parameters, "Patient", List.of("id"), List.of(), null, null);
    // joinTo is set but joinOn is missing.
    addViewPart(parameters, "Observation", List.of("id"), List.of(), "Patient", null);

    assertThatThrownBy(() -> provider.run(parameters, mockRequestDetails(), response))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("must specify joinOn");
  }

  @Test
  void nullResponseThrowsException() {
    final List<IBaseResource> resources = List.of(createPatient("p1", "Smith"));
    final ExtendedViewDefinitionRunProvider provider = createProvider(resources);

    final Parameters parameters =
        new Parameters().addParameter("anchorType", new CodeType("Patient"));
    addViewPart(parameters, "Patient", List.of("id"), List.of(), null, null);

    assertThatThrownBy(() -> provider.run(parameters, mockRequestDetails(), null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("HTTP response is required");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  @Nonnull
  private ExtendedViewDefinitionRunProvider createProvider(
      @Nonnull final List<IBaseResource> resources) {
    final QueryableDataSource dataSource =
        new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, resources);
    return new ExtendedViewDefinitionRunProvider(
        dataSource, viewExecutionHelper, serverConfiguration);
  }

  /**
   * Adds a view part to the Parameters resource.
   *
   * @param parameters the Parameters to add the view to
   * @param resourceType the FHIR resource type for the view
   * @param selectExprs FHIRPath select expressions
   * @param whereExprs FHIRPath where expressions
   * @param joinTo the resource type to join to, or null
   * @param joinOn the FHIRPath join expression, or null
   */
  private static void addViewPart(
      @Nonnull final Parameters parameters,
      @Nonnull final String resourceType,
      @Nonnull final List<String> selectExprs,
      @Nonnull final List<String> whereExprs,
      @jakarta.annotation.Nullable final String joinTo,
      @jakarta.annotation.Nullable final String joinOn) {
    final Parameters.ParametersParameterComponent viewParam = parameters.addParameter();
    viewParam.setName("view");
    viewParam.addPart().setName("resource").setValue(new CodeType(resourceType));
    for (final String expr : selectExprs) {
      viewParam.addPart().setName("select").setValue(new StringType(expr));
    }
    for (final String expr : whereExprs) {
      viewParam.addPart().setName("where").setValue(new StringType(expr));
    }
    if (joinTo != null) {
      viewParam.addPart().setName("joinTo").setValue(new CodeType(joinTo));
    }
    if (joinOn != null) {
      viewParam.addPart().setName("joinOn").setValue(new StringType(joinOn));
    }
  }

  @Nonnull
  private static Patient createPatient(@Nonnull final String id, @Nonnull final String family) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName().setFamily(family);
    return patient;
  }

  @Nonnull
  private static Patient createPatientWithGender(
      @Nonnull final String id, @Nonnull final String family, @Nonnull final String gender) {
    final Patient patient = createPatient(id, family);
    patient.setGender(org.hl7.fhir.r4.model.Enumerations.AdministrativeGender.fromCode(gender));
    return patient;
  }

  @Nonnull
  private static Observation createObservation(
      @Nonnull final String id, @Nonnull final String patientId) {
    final Observation observation = new Observation();
    observation.setId(id);
    observation.setSubject(new Reference("Patient/" + patientId));
    observation.setStatus(Observation.ObservationStatus.FINAL);
    return observation;
  }

  @Nonnull
  private static Condition createCondition(
      @Nonnull final String id, @Nonnull final String patientId) {
    final Condition condition = new Condition();
    condition.setId(id);
    condition.setSubject(new Reference("Patient/" + patientId));
    return condition;
  }

  @Nonnull
  private static ca.uhn.fhir.rest.server.servlet.ServletRequestDetails mockRequestDetails() {
    final jakarta.servlet.http.HttpServletRequest httpRequest =
        Mockito.mock(jakarta.servlet.http.HttpServletRequest.class);
    Mockito.when(httpRequest.getHeader("Accept")).thenReturn(null);

    final ca.uhn.fhir.rest.server.servlet.ServletRequestDetails requestDetails =
        Mockito.mock(ca.uhn.fhir.rest.server.servlet.ServletRequestDetails.class);
    Mockito.when(requestDetails.getServletRequest()).thenReturn(httpRequest);

    return requestDetails;
  }
}
