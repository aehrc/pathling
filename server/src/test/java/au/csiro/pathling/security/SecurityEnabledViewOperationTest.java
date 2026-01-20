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

package au.csiro.pathling.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.operations.compartment.GroupMemberService;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.operations.view.ViewDefinitionRunProvider;
import au.csiro.pathling.operations.view.ViewExecutionHelper;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.Nonnull;
import jakarta.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Security tests for ViewDefinition operations ($viewdefinition-run and $viewdefinition-export).
 * Verifies that resource-level authorization is enforced in addition to operation-level authority.
 *
 * @author John Grimes
 */
@TestPropertySource(
    properties = {
      "pathling.auth.enabled=true",
      "pathling.auth.issuer=https://pathling.acme.com/fhir"
    })
@MockitoBean(types = OidcConfiguration.class)
@MockitoBean(types = JwtDecoder.class)
@MockitoBean(types = JwtAuthenticationConverter.class)
@Import({
  FhirServerTestConfiguration.class,
  PatientCompartmentService.class,
  GroupMemberService.class,
  ViewExecutionHelper.class
})
class SecurityEnabledViewOperationTest extends SecurityTest {

  private static final String ERROR_MSG_TEMPLATE = "Missing authority: 'pathling:%s'";

  @Autowired private ApplicationContext applicationContext;

  @Autowired private FhirContext fhirContext;

  @Autowired private ViewExecutionHelper viewExecutionHelper;

  private final Gson gson = new GsonBuilder().create();

  // -------------------------------------------------------------------------
  // $viewdefinition-run tests
  // -------------------------------------------------------------------------

  @Test
  @DisplayName(
      "$viewdefinition-run: User with view-run authority but no read authority → AccessDeniedError")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:view-run"})
  void viewRunDeniedWithoutReadAuthority() {
    final ViewDefinitionRunProvider provider = new ViewDefinitionRunProvider(viewExecutionHelper);
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createPatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

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
                    mockRequestDetails(),
                    response))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessage(ERROR_MSG_TEMPLATE.formatted("read:Patient"));
  }

  @Test
  @DisplayName(
      "$viewdefinition-run: User with view-run and read:Patient authority → succeeds for Patient"
          + " view")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:view-run", "pathling:read:Patient"})
  void viewRunSucceedsWithSpecificReadAuthority() {
    final ViewDefinitionRunProvider provider = new ViewDefinitionRunProvider(viewExecutionHelper);
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createPatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    assertThatNoException()
        .isThrownBy(
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
                    mockRequestDetails(),
                    response));

    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  @DisplayName(
      "$viewdefinition-run: User with view-run and read (all resources) authority → succeeds")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:view-run", "pathling:read"})
  void viewRunSucceedsWithWildcardReadAuthority() {
    final ViewDefinitionRunProvider provider = new ViewDefinitionRunProvider(viewExecutionHelper);
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createPatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

    assertThatNoException()
        .isThrownBy(
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
                    mockRequestDetails(),
                    response));

    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  @DisplayName(
      "$viewdefinition-run: User with view-run and read:Observation but view targets Patient →"
          + " AccessDeniedError")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:view-run", "pathling:read:Observation"})
  void viewRunDeniedWithWrongResourceTypeAuthority() {
    final ViewDefinitionRunProvider provider = new ViewDefinitionRunProvider(viewExecutionHelper);
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createPatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

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
                    mockRequestDetails(),
                    response))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessage(ERROR_MSG_TEMPLATE.formatted("read:Patient"));
  }

  @Test
  @DisplayName("$viewdefinition-run: User without view-run authority → AccessDeniedError")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:read:Patient"})
  void viewRunDeniedWithoutOperationAuthority() {
    // Use the Spring-managed bean to get the AOP proxy that enforces @OperationAccess.
    final ViewDefinitionRunProvider provider =
        applicationContext.getBean(ViewDefinitionRunProvider.class);
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createPatientView());
    final String inlinePatient = createPatientJson("test-1", "Smith");

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
                    mockRequestDetails(),
                    response))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessage(ERROR_MSG_TEMPLATE.formatted("view-run"));
  }

  @Test
  @DisplayName(
      "$viewdefinition-run: User with view-run and read:Observation can run Observation view")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:view-run", "pathling:read:Observation"})
  void viewRunSucceedsWithMatchingResourceType() {
    final ViewDefinitionRunProvider provider = new ViewDefinitionRunProvider(viewExecutionHelper);
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final IBaseResource viewResource = parseViewResource(createObservationView());
    final String inlineObservation = createObservationJson("obs-1", "patient-1");

    assertThatNoException()
        .isThrownBy(
            () ->
                provider.run(
                    viewResource,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    List.of(inlineObservation),
                    mockRequestDetails(),
                    response));

    assertThat(response.getStatus()).isEqualTo(200);
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  @Nonnull
  private IBaseResource parseViewResource(@Nonnull final String viewJson) {
    return fhirContext.newJsonParser().parseResource(viewJson);
  }

  @Nonnull
  private String createPatientView() {
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
  private String createObservationView() {
    final Map<String, Object> view = new HashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("name", "test_observation_view");
    view.put("resource", "Observation");
    view.put("status", "active");
    view.put(
        "select",
        List.of(
            Map.of("column", List.of(Map.of("name", "id", "path", "id"))),
            Map.of("column", List.of(Map.of("name", "status", "path", "status")))));
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
  private String createObservationJson(@Nonnull final String id, @Nonnull final String patientId) {
    final org.hl7.fhir.r4.model.Observation observation = new org.hl7.fhir.r4.model.Observation();
    observation.setId(id);
    observation.setSubject(new org.hl7.fhir.r4.model.Reference("Patient/" + patientId));
    observation.setStatus(org.hl7.fhir.r4.model.Observation.ObservationStatus.FINAL);
    return fhirContext.newJsonParser().encodeResourceToString(observation);
  }

  @Nonnull
  private ServletRequestDetails mockRequestDetails() {
    final HttpServletRequest httpRequest = mock(HttpServletRequest.class);
    when(httpRequest.getHeader("Accept")).thenReturn(null);

    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    when(requestDetails.getServletRequest()).thenReturn(httpRequest);

    return requestDetails;
  }
}
