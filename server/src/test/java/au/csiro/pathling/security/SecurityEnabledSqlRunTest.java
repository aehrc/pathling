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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.GroupMemberService;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.operations.export.ExportDataSourceBuilder;
import au.csiro.pathling.operations.sql.ContextArtefactParser;
import au.csiro.pathling.operations.sql.ResolvedFilters;
import au.csiro.pathling.operations.sql.ResolvedSubject;
import au.csiro.pathling.operations.sql.SqlFilterResolver;
import au.csiro.pathling.operations.sql.SqlRunProvider;
import au.csiro.pathling.operations.sql.SubjectKind;
import au.csiro.pathling.operations.sql.SubjectResolver;
import au.csiro.pathling.operations.sql.SuppliedArtefacts;
import au.csiro.pathling.operations.sqlquery.SqlQueryPipeline;
import au.csiro.pathling.operations.sqlquery.SqlQueryResultStreamer;
import au.csiro.pathling.operations.view.ViewExecutionHelper;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.RequestTypeEnum;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.Nonnull;
import jakarta.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Security tests for the {@code $sql-run} operation over a ViewDefinition subject: running a view
 * is a way of reading the resources it projects, so the caller needs the read authority for the
 * projected type in addition to the operation authority.
 *
 * <p>The operation-level {@code @OperationAccess} mechanism itself is covered generically by {@link
 * SecurityAspectTest}; these tests cover the resource-level check that sits behind it.
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
  ViewExecutionHelper.class,
  ReadExecutor.class
})
class SecurityEnabledSqlRunTest extends SecurityTest {

  private static final String ERROR_MSG_TEMPLATE = "Missing authority: 'pathling:%s'";

  @Autowired private FhirContext fhirContext;

  @Autowired private ViewExecutionHelper viewExecutionHelper;

  private final Gson gson = new GsonBuilder().create();

  @Test
  @DisplayName("A caller who may not read Patient may not run a Patient view")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:sql-run"})
  void deniedWithoutReadAuthority() {
    assertThatThrownBy(() -> runView(createPatientView(), createPatientJson("test-1", "Smith")))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessage(ERROR_MSG_TEMPLATE.formatted("read:Patient"));
  }

  @Test
  @DisplayName("A caller who may read Patient may run a Patient view")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:sql-run", "pathling:read:Patient"})
  void succeedsWithSpecificReadAuthority() {
    final MockHttpServletResponse response = new MockHttpServletResponse();

    assertThatNoException()
        .isThrownBy(
            () -> runView(createPatientView(), createPatientJson("test-1", "Smith"), response));

    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  @DisplayName("A caller who may read every resource type may run any view")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:sql-run", "pathling:read"})
  void succeedsWithWildcardReadAuthority() {
    assertThatNoException()
        .isThrownBy(() -> runView(createPatientView(), createPatientJson("test-1", "Smith")));
  }

  @Test
  @DisplayName("Read authority for one type does not grant a view over another")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:sql-run", "pathling:read:Observation"})
  void deniedWithWrongResourceTypeAuthority() {
    assertThatThrownBy(() -> runView(createPatientView(), createPatientJson("test-1", "Smith")))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessage(ERROR_MSG_TEMPLATE.formatted("read:Patient"));
  }

  @Test
  @DisplayName("A caller who may read Observation may run an Observation view")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:sql-run", "pathling:read:Observation"})
  void succeedsWithMatchingResourceType() {
    assertThatNoException()
        .isThrownBy(
            () -> runView(createObservationView(), createObservationJson("obs-1", "test-1")));
  }

  // ---- helpers ----

  /** Runs an inline view over inline data, discarding the response. */
  private void runView(@Nonnull final String viewJson, @Nonnull final String resourceJson) {
    runView(viewJson, resourceJson, new MockHttpServletResponse());
  }

  /**
   * Runs an inline view over inline data through a provider whose collaborators are mocked, so the
   * only live behaviour is the view evaluation path and the authority checks along it.
   */
  private void runView(
      @Nonnull final String viewJson,
      @Nonnull final String resourceJson,
      @Nonnull final MockHttpServletResponse response) {
    final IBaseResource viewResource = fhirContext.newJsonParser().parseResource(viewJson);

    final SubjectResolver subjectResolver = mock(SubjectResolver.class);
    when(subjectResolver.resolve(any(), any(), any(), any()))
        .thenReturn(new ResolvedSubject(SubjectKind.VIEW_DEFINITION, viewResource, null));

    final SqlFilterResolver filterResolver = mock(SqlFilterResolver.class);
    when(filterResolver.resolve(any(), any(), any()))
        .thenReturn(new ResolvedFilters(Set.of(), null, List.of()));

    final ContextArtefactParser contextParser = mock(ContextArtefactParser.class);
    when(contextParser.parse(any())).thenReturn(SuppliedArtefacts.empty());

    final ExportDataSourceBuilder dataSourceBuilder = mock(ExportDataSourceBuilder.class);
    when(dataSourceBuilder.build(any(), any(), any())).thenReturn(mock(QueryableDataSource.class));

    final SqlRunProvider provider =
        new SqlRunProvider(
            subjectResolver,
            filterResolver,
            contextParser,
            viewExecutionHelper,
            mock(SqlQueryPipeline.class),
            mock(SqlQueryResultStreamer.class),
            mock(QueryableDataSource.class),
            dataSourceBuilder);

    provider.run(
        null,
        null,
        viewResource,
        null,
        null,
        List.of(resourceJson),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        mockRequestDetails(),
        response);
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
    when(requestDetails.getRequestType()).thenReturn(RequestTypeEnum.POST);

    return requestDetails;
  }
}
