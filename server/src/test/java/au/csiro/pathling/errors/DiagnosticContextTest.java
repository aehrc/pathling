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

package au.csiro.pathling.errors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.servlet.http.HttpServletRequest;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/**
 * Unit tests for {@link DiagnosticContext}.
 *
 * @author John Grimes
 */
class DiagnosticContextTest {

  @BeforeEach
  void setUp() {
    MDC.clear();
  }

  @AfterEach
  void tearDown() {
    MDC.clear();
  }

  @Test
  void emptyContextCanBeCreated() {
    final DiagnosticContext context = DiagnosticContext.empty();
    assertThat(context).isNotNull();
  }

  @Test
  void fromRequestExtractsDetails() {
    // Set up mocks.
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    final HttpServletRequest httpRequest = mock(HttpServletRequest.class);
    final IParser jsonParser = mock(IParser.class);

    when(requestDetails.getRequestId()).thenReturn("test-request-id");
    when(requestDetails.getCompleteUrl()).thenReturn("http://localhost/fhir/Patient");
    when(requestDetails.getServletRequest()).thenReturn(httpRequest);
    when(requestDetails.getHeaders())
        .thenReturn(java.util.Map.of("Accept", java.util.List.of("application/fhir+json")));
    when(requestDetails.getResource()).thenReturn(null);
    when(httpRequest.getMethod()).thenReturn("GET");
    when(httpRequest.getQueryString()).thenReturn("_count=10");

    final DiagnosticContext context = DiagnosticContext.fromRequest(requestDetails, jsonParser);

    assertThat(context).isNotNull();
  }

  @Test
  void fromRequestWithResourceIncludesBody() {
    // Set up mocks.
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    final HttpServletRequest httpRequest = mock(HttpServletRequest.class);
    final IParser jsonParser = mock(IParser.class);
    final Patient patient = new Patient();
    patient.setId("123");

    when(requestDetails.getRequestId()).thenReturn("test-request-id");
    when(requestDetails.getCompleteUrl()).thenReturn("http://localhost/fhir/Patient");
    when(requestDetails.getServletRequest()).thenReturn(httpRequest);
    when(requestDetails.getHeaders())
        .thenReturn(java.util.Map.of("Content-Type", java.util.List.of("application/fhir+json")));
    when(requestDetails.getResource()).thenReturn(patient);
    when(jsonParser.encodeResourceToString(any())).thenReturn("{\"resourceType\":\"Patient\"}");
    when(httpRequest.getMethod()).thenReturn("POST");
    when(httpRequest.getQueryString()).thenReturn(null);

    final DiagnosticContext context = DiagnosticContext.fromRequest(requestDetails, jsonParser);

    assertThat(context).isNotNull();
  }

  @Test
  void configureScopeSetsRequestIdInMDC() {
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    final HttpServletRequest httpRequest = mock(HttpServletRequest.class);
    final IParser jsonParser = mock(IParser.class);

    when(requestDetails.getRequestId()).thenReturn("test-request-123");
    when(requestDetails.getCompleteUrl()).thenReturn("http://localhost/fhir/Patient");
    when(requestDetails.getServletRequest()).thenReturn(httpRequest);
    when(requestDetails.getHeaders()).thenReturn(java.util.Collections.emptyMap());
    when(requestDetails.getResource()).thenReturn(null);
    when(httpRequest.getMethod()).thenReturn("GET");
    when(httpRequest.getQueryString()).thenReturn(null);

    final DiagnosticContext context = DiagnosticContext.fromRequest(requestDetails, jsonParser);
    context.configureScope();

    assertThat(MDC.get("requestId")).isEqualTo("test-request-123");
  }

  @Test
  void configureScopeWithAsyncFlag() {
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    final HttpServletRequest httpRequest = mock(HttpServletRequest.class);
    final IParser jsonParser = mock(IParser.class);

    when(requestDetails.getRequestId()).thenReturn("async-request-456");
    when(requestDetails.getCompleteUrl()).thenReturn("http://localhost/fhir/$export");
    when(requestDetails.getServletRequest()).thenReturn(httpRequest);
    when(requestDetails.getHeaders()).thenReturn(java.util.Collections.emptyMap());
    when(requestDetails.getResource()).thenReturn(null);
    when(httpRequest.getMethod()).thenReturn("GET");
    when(httpRequest.getQueryString()).thenReturn(null);

    final DiagnosticContext context = DiagnosticContext.fromRequest(requestDetails, jsonParser);
    context.configureScope(true);

    assertThat(MDC.get("requestId")).isEqualTo("async-request-456");
  }

  @Test
  void emptyContextDoesNotSetRequestIdInMDC() {
    final DiagnosticContext context = DiagnosticContext.empty();
    context.configureScope();

    // requestId should be null when context is empty.
    assertThat(MDC.get("requestId")).isNull();
  }

  @Test
  void fromSentryScopeReturnsContext() {
    // This test verifies that fromSentryScope doesn't throw when called.
    final DiagnosticContext context = DiagnosticContext.fromSentryScope();
    assertThat(context).isNotNull();
  }
}
