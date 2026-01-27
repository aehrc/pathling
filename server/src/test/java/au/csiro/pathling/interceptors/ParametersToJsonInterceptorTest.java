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

package au.csiro.pathling.interceptors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ParametersToJsonInterceptor}.
 *
 * @author John Grimes
 */
class ParametersToJsonInterceptorTest {

  private ParametersToJsonInterceptor interceptor;
  private ObjectMapper mapper;

  @BeforeEach
  void setUp() {
    interceptor = new ParametersToJsonInterceptor();
    mapper = new ObjectMapper();
  }

  // -------------------------------------------------------------------------
  // Accept header handling tests
  // -------------------------------------------------------------------------

  @Test
  void transformsToJsonWhenNoAcceptHeader() throws Exception {
    // When no Accept header is present, default to plain JSON.
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("test").setValue(new StringType("value"));

    final StringWriter writer = new StringWriter();
    final HttpServletRequest request = mockRequest(null);
    final HttpServletResponse response = mockResponse(writer);
    final RequestDetails requestDetails = mock(RequestDetails.class);

    final boolean result =
        interceptor.transformResponse(requestDetails, parameters, request, response);

    assertThat(result).isFalse();
    verify(response).setContentType("application/json");
    verify(response).setCharacterEncoding("UTF-8");
  }

  @Test
  void continuesNormallyWhenAcceptIsFhirJson() throws Exception {
    // When Accept is application/fhir+json, continue with normal FHIR processing.
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("test").setValue(new StringType("value"));

    final HttpServletRequest request = mockRequest("application/fhir+json");
    final HttpServletResponse response = mockResponse(new StringWriter());
    final RequestDetails requestDetails = mock(RequestDetails.class);

    final boolean result =
        interceptor.transformResponse(requestDetails, parameters, request, response);

    assertThat(result).isTrue();
    verify(response, never()).setContentType("application/json");
  }

  @Test
  void transformsToJsonWhenAcceptIsWildcard() throws Exception {
    // When Accept is */*, default to plain JSON.
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("test").setValue(new StringType("value"));

    final StringWriter writer = new StringWriter();
    final HttpServletRequest request = mockRequest("*/*");
    final HttpServletResponse response = mockResponse(writer);
    final RequestDetails requestDetails = mock(RequestDetails.class);

    final boolean result =
        interceptor.transformResponse(requestDetails, parameters, request, response);

    assertThat(result).isFalse();
    verify(response).setContentType("application/json");
    verify(response).setCharacterEncoding("UTF-8");
  }

  @Test
  void transformsWhenAcceptIsApplicationJson() throws Exception {
    // When Accept is application/json, transform to plain JSON.
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("test").setValue(new StringType("value"));

    final StringWriter writer = new StringWriter();
    final HttpServletRequest request = mockRequest("application/json");
    final HttpServletResponse response = mockResponse(writer);
    final RequestDetails requestDetails = mock(RequestDetails.class);

    final boolean result =
        interceptor.transformResponse(requestDetails, parameters, request, response);

    assertThat(result).isFalse();
    verify(response).setContentType("application/json");
    verify(response).setCharacterEncoding("UTF-8");
  }

  // -------------------------------------------------------------------------
  // Non-Parameters resource handling tests
  // -------------------------------------------------------------------------

  @Test
  void continuesNormallyForNonParametersResource() throws Exception {
    // Non-Parameters resources should not be transformed, regardless of Accept header.
    final Patient patient = new Patient();
    patient.addName().setFamily("Smith");

    final HttpServletRequest request = mockRequest("application/json");
    final HttpServletResponse response = mockResponse(new StringWriter());
    final RequestDetails requestDetails = mock(RequestDetails.class);

    final boolean result =
        interceptor.transformResponse(requestDetails, patient, request, response);

    assertThat(result).isTrue();
    verify(response, never()).setContentType("application/json");
  }

  // -------------------------------------------------------------------------
  // Parameters to JSON transformation tests
  // -------------------------------------------------------------------------

  @Test
  void transformsPrimitiveStringValue() throws Exception {
    // String parameter should become JSON property.
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("request").setValue(new StringType("http://example.org"));

    final String json = transformAndCapture(parameters);
    final JsonNode node = mapper.readTree(json);

    assertThat(node.get("request").asText()).isEqualTo("http://example.org");
  }

  @Test
  void transformsPrimitiveBooleanValue() throws Exception {
    // Boolean parameter should become JSON boolean.
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("requiresAccessToken").setValue(new BooleanType(false));

    final String json = transformAndCapture(parameters);
    final JsonNode node = mapper.readTree(json);

    assertThat(node.get("requiresAccessToken").asBoolean()).isFalse();
  }

  @Test
  void transformsPrimitiveInstantValue() throws Exception {
    // Instant parameter should become JSON string.
    final Parameters parameters = new Parameters();
    final InstantType instant = new InstantType("2024-01-15T10:30:00Z");
    parameters.addParameter().setName("transactionTime").setValue(instant);

    final String json = transformAndCapture(parameters);
    final JsonNode node = mapper.readTree(json);

    assertThat(node.get("transactionTime").asText()).isEqualTo("2024-01-15T10:30:00Z");
  }

  @Test
  void transformsUriValue() throws Exception {
    // Uri parameter should become JSON string.
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("url").setValue(new UriType("http://example.org/fhir"));

    final String json = transformAndCapture(parameters);
    final JsonNode node = mapper.readTree(json);

    assertThat(node.get("url").asText()).isEqualTo("http://example.org/fhir");
  }

  @Test
  void transformsCodeValue() throws Exception {
    // Code parameter should become JSON string.
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("type").setValue(new CodeType("Patient"));

    final String json = transformAndCapture(parameters);
    final JsonNode node = mapper.readTree(json);

    assertThat(node.get("type").asText()).isEqualTo("Patient");
  }

  @Test
  void transformsNestedParts() throws Exception {
    // Parameter with parts should become nested JSON object.
    final Parameters parameters = new Parameters();
    final Parameters.ParametersParameterComponent output =
        parameters.addParameter().setName("output");
    output.addPart().setName("type").setValue(new CodeType("Patient"));
    output.addPart().setName("url").setValue(new UriType("http://example.org/result"));

    final String json = transformAndCapture(parameters);
    final JsonNode node = mapper.readTree(json);

    final JsonNode outputNode = node.get("output");
    assertThat(outputNode.get("type").asText()).isEqualTo("Patient");
    assertThat(outputNode.get("url").asText()).isEqualTo("http://example.org/result");
  }

  @Test
  void transformsRepeatedParametersAsArray() throws Exception {
    // Repeated parameters with the same name should become JSON array.
    final Parameters parameters = new Parameters();

    final Parameters.ParametersParameterComponent output1 =
        parameters.addParameter().setName("output");
    output1.addPart().setName("type").setValue(new CodeType("Patient"));
    output1.addPart().setName("url").setValue(new UriType("http://example.org/patient"));

    final Parameters.ParametersParameterComponent output2 =
        parameters.addParameter().setName("output");
    output2.addPart().setName("type").setValue(new CodeType("Observation"));
    output2.addPart().setName("url").setValue(new UriType("http://example.org/observation"));

    final String json = transformAndCapture(parameters);
    final JsonNode node = mapper.readTree(json);

    final JsonNode outputArray = node.get("output");
    assertThat(outputArray.isArray()).isTrue();
    assertThat(outputArray.size()).isEqualTo(2);
    assertThat(outputArray.get(0).get("type").asText()).isEqualTo("Patient");
    assertThat(outputArray.get(1).get("type").asText()).isEqualTo("Observation");
  }

  @Test
  void transformsCompleteManifest() throws Exception {
    // Test a complete export manifest structure.
    final Parameters parameters = new Parameters();
    parameters
        .addParameter()
        .setName("transactionTime")
        .setValue(new InstantType("2024-01-15T10:30:00Z"));
    parameters
        .addParameter()
        .setName("request")
        .setValue(new UriType("http://example.org/fhir/$export"));
    parameters.addParameter().setName("requiresAccessToken").setValue(new BooleanType(false));

    final Parameters.ParametersParameterComponent output =
        parameters.addParameter().setName("output");
    output.addPart().setName("type").setValue(new CodeType("Patient"));
    output
        .addPart()
        .setName("url")
        .setValue(new UriType("http://example.org/fhir/$result?job=abc&file=Patient.ndjson"));

    final String json = transformAndCapture(parameters);
    final JsonNode node = mapper.readTree(json);

    assertThat(node.get("transactionTime").asText()).isEqualTo("2024-01-15T10:30:00Z");
    assertThat(node.get("request").asText()).isEqualTo("http://example.org/fhir/$export");
    assertThat(node.get("requiresAccessToken").asBoolean()).isFalse();
    assertThat(node.get("output").get("type").asText()).isEqualTo("Patient");
  }

  @Test
  void handlesEmptyParameters() throws Exception {
    // Empty Parameters should produce empty JSON object.
    final Parameters parameters = new Parameters();

    final String json = transformAndCapture(parameters);
    final JsonNode node = mapper.readTree(json);

    assertThat(node.isEmpty()).isTrue();
  }

  // -------------------------------------------------------------------------
  // Native JSON tests
  // -------------------------------------------------------------------------

  @Test
  void usesNativeJsonWhenAvailable() throws Exception {
    // When Parameters has native JSON attached, it should be used directly.
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("test").setValue(new StringType("converted-value"));

    // Attach native JSON with a different value to prove it's being used.
    final String nativeJson = "{\"test\":\"native-value\",\"output\":[],\"error\":[]}";
    parameters.setUserData("nativeJson", nativeJson);

    final String json = transformAndCapture(parameters);
    final JsonNode node = mapper.readTree(json);

    // Should use the native JSON value, not the converted one.
    assertThat(node.get("test").asText()).isEqualTo("native-value");
  }

  @Test
  void fallsBackToConversionWhenNoNativeJson() throws Exception {
    // When Parameters does not have native JSON, conversion should be used.
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("test").setValue(new StringType("converted-value"));

    final String json = transformAndCapture(parameters);
    final JsonNode node = mapper.readTree(json);

    // Should use the converted value.
    assertThat(node.get("test").asText()).isEqualTo("converted-value");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  private static HttpServletRequest mockRequest(@Nullable final String acceptHeader) {
    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader("Accept")).thenReturn(acceptHeader);
    return request;
  }

  private static HttpServletResponse mockResponse(final StringWriter writer) throws Exception {
    final HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(writer));
    return response;
  }

  private String transformAndCapture(final Parameters parameters) throws Exception {
    final StringWriter writer = new StringWriter();
    final HttpServletRequest request = mockRequest("application/json");
    final HttpServletResponse response = mockResponse(writer);
    final RequestDetails requestDetails = mock(RequestDetails.class);

    interceptor.transformResponse(requestDetails, parameters, request, response);

    return writer.toString();
  }
}
