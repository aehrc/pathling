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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * Tests for {@link ImportManifestInterceptor} covering JSON-to-Parameters conversion, non-JSON
 * pass-through, and error handling.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class ImportManifestInterceptorTest {

  @Mock private FilterChain filterChain;

  private Filter filter;

  @BeforeEach
  void setUp() {
    final ImportManifestInterceptor interceptor = new ImportManifestInterceptor();
    final FilterRegistrationBean<Filter> registration = interceptor.importManifestFilter();
    filter = registration.getFilter();
  }

  @Test
  void filterRegistrationConfiguredCorrectly() {
    // The filter registration should target /$import with highest precedence.
    final ImportManifestInterceptor interceptor = new ImportManifestInterceptor();
    final FilterRegistrationBean<Filter> registration = interceptor.importManifestFilter();
    assertNotNull(registration.getFilter());
    assertTrue(registration.getUrlPatterns().contains("/fhir/$import"));
  }

  @Test
  void convertsJsonManifestToFhirParameters() throws Exception {
    // A POST to /$import with application/json containing a valid manifest should be converted.
    final String jsonManifest =
        """
        {
          "inputFormat": "application/fhir+ndjson",
          "inputSource": "http://example.com",
          "input": [
            {"type": "Patient", "url": "http://example.com/Patient.ndjson"}
          ]
        }
        """;

    final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/fhir/$import");
    request.setContentType("application/json");
    request.setContent(jsonManifest.getBytes(StandardCharsets.UTF_8));

    final MockHttpServletResponse response = new MockHttpServletResponse();

    filter.doFilter(request, response, filterChain);

    // The filter chain should have been invoked with a wrapped request.
    final ArgumentCaptor<HttpServletRequest> requestCaptor =
        ArgumentCaptor.forClass(HttpServletRequest.class);
    verify(filterChain).doFilter(requestCaptor.capture(), eq(response));

    final HttpServletRequest wrappedRequest = requestCaptor.getValue();
    // The content type should have been changed to application/fhir+json.
    assertEquals("application/fhir+json", wrappedRequest.getContentType());
    assertEquals("application/fhir+json", wrappedRequest.getHeader("Content-Type"));
  }

  @Test
  void passesThoughNonJsonRequests() throws Exception {
    // A POST to /$import with application/fhir+json should pass through unmodified.
    final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/fhir/$import");
    request.setContentType("application/fhir+json");
    request.setContent("{}".getBytes(StandardCharsets.UTF_8));

    final MockHttpServletResponse response = new MockHttpServletResponse();

    filter.doFilter(request, response, filterChain);

    // The filter chain should receive the original request unchanged.
    verify(filterChain).doFilter(request, response);
  }

  @Test
  void passesThroughGetRequests() throws Exception {
    // Non-POST methods should pass through unmodified.
    final MockHttpServletRequest request = new MockHttpServletRequest("GET", "/fhir/$import");
    request.setContentType("application/json");

    final MockHttpServletResponse response = new MockHttpServletResponse();

    filter.doFilter(request, response, filterChain);

    verify(filterChain).doFilter(request, response);
  }

  @Test
  void passesThroughNonImportEndpoint() throws Exception {
    // A POST to a different endpoint should pass through unmodified.
    final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/fhir/Patient");
    request.setContentType("application/json");
    request.setContent("{}".getBytes(StandardCharsets.UTF_8));

    final MockHttpServletResponse response = new MockHttpServletResponse();

    filter.doFilter(request, response, filterChain);

    verify(filterChain).doFilter(request, response);
  }

  @Test
  void handlesInvalidJsonGracefully() throws Exception {
    // Invalid JSON should be passed through (HAPI will handle the error).
    final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/fhir/$import");
    request.setContentType("application/json");
    request.setContent("not valid json".getBytes(StandardCharsets.UTF_8));

    final MockHttpServletResponse response = new MockHttpServletResponse();

    filter.doFilter(request, response, filterChain);

    // The filter chain should still be invoked (pass-through on error).
    verify(filterChain).doFilter(any(HttpServletRequest.class), eq(response));
  }

  @Test
  void convertsManifestWithSaveMode() throws Exception {
    // A manifest with saveMode should include it in the converted Parameters.
    final String jsonManifest =
        """
        {
          "inputFormat": "application/fhir+ndjson",
          "saveMode": "merge",
          "input": [
            {"type": "Patient", "url": "http://example.com/Patient.ndjson"}
          ]
        }
        """;

    final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/fhir/$import");
    request.setContentType("application/json");
    request.setContent(jsonManifest.getBytes(StandardCharsets.UTF_8));

    final MockHttpServletResponse response = new MockHttpServletResponse();

    filter.doFilter(request, response, filterChain);

    // Verify the filter chain was invoked with a wrapped request.
    final ArgumentCaptor<HttpServletRequest> requestCaptor =
        ArgumentCaptor.forClass(HttpServletRequest.class);
    verify(filterChain).doFilter(requestCaptor.capture(), eq(response));

    final HttpServletRequest wrappedRequest = requestCaptor.getValue();
    assertEquals("application/fhir+json", wrappedRequest.getContentType());
  }

  @Test
  void usesExistingWrappedRequestOnRetry() throws Exception {
    // When the filter is invoked again (retry), it should reuse the cached wrapped request.
    final String jsonManifest =
        """
        {
          "inputFormat": "application/fhir+ndjson",
          "input": [
            {"type": "Patient", "url": "http://example.com/Patient.ndjson"}
          ]
        }
        """;

    final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/fhir/$import");
    request.setContentType("application/json");
    request.setContent(jsonManifest.getBytes(StandardCharsets.UTF_8));

    final MockHttpServletResponse response = new MockHttpServletResponse();

    // First invocation sets the attribute.
    filter.doFilter(request, response, filterChain);

    // Second invocation should use the cached wrapped request.
    filter.doFilter(request, response, filterChain);

    // Verify doFilter was called twice (once for each invocation).
    verify(filterChain, org.mockito.Mockito.times(2))
        .doFilter(any(HttpServletRequest.class), eq(response));
  }

  @Test
  void convertsManifestWithMultipleInputs() throws Exception {
    // A manifest with multiple input entries should produce corresponding parameters.
    final String jsonManifest =
        """
        {
          "inputFormat": "application/fhir+ndjson",
          "input": [
            {"type": "Patient", "url": "http://example.com/Patient.ndjson"},
            {"type": "Condition", "url": "http://example.com/Condition.ndjson"}
          ]
        }
        """;

    final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/fhir/$import");
    request.setContentType("application/json");
    request.setContent(jsonManifest.getBytes(StandardCharsets.UTF_8));

    final MockHttpServletResponse response = new MockHttpServletResponse();

    filter.doFilter(request, response, filterChain);

    final ArgumentCaptor<HttpServletRequest> requestCaptor =
        ArgumentCaptor.forClass(HttpServletRequest.class);
    verify(filterChain).doFilter(requestCaptor.capture(), eq(response));
    assertEquals("application/fhir+json", requestCaptor.getValue().getContentType());
  }
}
