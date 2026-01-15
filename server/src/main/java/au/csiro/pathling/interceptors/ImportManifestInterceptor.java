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

import au.csiro.pathling.operations.bulkimport.ImportManifest;
import au.csiro.pathling.operations.bulkimport.ImportManifestInput;
import ca.uhn.fhir.context.FhirContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UrlType;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

/**
 * Servlet filter that converts JSON manifest requests (SMART Bulk Data Import format) to FHIR
 * Parameters resources before HAPI processes them.
 *
 * <p>This is necessary because HAPI's {@code @ResourceParam} annotation expects a FHIR resource,
 * and will fail when Content-Type is application/json with a non-FHIR body. By converting the JSON
 * manifest to a Parameters resource and changing the Content-Type to application/fhir+json, we
 * allow the existing provider method to handle both formats transparently.
 *
 * @author John Grimes
 */
@Configuration
@Slf4j
public class ImportManifestInterceptor {

  /**
   * Creates a filter registration bean for the import manifest filter.
   *
   * @return the filter registration bean
   */
  @Bean
  public FilterRegistrationBean<Filter> importManifestFilter() {
    final FilterRegistrationBean<Filter> registrationBean = new FilterRegistrationBean<>();
    registrationBean.setFilter(new ImportManifestFilter());
    registrationBean.addUrlPatterns("/fhir/$import");
    registrationBean.setOrder(Ordered.HIGHEST_PRECEDENCE);
    return registrationBean;
  }

  /** The actual filter implementation. */
  @Slf4j
  private static class ImportManifestFilter implements Filter {

    private static final String PROCESSED_ATTR = "import.manifest.filter.processed";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final FhirContext fhirContext = FhirContext.forR4();

    @Override
    public void doFilter(
        final jakarta.servlet.ServletRequest request,
        final jakarta.servlet.ServletResponse response,
        final FilterChain chain)
        throws IOException, ServletException {
      if (!(request instanceof HttpServletRequest httpRequest)
          || !(response instanceof HttpServletResponse)) {
        chain.doFilter(request, response);
        return;
      }

      // Check if we've already processed and stored a wrapped request for retry handling.
      final Object existingWrapper = httpRequest.getAttribute(PROCESSED_ATTR);
      if (existingWrapper instanceof WrappedRequest wrappedRequest) {
        log.debug("Request already processed, using cached wrapped request");
        chain.doFilter(wrappedRequest, response);
        return;
      }

      // Only process POST requests to /$import with application/json content type.
      final boolean isJsonImport = isJsonImportRequest(httpRequest);
      log.debug(
          "ImportManifestFilter processing request: method={}, uri={}, contentType={},"
              + " isJsonImport={}",
          httpRequest.getMethod(),
          httpRequest.getRequestURI(),
          httpRequest.getContentType(),
          isJsonImport);
      if (!isJsonImport) {
        chain.doFilter(request, response);
        return;
      }

      log.info("Converting JSON manifest to FHIR Parameters for import request");

      // Read the original body.
      final byte[] originalBody = httpRequest.getInputStream().readAllBytes();
      final String originalBodyString = new String(originalBody, StandardCharsets.UTF_8);

      try {
        // Parse the JSON manifest.
        final ImportManifest manifest =
            objectMapper.readValue(originalBodyString, ImportManifest.class);

        // Convert to FHIR Parameters.
        final Parameters parameters = convertManifestToParameters(manifest);

        // Serialise the Parameters resource.
        final String parametersJson =
            fhirContext.newJsonParser().encodeResourceToString(parameters);
        final byte[] newBody = parametersJson.getBytes(StandardCharsets.UTF_8);

        // Wrap the request with the new body and content type.
        final WrappedRequest wrappedRequest = new WrappedRequest(httpRequest, newBody);
        // Store the wrapped request for potential retry scenarios.
        httpRequest.setAttribute(PROCESSED_ATTR, wrappedRequest);
        chain.doFilter(wrappedRequest, response);
      } catch (final Exception e) {
        log.warn(
            "Failed to parse JSON manifest, passing through original request: {}", e.getMessage());
        // If parsing fails, pass through the original request - HAPI will handle the error.
        final WrappedRequest wrappedRequest =
            new WrappedRequest(httpRequest, originalBody) {
              @Override
              public String getContentType() {
                return httpRequest.getContentType();
              }

              @Override
              public String getHeader(final String name) {
                return httpRequest.getHeader(name);
              }
            };
        // Store for potential retry.
        httpRequest.setAttribute(PROCESSED_ATTR, wrappedRequest);
        chain.doFilter(wrappedRequest, response);
      }
    }

    /**
     * Determines whether the request is a JSON manifest import request.
     *
     * @param request the HTTP request
     * @return true if this is a POST to /$import with application/json content type
     */
    private boolean isJsonImportRequest(@Nonnull final HttpServletRequest request) {
      if (!"POST".equalsIgnoreCase(request.getMethod())) {
        return false;
      }
      // Check the request URI for the import endpoint. The URI may be /fhir/$import or just
      // /$import depending on the context.
      final String requestUri = request.getRequestURI();
      if (requestUri == null || !requestUri.endsWith("/$import")) {
        return false;
      }
      final String contentType = request.getContentType();
      return contentType != null
          && contentType.toLowerCase().contains("application/json")
          && !contentType.toLowerCase().contains("application/fhir+json");
    }

    /**
     * Converts an ImportManifest to a FHIR Parameters resource.
     *
     * @param manifest the JSON manifest
     * @return the equivalent Parameters resource
     */
    @Nonnull
    private Parameters convertManifestToParameters(@Nonnull final ImportManifest manifest) {
      final Parameters parameters = new Parameters();

      // Add inputSource parameter if present (optional for backwards compatibility).
      if (manifest.inputSource() != null && !manifest.inputSource().isBlank()) {
        parameters
            .addParameter()
            .setName("inputSource")
            .setValue(new StringType(manifest.inputSource()));
      }

      // Add inputFormat parameter if present.
      if (manifest.inputFormat() != null && !manifest.inputFormat().isBlank()) {
        parameters
            .addParameter()
            .setName("inputFormat")
            .setValue(new CodeType(manifest.inputFormat()));
      }

      // Add saveMode parameter if present.
      if (manifest.mode() != null && !manifest.mode().isBlank()) {
        parameters.addParameter().setName("saveMode").setValue(new CodeType(manifest.mode()));
      }

      // Add input parameters.
      for (final ImportManifestInput input : manifest.input()) {
        final ParametersParameterComponent inputParam = parameters.addParameter().setName("input");

        inputParam.addPart().setName("resourceType").setValue(new CodeType(input.type()));

        inputParam.addPart().setName("url").setValue(new UrlType(input.url()));
      }

      return parameters;
    }
  }

  /** Wrapper for HttpServletRequest that replaces the body and content type. */
  private static class WrappedRequest extends HttpServletRequestWrapper {

    private final byte[] body;

    WrappedRequest(@Nonnull final HttpServletRequest request, @Nonnull final byte[] body) {
      super(request);
      this.body = body;
    }

    @Override
    public ServletInputStream getInputStream() {
      final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(body);
      return new ServletInputStream() {
        @Override
        public boolean isFinished() {
          return byteArrayInputStream.available() == 0;
        }

        @Override
        public boolean isReady() {
          return true;
        }

        @Override
        public void setReadListener(final ReadListener listener) {
          throw new UnsupportedOperationException("ReadListener is not supported");
        }

        @Override
        public int read() {
          return byteArrayInputStream.read();
        }
      };
    }

    @Override
    public BufferedReader getReader() {
      return new BufferedReader(
          new InputStreamReader(new ByteArrayInputStream(body), StandardCharsets.UTF_8));
    }

    @Override
    public String getContentType() {
      return "application/fhir+json";
    }

    @Override
    public String getHeader(final String name) {
      if ("Content-Type".equalsIgnoreCase(name)) {
        return "application/fhir+json";
      }
      return super.getHeader(name);
    }

    @Override
    public Enumeration<String> getHeaders(final String name) {
      if ("Content-Type".equalsIgnoreCase(name)) {
        return Collections.enumeration(List.of("application/fhir+json"));
      }
      return super.getHeaders(name);
    }

    @Override
    public int getContentLength() {
      return body.length;
    }

    @Override
    public long getContentLengthLong() {
      return body.length;
    }
  }
}
