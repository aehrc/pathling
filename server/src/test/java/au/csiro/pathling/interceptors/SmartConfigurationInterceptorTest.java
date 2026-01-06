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

import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.AUTH_URL;
import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.REVOKE_URL;
import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.TOKEN_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.security.OidcConfiguration;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SmartConfigurationInterceptor}.
 *
 * @author John Grimes
 */
class SmartConfigurationInterceptorTest {

  private static final String ISSUER = "https://auth.example.com";
  private static final String AUTH_ENDPOINT = "https://auth.example.com/authorize";
  private static final String TOKEN_ENDPOINT = "https://auth.example.com/token";
  private static final String REVOKE_ENDPOINT = "https://auth.example.com/revoke";
  private static final String ADMIN_UI_CLIENT_ID = "my-custom-client-id";

  private OidcConfiguration oidcConfiguration;

  @BeforeEach
  void setUp() {
    oidcConfiguration = mock(OidcConfiguration.class);
    when(oidcConfiguration.get(AUTH_URL)).thenReturn(Optional.of(AUTH_ENDPOINT));
    when(oidcConfiguration.get(TOKEN_URL)).thenReturn(Optional.of(TOKEN_ENDPOINT));
    when(oidcConfiguration.get(REVOKE_URL)).thenReturn(Optional.of(REVOKE_ENDPOINT));
  }

  // -------------------------------------------------------------------------
  // Admin UI client ID tests
  // -------------------------------------------------------------------------

  @Test
  void includesAdminUiClientIdWhenConfigured() throws Exception {
    // When adminUiClientId is configured, it should appear in the response.
    final SmartConfigurationInterceptor interceptor =
        new SmartConfigurationInterceptor(
            ISSUER, oidcConfiguration, Optional.of(ADMIN_UI_CLIENT_ID));

    final String response = captureResponse(interceptor);
    final JsonObject json = JsonParser.parseString(response).getAsJsonObject();

    assertThat(json.has("admin_ui_client_id")).isTrue();
    assertThat(json.get("admin_ui_client_id").getAsString()).isEqualTo(ADMIN_UI_CLIENT_ID);
  }

  @Test
  void omitsAdminUiClientIdWhenNotConfigured() throws Exception {
    // When adminUiClientId is not configured, it should not appear in the response.
    final SmartConfigurationInterceptor interceptor =
        new SmartConfigurationInterceptor(ISSUER, oidcConfiguration, Optional.empty());

    final String response = captureResponse(interceptor);
    final JsonObject json = JsonParser.parseString(response).getAsJsonObject();

    assertThat(json.has("admin_ui_client_id")).isFalse();
  }

  // -------------------------------------------------------------------------
  // Existing fields tests
  // -------------------------------------------------------------------------

  @Test
  void includesIssuer() throws Exception {
    // The issuer should always be included in the response.
    final SmartConfigurationInterceptor interceptor =
        new SmartConfigurationInterceptor(ISSUER, oidcConfiguration, Optional.empty());

    final String response = captureResponse(interceptor);
    final JsonObject json = JsonParser.parseString(response).getAsJsonObject();

    assertThat(json.get("issuer").getAsString()).isEqualTo(ISSUER);
  }

  @Test
  void includesAuthorizationEndpoint() throws Exception {
    // The authorization_endpoint should be included when configured in OIDC.
    final SmartConfigurationInterceptor interceptor =
        new SmartConfigurationInterceptor(ISSUER, oidcConfiguration, Optional.empty());

    final String response = captureResponse(interceptor);
    final JsonObject json = JsonParser.parseString(response).getAsJsonObject();

    assertThat(json.get("authorization_endpoint").getAsString()).isEqualTo(AUTH_ENDPOINT);
  }

  @Test
  void includesTokenEndpoint() throws Exception {
    // The token_endpoint should be included when configured in OIDC.
    final SmartConfigurationInterceptor interceptor =
        new SmartConfigurationInterceptor(ISSUER, oidcConfiguration, Optional.empty());

    final String response = captureResponse(interceptor);
    final JsonObject json = JsonParser.parseString(response).getAsJsonObject();

    assertThat(json.get("token_endpoint").getAsString()).isEqualTo(TOKEN_ENDPOINT);
  }

  @Test
  void includesRevocationEndpoint() throws Exception {
    // The revocation_endpoint should be included when configured in OIDC.
    final SmartConfigurationInterceptor interceptor =
        new SmartConfigurationInterceptor(ISSUER, oidcConfiguration, Optional.empty());

    final String response = captureResponse(interceptor);
    final JsonObject json = JsonParser.parseString(response).getAsJsonObject();

    assertThat(json.get("revocation_endpoint").getAsString()).isEqualTo(REVOKE_ENDPOINT);
  }

  @Test
  void includesCapabilities() throws Exception {
    // The capabilities array should include launch-standalone.
    final SmartConfigurationInterceptor interceptor =
        new SmartConfigurationInterceptor(ISSUER, oidcConfiguration, Optional.empty());

    final String response = captureResponse(interceptor);
    final JsonObject json = JsonParser.parseString(response).getAsJsonObject();

    assertThat(json.has("capabilities")).isTrue();
    assertThat(json.getAsJsonArray("capabilities").get(0).getAsString())
        .isEqualTo("launch-standalone");
  }

  @Test
  void omitsEndpointsWhenNotConfigured() throws Exception {
    // When OIDC endpoints are not configured, they should be omitted from the response.
    when(oidcConfiguration.get(AUTH_URL)).thenReturn(Optional.empty());
    when(oidcConfiguration.get(TOKEN_URL)).thenReturn(Optional.empty());
    when(oidcConfiguration.get(REVOKE_URL)).thenReturn(Optional.empty());

    final SmartConfigurationInterceptor interceptor =
        new SmartConfigurationInterceptor(ISSUER, oidcConfiguration, Optional.empty());

    final String response = captureResponse(interceptor);
    final JsonObject json = JsonParser.parseString(response).getAsJsonObject();

    assertThat(json.has("authorization_endpoint")).isFalse();
    assertThat(json.has("token_endpoint")).isFalse();
    assertThat(json.has("revocation_endpoint")).isFalse();
  }

  // -------------------------------------------------------------------------
  // Path matching tests
  // -------------------------------------------------------------------------

  @Test
  void servesResponseForSmartConfigurationPath() throws Exception {
    // Should return false (handled) for /.well-known/smart-configuration path.
    final SmartConfigurationInterceptor interceptor =
        new SmartConfigurationInterceptor(ISSUER, oidcConfiguration, Optional.empty());

    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getPathInfo()).thenReturn("/.well-known/smart-configuration");

    final StringWriter writer = new StringWriter();
    final HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(writer));

    final boolean result = interceptor.serveUris(request, response);

    assertThat(result).isFalse();
  }

  @Test
  void continuesForOtherPaths() throws Exception {
    // Should return true (continue processing) for other paths.
    final SmartConfigurationInterceptor interceptor =
        new SmartConfigurationInterceptor(ISSUER, oidcConfiguration, Optional.empty());

    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getPathInfo()).thenReturn("/metadata");

    final HttpServletResponse response = mock(HttpServletResponse.class);

    final boolean result = interceptor.serveUris(request, response);

    assertThat(result).isTrue();
  }

  @Test
  void continuesForNullPath() throws Exception {
    // Should return true (continue processing) when path is null.
    final SmartConfigurationInterceptor interceptor =
        new SmartConfigurationInterceptor(ISSUER, oidcConfiguration, Optional.empty());

    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getPathInfo()).thenReturn(null);

    final HttpServletResponse response = mock(HttpServletResponse.class);

    final boolean result = interceptor.serveUris(request, response);

    assertThat(result).isTrue();
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  private String captureResponse(final SmartConfigurationInterceptor interceptor) throws Exception {
    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getPathInfo()).thenReturn("/.well-known/smart-configuration");

    final StringWriter writer = new StringWriter();
    final HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(writer));

    interceptor.serveUris(request, response);

    return writer.toString();
  }
}
