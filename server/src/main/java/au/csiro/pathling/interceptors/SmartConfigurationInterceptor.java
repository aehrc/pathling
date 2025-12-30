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

package au.csiro.pathling.interceptors;

import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.AUTH_URL;
import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.REVOKE_URL;
import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.TOKEN_URL;

import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.shaded.com.google.gson.FieldNamingPolicy;
import au.csiro.pathling.shaded.com.google.gson.Gson;
import au.csiro.pathling.shaded.com.google.gson.GsonBuilder;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * This class intercepts requests to `.well-known/smart-configuration` and returns a Well-Known
 * Uniform Resource Identifiers document with the configured authorization URIs.
 *
 * @author John Grimes
 */
@Interceptor
@Slf4j
public class SmartConfigurationInterceptor {

  @Nonnull private final String response;

  /**
   * @param issuer the required issuer of tokens
   * @param oidcConfiguration a {@link OidcConfiguration} object containing configuration retrieved
   *     from OIDC discovery
   */
  public SmartConfigurationInterceptor(
      @Nonnull final String issuer, @Nonnull final OidcConfiguration oidcConfiguration) {
    response = buildResponse(issuer, oidcConfiguration);
  }

  @Nonnull
  private static String buildResponse(
      @Nonnull final String issuer, @Nonnull final OidcConfiguration oidcConfiguration) {
    final SmartConfiguration smartConfiguration = new SmartConfiguration();

    final Optional<String> authUrl = oidcConfiguration.get(AUTH_URL);
    final Optional<String> tokenUrl = oidcConfiguration.get(TOKEN_URL);
    final Optional<String> revokeUrl = oidcConfiguration.get(REVOKE_URL);

    smartConfiguration.setIssuer(issuer);
    authUrl.ifPresent(smartConfiguration::setAuthorizationEndpoint);
    tokenUrl.ifPresent(smartConfiguration::setTokenEndpoint);
    revokeUrl.ifPresent(smartConfiguration::setRevocationEndpoint);

    final Gson gson =
        new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();
    return gson.toJson(smartConfiguration);
  }

  /**
   * HAPI hook to selectively serve the SMART configuration document, when the URL matches and
   * authorization is enabled.
   *
   * @param servletRequest the details of the request
   * @param servletResponse the response that will be sent
   * @return a boolean value indicating whether to continue processing through HAPI
   * @throws IOException if there is a problem writing to the response
   */
  @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_PROCESSED)
  @SuppressWarnings("unused")
  public boolean serveUris(
      @Nullable final HttpServletRequest servletRequest,
      @Nullable final HttpServletResponse servletResponse)
      throws IOException {
    if (servletRequest == null || servletResponse == null) {
      log.warn("SMART configuration interceptor invoked with missing servlet request or response");
      return true;
    }

    if (servletRequest.getPathInfo() != null
        && servletRequest.getPathInfo().equals("/.well-known/smart-configuration")) {
      servletResponse.setStatus(200);
      servletResponse.setContentType("application/json");
      servletResponse.getWriter().append(response);
      return false;
    } else {
      return true;
    }
  }

  @Setter
  @SuppressWarnings("unused")
  private static class SmartConfiguration {

    private String issuer;

    private String authorizationEndpoint;

    private String tokenEndpoint;

    private String revocationEndpoint;

    private final List<String> capabilities = Collections.singletonList("launch-standalone");
  }
}
