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

import au.csiro.pathling.security.OidcConfiguration;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * This class intercepts requests to `.well-known/smart-configuration` and returns a Well-Known
 * Uniform Resource Identifiers document with the configured authorization URIs. It merges fields
 * from the issuer's OIDC discovery document, with SMART-specific configuration taking precedence.
 *
 * @author John Grimes
 */
@Interceptor
@Slf4j
public class SmartConfigurationInterceptor {

  @Nonnull private final String issuer;
  @Nonnull private final OidcConfiguration oidcConfiguration;
  @Nullable private final OidcDiscoveryFetcher oidcDiscoveryFetcher;
  @Nonnull private final Optional<String> adminUiClientId;
  @Nonnull private final List<String> capabilities;
  @Nonnull private final List<String> grantTypesSupported;
  @Nonnull private final List<String> codeChallengeMethodsSupported;
  @Nonnull private final Gson gson;

  /**
   * Constructs a new SmartConfigurationInterceptor without OIDC discovery merging. Uses default
   * values for grant types and code challenge methods.
   *
   * @param issuer the required issuer of tokens
   * @param oidcConfiguration a {@link OidcConfiguration} object containing configuration retrieved
   *     from OIDC discovery
   * @param adminUiClientId the optional OAuth client ID for the admin UI
   * @param capabilities the list of SMART capabilities to advertise
   */
  public SmartConfigurationInterceptor(
      @Nonnull final String issuer,
      @Nonnull final OidcConfiguration oidcConfiguration,
      @Nonnull final Optional<String> adminUiClientId,
      @Nonnull final List<String> capabilities) {
    this(
        issuer,
        oidcConfiguration,
        null,
        adminUiClientId,
        capabilities,
        List.of("authorization_code"),
        List.of("S256"));
  }

  /**
   * Constructs a new SmartConfigurationInterceptor without OIDC discovery merging.
   *
   * @param issuer the required issuer of tokens
   * @param oidcConfiguration a {@link OidcConfiguration} object containing configuration retrieved
   *     from OIDC discovery
   * @param adminUiClientId the optional OAuth client ID for the admin UI
   * @param capabilities the list of SMART capabilities to advertise
   * @param grantTypesSupported the list of grant types to advertise
   * @param codeChallengeMethodsSupported the list of code challenge methods to advertise
   */
  public SmartConfigurationInterceptor(
      @Nonnull final String issuer,
      @Nonnull final OidcConfiguration oidcConfiguration,
      @Nonnull final Optional<String> adminUiClientId,
      @Nonnull final List<String> capabilities,
      @Nonnull final List<String> grantTypesSupported,
      @Nonnull final List<String> codeChallengeMethodsSupported) {
    this(
        issuer,
        oidcConfiguration,
        null,
        adminUiClientId,
        capabilities,
        grantTypesSupported,
        codeChallengeMethodsSupported);
  }

  /**
   * Constructs a new SmartConfigurationInterceptor with OIDC discovery merging. Uses default values
   * for grant types and code challenge methods.
   *
   * @param issuer the required issuer of tokens
   * @param oidcConfiguration a {@link OidcConfiguration} object containing configuration retrieved
   *     from OIDC discovery
   * @param oidcDiscoveryFetcher the fetcher for OIDC discovery documents, or null to disable
   *     merging
   * @param adminUiClientId the optional OAuth client ID for the admin UI
   * @param capabilities the list of SMART capabilities to advertise
   */
  public SmartConfigurationInterceptor(
      @Nonnull final String issuer,
      @Nonnull final OidcConfiguration oidcConfiguration,
      @Nullable final OidcDiscoveryFetcher oidcDiscoveryFetcher,
      @Nonnull final Optional<String> adminUiClientId,
      @Nonnull final List<String> capabilities) {
    this(
        issuer,
        oidcConfiguration,
        oidcDiscoveryFetcher,
        adminUiClientId,
        capabilities,
        List.of("authorization_code"),
        List.of("S256"));
  }

  /**
   * Constructs a new SmartConfigurationInterceptor with OIDC discovery merging.
   *
   * @param issuer the required issuer of tokens
   * @param oidcConfiguration a {@link OidcConfiguration} object containing configuration retrieved
   *     from OIDC discovery
   * @param oidcDiscoveryFetcher the fetcher for OIDC discovery documents, or null to disable
   *     merging
   * @param adminUiClientId the optional OAuth client ID for the admin UI
   * @param capabilities the list of SMART capabilities to advertise
   * @param grantTypesSupported the list of grant types to advertise
   * @param codeChallengeMethodsSupported the list of code challenge methods to advertise
   */
  public SmartConfigurationInterceptor(
      @Nonnull final String issuer,
      @Nonnull final OidcConfiguration oidcConfiguration,
      @Nullable final OidcDiscoveryFetcher oidcDiscoveryFetcher,
      @Nonnull final Optional<String> adminUiClientId,
      @Nonnull final List<String> capabilities,
      @Nonnull final List<String> grantTypesSupported,
      @Nonnull final List<String> codeChallengeMethodsSupported) {
    this.issuer = issuer;
    this.oidcConfiguration = oidcConfiguration;
    this.oidcDiscoveryFetcher = oidcDiscoveryFetcher;
    this.adminUiClientId = adminUiClientId;
    this.capabilities = capabilities;
    this.grantTypesSupported = grantTypesSupported;
    this.codeChallengeMethodsSupported = codeChallengeMethodsSupported;
    this.gson = new Gson();
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
      final String response = buildResponse();
      servletResponse.setStatus(200);
      servletResponse.setContentType("application/json");
      servletResponse.getWriter().append(response);
      return false;
    } else {
      return true;
    }
  }

  @Nonnull
  private String buildResponse() {
    // Start with OIDC discovery fields as the base if fetcher is available.
    final Map<String, Object> merged;
    if (oidcDiscoveryFetcher != null) {
      merged = new LinkedHashMap<>(oidcDiscoveryFetcher.fetch().orElse(Map.of()));
    } else {
      merged = new LinkedHashMap<>();
    }

    // Override with SMART-specific fields (these take precedence).
    merged.put("issuer", issuer);

    final Optional<String> authUrl = oidcConfiguration.get(AUTH_URL);
    final Optional<String> tokenUrl = oidcConfiguration.get(TOKEN_URL);
    final Optional<String> revokeUrl = oidcConfiguration.get(REVOKE_URL);

    authUrl.ifPresent(url -> merged.put("authorization_endpoint", url));
    tokenUrl.ifPresent(url -> merged.put("token_endpoint", url));
    revokeUrl.ifPresent(url -> merged.put("revocation_endpoint", url));
    adminUiClientId.ifPresent(id -> merged.put("admin_ui_client_id", id));
    merged.put("capabilities", capabilities);
    merged.put("grant_types_supported", grantTypesSupported);
    merged.put("code_challenge_methods_supported", codeChallengeMethodsSupported);

    return gson.toJson(merged);
  }
}
