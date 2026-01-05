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

package au.csiro.pathling.operations.bulksubmit;

import au.csiro.fhir.auth.AuthConfig;
import au.csiro.fhir.auth.AuthTokenProvider;
import au.csiro.fhir.auth.ClientAuthMethod;
import au.csiro.fhir.auth.SMARTDiscoveryResponse;
import au.csiro.fhir.auth.Token;
import au.csiro.pathling.config.BulkSubmitConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.SubmitterConfiguration;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.springframework.stereotype.Component;

/**
 * Provides OAuth2 authentication for bulk submit file downloads using the SMART Backend Services
 * specification. Supports both symmetric (client_secret) and asymmetric (private key JWT)
 * authentication methods.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/smart-app-launch/backend-services.html">SMART Backend
 *     Services</a>
 */
@Component
@Slf4j
public class BulkSubmitAuthProvider implements Closeable {

  /** Connection timeout for authentication requests in milliseconds. */
  public static final int AUTH_CONNECT_TIMEOUT = 5_000;

  /** Connection request timeout for authentication requests in milliseconds. */
  public static final int AUTH_CONNECTION_REQUEST_TIMEOUT = 5_000;

  /** Socket timeout for authentication requests in milliseconds. */
  public static final int AUTH_SOCKET_TIMEOUT = 30_000;

  /** Number of retry attempts for authentication requests. */
  public static final int AUTH_RETRY_COUNT = 3;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final CloseableHttpClient httpClient;

  @Nonnull private final AuthTokenProvider tokenProvider;

  /**
   * Creates a new BulkSubmitAuthProvider.
   *
   * @param serverConfiguration the server configuration containing bulk submit settings.
   */
  public BulkSubmitAuthProvider(@Nonnull final ServerConfiguration serverConfiguration) {
    this.serverConfiguration = serverConfiguration;
    this.httpClient = createHttpClient();
    this.tokenProvider =
        new AuthTokenProvider(httpClient, SubmitterConfiguration.DEFAULT_TOKEN_EXPIRY_TOLERANCE);
  }

  /**
   * Acquires an OAuth2 access token for the given submitter.
   *
   * @param submitter the submitter requesting authentication.
   * @param fhirBaseUrl the FHIR base URL of the server being accessed.
   * @param oauthMetadataUrl optional explicit URL to OAuth 2.0 metadata.
   * @return an access token if credentials are configured, empty otherwise.
   * @throws IOException if token acquisition fails.
   */
  @Nonnull
  public Optional<String> acquireToken(
      @Nonnull final SubmitterIdentifier submitter,
      @Nonnull final String fhirBaseUrl,
      @Nullable final String oauthMetadataUrl)
      throws IOException {
    // Look up submitter credentials.
    final Optional<SubmitterConfiguration> submitterConfig = findSubmitterConfig(submitter);

    if (submitterConfig.isEmpty()) {
      log.debug("No configuration found for submitter: {}", submitter.toKey());
      return Optional.empty();
    }

    if (!submitterConfig.get().hasCredentials()) {
      log.debug("Submitter {} has no OAuth credentials configured", submitter.toKey());
      return Optional.empty();
    }

    // Discover token endpoint.
    final String tokenEndpoint = discoverTokenEndpoint(fhirBaseUrl, oauthMetadataUrl);
    log.debug("Using token endpoint: {} for submitter: {}", tokenEndpoint, submitter.toKey());

    // Create AuthConfig and get token via shared library.
    final AuthConfig authConfig = submitterConfig.get().toAuthConfig(tokenEndpoint);
    final ClientAuthMethod authMethod = ClientAuthMethod.create(tokenEndpoint, authConfig);

    final Token token = tokenProvider.getToken(authMethod);
    final String accessToken = token.getAccessToken();

    if (accessToken == null) {
      log.warn("Token provider returned null access token for submitter: {}", submitter.toKey());
      return Optional.empty();
    }

    log.debug("Successfully acquired token for submitter: {}", submitter.toKey());
    return Optional.of(accessToken);
  }

  /** Clears all cached tokens, forcing re-authentication on next request. */
  public void clearTokenCache() {
    tokenProvider.clearAccessContexts();
    log.debug("Cleared token cache");
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }

  @Nonnull
  private Optional<SubmitterConfiguration> findSubmitterConfig(
      @Nonnull final SubmitterIdentifier submitter) {
    final BulkSubmitConfiguration bulkSubmitConfig = serverConfiguration.getBulkSubmit();
    if (bulkSubmitConfig == null) {
      return Optional.empty();
    }
    return bulkSubmitConfig.findSubmitterConfig(submitter);
  }

  @Nonnull
  private String discoverTokenEndpoint(
      @Nonnull final String fhirBaseUrl, @Nullable final String oauthMetadataUrl)
      throws IOException {
    if (oauthMetadataUrl != null) {
      // Fetch OAuth metadata from explicit URL (no well-known path appended).
      log.debug("Using explicit OAuth metadata URL: {}", oauthMetadataUrl);
      final SMARTDiscoveryResponse discovery =
          SMARTDiscoveryResponse.getFromUrl(URI.create(oauthMetadataUrl), httpClient);
      return discovery.getTokenEndpoint();
    } else {
      // Use SMART discovery from fhirBaseUrl (appends well-known path).
      log.debug("Discovering OAuth metadata from FHIR base URL: {}", fhirBaseUrl);
      final SMARTDiscoveryResponse discovery =
          SMARTDiscoveryResponse.get(URI.create(fhirBaseUrl), httpClient);
      return discovery.getTokenEndpoint();
    }
  }

  @Nonnull
  private static CloseableHttpClient createHttpClient() {
    final RequestConfig requestConfig =
        RequestConfig.custom()
            .setConnectTimeout(AUTH_CONNECT_TIMEOUT)
            .setConnectionRequestTimeout(AUTH_CONNECTION_REQUEST_TIMEOUT)
            .setSocketTimeout(AUTH_SOCKET_TIMEOUT)
            .build();
    return HttpClients.custom()
        .setRetryHandler(new DefaultHttpRequestRetryHandler(AUTH_RETRY_COUNT, true))
        .setDefaultRequestConfig(requestConfig)
        .build();
  }
}
