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

package au.csiro.pathling.auth;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.config.AuthConfiguration;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;

/**
 * Factory for creating token credentials. It tries to use SMART configuration to discover token
 * endpoint if enabled, otherwise it uses the token endpoint from the configuration.
 */
@Slf4j
public class SMARTTokenCredentialFactory implements TokenCredentialFactory {

  /**
   * Timeout values for the HTTP client.
   */
  public static final int AUTH_CONNECT_TIMEOUT = 5_000;
  /**
   * Timeout values for the HTTP client.
   */
  public static final int AUTH_CONNECTION_REQUEST_TIMEOUT = 5_000;
  /**
   * Timeout values for the HTTP client.
   */
  public static final int AUTH_SOCKET_TIMEOUT = 5_000;
  /**
   * Retry count for the HTTP client.
   */
  public static final int AUTH_RETRY_COUNT = 3;

  @Nonnull
  private final CloseableHttpClient httpClient;

  @Nonnull
  private final AuthTokenProvider authTokenProvider;

  SMARTTokenCredentialFactory(@Nonnull final CloseableHttpClient httpClient,
      @Nonnull final AuthTokenProvider authTokenProvider) {
    this.httpClient = httpClient;
    this.authTokenProvider = authTokenProvider;
  }

  @Value
  static
  class ProviderTokenCredentials implements TokenCredentials {

    @Nonnull
    AuthTokenProvider authTokenProvider;

    @Nonnull
    ClientAuthMethod clientAuthMethod;

    @Override
    @Nonnull
    public Token getToken() {
      return authTokenProvider.getToken(clientAuthMethod);
    }
  }

  @Override
  @Nonnull
  public Optional<TokenCredentials> createCredentials(@Nonnull final URI fhirEndpoint,
      @Nonnull final AuthConfiguration authConfiguration) {
    return createAuthMethod(fhirEndpoint, authConfiguration).map(
        authMethod -> new ProviderTokenCredentials(authTokenProvider, authMethod));
  }

  @Nonnull
  String getToken(@Nonnull final ClientAuthMethod authMethod) {
    return authTokenProvider.getToken(authMethod).getAccessToken();
  }

  @Nonnull
  Optional<? extends ClientAuthMethod> createAuthMethod(@Nonnull final URI fhirEndpoint,
      @Nonnull final AuthConfiguration authConfig) {
    if (authConfig.isEnabled()) {
      if (authConfig.isUseSMART()) {
        return Optional.of(createSMARTAuthMethod(fhirEndpoint, authConfig));
      } else {
        return Optional.of(
            ClientAuthMethod.create(requireNonNull(authConfig.getTokenEndpoint()), authConfig));
      }
    } else {
      return Optional.empty();
    }
  }

  @Nonnull
  private ClientAuthMethod createSMARTAuthMethod(@Nonnull final URI fhirEndpoint,
      @Nonnull final AuthConfiguration authConfig) {
    try {
      log.debug("Retrieving SMART configuration for fhirEndpoint: {}", fhirEndpoint);
      final SMARTDiscoveryResponse discoveryResponse = SMARTDiscoveryResponse.get(fhirEndpoint,
          httpClient);
      log.debug("SMART configuration retrieved: {}", discoveryResponse);
      return ClientAuthMethod.create(discoveryResponse.getTokenEndpoint(), authConfig);
      // TODO: Maybe add validation of the capabilities here
    } catch (final IOException ex) {
      log.error("Failed to retrieve SMART configuration for fhirEndpoint: {}, ex: {}", fhirEndpoint,
          ex);
      throw new RuntimeException("Failed to retrieve SMART configuration", ex);
    }
  }

  @Nonnull
  private static CloseableHttpClient getHttpClient() {
    final RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(AUTH_CONNECT_TIMEOUT)
        .setConnectionRequestTimeout(AUTH_CONNECTION_REQUEST_TIMEOUT)
        .setSocketTimeout(AUTH_SOCKET_TIMEOUT)
        .build();
    return HttpClients.custom()
        .setRetryHandler(new DefaultHttpRequestRetryHandler(AUTH_RETRY_COUNT, true))
        .setDefaultRequestConfig(requestConfig)
        .build();
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }

  /**
   * Creates a new instance of {@link SMARTTokenCredentialFactory}.
   *
   * @param configuration the configuration to use
   * @return a new instance of {@link SMARTTokenCredentialFactory}.
   */
  @Nonnull
  public static SMARTTokenCredentialFactory create(@Nonnull final AuthConfiguration configuration) {
    final CloseableHttpClient httpClient = getHttpClient();
    return new SMARTTokenCredentialFactory(httpClient, new AuthTokenProvider(httpClient,
        configuration.getTokenExpiryTolerance()));
  }
}
