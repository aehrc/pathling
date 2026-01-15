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

package au.csiro.pathling.fhir;

import static java.util.Objects.requireNonNull;

import au.csiro.fhir.auth.AuthConfig;
import au.csiro.fhir.auth.AuthTokenProvider;
import au.csiro.fhir.auth.ClientAuthMethod;
import au.csiro.fhir.auth.Token;
import au.csiro.pathling.config.TerminologyAuthConfiguration;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.client.api.IHttpRequest;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;

/**
 * A HAPI FHIR interceptor that handles client authentication using OAuth2 client credentials flow.
 * Supports both symmetric (client secret) and asymmetric (private key JWT) authentication methods.
 *
 * @author John Grimes
 */
@Interceptor
@Slf4j
public class ClientAuthInterceptor implements Closeable {

  /** Connection timeout for authentication requests in milliseconds. */
  public static final int AUTH_CONNECT_TIMEOUT = 5_000;

  /** Connection request timeout for authentication requests in milliseconds. */
  public static final int AUTH_CONNECTION_REQUEST_TIMEOUT = 5_000;

  /** Socket timeout for authentication requests in milliseconds. */
  public static final int AUTH_SOCKET_TIMEOUT = 5_000;

  /** Number of retry attempts for authentication requests. */
  public static final int AUTH_RETRY_COUNT = 3;

  @Nonnull private final CloseableHttpClient httpClient;

  @Nonnull private final AuthTokenProvider tokenProvider;

  @Nonnull private final ClientAuthMethod authMethod;

  /**
   * Creates a new ClientAuthInterceptor with the specified configuration.
   *
   * @param configuration the authentication configuration
   */
  public ClientAuthInterceptor(@Nonnull final TerminologyAuthConfiguration configuration) {
    this(getHttpClient(), configuration);
  }

  /**
   * Creates a new ClientAuthInterceptor with a custom HTTP client.
   *
   * @param httpClient the HTTP client to use for token requests
   * @param configuration the authentication configuration
   */
  ClientAuthInterceptor(
      @Nonnull final CloseableHttpClient httpClient,
      @Nonnull final TerminologyAuthConfiguration configuration) {
    this.httpClient = httpClient;
    final AuthConfig authConfig = configuration.toAuthConfig();
    this.tokenProvider = new AuthTokenProvider(httpClient, authConfig.getTokenExpiryTolerance());
    this.authMethod =
        ClientAuthMethod.create(requireNonNull(configuration.getTokenEndpoint()), authConfig);
  }

  /**
   * Handles outgoing client requests by adding OAuth2 authentication headers.
   *
   * @param httpRequest the HTTP request to authenticate
   * @throws IOException if authentication fails
   */
  @SuppressWarnings("unused")
  @Hook(Pointcut.CLIENT_REQUEST)
  public void handleClientRequest(@Nullable final IHttpRequest httpRequest) throws IOException {
    if (httpRequest != null) {
      final Token token = tokenProvider.getToken(authMethod);
      final String accessToken = token.getAccessToken();
      requireNonNull(accessToken);
      httpRequest.addHeader("Authorization", "Bearer " + accessToken);
    }
  }

  /**
   * Clears all cached access contexts in the token provider, forcing re-authentication on next
   * request.
   */
  public void clearAccessContexts() {
    tokenProvider.clearAccessContexts();
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }

  @Nonnull
  private static CloseableHttpClient getHttpClient() {
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
