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

import au.csiro.pathling.config.AuthConfiguration;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

@Slf4j
public class AuthTokenAuthFactory implements TokenAuthFactory {

  public static final int AUTH_CONNECT_TIMEOUT = 5_000;
  public static final int AUTH_CONNECTION_REQUEST_TIMEOUT = 5_000;
  public static final int AUTH_SOCKET_TIMEOUT = 5_000;
  public static final int AUTH_RETRY_COUNT = 3;

  @Nonnull
  private final CloseableHttpClient httpClient;

  private final long tokenExpiryTolerance;

  @Nonnull
  private static final Map<ClientAuthMethod.AccessScope, AccessContext> accessContexts = new HashMap<>();


  public AuthTokenAuthFactory(@Nonnull final AuthConfiguration configuration) {
    this.httpClient = getHttpClient();
    this.tokenExpiryTolerance = configuration.getTokenExpiryTolerance();
  }


  @Value
  class ProviderTokenCredentials implements TokenCredentials {

    @Nonnull
    ClientAuthMethod clientAuthMethod;

    @Override
    @Nonnull
    public String getToken() {
      return AuthTokenAuthFactory.this.getToken(clientAuthMethod)
          .orElseThrow(() -> new IllegalStateException("No token"));
    }
  }

  @Override
  @Nonnull
  public Optional<TokenCredentials> createCredentials(@Nonnull final URI fhirEndpoint,
      @Nonnull final AuthConfiguration authConfiguration) {
    return createAuthMethod(fhirEndpoint, authConfiguration).map(ProviderTokenCredentials::new);
  }

  @Nonnull
  Optional<? extends ClientAuthMethod> createAuthMethod(@Nonnull final URI fhirEndpoint,
      @Nonnull final AuthConfiguration authConfig) {
    if (authConfig.isEnabled()) {
      if (authConfig.isUseSMART()) {
        return Optional.of(createSMARTAuthMethod(fhirEndpoint, authConfig));
      } else {
        return Optional.of(
            doCreateAuthMethods(requireNonNull(authConfig.getTokenEndpoint()), authConfig));
      }
    } else {
      return Optional.empty();
    }
  }

  @Nonnull
  ClientAuthMethod createSMARTAuthMethod(@Nonnull final URI fhirEndpoint,
      @Nonnull final AuthConfiguration authConfig) {
    try {
      log.debug("Retrieving SMART configuration for fhirEndpoint: {}", fhirEndpoint);
      final SMARTDiscoveryResponse discoveryResponse = SMARTDiscoveryResponse.get(fhirEndpoint,
          httpClient);
      log.debug("SMART configuration retrieved: {}", discoveryResponse);
      return doCreateAuthMethods(discoveryResponse.getTokenEndpoint(), authConfig);
      // TODO: Maybe add validation of the capabilities here
    } catch (final IOException ex) {
      log.error("Failed to retrieve SMART configuration for fhirEndpoint: {}, ex: {}", fhirEndpoint,
          ex);
      throw new RuntimeException("Failed to retrieve SMART configuration", ex);
    }
  }

  ClientAuthMethod doCreateAuthMethods(@Nonnull final String tokenEndpoint,
      @Nonnull final AuthConfiguration authConfig) {
    if (nonNull(authConfig.getPrivateKeyJWK())) {
      return AsymmetricClientAuthMethod.builder()
          .tokenEndpoint(tokenEndpoint)
          .clientId(requireNonNull(authConfig.getClientId()))
          .privateKeyJWK(requireNonNull(authConfig.getPrivateKeyJWK()))
          .scope(authConfig.getScope())
          .build();
    } else {
      return SymmetricClientAuthMethod.builder()
          .tokenEndpoint(tokenEndpoint)
          .clientId(requireNonNull(authConfig.getClientId()))
          .clientSecret(requireNonNull(authConfig.getClientSecret()))
          .scope(authConfig.getScope())
          .sendClientCredentialsInBody(authConfig.isUseFormForBasicAuth())
          .build();
    }
  }

  public
  @Override
  @Nonnull
  Optional<String> getToken(@Nonnull final ClientAuthMethod credentials) {
    final AccessContext accessContext;
    try {
      accessContext = ensureAccessContext(credentials, tokenExpiryTolerance);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    // Now we should have a valid token, so we can add it to the request.
    final String accessToken = accessContext.getClientCredentialsResponse().getAccessToken();
    return Optional.ofNullable(accessToken);
  }

  @Nonnull
  private AccessContext ensureAccessContext(@Nonnull final ClientAuthMethod credentials,
      final long tokenExpiryTolerance)
      throws IOException {
    synchronized (accessContexts) {
      final ClientAuthMethod.AccessScope accessScope = credentials.getAccessScope();
      AccessContext accessContext = accessContexts.get(accessScope);
      if (accessContext == null || accessContext.getExpiryTime()
          .isBefore(Instant.now().plusSeconds(tokenExpiryTolerance))) {
        // We need to get a new token if:
        // (1) We don't have a token yet;
        // (2) The token is expired, or;
        // (3) The token is about to expire (within the tolerance).
        log.debug("Getting new token");
        accessContext = getNewAccessContext(credentials,
            tokenExpiryTolerance);
        accessContexts.put(accessScope, accessContext);
      }
      return accessContext;
    }
  }

  @Nonnull
  private AccessContext getNewAccessContext(@Nonnull final ClientAuthMethod authParams,
      final long tokenExpiryTolerance) throws IOException {
    final ClientCredentialsResponse response = clientCredentialsGrant(authParams,
        tokenExpiryTolerance);
    final Instant expires = getExpiryTime(response);
    log.debug("New token will expire at {}", expires);
    return new AccessContext(response, expires);
  }

  @Nonnull
  private ClientCredentialsResponse clientCredentialsGrant(
      @Nonnull final ClientAuthMethod authParams, final long tokenExpiryTolerance)
      throws IOException {
    log.debug("Performing client credentials grant using token endpoint: {}",
        authParams.getTokenEndpoint());
    final HttpPost request = new HttpPost(authParams.getTokenEndpoint());
    request.addHeader("Content-Type", "application/x-www-form-urlencoded");
    request.addHeader("Accept", "application/json");
    request.addHeader("Cache-Control", "no-cache");
    authParams.getAuthHeaders().forEach(request::addHeader);

    final List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair(AuthConst.PARAM_GRANT_TYPE,
        AuthConst.GRANT_TYPE_CLIENT_CREDENTIALS));
    if (authParams.getScope() != null) {
      params.add(new BasicNameValuePair(AuthConst.PARAM_SCOPE, authParams.getScope()));
    }
    params.addAll(authParams.getAuthParams());

    request.setEntity(new UrlEncodedFormEntity(params));
    final String responseString;
    try (final CloseableHttpResponse response = httpClient.execute(request)) {
      @Nullable final Header contentTypeHeader = response.getFirstHeader("Content-Type");
      if (contentTypeHeader == null) {
        throw new ClientProtocolException(
            "Client credentials response contains no Content-Type header");
      }
      log.debug("Content-Type: {}", contentTypeHeader.getValue());
      final boolean responseIsJson = contentTypeHeader.getValue()
          .startsWith("application/json");
      if (!responseIsJson) {
        throw new ClientProtocolException(
            "Invalid response from token endpoint: content type is not application/json");
      }
      responseString = EntityUtils.toString(response.getEntity());
    }

    final Gson gson = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create();
    final ClientCredentialsResponse grant = gson.fromJson(
        responseString,
        ClientCredentialsResponse.class);
    if (grant.getAccessToken() == null) {
      throw new ClientProtocolException("Client credentials grant does not contain access token");
    }
    if (grant.getExpiresIn() < tokenExpiryTolerance) {
      throw new ClientProtocolException(
          "Client credentials grant expiry is less than the tolerance: " + grant.getExpiresIn());
    }
    return grant;
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

  private static Instant getExpiryTime(@Nonnull final ClientCredentialsResponse response) {
    return Instant.now().plusSeconds(response.getExpiresIn());
  }

  public static void clearAccessContexts() {
    synchronized (accessContexts) {
      accessContexts.clear();
    }
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }

  @Value
  static
  class AccessContext {

    @Nonnull
    ClientCredentialsResponse clientCredentialsResponse;

    @Nonnull
    Instant expiryTime;
  }
}
