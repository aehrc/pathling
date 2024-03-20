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
import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.auth.Credentials;
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

@Slf4j
public class SymmetricAuthTokenProvider implements Closeable, TokenProvider {

  public static final int AUTH_CONNECT_TIMEOUT = 5_000;
  public static final int AUTH_CONNECTION_REQUEST_TIMEOUT = 5_000;
  public static final int AUTH_SOCKET_TIMEOUT = 5_000;
  public static final int AUTH_RETRY_COUNT = 3;

  @Nonnull
  private final CloseableHttpClient httpClient;

  private final long tokenExpiryTolerance;

  private final boolean useBasicAuth;

  @Nonnull
  private static final Map<AccessScope, AccessContext> accessContexts = new HashMap<>();


  public SymmetricAuthTokenProvider(@Nonnull final AuthConfiguration configuration) {
    this.httpClient = getHttpClient();
    this.tokenExpiryTolerance = configuration.getTokenExpiryTolerance();
    this.useBasicAuth = configuration.isUseBasicAuth();
  }


  @Nonnull
  @Override
  public Optional<String> getToken(@Nonnull final Credentials credentials) {

    if (credentials instanceof SymmetricCredentials) {
      return doGetToken((SymmetricCredentials) credentials);
    } else {
      throw new IllegalArgumentException("Credentials must be of type SymmetricAuthCredentials");
    }
  }


  @Nonnull
  Optional<String> doGetToken(@Nonnull final SymmetricCredentials credentials) {
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
  private AccessContext ensureAccessContext(@Nonnull final SymmetricCredentials credentials,
      final long tokenExpiryTolerance)
      throws IOException {
    synchronized (accessContexts) {
      final AccessScope accessScope = new AccessScope(credentials.getTokenEndpoint(),
          credentials.getClientId(), credentials.getScope());
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
  private AccessContext getNewAccessContext(@Nonnull final SymmetricCredentials authParams,
      final long tokenExpiryTolerance) throws IOException {
    final ClientCredentialsResponse response = clientCredentialsGrant(authParams,
        tokenExpiryTolerance);
    final Instant expires = getExpiryTime(response);
    log.debug("New token will expire at {}", expires);
    return new AccessContext(response, expires);
  }

  @Nonnull
  private ClientCredentialsResponse clientCredentialsGrant(
      @Nonnull final SymmetricCredentials authParams, final long tokenExpiryTolerance)
      throws IOException {
    log.debug("Performing client credentials grant using token endpoint: {}",
        authParams.getTokenEndpoint());
    final HttpPost request = new HttpPost(authParams.getTokenEndpoint());
    request.addHeader("Content-Type", "application/x-www-form-urlencoded");
    request.addHeader("Accept", "application/json");
    request.addHeader("Cache-Control", "no-cache");

    final List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("grant_type", "client_credentials"));
    if (authParams.getScope() != null) {
      params.add(new BasicNameValuePair("scope", authParams.getScope()));
    }
    if (useBasicAuth) {
      request.addHeader("Authorization", "Basic " + authParams.toAuthString());
    } else {
      params.addAll(authParams.toFormParams());
    }
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
}
