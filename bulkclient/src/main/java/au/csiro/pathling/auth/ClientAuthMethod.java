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

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.config.AuthConfiguration;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;

/**
 * Authentication method for one of the FHIR SMART client authentication profiles.
 */

@Slf4j
public abstract class ClientAuthMethod {

  /**
   * Request Client Credentials Grant with this method using the provided HTTP client.
   *
   * @param httpClient the HTTP client to use
   * @return the response from the token endpoint
   * @throws IOException if an error occurs
   */
  @Nonnull
  public ClientCredentialsResponse requestClientCredentials(
      @Nonnull final HttpClient httpClient)
      throws IOException {
    log.debug("Performing client credentials grant using token endpoint: {}", getTokenEndpoint());
    final HttpUriRequest request = createClientCredentialsRequest();
    return ensureValidResponse(
        httpClient.execute(request, JsonResponseHandler.of(ClientCredentialsResponse.class)));
  }

  /**
   * Creates a new client authentication method from the given configuration. It assumes
   * 'client-confidential-asymmetric' if `privateKey` is set in the configuration and
   * 'client-confidential-symmetric' otherwise.
   * <p>
   * It's assumed (but not checked that the authentication is enabled).
   *
   * @param tokenEndpoint the token endpoint URL
   * @param authConfig the authentication configuration
   * @return the new client authentication method
   */
  @Nonnull
  static ClientAuthMethod create(@Nonnull final String tokenEndpoint,
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

  /**
   * Represents the access scope for this method. Client Credentials for methods with the same
   * access scope can be reused.
   */
  @Value
  public static class AccessScope {

    @Nonnull
    String tokenEndpoint;

    @Nonnull
    String clientId;

    @Nullable
    String scope;
  }

  /**
   * Gets the access scope for these credentials.
   *
   * @return the access scope
   */
  @Nonnull
  public AccessScope getAccessScope() {
    return new AccessScope(getTokenEndpoint(), getClientId(), getScope());
  }

  /**
   * Gets the client ID.
   *
   * @return the client ID
   */
  @Nonnull
  abstract String getClientId();

  /**
   * Gets the token endpoint URL.
   *
   * @return the token endpoint URL
   */
  @Nonnull
  abstract String getTokenEndpoint();

  /**
   * Gets the scope.
   *
   * @return the scope
   */
  @Nullable
  abstract String getScope();

  /**
   * Gets the authentication headers required for these credentials.
   *
   * @return the authentication headers
   */
  @Nonnull
  protected List<Header> getAuthHeaders() {
    return Collections.emptyList();
  }

  /**
   * Gets the authentication parameters to be sent in the POST body form for these credentials.
   *
   * @return the authentication parameters
   */
  @Nonnull
  protected List<BasicNameValuePair> getAuthParams() {
    return getAuthParams(Instant.now());
  }

  /**
   * Gets the authentication parameters to be sent in the POST body form for these credentials.
   *
   * @param now the current time (to be used to calculate expiry if necessary)
   * @return the authentication parameters
   */
  @Nonnull
  abstract protected List<BasicNameValuePair> getAuthParams(@Nonnull final Instant now);


  /**
   * Creates the http client request instance configured to request the authentication token.
   *
   * @return the {@link HttpUriRequest} to request the token
   */
  @Nonnull
  protected HttpUriRequest createClientCredentialsRequest() {
    final HttpPost request = new HttpPost(getTokenEndpoint());
    request.addHeader(HttpHeaders.ACCEPT, "application/json");
    request.addHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
    getAuthHeaders().forEach(request::addHeader);

    final List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair(AuthConst.PARAM_GRANT_TYPE,
        AuthConst.GRANT_TYPE_CLIENT_CREDENTIALS));
    if (getScope() != null) {
      params.add(new BasicNameValuePair(AuthConst.PARAM_SCOPE, getScope()));
    }
    params.addAll(getAuthParams());
    request.setEntity(new UrlEncodedFormEntity(params, HTTP.DEF_CONTENT_CHARSET));
    return request;
  }

  @Nonnull
  private ClientCredentialsResponse ensureValidResponse(
      @Nonnull final ClientCredentialsResponse grant) throws IOException {
    if (grant.getAccessToken() == null) {
      throw new ClientProtocolException("Client credentials grant does not contain access token");
    }
    return grant;
  }
}
