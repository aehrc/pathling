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
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

/**
 * Authentication method for one of the FHIR SMART client authentication profiles.
 */

public interface ClientAuthMethod {

  /**
   * Gets the client ID.
   *
   * @return the client ID
   */
  @Nonnull
  String getClientId();

  /**
   * Gets the token endpoint URL.
   */
  @Nonnull
  String getTokenEndpoint();

  /**
   * Gets the scope.
   */
  @Nullable
  String getScope();

  /**
   * Gets the authentication headers required for these credentials.
   *
   * @return the authentication headers
   */
  @Nonnull
  default List<Header> getAuthHeaders() {
    return Collections.emptyList();
  }

  /**
   * Gets the authentication parameters to be sent in the POST body form for these credentials.
   *
   * @return the authentication parameters
   */
  @Nonnull
  default List<BasicNameValuePair> getAuthParams() {
    return getAuthParams(Instant.now());
  }

  /**
   * Gets the authentication parameters to be sent in the POST body form for these credentials.
   *
   * @param now the current time (to be used to calculate expiry if necessary)
   * @return the authentication parameters
   */
  @Nonnull
  List<BasicNameValuePair> getAuthParams(@Nonnull final Instant now);

  /**
   * Gets the access scope for these credentials.
   *
   * @return the access scope
   */
  @Nonnull
  default AccessScope getAccessScope() {
    return new AccessScope(getTokenEndpoint(), getClientId(), getScope());
  }

  /**
   * Represents the access scope for these credentials. Credentials with the same access scope can
   * be reused.
   */
  @Value
  class AccessScope {

    @Nonnull
    String tokenEndpoint;

    @Nonnull
    String clientId;

    @Nullable
    String scope;
  }
  
  @Nonnull
  default ClientCredentialsResponse clientCredentialsGrant(
      @Nonnull final HttpClient httpClient)
      throws IOException {
    // log.debug("Performing client credentials grant using token endpoint: {}",
    //     authParams.getTokenEndpoint());
    final HttpPost request = new HttpPost(getTokenEndpoint());
    request.addHeader("Content-Type", "application/x-www-form-urlencoded");
    request.addHeader("Accept", "application/json");
    request.addHeader("Cache-Control", "no-cache");
    getAuthHeaders().forEach(request::addHeader);

    final List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair(AuthConst.PARAM_GRANT_TYPE,
        AuthConst.GRANT_TYPE_CLIENT_CREDENTIALS));
    if (getScope() != null) {
      params.add(new BasicNameValuePair(AuthConst.PARAM_SCOPE, getScope()));
    }
    params.addAll(getAuthParams());

    request.setEntity(new UrlEncodedFormEntity(params));
    final String responseString;
    final HttpResponse response = httpClient.execute(request);
    @Nullable final Header contentTypeHeader = response.getFirstHeader("Content-Type");
    if (contentTypeHeader == null) {
      throw new ClientProtocolException(
          "Client credentials response contains no Content-Type header");
    }
    // log.debug("Content-Type: {}", contentTypeHeader.getValue());
    final boolean responseIsJson = contentTypeHeader.getValue()
        .startsWith("application/json");
    if (!responseIsJson) {
      throw new ClientProtocolException(
          "Invalid response from token endpoint: content type is not application/json");
    }
    responseString = EntityUtils.toString(response.getEntity());

    final Gson gson = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create();
    final ClientCredentialsResponse grant = gson.fromJson(
        responseString,
        ClientCredentialsResponse.class);
    if (grant.getAccessToken() == null) {
      throw new ClientProtocolException("Client credentials grant does not contain access token");
    }
    return grant;
  }

  /**
   * Creates a new client authentication method from the given configuration.
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
}
