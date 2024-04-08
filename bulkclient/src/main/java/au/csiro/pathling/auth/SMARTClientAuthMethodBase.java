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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicNameValuePair;

/**
 * Authentication method for one of the FHIR SMART client authentication profiles.
 */

@Slf4j
abstract class SMARTClientAuthMethodBase implements ClientAuthMethod {

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


  @Nonnull
  @Override
  public ClientCredentialsResponse requestClientCredentials(
      @Nonnull final HttpClient httpClient)
      throws IOException {
    log.debug("Performing client credentials grant using token endpoint: {}", getTokenEndpoint());
    final HttpUriRequest request = createClientCredentialsRequest();
    return ensureValidResponse(
        httpClient.execute(request, JsonResponseHandler.of(ClientCredentialsResponse.class)));
  }

  @Nonnull
  protected HttpUriRequest createClientCredentialsRequest() throws UnsupportedEncodingException {
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
