/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.TerminologyAuthConfiguration;
import ca.uhn.fhir.rest.client.api.IHttpRequest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

@Slf4j
class ClientAuthInterceptorTest {

  public static final String TOKEN_URL =
      "https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/token";
  public static final String CLIENT_ID = "someclient";
  public static final String CLIENT_SECRET = "somesecret";
  public static final String SCOPE = "openid";
  public static final int TOKEN_EXPIRY_TOLERANCE = 0;
  public static final StringEntity VALID_RESPONSE_BODY =
      new StringEntity(
          """
          {
            "access_token": "foo",
            "token_type": "access_token",
            "expires_in": 1,
            "refresh_token": "bar",
            "scope": "openid"
          }\
          """,
          StandardCharsets.UTF_8);

  ClientAuthInterceptor interceptor;
  CloseableHttpClient httpClient;
  IHttpRequest request;
  CloseableHttpResponse response;

  @BeforeEach
  void setUp() {
    httpClient = mock(CloseableHttpClient.class);
    request = mock(IHttpRequest.class);
    response = mock(CloseableHttpResponse.class);
    final TerminologyAuthConfiguration configuration =
        TerminologyAuthConfiguration.builder()
            .tokenEndpoint(TOKEN_URL)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .scope(SCOPE)
            .tokenExpiryTolerance(TOKEN_EXPIRY_TOLERANCE)
            .build();
    interceptor = new ClientAuthInterceptor(httpClient, configuration);
  }

  @Test
  void authorization() throws IOException, InterruptedException {
    final Header contentTypeHeader = mock(Header.class);
    when(contentTypeHeader.getValue()).thenReturn("application/json");
    when(response.getFirstHeader("Content-Type")).thenReturn(contentTypeHeader);

    when(response.getEntity()).thenReturn(VALID_RESPONSE_BODY);
    when(httpClient.execute(argThat(matchesRequest()))).thenReturn(response);

    interceptor.handleClientRequest(request);
    interceptor.handleClientRequest(request);
    // Wait until the initial token is expired.
    Thread.sleep(2_000);
    interceptor.handleClientRequest(request);

    verify(httpClient, times(2)).execute(any());
    verify(httpClient, times(2)).execute(argThat(matchesRequest()));
  }

  @Test
  void missingContentType() throws IOException {
    when(response.getFirstHeader("Content-Type")).thenReturn(null);
    when(response.getEntity()).thenReturn(VALID_RESPONSE_BODY);
    when(httpClient.execute(argThat(matchesRequest()))).thenReturn(response);

    final ClientProtocolException error =
        assertThrows(ClientProtocolException.class, () -> interceptor.handleClientRequest(request));
    assertEquals("Client credentials response contains no Content-Type header", error.getMessage());
  }

  @Test
  void incorrectContentType() throws IOException {
    final Header contentTypeHeader = mock(Header.class);
    when(contentTypeHeader.getValue()).thenReturn("audio/x-midi");
    when(response.getFirstHeader("Content-Type")).thenReturn(contentTypeHeader);

    when(response.getEntity()).thenReturn(VALID_RESPONSE_BODY);
    when(httpClient.execute(argThat(matchesRequest()))).thenReturn(response);

    final ClientProtocolException error =
        assertThrows(ClientProtocolException.class, () -> interceptor.handleClientRequest(request));
    assertEquals(
        "Invalid response from token endpoint: content type is not application/json",
        error.getMessage());
  }

  @Test
  void missingAccessToken() throws IOException {
    final Header contentTypeHeader = mock(Header.class);
    when(contentTypeHeader.getValue()).thenReturn("application/json");
    when(response.getFirstHeader("Content-Type")).thenReturn(contentTypeHeader);

    final StringEntity bodyWithMissingToken =
        new StringEntity(
            """
            {
              "token_type": "access_token",
              "expires_in": 1,
              "refresh_token": "bar",
              "scope": "openid"
            }\
            """,
            StandardCharsets.UTF_8);
    when(response.getEntity()).thenReturn(bodyWithMissingToken);
    when(httpClient.execute(argThat(matchesRequest()))).thenReturn(response);
    when(httpClient.execute(argThat(matchesRequest()))).thenReturn(response);

    final ClientProtocolException error =
        assertThrows(ClientProtocolException.class, () -> interceptor.handleClientRequest(request));
    assertEquals("Client credentials grant does not contain access token", error.getMessage());
  }

  @Test
  void expiryLessThanTolerance() throws IOException {
    final TerminologyAuthConfiguration configuration =
        TerminologyAuthConfiguration.builder()
            .tokenEndpoint(TOKEN_URL)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .scope(SCOPE)
            .tokenExpiryTolerance(10)
            .build();
    interceptor = new ClientAuthInterceptor(httpClient, configuration);

    final Header contentTypeHeader = mock(Header.class);
    when(contentTypeHeader.getValue()).thenReturn("application/json");
    when(response.getFirstHeader("Content-Type")).thenReturn(contentTypeHeader);

    when(response.getEntity()).thenReturn(VALID_RESPONSE_BODY);
    when(httpClient.execute(argThat(matchesRequest()))).thenReturn(response);

    final ClientProtocolException error =
        assertThrows(ClientProtocolException.class, () -> interceptor.handleClientRequest(request));
    assertEquals(
        "Client credentials grant expiry is less than the tolerance: 1", error.getMessage());
  }

  @AfterEach
  void tearDown() throws IOException {
    interceptor.close();
    ClientAuthInterceptor.clearAccessContexts();
  }

  private static ArgumentMatcher<HttpUriRequest> matchesRequest() {
    return request -> {
      if (!(request instanceof final HttpPost post)) {
        return false;
      }
      final boolean headerMatches =
          request.getURI().toString().equals(TOKEN_URL)
              && request
                  .getFirstHeader("Content-Type")
                  .getValue()
                  .equals("application/x-www-form-urlencoded")
              && request.getFirstHeader("Accept").getValue().equals("application/json");
      final HttpEntity entity = post.getEntity();
      final boolean entityMatches;
      try {
        entityMatches =
            entity instanceof UrlEncodedFormEntity
                && EntityUtils.toString(entity)
                    .equals(
                        "grant_type=client_credentials&client_id=someclient&client_secret=somesecret&scope=openid");
      } catch (final IOException e) {
        return false;
      }
      return headerMatches && entityMatches;
    };
  }
}
