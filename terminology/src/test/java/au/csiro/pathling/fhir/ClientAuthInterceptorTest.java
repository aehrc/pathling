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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.TerminologyAuthConfiguration;
import ca.uhn.fhir.rest.client.api.IHttpRequest;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ClientAuthInterceptor}.
 *
 * @author John Grimes
 */
@Slf4j
class ClientAuthInterceptorTest {

  public static final String TOKEN_URL = "https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/token";
  public static final String CLIENT_ID = "someclient";
  public static final String CLIENT_SECRET = "somesecret";
  public static final String SCOPE = "openid";
  public static final int TOKEN_EXPIRY_TOLERANCE = 0;
  public static final String VALID_RESPONSE_JSON = """
      {
        "access_token": "foo",
        "token_type": "access_token",
        "expires_in": 1,
        "refresh_token": "bar",
        "scope": "openid"
      }""";

  ClientAuthInterceptor interceptor;
  CloseableHttpClient httpClient;
  IHttpRequest request;
  CloseableHttpResponse response;

  @BeforeEach
  void setUp() {
    httpClient = mock(CloseableHttpClient.class);
    request = mock(IHttpRequest.class);
    response = mock(CloseableHttpResponse.class);
    final TerminologyAuthConfiguration configuration = TerminologyAuthConfiguration.builder()
        .tokenEndpoint(TOKEN_URL)
        .clientId(CLIENT_ID)
        .clientSecret(CLIENT_SECRET)
        .scope(SCOPE)
        .tokenExpiryTolerance(TOKEN_EXPIRY_TOLERANCE)
        .build();
    interceptor = new ClientAuthInterceptor(httpClient, configuration);
  }

  @SuppressWarnings("unchecked")
  @Test
  void authorization() throws IOException, InterruptedException {
    // Mock the execute(request, responseHandler) method to invoke the handler with a valid
    // response.
    final StringEntity validResponseBody = new StringEntity(VALID_RESPONSE_JSON,
        ContentType.APPLICATION_JSON);
    when(response.getStatusLine()).thenReturn(
        new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
    when(response.getEntity()).thenReturn(validResponseBody);

    when(httpClient.execute(any(HttpUriRequest.class), any(ResponseHandler.class)))
        .thenAnswer(invocation -> {
          final ResponseHandler<?> handler = invocation.getArgument(1);
          return handler.handleResponse(response);
        });

    interceptor.handleClientRequest(request);
    interceptor.handleClientRequest(request);
    // Wait until the initial token is expired.
    Thread.sleep(2_000);
    interceptor.handleClientRequest(request);

    // Verify that the httpClient was called twice (once for initial token, once for refresh).
    verify(httpClient, times(2)).execute(any(HttpUriRequest.class), any(ResponseHandler.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  void incorrectContentType() throws IOException {
    // Use StringEntity with audio content type to simulate incorrect content type.
    final StringEntity audioBody = new StringEntity(VALID_RESPONSE_JSON,
        ContentType.create("audio/x-midi"));
    when(response.getStatusLine()).thenReturn(
        new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
    when(response.getEntity()).thenReturn(audioBody);

    when(httpClient.execute(any(HttpUriRequest.class), any(ResponseHandler.class)))
        .thenAnswer(invocation -> {
          final ResponseHandler<?> handler = invocation.getArgument(1);
          return handler.handleResponse(response);
        });

    // The fhir-auth library throws an exception when content type is invalid. Mockito may wrap it.
    final Exception ex = assertThrows(Exception.class,
        () -> interceptor.handleClientRequest(request));
    assertExceptionChainContains(ex, ClientProtocolException.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  void missingAccessToken() throws IOException {
    // Use StringEntity with JSON content type but missing access_token.
    final String bodyWithMissingToken = """
        {
          "token_type": "access_token",
          "expires_in": 1,
          "refresh_token": "bar",
          "scope": "openid"
        }""";
    final StringEntity entity = new StringEntity(bodyWithMissingToken,
        ContentType.APPLICATION_JSON);
    when(response.getStatusLine()).thenReturn(
        new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
    when(response.getEntity()).thenReturn(entity);

    when(httpClient.execute(any(HttpUriRequest.class), any(ResponseHandler.class)))
        .thenAnswer(invocation -> {
          final ResponseHandler<?> handler = invocation.getArgument(1);
          return handler.handleResponse(response);
        });

    // The fhir-auth library throws an exception when access token is missing.
    final Exception ex = assertThrows(Exception.class,
        () -> interceptor.handleClientRequest(request));
    assertExceptionChainContains(ex, ClientProtocolException.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  void expiryLessThanTolerance() throws IOException {
    final TerminologyAuthConfiguration configuration = TerminologyAuthConfiguration.builder()
        .tokenEndpoint(TOKEN_URL)
        .clientId(CLIENT_ID)
        .clientSecret(CLIENT_SECRET)
        .scope(SCOPE)
        .tokenExpiryTolerance(10)
        .build();
    interceptor = new ClientAuthInterceptor(httpClient, configuration);

    // Token expires_in is 1 second, but tolerance is 10 seconds.
    final StringEntity entity = new StringEntity(VALID_RESPONSE_JSON, ContentType.APPLICATION_JSON);
    when(response.getStatusLine()).thenReturn(
        new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
    when(response.getEntity()).thenReturn(entity);

    when(httpClient.execute(any(HttpUriRequest.class), any(ResponseHandler.class)))
        .thenAnswer(invocation -> {
          final ResponseHandler<?> handler = invocation.getArgument(1);
          return handler.handleResponse(response);
        });

    // The fhir-auth library throws an exception when expiry is less than tolerance.
    final Exception ex = assertThrows(Exception.class,
        () -> interceptor.handleClientRequest(request));
    assertExceptionChainContains(ex, ClientProtocolException.class);
  }

  @AfterEach
  void tearDown() throws IOException {
    interceptor.close();
    interceptor.clearAccessContexts();
  }

  /**
   * Checks if the exception chain contains an exception of the specified type.
   */
  private static void assertExceptionChainContains(final Throwable ex,
      final Class<? extends Throwable> expectedType) {
    Throwable current = ex;
    while (current != null) {
      if (expectedType.isInstance(current)) {
        return;
      }
      current = current.getCause();
    }
    assertTrue(false,
        "Expected exception chain to contain " + expectedType.getName() + " but got: " + ex);
  }

}
