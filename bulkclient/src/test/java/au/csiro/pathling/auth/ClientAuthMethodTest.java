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

import static au.csiro.pathling.test.TestUtils.getResourceAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthConfig;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ClientAuthMethodTest {

  static final ClientCredentialsResponse SYNC_RESPONSE = ClientCredentialsResponse.builder()
      .accessToken("access_token_1")
      .tokenType("token_type")
      .expiresIn(3600)
      .scope("scope_1")
      .build();

  static final ClientCredentialsResponse ASYNC_RESPONSE = ClientCredentialsResponse.builder()
      .accessToken("access_token_2")
      .tokenType("token_type")
      .expiresIn(3600)
      .scope("scope_2")
      .build();

  @Mock
  HttpClient httpClient;

  @Captor
  ArgumentCaptor<HttpUriRequest> httpRequestCaptor;

  @Test
  public void testSymmetricClientAuthMethod() throws IOException {

    when(httpClient.execute(httpRequestCaptor.capture(), any(ResponseHandler.class)))
        .thenReturn(SYNC_RESPONSE);

    final ClientAuthMethod authMethod = ClientAuthMethod.create("http://token.endpoint",
        AuthConfig.builder()
            .clientId("client_id")
            .clientSecret("client_secret")
            .scope("scope_value1")
            .build());

    final ClientCredentialsResponse response = authMethod.requestClientCredentials(httpClient);

    assertEquals(SYNC_RESPONSE, response);
    assertInstanceOf(HttpPost.class, httpRequestCaptor.getValue());
    final HttpPost postRequest = (HttpPost) httpRequestCaptor.getValue();
    assertEquals("http://token.endpoint", postRequest.getURI().toString());
    assertEquals("Basic Y2xpZW50X2lkOmNsaWVudF9zZWNyZXQ=", // base64(client_id:client_secret)
        postRequest.getFirstHeader("Authorization").getValue());

    final Set<NameValuePair> requestForm = URLEncodedUtils.parse(postRequest.getEntity())
        .stream().collect(Collectors.toUnmodifiableSet());
    assertEquals(Set.of(
        new BasicNameValuePair("grant_type", "client_credentials"),
        new BasicNameValuePair("scope", "scope_value1")
    ), requestForm);
  }

  @Test
  public void testAsymmetricClientAuthMethod() throws IOException {

    when(httpClient.execute(httpRequestCaptor.capture(), any(ResponseHandler.class)))
        .thenReturn(ASYNC_RESPONSE);

    final ClientAuthMethod authMethod = ClientAuthMethod.create("http://token.endpoint/async",
        AuthConfig.builder()
            .clientId("client_id")
            .privateKeyJWK(getResourceAsString("auth/bulk_rs384_priv_jwk.json"))
            .scope("scope_value1")
            .build());

    final ClientCredentialsResponse response = authMethod.requestClientCredentials(httpClient);

    assertEquals(ASYNC_RESPONSE, response);
    assertInstanceOf(HttpPost.class, httpRequestCaptor.getValue());
    final HttpPost postRequest = (HttpPost) httpRequestCaptor.getValue();
    assertEquals("http://token.endpoint/async", postRequest.getURI().toString());
    final List<NameValuePair> requestForm = URLEncodedUtils.parse(postRequest.getEntity());

    assertEquals(Set.of(
        new BasicNameValuePair("grant_type", "client_credentials"),
        new BasicNameValuePair("scope", "scope_value1"),
        new BasicNameValuePair("client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
    ), requestForm.stream().filter(p -> !p.getName().equals("client_assertion"))
        .collect(Collectors.toUnmodifiableSet()));

    // just check thet the client assertion is present
    // we test the content of the assertion in AsymmetricClientAuthMethodTest
    assertEquals(1,
        requestForm.stream().filter(p -> p.getName().equals("client_assertion")).count());
  }
}
