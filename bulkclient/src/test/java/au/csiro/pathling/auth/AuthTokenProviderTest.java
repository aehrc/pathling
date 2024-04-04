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

import au.csiro.pathling.auth.ClientAuthMethod.AccessScope;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class AuthTokenProviderTest {

  static final ClientAuthMethod.AccessScope ACCESS_SCOPE = new AccessScope("http://foo.bar", "clinet-id",
      "scope");
  
  @Mock
  HttpClient httpClient;

  @Mock
  ClientAuthMethod clientAuthMethod;
  

  @BeforeEach
  void setUp() {
    when(clientAuthMethod.getAccessScope()).thenReturn(ACCESS_SCOPE);
  }


  @Test
  void testGetsNewTokenForNewAccessScope() throws IOException {
    
    when(clientAuthMethod.clientCredentialsGrant(eq(httpClient))).thenReturn(ClientCredentialsResponse.builder()
        .accessToken("XYZ")
        .expiresIn(3600)
        .scope("scope")
        .build());
    
    final AuthTokenProvider authTokenProvider = new AuthTokenProvider(httpClient, 60);
    final Token token = authTokenProvider.getToken(clientAuthMethod);
    assertEquals(Token.of("XYZ"), token);
  }

  @Test
  void testReturnsExistingTokenIfNotExpired() throws IOException {

    when(clientAuthMethod.clientCredentialsGrant(eq(httpClient))).thenReturn(ClientCredentialsResponse.builder()
        .accessToken("XYZ")
        .expiresIn(3600)
        .scope("scope")
        .build());
    
    // setup a valid token
    final AuthTokenProvider authTokenProvider = new AuthTokenProvider(httpClient, 60);
    final Token token = authTokenProvider.getToken(clientAuthMethod);
    assertEquals(Token.of("XYZ"), token);
    // get a cached token 
    final Token cachedToken = authTokenProvider.getToken(clientAuthMethod);
    assertEquals(Token.of("XYZ"), cachedToken);
    verify(clientAuthMethod, atMostOnce()).clientCredentialsGrant(eq(httpClient));
  }

  @Test
  void testReturnsTokenWhenExistingOneIsExpired() throws IOException {

    when(clientAuthMethod.clientCredentialsGrant(eq(httpClient))).thenReturn(ClientCredentialsResponse.builder()
        .accessToken("XYZ")
        .expiresIn(0)
        .scope("scope")
        .build());

    // setup a valid token
    final AuthTokenProvider authTokenProvider = new AuthTokenProvider(httpClient, 0);
    final Token token = authTokenProvider.getToken(clientAuthMethod);
    assertEquals(Token.of("XYZ"), token);
    // setup next token
    when(clientAuthMethod.clientCredentialsGrant(eq(httpClient))).thenReturn(ClientCredentialsResponse.builder()
        .accessToken("abc")
        .expiresIn(60)
        .scope("scope")
        .build());
    final Token newToken = authTokenProvider.getToken(clientAuthMethod);
    assertEquals(Token.of("abc"), newToken);
  }

  
  
  @Test
  void testThrowsExceptionIfTokenExpiryLessThenRequiredTolerance() throws IOException {

    when(clientAuthMethod.clientCredentialsGrant(eq(httpClient))).thenReturn(ClientCredentialsResponse.builder()
        .accessToken("XYZ")
        .expiresIn(30)
        .scope("scope")
        .build());

    final AuthTokenProvider authTokenProvider = new AuthTokenProvider(httpClient, 60);
    final RuntimeException runtimeException = assertThrows(RuntimeException.class, () -> authTokenProvider.getToken(clientAuthMethod));
    final ClientProtocolException ex = (ClientProtocolException) runtimeException.getCause();
    assertEquals("Client credentials grant expiry is less than the tolerance: 30", ex.getMessage());
  }

}
