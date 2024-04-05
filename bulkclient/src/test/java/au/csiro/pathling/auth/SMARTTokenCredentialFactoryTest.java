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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthConfiguration;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SMARTTokenCredentialFactoryTest {

  @Mock
  CloseableHttpClient httpClient;

  @Captor
  ArgumentCaptor<HttpUriRequest> httpRequestCaptor;

  @Mock
  AuthTokenProvider authTokenProvider;

  @Test
  void testNoAuthMethodWhenAuthDisabled() {
    final SMARTTokenCredentialFactory factory = new SMARTTokenCredentialFactory(httpClient,
        authTokenProvider);
    assertEquals(Optional.empty(), factory.createAuthMethod(URI.create("http://example.com"),
        AuthConfiguration.builder().enabled(false).build()));
  }

  @Test
  void testUsedSMARTDiscoveryWhenEnabledToCreateAuthMethod() throws IOException {
    final SMARTTokenCredentialFactory factory = new SMARTTokenCredentialFactory(httpClient,
        authTokenProvider);

    final SMARTDiscoveryResponse discoveryResponse = SMARTDiscoveryResponse.builder()
        .tokenEndpoint("http://example.com/token")
        .build();
    when(httpClient.execute(httpRequestCaptor.capture(),
        ArgumentMatchers.any(ResponseHandler.class)))
        .thenReturn(discoveryResponse);
    assertEquals(Optional.of(
            SymmetricClientAuthMethod.builder().clientId("client_id").clientSecret("client_secret")
                .tokenEndpoint("http://example.com/token").scope("scope").build()),
        factory.createAuthMethod(URI.create("http://example.com/fhir"),
            AuthConfiguration.builder()
                .enabled(true)
                .useSMART(true)
                .clientId("client_id")
                .clientSecret("client_secret")
                .scope("scope")
                .build()));
    
    assertEquals("http://example.com/fhir/.well-known/smart-configuration",
        httpRequestCaptor.getValue().getURI().toString());
    assertInstanceOf(HttpGet.class, httpRequestCaptor.getValue());
    assertEquals("application/json", httpRequestCaptor.getValue().getFirstHeader("Accept").getValue());
  }
  
  
}
