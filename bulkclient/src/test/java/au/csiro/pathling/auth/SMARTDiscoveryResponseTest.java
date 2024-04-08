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
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SMARTDiscoveryResponseTest {

  @Mock
  HttpClient httpClient;

  @Captor
  ArgumentCaptor<HttpUriRequest> httpRequestCaptor;

  @Test
  void testSMARTDiscoveryResponse() throws IOException {

    final SMARTDiscoveryResponse expectedResponse = SMARTDiscoveryResponse.builder()
        .tokenEndpoint("https://for.bar/token")
        .build();
    when(httpClient.execute(httpRequestCaptor.capture(),
        ArgumentMatchers.any(ResponseHandler.class)))
        .thenReturn(expectedResponse);

    final SMARTDiscoveryResponse response = SMARTDiscoveryResponse.get(
        URI.create("https://for.bar/fhir"),
        httpClient);

    assertEquals(expectedResponse, response);
    assertEquals("https://for.bar/fhir/.well-known/smart-configuration",
        httpRequestCaptor.getValue().getURI().toString());
    assertEquals("GET", httpRequestCaptor.getValue().getMethod());
    assertEquals("application/json",
        httpRequestCaptor.getValue().getFirstHeader("Accept").getValue());
  }
}

