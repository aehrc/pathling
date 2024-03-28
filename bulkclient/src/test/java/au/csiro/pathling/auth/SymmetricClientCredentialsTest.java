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

import java.util.Collections;
import java.util.List;
import org.apache.http.Header;
import org.apache.http.message.BasicNameValuePair;
import org.junit.jupiter.api.Test;

class SymmetricClientCredentialsTest {

  @Test
  void testCreatesCorrectAssertionAndHeadersForBasicAuth() {
    final SymmetricClientCredentials credentials = SymmetricClientCredentials.builder()
        .clientId("client_id_1")
        .clientSecret("client_secret_1")
        .scope("scope")
        .tokenEndpoint("token_endpoint")
        .sendClientCredentialsInBody(false)
        .build();

    assertEquals(Collections.emptyList(), credentials.getAuthParams());
    final List<Header> authHeaders = credentials.getAuthHeaders();
    assertEquals(1, authHeaders.size());
    final Header header = authHeaders.get(0);
    assertEquals("Authorization", header.getName());
    assertEquals("Basic Y2xpZW50X2lkXzE6Y2xpZW50X3NlY3JldF8x", header.getValue());
  }

  @Test
  void testCreatesCorrectAssertionAndHeadersForFormAuth() {
    final SymmetricClientCredentials credentials = SymmetricClientCredentials.builder()
        .clientId("client_id_2")
        .clientSecret("client_secret_2")
        .scope("scope")
        .tokenEndpoint("token_endpoint")
        .sendClientCredentialsInBody(true)
        .build();

    assertEquals(List.of(
        new BasicNameValuePair("client_id", "client_id_2"),
        new BasicNameValuePair("client_secret", "client_secret_2")
    ), credentials.getAuthParams());

    assertEquals(Collections.emptyList(), credentials.getAuthHeaders());
  }
}
