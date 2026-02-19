/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.interceptors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OidcDiscoveryFetcher} covering successful fetch, HTTP errors, and malformed JSON
 * responses. Uses WireMock to simulate the OIDC discovery endpoint.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@WireMockTest
class OidcDiscoveryFetcherTest {

  @Test
  void fetchReturnsDiscoveryDocumentOnSuccess(final WireMockRuntimeInfo wmRuntimeInfo) {
    // A 200 response with valid JSON should return the parsed document.
    WireMock.stubFor(
        WireMock.get("/.well-known/openid-configuration")
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        """
                        {
                          "issuer": "http://localhost",
                          "token_endpoint": "http://localhost/token",
                          "jwks_uri": "http://localhost/jwks"
                        }
                        """)));

    final OidcDiscoveryFetcher fetcher = new OidcDiscoveryFetcher(wmRuntimeInfo.getHttpBaseUrl());

    final Optional<Map<String, Object>> result = fetcher.fetch();

    assertTrue(result.isPresent());
    assertEquals("http://localhost", result.get().get("issuer"));
    assertEquals("http://localhost/token", result.get().get("token_endpoint"));
    assertEquals("http://localhost/jwks", result.get().get("jwks_uri"));
  }

  @Test
  void fetchReturnsEmptyOnNon200Status(final WireMockRuntimeInfo wmRuntimeInfo) {
    // A non-200 response should return empty.
    WireMock.stubFor(
        WireMock.get("/.well-known/openid-configuration")
            .willReturn(WireMock.aResponse().withStatus(404)));

    final OidcDiscoveryFetcher fetcher = new OidcDiscoveryFetcher(wmRuntimeInfo.getHttpBaseUrl());

    final Optional<Map<String, Object>> result = fetcher.fetch();

    assertTrue(result.isEmpty());
  }

  @Test
  void fetchReturnsEmptyOnServerError(final WireMockRuntimeInfo wmRuntimeInfo) {
    // A 500 server error should return empty.
    WireMock.stubFor(
        WireMock.get("/.well-known/openid-configuration")
            .willReturn(WireMock.aResponse().withStatus(500)));

    final OidcDiscoveryFetcher fetcher = new OidcDiscoveryFetcher(wmRuntimeInfo.getHttpBaseUrl());

    final Optional<Map<String, Object>> result = fetcher.fetch();

    assertTrue(result.isEmpty());
  }

  @Test
  void fetchReturnsEmptyOnConnectionError() {
    // A connection to an unreachable host should return empty.
    final OidcDiscoveryFetcher fetcher = new OidcDiscoveryFetcher("http://localhost:1");

    final Optional<Map<String, Object>> result = fetcher.fetch();

    assertTrue(result.isEmpty());
  }

  @Test
  void fetchHandlesMalformedJson(final WireMockRuntimeInfo wmRuntimeInfo) {
    // A 200 response with valid JSON but missing expected structure should still succeed.
    WireMock.stubFor(
        WireMock.get("/.well-known/openid-configuration")
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"unexpected\": true}")));

    final OidcDiscoveryFetcher fetcher = new OidcDiscoveryFetcher(wmRuntimeInfo.getHttpBaseUrl());

    // Valid JSON with unexpected structure should still be parsed.
    final Optional<Map<String, Object>> result = fetcher.fetch();
    assertTrue(result.isPresent());
  }
}
