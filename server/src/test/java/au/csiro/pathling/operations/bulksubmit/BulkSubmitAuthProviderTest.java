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

package au.csiro.pathling.operations.bulksubmit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.BulkSubmitConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.SubmitterConfiguration;
import au.csiro.pathling.errors.InvalidUserInputError;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BulkSubmitAuthProvider} covering token acquisition when no config is found, when
 * credentials are not configured, cache clearing, resource cleanup, and rejection of discovered
 * token endpoints that fall outside {@code pathling.bulkSubmit.allowableSources}.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class BulkSubmitAuthProviderTest {

  private BulkSubmitAuthProvider authProvider;

  @BeforeEach
  void setUp() {
    // Create a provider with default (empty) configuration.
    final ServerConfiguration serverConfig = new ServerConfiguration();
    authProvider = new BulkSubmitAuthProvider(serverConfig, false);
  }

  @AfterEach
  void tearDown() throws IOException {
    authProvider.close();
  }

  @Test
  void acquireTokenReturnsEmptyWhenNoBulkSubmitConfig() throws IOException {
    // When there is no bulk submit configuration, acquireToken should return empty.
    final SubmitterIdentifier submitter = new SubmitterIdentifier("http://system.org", "sub-1");

    final Optional<String> token =
        authProvider.acquireToken(submitter, "http://localhost/fhir", null);

    assertTrue(token.isEmpty());
  }

  @Test
  void acquireTokenReturnsEmptyWhenSubmitterNotConfigured() throws IOException {
    // When the submitter is not in the allowed submitters list, acquireToken should return empty.
    final ServerConfiguration serverConfig = new ServerConfiguration();
    final BulkSubmitConfiguration bulkSubmitConfig = new BulkSubmitConfiguration();
    serverConfig.setBulkSubmit(bulkSubmitConfig);

    try (final BulkSubmitAuthProvider provider = new BulkSubmitAuthProvider(serverConfig, false)) {
      final SubmitterIdentifier submitter = new SubmitterIdentifier("http://system.org", "unknown");

      final Optional<String> token =
          provider.acquireToken(submitter, "http://localhost/fhir", null);

      assertTrue(token.isEmpty());
    }
  }

  @Test
  void acquireTokenReturnsEmptyWhenNoCredentials() throws IOException {
    // When the submitter has no credentials configured, acquireToken should return empty.
    final ServerConfiguration serverConfig = new ServerConfiguration();
    final BulkSubmitConfiguration bulkSubmitConfig = new BulkSubmitConfiguration();
    final SubmitterConfiguration submitterConfig =
        new SubmitterConfiguration(
            "http://system.org", "sub-1", null, null, null, null, null, null);
    bulkSubmitConfig.getAllowedSubmitters().add(submitterConfig);
    serverConfig.setBulkSubmit(bulkSubmitConfig);

    try (final BulkSubmitAuthProvider provider = new BulkSubmitAuthProvider(serverConfig, false)) {
      final SubmitterIdentifier submitter = new SubmitterIdentifier("http://system.org", "sub-1");

      final Optional<String> token =
          provider.acquireToken(submitter, "http://localhost/fhir", null);

      assertTrue(token.isEmpty());
    }
  }

  @Test
  void clearTokenCacheDoesNotThrow() {
    // Clearing the token cache on a fresh provider should not throw.
    assertDoesNotThrow(() -> authProvider.clearTokenCache());
  }

  @Test
  void closeReleasesResources() {
    // Closing the provider should not throw.
    assertDoesNotThrow(() -> authProvider.close());
  }

  /**
   * Regression test for the discovered-token-endpoint allowlist check. Even when the explicit
   * oauthMetadataUrl is allowlisted, a discovery document whose {@code token_endpoint} points at a
   * host outside the allowlist must be rejected before any credentials are sent. This prevents a
   * compromised or misconfigured discovery document from redirecting the client_secret to an
   * attacker-controlled host.
   */
  @Test
  void rejectsDiscoveredTokenEndpointOutsideAllowlist() throws IOException {
    final WireMockServer wireMock =
        new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMock.start();
    try {
      final String allowlistedBase = "http://localhost:" + wireMock.port();
      final String oauthMetadataUrl = allowlistedBase + "/.well-known/oauth-authorization-server";
      // The discovery document is fetched from the allowlisted host but advertises a
      // token_endpoint on an off-allowlist host.
      final String maliciousDiscovery =
          "{\"token_endpoint\":\"http://attacker.example.invalid/oauth/token\","
              + "\"authorization_endpoint\":\""
              + allowlistedBase
              + "/oauth/authorize\"}";
      wireMock.stubFor(
          get(urlEqualTo("/.well-known/oauth-authorization-server"))
              .willReturn(
                  aResponse()
                      .withStatus(200)
                      .withHeader("Content-Type", "application/json")
                      .withBody(maliciousDiscovery)));

      final ServerConfiguration serverConfig = new ServerConfiguration();
      final BulkSubmitConfiguration bulkSubmitConfig = new BulkSubmitConfiguration();
      bulkSubmitConfig.setAllowableSources(List.of(allowlistedBase + "/"));
      final SubmitterConfiguration submitterConfig =
          new SubmitterConfiguration(
              "http://system.org",
              "sub-1",
              "client-id",
              "client-secret",
              null,
              "system/*.read",
              null,
              null);
      bulkSubmitConfig.getAllowedSubmitters().add(submitterConfig);
      serverConfig.setBulkSubmit(bulkSubmitConfig);

      try (final BulkSubmitAuthProvider provider = new BulkSubmitAuthProvider(serverConfig, true)) {
        // allowInsecureUrls=true to allow the WireMock http URL at all; the allowlist still
        // restricts which http hosts are accepted, so the off-allowlist attacker host is rejected.
        final SubmitterIdentifier submitter = new SubmitterIdentifier("http://system.org", "sub-1");

        final InvalidUserInputError thrown =
            assertThrows(
                InvalidUserInputError.class,
                () ->
                    provider.acquireToken(submitter, allowlistedBase + "/fhir", oauthMetadataUrl));
        assertTrue(
            thrown.getMessage().contains("attacker.example.invalid"),
            "Error message should identify the rejected token endpoint: " + thrown.getMessage());
        assertTrue(
            thrown.getMessage().contains("does not match any allowed source prefixes"),
            "Error message should indicate allowlist failure: " + thrown.getMessage());

        // The token endpoint must not have been called.
        assertEquals(
            0,
            wireMock
                .findAll(
                    com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor(
                        urlEqualTo("/oauth/token")))
                .size());
      }
    } finally {
      wireMock.stop();
    }
  }

  /**
   * The allowlisted token endpoint must pass the policy check. We do not exercise the full token
   * exchange here (covered by {@code BulkSubmitOAuthIT}); we only assert that the validator does
   * not throw for an in-allowlist endpoint by stubbing a token response and observing that the call
   * proceeds past the validation step.
   */
  @Test
  void acceptsDiscoveredTokenEndpointWithinAllowlist() throws IOException {
    final WireMockServer wireMock =
        new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMock.start();
    try {
      final String allowlistedBase = "http://localhost:" + wireMock.port();
      final String oauthMetadataUrl = allowlistedBase + "/.well-known/oauth-authorization-server";
      final String discovery =
          "{\"token_endpoint\":\""
              + allowlistedBase
              + "/oauth/token\",\"authorization_endpoint\":\""
              + allowlistedBase
              + "/oauth/authorize\"}";
      wireMock.stubFor(
          get(urlEqualTo("/.well-known/oauth-authorization-server"))
              .willReturn(
                  aResponse()
                      .withStatus(200)
                      .withHeader("Content-Type", "application/json")
                      .withBody(discovery)));
      // Stub the token endpoint to fail with 500; we only care that validation passes and the
      // call proceeds to the token exchange (which is exercised end-to-end by the IT).
      wireMock.stubFor(
          com.github.tomakehurst.wiremock.client.WireMock.post(urlEqualTo("/oauth/token"))
              .willReturn(aResponse().withStatus(500)));

      final ServerConfiguration serverConfig = new ServerConfiguration();
      final BulkSubmitConfiguration bulkSubmitConfig = new BulkSubmitConfiguration();
      bulkSubmitConfig.setAllowableSources(List.of(allowlistedBase + "/"));
      final SubmitterConfiguration submitterConfig =
          new SubmitterConfiguration(
              "http://system.org",
              "sub-1",
              "client-id",
              "client-secret",
              null,
              "system/*.read",
              null,
              null);
      bulkSubmitConfig.getAllowedSubmitters().add(submitterConfig);
      serverConfig.setBulkSubmit(bulkSubmitConfig);

      try (final BulkSubmitAuthProvider provider = new BulkSubmitAuthProvider(serverConfig, true)) {
        final SubmitterIdentifier submitter = new SubmitterIdentifier("http://system.org", "sub-1");

        // The validation must not throw InvalidUserInputError; any other failure (token exchange
        // returning 500) is acceptable here because it means we got past the allowlist check.
        final Throwable thrown =
            assertThrows(
                Throwable.class,
                () ->
                    provider.acquireToken(submitter, allowlistedBase + "/fhir", oauthMetadataUrl));
        assertTrue(
            !(thrown instanceof InvalidUserInputError)
                || !thrown.getMessage().contains("does not match any allowed source prefixes"),
            "Allowlisted token endpoint should not be rejected by the allowlist: "
                + thrown.getMessage());
      }
    } finally {
      wireMock.stop();
    }
  }
}
