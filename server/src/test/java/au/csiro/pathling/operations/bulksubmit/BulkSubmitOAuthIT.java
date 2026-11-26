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
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.util.TestDataSetup;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Integration tests for OAuth2 authentication in the $bulk-submit operation. Tests the SMART
 * Backend Services flow for authenticated file downloads.
 *
 * <p>Submitter credentials are only usable with authentication enabled, so the server runs with
 * authentication enabled and a mocked JWT decoder. The submitter's own allowable sources cover the
 * WireMock server that plays its FHIR server, while the shared allowable sources cover a second
 * WireMock server that plays a different host.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/smart-app-launch/backend-services.html">SMART Backend
 *     Services</a>
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles("integration-test")
@TestPropertySource(
    properties = {
      "pathling.async.enabled=true",
      "pathling.auth.enabled=true",
      "pathling.auth.issuer=https://pathling.acme.com/fhir"
    })
@MockitoBean(types = OidcConfiguration.class)
class BulkSubmitOAuthIT {

  private static final String SUBMITTER_SYSTEM = "http://example.org/submitters";
  private static final String SUBMITTER_VALUE = "oauth-submitter";
  private static final String TEST_ACCESS_TOKEN = "test-oauth-access-token-12345";
  private static final String CALLER_ACCESS_TOKEN = "caller-access-token";

  private static WireMockServer wireMockServer;

  private static WireMockServer otherHostServer;

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @Autowired BulkSubmitAuthProvider authProvider;

  @MockitoBean private JwtDecoder jwtDecoder;

  @TempDir private static Path warehouseDir;

  @TempDir private static Path stagingDir;

  @BeforeAll
  static void setupWireMock() {
    wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMockServer.start();
    otherHostServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    otherHostServer.start();
    log.info(
        "WireMock servers started on ports: {}, {}", wireMockServer.port(), otherHostServer.port());
  }

  @AfterAll
  static void tearDownWireMock() {
    for (final WireMockServer server : List.of(wireMockServer, otherHostServer)) {
      if (server != null && server.isRunning()) {
        server.stop();
      }
    }
    log.info("WireMock servers stopped");
  }

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    // Copy only Condition data - exclude Patient which will be imported.
    TestDataSetup.copyTestDataToTempDir(warehouseDir, "Condition");
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
    registry.add("pathling.bulk-submit.staging-directory", stagingDir::toString);
    // Configure submitter with OAuth credentials for symmetric (client_secret) auth. Spring binds a
    // list from a single property source, so every property of the submitter is set here, where
    // its allowable sources must be. The allowable sources must include the WireMock port. Bare
    // "http://localhost" (port 80) does not match "http://localhost:{port}" under the URI-aware
    // UrlAllowlist matching.
    final String submitter = "pathling.bulk-submit.allowed-submitters[0].";
    registry.add(submitter + "system", () -> SUBMITTER_SYSTEM);
    registry.add(submitter + "value", () -> SUBMITTER_VALUE);
    registry.add(submitter + "clientId", () -> "test-client-id");
    registry.add(submitter + "clientSecret", () -> "test-client-secret");
    registry.add(submitter + "scope", () -> "system/*.read");
    registry.add(
        submitter + "allowable-sources[0]", () -> "http://localhost:" + wireMockServer.port());
    registry.add(
        "pathling.bulk-submit.allowable-sources[0]",
        () -> "http://localhost:" + otherHostServer.port());
  }

  @BeforeEach
  void setup() {
    // The mocked decoder accepts the caller's token and grants it the super authority, so that the
    // bulk submit, import and status operations are all permitted.
    final Jwt jwt =
        Jwt.withTokenValue(CALLER_ACCESS_TOKEN)
            .header("alg", "none")
            .subject("test-user")
            .claim("authorities", List.of("pathling"))
            .issuedAt(Instant.now())
            .expiresAt(Instant.now().plusSeconds(3600))
            .build();
    when(jwtDecoder.decode(CALLER_ACCESS_TOKEN)).thenReturn(jwt);

    webTestClient =
        webTestClient
            .mutate()
            .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + CALLER_ACCESS_TOKEN)
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .responseTimeout(Duration.ofMinutes(2))
            .build();

    // Reset WireMock stubs before each test.
    wireMockServer.resetAll();
    otherHostServer.resetAll();

    // Clear the auth provider's token cache to ensure test isolation.
    authProvider.clearTokenCache();
  }

  @AfterEach
  void cleanup() throws IOException {
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  /** Sets up WireMock stubs for the SMART discovery endpoint. */
  private void setupSmartDiscoveryStub() {
    final String baseUrl = "http://localhost:" + wireMockServer.port();
    final String smartConfig =
        String.format(
            """
            {
              "token_endpoint": "%s/oauth/token",
              "authorization_endpoint": "%s/oauth/authorize",
              "capabilities": ["client-confidential-symmetric"]
            }
            """,
            baseUrl, baseUrl);

    wireMockServer.stubFor(
        get(urlEqualTo("/.well-known/smart-configuration"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(smartConfig)));

    log.info("Set up SMART discovery stub");
  }

  /** Sets up WireMock stubs for the OAuth token endpoint. */
  private void setupTokenEndpointStub() {
    // Token response with a short expiry for testing.
    final String tokenResponse =
        String.format(
            """
            {
              "access_token": "%s",
              "token_type": "Bearer",
              "expires_in": 3600,
              "scope": "system/*.read"
            }
            """,
            TEST_ACCESS_TOKEN);

    wireMockServer.stubFor(
        post(urlEqualTo("/oauth/token"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(tokenResponse)));

    log.info("Set up OAuth token endpoint stub");
  }

  /** Sets up WireMock stubs for a manifest that requires authentication. */
  private void setupAuthenticatedManifestStubs() {
    final String baseUrl = "http://localhost:" + wireMockServer.port();

    // Manifest with requiresAccessToken: true.
    final String manifest =
        String.format(
            """
            {
              "transactionTime": "2025-11-28T00:00:00Z",
              "request": "%s/fhir/$export",
              "requiresAccessToken": true,
              "output": [
                {"type": "Patient", "url": "%s/data/Patient.ndjson"}
              ],
              "error": []
            }
            """,
            baseUrl, baseUrl);

    wireMockServer.stubFor(
        get(urlEqualTo("/manifest.json"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(manifest)));

    // Patient NDJSON file.
    final String patientNdjson =
        """
        {"resourceType":"Patient","id":"patient1","name":[{"family":"Smith","given":["John"]}]}
        """;
    wireMockServer.stubFor(
        get(urlEqualTo("/data/Patient.ndjson"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/fhir+ndjson")
                    .withBody(patientNdjson)));

    log.info("Set up authenticated manifest stubs");
  }

  /** Builds a $bulk-submit request Parameters resource. */
  private String buildBulkSubmitRequest(
      final String submissionId,
      final String status,
      final String manifestUrl,
      final String fhirBaseUrl) {
    final StringBuilder params = new StringBuilder();
    params.append(
        """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "submitter",
              "valueIdentifier": {
                "system": "%s",
                "value": "%s"
              }
            },
            {"name": "submissionId", "valueString": "%s"},
            {
              "name": "submissionStatus",
              "valueCoding": {"code": "%s"}
            }
        """
            .formatted(SUBMITTER_SYSTEM, SUBMITTER_VALUE, submissionId, status));

    if (manifestUrl != null) {
      params.append(
          """
            ,{"name": "manifestUrl", "valueString": "%s"}
          """
              .formatted(manifestUrl));
    }

    if (fhirBaseUrl != null) {
      params.append(
          """
            ,{"name": "fhirBaseUrl", "valueString": "%s"}
          """
              .formatted(fhirBaseUrl));
    }

    params.append("]}");
    return params.toString();
  }

  /** Builds a $bulk-submit-status request. */
  private String buildBulkSubmitStatusRequest(final String submissionId) {
    return """
    {
      "resourceType": "Parameters",
      "parameter": [
        {"name": "submissionId", "valueString": "%s"},
        {
          "name": "submitter",
          "valueIdentifier": {
            "system": "%s",
            "value": "%s"
          }
        }
      ]
    }
    """
        .formatted(submissionId, SUBMITTER_SYSTEM, SUBMITTER_VALUE);
  }

  @Test
  void testOAuthAuthenticationFlowWithSymmetricCredentials() {
    // This test verifies that when a submitter has OAuth credentials configured, the system:
    // 1. Discovers the token endpoint via SMART configuration
    // 2. Acquires an access token using client credentials
    // 3. Uses the access token in manifest and file requests.

    TestDataSetup.copyTestDataToTempDir(warehouseDir, "Condition");
    setupSmartDiscoveryStub();
    setupTokenEndpointStub();
    setupAuthenticatedManifestStubs();

    final String submissionId = UUID.randomUUID().toString();
    final String manifestUrl = "http://localhost:" + wireMockServer.port() + "/manifest.json";
    final String fhirBaseUrl = "http://localhost:" + wireMockServer.port();
    final String uri = "http://localhost:" + port + "/fhir/$bulk-submit";

    // Send in-progress notification with manifest URL.
    final String inProgressRequest =
        buildBulkSubmitRequest(submissionId, "in-progress", manifestUrl, fhirBaseUrl);

    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(inProgressRequest)
        .exchange()
        .expectStatus()
        .isOk();

    log.info("In-progress notification accepted for submission: {}", submissionId);

    // Poll $bulk-submit-status until downloads are complete.
    final String statusUri = "http://localhost:" + port + "/fhir/$bulk-submit-status";
    final String statusRequest = buildBulkSubmitStatusRequest(submissionId);

    // Wait for downloads to complete.
    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              webTestClient
                  .post()
                  .uri(statusUri)
                  .header("Content-Type", "application/fhir+json")
                  .header("Accept", "application/fhir+json")
                  .header("Prefer", "respond-async")
                  .bodyValue(statusRequest)
                  .exchange()
                  .expectStatus()
                  .is2xxSuccessful();
            });

    // Log all requests made to WireMock for debugging.
    log.info("All WireMock requests: {}", wireMockServer.getAllServeEvents());

    // Verify that SMART discovery was called.
    wireMockServer.verify(getRequestedFor(urlEqualTo("/.well-known/smart-configuration")));

    // Verify that the token endpoint was called with correct client credentials.
    wireMockServer.verify(
        postRequestedFor(urlEqualTo("/oauth/token"))
            .withHeader("Content-Type", containing("application/x-www-form-urlencoded"))
            .withRequestBody(containing("grant_type=client_credentials"))
            .withRequestBody(containing("client_id=test-client-id"))
            .withRequestBody(containing("client_secret=test-client-secret")));

    // Verify that the Authorization header was included in the manifest request.
    wireMockServer.verify(
        getRequestedFor(urlEqualTo("/manifest.json"))
            .withHeader("Authorization", equalTo("Bearer " + TEST_ACCESS_TOKEN)));

    // Verify that the Authorization header was included in the data file request.
    wireMockServer.verify(
        getRequestedFor(urlEqualTo("/data/Patient.ndjson"))
            .withHeader("Authorization", equalTo("Bearer " + TEST_ACCESS_TOKEN)));

    log.info("OAuth authentication flow verified successfully");
  }

  @Test
  void testOAuthWithExplicitMetadataUrl() {
    // This test verifies that when oauthMetadataUrl is provided explicitly in the request,
    // the system uses it instead of SMART discovery from fhirBaseUrl.

    TestDataSetup.copyTestDataToTempDir(warehouseDir, "Condition");
    setupTokenEndpointStub();
    setupAuthenticatedManifestStubs();

    // Set up an alternative discovery endpoint.
    final String baseUrl = "http://localhost:" + wireMockServer.port();
    final String alternativeConfig =
        String.format(
            """
            {
              "token_endpoint": "%s/oauth/token",
              "authorization_endpoint": "%s/oauth/authorize"
            }
            """,
            baseUrl, baseUrl);

    wireMockServer.stubFor(
        get(urlEqualTo("/alternative/.well-known/oauth-authorization-server"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(alternativeConfig)));

    final String submissionId = UUID.randomUUID().toString();
    final String manifestUrl = "http://localhost:" + wireMockServer.port() + "/manifest.json";
    final String fhirBaseUrl = "http://localhost:" + wireMockServer.port();
    final String oauthMetadataUrl =
        "http://localhost:"
            + wireMockServer.port()
            + "/alternative/.well-known/oauth-authorization-server";
    final String uri = "http://localhost:" + port + "/fhir/$bulk-submit";

    // Build request with explicit oauthMetadataUrl.
    final String requestBody =
        """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "submitter",
              "valueIdentifier": {
                "system": "%s",
                "value": "%s"
              }
            },
            {"name": "submissionId", "valueString": "%s"},
            {"name": "submissionStatus", "valueCoding": {"code": "in-progress"}},
            {"name": "manifestUrl", "valueString": "%s"},
            {"name": "fhirBaseUrl", "valueString": "%s"},
            {"name": "oauthMetadataUrl", "valueString": "%s"}
          ]
        }
        """
            .formatted(
                SUBMITTER_SYSTEM,
                SUBMITTER_VALUE,
                submissionId,
                manifestUrl,
                fhirBaseUrl,
                oauthMetadataUrl);

    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus()
        .isOk();

    log.info("In-progress with oauthMetadataUrl accepted for submission: {}", submissionId);

    // Poll until downloads complete.
    final String statusUri = "http://localhost:" + port + "/fhir/$bulk-submit-status";
    final String statusRequest = buildBulkSubmitStatusRequest(submissionId);

    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              webTestClient
                  .post()
                  .uri(statusUri)
                  .header("Content-Type", "application/fhir+json")
                  .header("Accept", "application/fhir+json")
                  .header("Prefer", "respond-async")
                  .bodyValue(statusRequest)
                  .exchange()
                  .expectStatus()
                  .is2xxSuccessful();
            });

    // Verify that the alternative metadata endpoint was called instead of the standard SMART path.
    wireMockServer.verify(
        getRequestedFor(urlEqualTo("/alternative/.well-known/oauth-authorization-server")));

    // Verify that the token was acquired and used.
    wireMockServer.verify(postRequestedFor(urlEqualTo("/oauth/token")));
    wireMockServer.verify(
        getRequestedFor(urlEqualTo("/manifest.json"))
            .withHeader("Authorization", equalTo("Bearer " + TEST_ACCESS_TOKEN)));

    log.info("OAuth with explicit metadata URL verified successfully");
  }

  @Test
  void testOAuthWithDisallowedMetadataUrl() {
    // This test verifies that when oauthMetadataUrl is provided but does not match
    // allowableSources, the request is rejected with a 4xx error and no outbound
    // requests are made to the attacker-supplied URL.

    final String submissionId = UUID.randomUUID().toString();
    final String manifestUrl = "http://localhost:" + wireMockServer.port() + "/manifest.json";
    final String fhirBaseUrl = "http://localhost:" + wireMockServer.port();
    final String oauthMetadataUrl = "http://attacker.example.com/.well-known/smart-configuration";
    final String uri = "http://localhost:" + port + "/fhir/$bulk-submit";

    final String requestBody =
        """
        {
          "resourceType": "Parameters",
          "parameter": [
            {
              "name": "submitter",
              "valueIdentifier": {
                "system": "%s",
                "value": "%s"
              }
            },
            {"name": "submissionId", "valueString": "%s"},
            {"name": "submissionStatus", "valueCoding": {"code": "in-progress"}},
            {"name": "manifestUrl", "valueString": "%s"},
            {"name": "fhirBaseUrl", "valueString": "%s"},
            {"name": "oauthMetadataUrl", "valueString": "%s"}
          ]
        }
        """
            .formatted(
                SUBMITTER_SYSTEM,
                SUBMITTER_VALUE,
                submissionId,
                manifestUrl,
                fhirBaseUrl,
                oauthMetadataUrl);

    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus()
        .is4xxClientError();

    log.info("Disallowed oauthMetadataUrl correctly rejected");
  }

  @Test
  void rejectsManifestUrlAndFhirBaseUrlOnHostOutsideSubmitterSources() {
    // The other host is covered by the shared allowable sources but not by the submitter's own.
    // Requests that name the submitter with a manifestUrl or a fhirBaseUrl on the other host are
    // rejected, no token is requested with the submitter's credentials, and the other host
    // receives no requests.
    setupSmartDiscoveryStub();
    setupTokenEndpointStub();
    final String submitterBase = "http://localhost:" + wireMockServer.port();
    final String otherBase = "http://localhost:" + otherHostServer.port();

    // A manifestUrl on the other host, with the submitter's own FHIR base.
    postBulkSubmitExpectingRejection(
        buildBulkSubmitRequest(
            UUID.randomUUID().toString(), "in-progress", otherBase + "/m.json", submitterBase),
        "manifestUrl");

    // A fhirBaseUrl on the other host, with a manifest on the submitter's own host.
    postBulkSubmitExpectingRejection(
        buildBulkSubmitRequest(
            UUID.randomUUID().toString(),
            "in-progress",
            submitterBase + "/manifest.json",
            otherBase + "/fhir"),
        "fhirBaseUrl");

    wireMockServer.verify(0, postRequestedFor(urlEqualTo("/oauth/token")));
    assertThat(otherHostServer.getAllServeEvents()).isEmpty();
  }

  private void postBulkSubmitExpectingRejection(
      final String requestBody, final String rejectedParameter) {
    webTestClient
        .post()
        .uri("http://localhost:" + port + "/fhir/$bulk-submit")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(requestBody)
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .value(
            body ->
                assertThat(body)
                    .contains(rejectedParameter)
                    .contains("does not match any allowed source prefixes"));
  }

  @Test
  void testNoAuthWhenManifestDoesNotRequireToken() {
    // This test verifies that when the manifest has requiresAccessToken: false,
    // the system does not include an Authorization header in file requests,
    // even if the submitter has OAuth credentials configured.

    TestDataSetup.copyTestDataToTempDir(warehouseDir, "Condition");
    setupSmartDiscoveryStub();
    setupTokenEndpointStub();

    final String baseUrl = "http://localhost:" + wireMockServer.port();

    // Manifest with requiresAccessToken: false.
    final String manifest =
        String.format(
            """
            {
              "transactionTime": "2025-11-28T00:00:00Z",
              "request": "%s/fhir/$export",
              "requiresAccessToken": false,
              "output": [
                {"type": "Patient", "url": "%s/data/Patient-public.ndjson"}
              ],
              "error": []
            }
            """,
            baseUrl, baseUrl);

    wireMockServer.stubFor(
        get(urlEqualTo("/public-manifest.json"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(manifest)));

    // Patient NDJSON file (public).
    final String patientNdjson =
        """
        {"resourceType":"Patient","id":"patient1","name":[{"family":"Public","given":["User"]}]}
        """;
    wireMockServer.stubFor(
        get(urlEqualTo("/data/Patient-public.ndjson"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/fhir+ndjson")
                    .withBody(patientNdjson)));

    final String submissionId = UUID.randomUUID().toString();
    final String manifestUrl =
        "http://localhost:" + wireMockServer.port() + "/public-manifest.json";
    final String fhirBaseUrl = "http://localhost:" + wireMockServer.port();
    final String uri = "http://localhost:" + port + "/fhir/$bulk-submit";

    final String inProgressRequest =
        buildBulkSubmitRequest(submissionId, "in-progress", manifestUrl, fhirBaseUrl);

    webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(inProgressRequest)
        .exchange()
        .expectStatus()
        .isOk();

    // Poll until downloads complete.
    final String statusUri = "http://localhost:" + port + "/fhir/$bulk-submit-status";
    final String statusRequest = buildBulkSubmitStatusRequest(submissionId);

    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              webTestClient
                  .post()
                  .uri(statusUri)
                  .header("Content-Type", "application/fhir+json")
                  .header("Accept", "application/fhir+json")
                  .header("Prefer", "respond-async")
                  .bodyValue(statusRequest)
                  .exchange()
                  .expectStatus()
                  .is2xxSuccessful();
            });

    // Verify that the manifest was fetched with an Authorization header (for manifest itself,
    // tokens are always acquired if credentials are available).
    wireMockServer.verify(
        getRequestedFor(urlEqualTo("/public-manifest.json"))
            .withHeader("Authorization", equalTo("Bearer " + TEST_ACCESS_TOKEN)));

    // Verify that the data file was fetched WITHOUT an Authorization header since
    // requiresAccessToken: false.
    wireMockServer.verify(
        getRequestedFor(urlEqualTo("/data/Patient-public.ndjson")).withoutHeader("Authorization"));

    log.info("No-auth for public manifest verified successfully");
  }
}
