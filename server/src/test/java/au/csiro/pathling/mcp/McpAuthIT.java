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

package au.csiro.pathling.mcp;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.mcp.McpTestClient.ToolResult;
import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.util.TestDataSetup;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.nio.file.Path;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Exercises authorisation at the MCP endpoint the way Claude and other MCP clients meet it: a 401
 * challenge pointing at the protected resource metadata, metadata naming the MCP issuer, and tokens
 * that must come from that issuer and be issued for the MCP endpoint. The MCP issuer is served by
 * WireMock and is distinct from the issuer that protects the FHIR API.
 *
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@TestPropertySource(
    properties = {
      "pathling.auth.enabled=true",
      "pathling.auth.issuer=https://fhir-issuer.example.com",
      "pathling.auth.audience=https://pathling.example.com/fhir",
      "pathling.mcp.enabled=true",
      "pathling.mcp.auth.scopesSupported=pathling:mcp"
    })
class McpAuthIT {

  private static final String SCOPE = "pathling:mcp";

  @TempDir private static Path warehouseDir;

  private static WireMockServer issuerServer;

  private static RSAKey signingKey;

  private static String mcpIssuer;

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) throws JOSEException {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());

    // The MCP issuer publishes its discovery document and keys, which the server fetches when it
    // starts and when it first verifies a token.
    signingKey = new RSAKeyGenerator(2048).keyID("mcp-key").keyUse(KeyUse.SIGNATURE).generate();
    issuerServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    issuerServer.start();
    mcpIssuer = "http://localhost:" + issuerServer.port() + "/mcp-issuer";
    issuerServer.stubFor(
        get(urlEqualTo("/mcp-issuer/.well-known/openid-configuration"))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        """
                        {"issuer": "%s", "jwks_uri": "%s/jwks",
                         "authorization_endpoint": "%s/authorize",
                         "token_endpoint": "%s/token",
                         "response_types_supported": ["code"],
                         "subject_types_supported": ["public"],
                         "id_token_signing_alg_values_supported": ["RS256"]}
                        """
                            .formatted(mcpIssuer, mcpIssuer, mcpIssuer, mcpIssuer))));
    issuerServer.stubFor(
        get(urlEqualTo("/mcp-issuer/jwks"))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(new JWKSet(signingKey.toPublicJWK()).toString())));
    registry.add("pathling.mcp.auth.issuer", () -> mcpIssuer);
  }

  @AfterAll
  static void stopIssuer() {
    if (issuerServer != null) {
      issuerServer.stop();
    }
  }

  // -- Discovery --

  @Test
  void unauthenticatedRequestIsChallengedWithTheResourceMetadataLocation() {
    final McpTestClient mcp = new McpTestClient(webTestClient, port, null);

    final String challenge =
        mcp.post("tools/list", Map.of())
            .expectStatus()
            .isUnauthorized()
            .returnResult(String.class)
            .getResponseHeaders()
            .getFirst(HttpHeaders.WWW_AUTHENTICATE);

    assertThat(challenge)
        .startsWith("Bearer ")
        .contains("resource_metadata=\"" + metadataUrl() + "\"")
        .contains("scope=\"" + SCOPE + "\"");
  }

  @Test
  void protectedResourceMetadataIsPublicAndNamesTheMcpIssuer() {
    final JsonNode metadata = getJson("/.well-known/oauth-protected-resource/mcp");

    assertThat(metadata.path("resource").asText()).isEqualTo(resourceUrl());
    assertThat(metadata.path("authorization_servers")).hasSize(1);
    assertThat(metadata.path("authorization_servers").get(0).asText()).isEqualTo(mcpIssuer);
    assertThat(metadata.path("scopes_supported").get(0).asText()).isEqualTo(SCOPE);
    assertThat(metadata.path("bearer_methods_supported").get(0).asText()).isEqualTo("header");
  }

  @Test
  void protectedResourceMetadataIsAlsoServedAtTheRootLocation() {
    // Clients that cannot read the challenge probe the root well-known location as a fallback.
    final JsonNode metadata = getJson("/.well-known/oauth-protected-resource");

    assertThat(metadata.path("resource").asText()).isEqualTo(resourceUrl());
  }

  // -- Token validation --

  @Test
  void tokenIssuedForAnotherAudienceIsRejected() throws JOSEException {
    final String token = token(mcpIssuer, "https://pathling.example.com/fhir", List.of());
    final McpTestClient mcp = new McpTestClient(webTestClient, port, token);

    final String challenge =
        mcp.post("tools/list", Map.of())
            .expectStatus()
            .isUnauthorized()
            .returnResult(String.class)
            .getResponseHeaders()
            .getFirst(HttpHeaders.WWW_AUTHENTICATE);

    assertThat(challenge)
        .contains("error=\"invalid_token\"")
        .contains("resource_metadata=\"" + metadataUrl() + "\"");
  }

  @Test
  void tokenFromAnotherIssuerIsRejected() throws JOSEException {
    final String token = token("https://fhir-issuer.example.com", resourceUrl(), List.of());

    new McpTestClient(webTestClient, port, token)
        .post("tools/list", Map.of())
        .expectStatus()
        .isUnauthorized();
  }

  @Test
  void validTokenIsAccepted() throws JOSEException {
    final McpTestClient mcp = clientWithAuthorities(List.of());

    final JsonNode tools = mcp.request("tools/list", Map.of()).path("tools");

    assertThat(tools).hasSize(5);
  }

  // -- Authorities --

  @Test
  void searchIsPermittedWithSearchAndReadAuthorities() throws JOSEException {
    final McpTestClient mcp =
        clientWithAuthorities(List.of("pathling:search", "pathling:read:Patient"));

    final ToolResult result = mcp.callTool("search", Map.of("resourceType", "Patient"));

    assertThat(result.isError()).isFalse();
    assertThat(result.json().path("total").asInt()).isEqualTo(125);
  }

  @Test
  void searchWithoutTheDataAuthorityIsAToolError() throws JOSEException {
    final McpTestClient mcp = clientWithAuthorities(List.of("pathling:search"));

    final ToolResult result = mcp.callTool("search", Map.of("resourceType", "Patient"));

    assertThat(result.isError()).isTrue();
    assertThat(result.text()).contains("Missing authority").contains("pathling:read:Patient");
  }

  @Test
  void readRequiresTheReadResourceAuthority() throws JOSEException {
    final McpTestClient mcp = clientWithAuthorities(List.of("pathling:read:Patient"));

    final ToolResult result =
        mcp.callTool(
            "read_resource",
            Map.of("resourceType", "Patient", "id", "72df0f76-2758-fac4-67cd-de33c4a2c95e"));

    assertThat(result.isError()).isTrue();
    assertThat(result.text()).contains("pathling:read-resource");
  }

  @Test
  void runViewDefinitionRequiresTheSqlRunAuthority() throws JOSEException {
    final McpTestClient mcp = clientWithAuthorities(List.of("pathling:read:Patient"));

    final ToolResult result =
        mcp.callTool("run_view_definition", Map.of("viewDefinition", patientView()));

    assertThat(result.isError()).isTrue();
    assertThat(result.text()).contains("pathling:sql-run");
  }

  @Test
  void runViewDefinitionRequiresReadOnTheProjectedResource() throws JOSEException {
    final McpTestClient mcp = clientWithAuthorities(List.of("pathling:sql-run"));

    final ToolResult result =
        mcp.callTool("run_view_definition", Map.of("viewDefinition", patientView()));

    assertThat(result.isError()).isTrue();
    assertThat(result.text()).contains("pathling:read:Patient");
  }

  @Test
  void runViewDefinitionIsPermittedWithSqlRunAndRead() throws JOSEException {
    final McpTestClient mcp =
        clientWithAuthorities(List.of("pathling:sql-run", "pathling:read:Patient"));

    final ToolResult result =
        mcp.callTool("run_view_definition", Map.of("viewDefinition", patientView(), "limit", 2));

    assertThat(result.isError()).isFalse();
    assertThat(result.json().path("rows")).hasSize(2);
  }

  // -- Helpers --

  private String resourceUrl() {
    return "http://localhost:" + port + "/mcp";
  }

  private String metadataUrl() {
    return "http://localhost:" + port + "/.well-known/oauth-protected-resource/mcp";
  }

  private JsonNode getJson(final String path) {
    final byte[] body =
        webTestClient
            .get()
            .uri("http://localhost:" + port + path)
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult()
            .getResponseBody();
    try {
      return McpTestClient.MAPPER.readTree(body);
    } catch (final java.io.IOException e) {
      throw new AssertionError(e);
    }
  }

  private McpTestClient clientWithAuthorities(final List<String> authorities) throws JOSEException {
    return new McpTestClient(webTestClient, port, token(mcpIssuer, resourceUrl(), authorities));
  }

  private static String token(
      final String issuer, final String audience, final List<String> authorities)
      throws JOSEException {
    final JWTClaimsSet claims =
        new JWTClaimsSet.Builder()
            .issuer(issuer)
            .audience(audience)
            .subject("mcp-user")
            .issueTime(new Date())
            .expirationTime(new Date(System.currentTimeMillis() + 300_000))
            .claim("authorities", authorities)
            .claim("scope", SCOPE)
            .build();
    final SignedJWT jwt =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(signingKey.getKeyID()).build(), claims);
    jwt.sign(new RSASSASigner(signingKey));
    return jwt.serialize();
  }

  private static Map<String, Object> patientView() {
    final Map<String, Object> view = new LinkedHashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("name", "patients");
    view.put("status", "active");
    view.put("resource", "Patient");
    view.put("select", List.of(Map.of("column", List.of(Map.of("name", "id", "path", "id")))));
    return view;
  }

  @TestConfiguration
  static class McpAuthITConfiguration {

    @Bean
    @Primary
    OidcConfiguration oidcConfiguration() {
      // The FHIR API's issuer is never contacted: its discovery document is canned.
      return new OidcConfiguration(
          Map.of(
              "authorization_endpoint", "https://fhir-issuer.example.com/authorize",
              "token_endpoint", "https://fhir-issuer.example.com/token",
              "jwks_uri", "https://fhir-issuer.example.com/jwks"));
    }
  }
}
