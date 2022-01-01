/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.assertions.Assertions.assertJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.security.OidcConfiguration.ConfigItem;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.test.context.TestPropertySource;

/**
 * @author John Grimes
 */
@TestPropertySource(properties = {
    "pathling.auth.enabled=true",
    "pathling.auth.issuer=https://auth.ontoserver.csiro.au/auth/realms/aehrc",
    "pathling.auth.audience=https://pathling.acme.com/fhir",
    "pathling.cors.maxAge=800",
    "pathling.cors.allowedMethods=GET,POST",
    "pathling.cors.allowedOrigins=http://foo.bar,http://boo.bar",
    "pathling.cors.allowedHeaders=X-Mine,X-Other"
})
@Slf4j
class AuthorizationConfigurationTest extends IntegrationTest {

  @LocalServerPort
  private int port;

  @Autowired
  private TestRestTemplate restTemplate;

  @MockBean
  private OidcConfiguration oidcConfiguration;

  @MockBean
  @SuppressWarnings("unused")
  private JwtDecoder jwtDecoder;

  @MockBean
  @SuppressWarnings("unused")
  private JwtAuthenticationConverter jwtAuthenticationConverter;


  @Autowired
  private Configuration configuration;

  @BeforeEach
  public void setUp() {
    when(oidcConfiguration.get(ConfigItem.AUTH_URL)).thenReturn(
        Optional
            .of("https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/auth"));
    when(oidcConfiguration.get(ConfigItem.TOKEN_URL)).thenReturn(
        Optional
            .of("https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/token"));
    when(oidcConfiguration.get(ConfigItem.REVOKE_URL)).thenReturn(
        Optional
            .of("https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/revoke"));
  }

  @Test
  void capabilityStatement() {
    final String response = restTemplate
        .getForObject("http://localhost:" + port + "/fhir/metadata", String.class);
    assertJson(
        "responses/AuthorizationConfigurationTest/capabilityStatement.CapabilityStatement.json",
        response, JSONCompareMode.LENIENT);
  }

  @Test
  void smartConfiguration() {
    final String response = restTemplate
        .getForObject("http://localhost:" + port + "/fhir/.well-known/smart-configuration",
            String.class);
    final Gson gson = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create();
    final SmartConfiguration smartConfiguration = gson.fromJson(response, SmartConfiguration.class);

    assertEquals("https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/auth",
        smartConfiguration.getAuthorizationEndpoint());
    assertEquals("https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/token",
        smartConfiguration.getTokenEndpoint());
    assertEquals(
        "https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/revoke",
        smartConfiguration.getRevocationEndpoint());
  }


  @Test
  void corsPreflight() throws JSONException {
    final HttpHeaders corsHeaders = new HttpHeaders();
    corsHeaders.setOrigin("http://foo.bar");
    corsHeaders.setAccessControlRequestMethod(HttpMethod.POST);
    corsHeaders.setAccessControlRequestHeaders(Arrays.asList("X-Mine", "X-Skip"));

    final ResponseEntity<String> response = restTemplate
        .exchange("http://localhost:" + port + "/fhir/$aggregate", HttpMethod.OPTIONS,
            new HttpEntity<String>(corsHeaders),
            String.class);

    final HttpHeaders responseHeaders = response.getHeaders();
    assertEquals(HttpStatus.OK, response.getStatusCode());
    assertEquals("http://foo.bar", responseHeaders.getAccessControlAllowOrigin());
    assertEquals(Arrays.asList(HttpMethod.GET, HttpMethod.POST),
        responseHeaders.getAccessControlAllowMethods());
    assertEquals(800L, responseHeaders.getAccessControlMaxAge());
    assertEquals(Collections.singletonList("X-Mine"),
        responseHeaders.getAccessControlAllowHeaders());
    assertTrue(responseHeaders.getAccessControlAllowCredentials());
  }

  @Test
  void corsForbiddenForIllegalRealm() throws JSONException {
    final HttpHeaders corsHeaders = new HttpHeaders();
    corsHeaders.setOrigin("http://otgher.bar");

    final ResponseEntity<String> response = restTemplate
        .exchange("http://localhost:" + port + "/fhir/metadata", HttpMethod.GET,
            new HttpEntity<String>(corsHeaders),
            String.class);

    assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
  }


  @Getter
  @SuppressWarnings("unused")
  private static class SmartConfiguration {

    private String authorizationEndpoint;

    private String tokenEndpoint;

    private String revocationEndpoint;

    private List<String> capabilities;

  }

}
