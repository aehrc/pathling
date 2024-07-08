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

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.TestResources.assertJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.security.OidcConfiguration.ConfigItem;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
@Tag("Tranche2")
@Slf4j
class AuthorizationConfigurationTest extends IntegrationTest {

  @LocalServerPort
  int port;

  @Autowired
  TestRestTemplate restTemplate;

  @MockBean
  JwtDecoder jwtDecoder;

  @MockBean
  JwtAuthenticationConverter jwtAuthenticationConverter;

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
  static class SmartConfiguration {

    String authorizationEndpoint;

    String tokenEndpoint;

    String revocationEndpoint;

    List<String> capabilities;

  }

  @TestConfiguration
  public static class AuthorizationConfigurationTestDependencies {

    @Bean
    @Primary
    OidcConfiguration oidcConfiguration() {
      final Map<String, Object> oidcConfiguration = new HashMap<>();
      oidcConfiguration.put(ConfigItem.AUTH_URL.getKey(),
          "https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/auth");
      oidcConfiguration.put(ConfigItem.TOKEN_URL.getKey(),
          "https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/token");
      oidcConfiguration.put(ConfigItem.REVOKE_URL.getKey(),
          "https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/revoke");
      return new OidcConfiguration(oidcConfiguration);
    }

  }

}
