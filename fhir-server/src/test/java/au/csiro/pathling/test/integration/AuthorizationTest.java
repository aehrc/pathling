/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.TestPropertySource;

/**
 * @author John Grimes
 */
@TestPropertySource(locations = {"classpath:/configuration/authorization.properties"})
class AuthorizationTest extends IntegrationTest {

  @LocalServerPort
  private int port;

  @Autowired
  private TestRestTemplate restTemplate;

  @Test
  void smartConfiguration() {
    final String response = restTemplate
        .getForObject("http://localhost:" + port + "/fhir/.well-known/smart-configuration",
            String.class);
    final Gson gson = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create();
    final SmartConfiguration smartConfiguration = gson.fromJson(response, SmartConfiguration.class);

    assertEquals("https://sso.acme.com/auth/authorize",
        smartConfiguration.getAuthorizationEndpoint());
    assertEquals("https://sso.acme.com/auth/token", smartConfiguration.getTokenEndpoint());
    assertEquals("https://sso.acme.com/auth/revoke", smartConfiguration.getRevocationEndpoint());
  }

  // TODO: Add tests for enforcement of authorization. Use WireMock for mocking out the JWKS fetch.

  @Getter
  @SuppressWarnings("unused")
  private static class SmartConfiguration {

    private String authorizationEndpoint;

    private String tokenEndpoint;

    private String revocationEndpoint;

    private List<String> capabilities;

  }

}
