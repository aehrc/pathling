/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import lombok.Getter;
import org.apache.catalina.webresources.TomcatURLStreamHandlerFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.TestPropertySource;

/**
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@TestPropertySource(locations = {"classpath:/configuration/authorisation-enabled.properties"})
class AuthorisationTest {

  @LocalServerPort
  private int port;

  @Autowired
  private TestRestTemplate restTemplate;

  @BeforeAll
  static void beforeAll() {
    // See: https://github.com/spring-projects/spring-boot/issues/21535#issuecomment-634088332
    TomcatURLStreamHandlerFactory.disable();
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

    assertEquals("https://sso.acme.com/auth/authorize",
        smartConfiguration.getAuthorizationEndpoint());
    assertEquals("https://sso.acme.com/auth/token", smartConfiguration.getTokenEndpoint());
    assertEquals("https://sso.acme.com/auth/revoke", smartConfiguration.getRevocationEndpoint());
  }

  // TODO: Add tests for enforcement of authorisation. Use WireMock for mocking out the JWKS fetch.

  @Getter
  @SuppressWarnings("unused")
  private static class SmartConfiguration {

    private String authorizationEndpoint;

    private String tokenEndpoint;

    private String revocationEndpoint;

    private List<String> capabilities;

  }

}