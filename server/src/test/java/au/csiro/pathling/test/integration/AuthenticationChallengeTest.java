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

package au.csiro.pathling.test.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.security.OidcConfiguration.ConfigItem;
import jakarta.servlet.http.HttpSessionEvent;
import jakarta.servlet.http.HttpSessionListener;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Verifies that an auth-enabled server issues a clean HTTP 401 challenge for unauthenticated
 * requests, never creates HTTP sessions or emits session cookies, and leaves public endpoints
 * accessible.
 *
 * @author John Grimes
 */
@TestPropertySource(
    properties = {
      "pathling.auth.enabled=true",
      "pathling.auth.issuer=https://auth.ontoserver.csiro.au/auth/realms/aehrc",
      "pathling.auth.audience=https://pathling.acme.com/fhir"
    })
@Tag("Tranche2")
class AuthenticationChallengeTest extends IntegrationTest {

  @LocalServerPort int port;

  TestRestTemplate restTemplate;

  @MockitoBean private JwtDecoder jwtDecoder;

  @MockitoBean private JwtAuthenticationConverter jwtAuthenticationConverter;

  @Autowired private SessionCreationRecorder sessionCreationRecorder;

  @BeforeEach
  void setup() {
    // The JDK request factory is used so that response headers are reported faithfully.
    restTemplate =
        new TestRestTemplate(
            new RestTemplateBuilder().requestFactory(JdkClientHttpRequestFactory.class));
    // Any token offered to the decoder is rejected as invalid.
    when(jwtDecoder.decode(anyString())).thenThrow(new BadJwtException("Invalid token"));
    sessionCreationRecorder.reset();
  }

  @Test
  void tokenlessRequestReceivesChallengeWithoutSession() {
    // A request with no Authorization header to a protected endpoint must be challenged. The
    // browser-like Accept header matters: it is what makes Spring Security's request cache
    // attempt to save the request in an HTTP session, which is the code path that must never
    // create a session on a stateless bearer-token API.
    final HttpHeaders headers = new HttpHeaders();
    headers.set(HttpHeaders.ACCEPT, "text/html,application/xhtml+xml");
    final ResponseEntity<String> response =
        restTemplate.exchange(
            "http://localhost:" + port + "/fhir/Patient",
            HttpMethod.GET,
            new HttpEntity<String>(headers),
            String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(response.getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE)).startsWith("Bearer");
    // No session may be created and no session cookie may be emitted.
    assertThat(response.getHeaders().get(HttpHeaders.SET_COOKIE)).isNull();
    assertThat(sessionCreationRecorder.getCount()).isZero();
  }

  @Test
  void invalidTokenReceivesChallengeWithErrorDetailAndWithoutSession() {
    // A request bearing a token the decoder rejects must be challenged with error detail.
    final HttpHeaders headers = new HttpHeaders();
    headers.set(HttpHeaders.ACCEPT, "text/html,application/xhtml+xml");
    headers.setBearerAuth("not-a-valid-token");
    final ResponseEntity<String> response =
        restTemplate.exchange(
            "http://localhost:" + port + "/fhir/Patient",
            HttpMethod.GET,
            new HttpEntity<String>(headers),
            String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(response.getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE))
        .startsWith("Bearer")
        .contains("error=\"invalid_token\"");
    // No session may be created and no session cookie may be emitted.
    assertThat(response.getHeaders().get(HttpHeaders.SET_COOKIE)).isNull();
    assertThat(sessionCreationRecorder.getCount()).isZero();
  }

  @Test
  void publicEndpointRemainsAccessibleWithoutToken() {
    // The capability statement is a public endpoint and must not require authentication.
    final ResponseEntity<String> response =
        restTemplate.exchange(
            "http://localhost:" + port + "/fhir/metadata",
            HttpMethod.GET,
            new HttpEntity<String>(new HttpHeaders()),
            String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    // Public endpoints must be session-free as well.
    assertThat(response.getHeaders().get(HttpHeaders.SET_COOKIE)).isNull();
    assertThat(sessionCreationRecorder.getCount()).isZero();
  }

  /**
   * Records HTTP session creations so tests can assert that the server never creates a session. The
   * client-visible {@code Set-Cookie} header alone is not a sufficient signal, because a session
   * created after the response has been committed (as happens during the error dispatch of a 401)
   * produces no cookie yet still exercises the session-cookie code path that failed in production.
   */
  static class SessionCreationRecorder implements HttpSessionListener {

    private final AtomicInteger count = new AtomicInteger();

    @Override
    public void sessionCreated(final HttpSessionEvent event) {
      count.incrementAndGet();
    }

    int getCount() {
      return count.get();
    }

    void reset() {
      count.set(0);
    }
  }

  @TestConfiguration
  public static class AuthenticationChallengeTestDependencies {

    @Bean
    SessionCreationRecorder sessionCreationRecorder() {
      return new SessionCreationRecorder();
    }

    @Bean
    ServletListenerRegistrationBean<HttpSessionListener> sessionListenerRegistration(
        final SessionCreationRecorder recorder) {
      return new ServletListenerRegistrationBean<>(recorder);
    }

    @Bean
    @Primary
    OidcConfiguration oidcConfiguration() {
      // A canned OIDC configuration avoids network access to the issuer during startup.
      final Map<String, Object> oidcConfiguration = new HashMap<>();
      oidcConfiguration.put(
          ConfigItem.AUTH_URL.getKey(),
          "https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/auth");
      oidcConfiguration.put(
          ConfigItem.TOKEN_URL.getKey(),
          "https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/token");
      oidcConfiguration.put(
          ConfigItem.REVOKE_URL.getKey(),
          "https://auth.ontoserver.csiro.au/auth/realms/aehrc/protocol/openid-connect/revoke");
      return new OidcConfiguration(oidcConfiguration);
    }
  }
}
