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

import static org.mockito.Mockito.when;

import au.csiro.pathling.security.OidcConfiguration;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
import org.springframework.test.web.reactive.server.WebTestClient.ResponseSpec;

/**
 * Integration test verifying that the bulk submit operations only act on submissions owned by the
 * caller when authorisation is enabled.
 *
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("integration-test")
@TestPropertySource(
    properties = {
      "pathling.auth.enabled=true",
      "pathling.auth.issuer=https://pathling.acme.com/fhir",
      "pathling.bulk-submit.allowed-submitters[0].system=urn:test",
      "pathling.bulk-submit.allowed-submitters[0].value=hospital-a",
      "pathling.bulk-submit.allowable-sources[0]=https://example.org/"
    })
@MockitoBean(types = OidcConfiguration.class)
class BulkSubmitOwnershipIT {

  private static final String OWNER_TOKEN = "owner-token";
  private static final String OTHER_TOKEN = "other-token";

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @MockitoBean private JwtDecoder jwtDecoder;

  @TempDir private static Path warehouseDir;

  private String submissionId;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  @BeforeEach
  void setup() {
    // Both tokens hold only the bulk submit authority, and differ only in their subject.
    when(jwtDecoder.decode(OWNER_TOKEN)).thenReturn(jwt(OWNER_TOKEN, "hospital-user"));
    when(jwtDecoder.decode(OTHER_TOKEN)).thenReturn(jwt(OTHER_TOKEN, "mallory"));

    // A status request for a submission with no jobs waits about five seconds for one to start.
    webTestClient = webTestClient.mutate().responseTimeout(Duration.ofSeconds(30)).build();

    // The owner creates a submission with an in-progress notification that carries no manifest,
    // so that nothing is downloaded.
    submissionId = UUID.randomUUID().toString();
    bulkSubmit(OWNER_TOKEN, "in-progress").expectStatus().isOk();
  }

  @Test
  void statusOfAnotherUsersSubmissionIsNotFound() {
    // Another caller who knows the submitter and submission identifiers cannot read the status,
    // while the owner is told that processing has not started yet.
    bulkSubmitStatus(OTHER_TOKEN).expectStatus().isNotFound();
    bulkSubmitStatus(OWNER_TOKEN).expectStatus().isAccepted();
  }

  @Test
  void abortOfAnotherUsersSubmissionIsNotFound() {
    // Another caller cannot abort the submission, and the owner can still abort it afterwards,
    // which shows that it was left unaffected.
    bulkSubmit(OTHER_TOKEN, "aborted").expectStatus().isNotFound();
    bulkSubmit(OWNER_TOKEN, "aborted")
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.parameter[1].valueString")
        .isEqualTo("aborted");
  }

  @Test
  void updateOfAnotherUsersSubmissionIsNotFound() {
    // Another caller cannot add to the submission through an in-progress notification.
    bulkSubmit(OTHER_TOKEN, "in-progress").expectStatus().isNotFound();
  }

  private ResponseSpec bulkSubmit(final String token, final String status) {
    final String body =
        """
        {
          "resourceType": "Parameters",
          "parameter": [
            {"name": "submitter", "valueIdentifier": {"system": "urn:test", "value": "hospital-a"}},
            {"name": "submissionId", "valueString": "%s"},
            {"name": "submissionStatus", "valueCoding": {"code": "%s"}}
          ]
        }
        """
            .formatted(submissionId, status);
    return webTestClient
        .post()
        .uri("http://localhost:" + port + "/fhir/$bulk-submit")
        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(body)
        .exchange();
  }

  private ResponseSpec bulkSubmitStatus(final String token) {
    final String body =
        """
        {
          "resourceType": "Parameters",
          "parameter": [
            {"name": "submitter", "valueIdentifier": {"system": "urn:test", "value": "hospital-a"}},
            {"name": "submissionId", "valueString": "%s"}
          ]
        }
        """
            .formatted(submissionId);
    return webTestClient
        .post()
        .uri("http://localhost:" + port + "/fhir/$bulk-submit-status")
        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(body)
        .exchange();
  }

  private static Jwt jwt(final String token, final String subject) {
    return Jwt.withTokenValue(token)
        .header("alg", "none")
        .subject(subject)
        .claim("authorities", List.of("pathling:bulk-submit"))
        .issuedAt(Instant.now())
        .expiresAt(Instant.now().plusSeconds(3600))
        .build();
  }
}
