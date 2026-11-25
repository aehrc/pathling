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

package au.csiro.pathling;

import static au.csiro.pathling.util.ExportOperationUtil.doPolling;
import static au.csiro.pathling.util.ExportOperationUtil.kickOffRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

import au.csiro.pathling.library.io.sink.FileInformation;
import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.util.ExportOperationUtil;
import au.csiro.pathling.util.TestDataSetup;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
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
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Verifies that the output of an asynchronous job can only be read through the {@code $result}
 * operation, which checks the caller's authorities and their ownership of the job. A second user
 * holding no authorities must not be able to read another user's job output by any route.
 *
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
@TestPropertySource(
    properties = {
      "pathling.auth.enabled=true",
      "pathling.auth.issuer=https://pathling.acme.com/fhir"
    })
@MockitoBean(types = OidcConfiguration.class)
class JobOutputAccessIT {

  private static final String OWNER_TOKEN = "alice-token";
  private static final String OTHER_USER_TOKEN = "bob-token";

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @TempDir private static Path warehouseDir;

  @MockitoBean private JwtDecoder jwtDecoder;

  private WebTestClient ownerClient;

  private WebTestClient otherUserClient;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    TestDataSetup.copyTestDataToTempDir(warehouseDir.resolve("delta"), "Patient");
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  @BeforeEach
  void setup() {
    // The owner may run exports and read all resources. The other user is authenticated but holds
    // no authorities at all.
    when(jwtDecoder.decode(OWNER_TOKEN))
        .thenReturn(jwt(OWNER_TOKEN, "alice", List.of("pathling:export", "pathling:read")));
    when(jwtDecoder.decode(OTHER_USER_TOKEN)).thenReturn(jwt(OTHER_USER_TOKEN, "bob", List.of()));

    ownerClient = clientWithToken(OWNER_TOKEN);
    otherUserClient = clientWithToken(OTHER_USER_TOKEN);
  }

  @AfterAll
  static void cleanupAll() throws IOException {
    // Clean the temp directory before JUnit's @TempDir cleanup runs, so that Spark and Delta file
    // handles do not prevent its deletion.
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  @Test
  void otherUserCannotReadJobOutput() {
    // Arrange: the owner runs an export to completion and takes the job ID and file name of one of
    // its outputs from the manifest.
    final String uri =
        "http://localhost:"
            + port
            + "/fhir/$export?_type=Patient&_outputFormat=application/fhir+ndjson";
    final String pollUrl = kickOffRequest(ownerClient, uri);
    final AtomicReference<String> resultUrl = new AtomicReference<>();
    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () ->
                doPolling(
                    ownerClient,
                    pollUrl,
                    result -> resultUrl.set(firstOutputUrl(result.getResponseBody()))));
    final MultiValueMap<String, String> query =
        UriComponentsBuilder.fromUriString(resultUrl.get()).build().getQueryParams();
    final String jobId = query.getFirst("job");
    final String file = query.getFirst("file");
    assertThat(jobId).isNotBlank();
    assertThat(file).isNotBlank();

    // Positive control: the owner can download the file through $result, so the file exists and
    // the requests below are refused for a reason other than its absence.
    ownerClient.get().uri(resultUrl.get()).exchange().expectStatus().isOk();

    // Act and assert: the other user is refused by $result, for want of an export authority.
    otherUserClient.get().uri(resultUrl.get()).exchange().expectStatus().isForbidden();

    // Act and assert: the other user cannot read the file from the job directory outside of
    // $result either. No such route exists, so the request is not found for the owner too.
    otherUserClient
        .get()
        .uri("http://localhost:" + port + "/jobs/{jobId}/{file}", jobId, file)
        .exchange()
        .expectStatus()
        .isNotFound();
    ownerClient
        .get()
        .uri("http://localhost:" + port + "/jobs/{jobId}/{file}", jobId, file)
        .exchange()
        .expectStatus()
        .isNotFound();
  }

  /** Builds a JWT for the given token, subject and authorities. */
  private static Jwt jwt(final String token, final String subject, final List<String> authorities) {
    return Jwt.withTokenValue(token)
        .header("alg", "none")
        .subject(subject)
        .claim("authorities", authorities)
        .issuedAt(Instant.now())
        .expiresAt(Instant.now().plusSeconds(3600))
        .build();
  }

  /** Builds a client that sends the given bearer token with every request. */
  private WebTestClient clientWithToken(final String token) {
    return webTestClient
        .mutate()
        .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token)
        .build();
  }

  /** Returns the URL of the first output file listed in a completed export manifest. */
  private static String firstOutputUrl(final String manifest) {
    final JsonNode parameters;
    try {
      parameters = new ObjectMapper().readTree(manifest).get("parameter");
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    final List<FileInformation> fileInfos = ExportOperationUtil.extractFileInfos(parameters);
    assertThat(fileInfos).isNotEmpty();
    return fileInfos.getFirst().absoluteUrl();
  }
}
