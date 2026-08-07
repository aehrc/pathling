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

package au.csiro.pathling.operations.bulkexport;

import static au.csiro.pathling.util.ExportOperationUtil.doPolling;
import static au.csiro.pathling.util.ExportOperationUtil.kickOffRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import au.csiro.pathling.library.io.sink.FileInformation;
import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.util.ExportOperationUtil;
import au.csiro.pathling.util.TestDataSetup;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.hl7.fhir.r4.model.Resource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * End-to-end coverage for the system-level $export path that silently narrows the exported resource
 * types to those the caller is permitted to read. The kick-off, async execution, polling and
 * download all run over HTTP against a server with authorisation enabled, so the permission-based
 * filtering of the lazily derived source is exercised on the async worker thread rather than
 * through a directly invoked provider.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
@TestPropertySource(
    properties = {
      "pathling.auth.enabled=true",
      "pathling.auth.issuer=https://pathling.acme.com/fhir"
    })
@MockitoBean(types = OidcConfiguration.class)
class ExportAuthorisationFilteringIT {

  private static final String ACCESS_TOKEN = "partial-read-access-token";
  private static final String USERNAME = "test-user";

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @TempDir private static Path warehouseDir;

  @Autowired private FhirContext fhirContext;

  @MockitoBean private JwtDecoder jwtDecoder;

  private WebTestClient authenticatedClient;

  private IParser parser;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    // The warehouse holds both Patient and Encounter tables, so the narrowing of the export to
    // the single permitted type is observable in the manifest.
    TestDataSetup.copyTestDataToTempDir(warehouseDir.resolve("delta"), "Patient", "Encounter");
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  @BeforeEach
  void setup() {
    parser = fhirContext.newJsonParser();

    // The mocked decoder accepts the test token and yields a JWT whose authorities grant the
    // export operation plus read access to Patient only. The real PathlingAuthenticationConverter
    // maps the authorities claim, so everything below the decoder in the authorisation chain is
    // exercised for real.
    final Jwt jwt =
        Jwt.withTokenValue(ACCESS_TOKEN)
            .header("alg", "none")
            .subject(USERNAME)
            .claim("authorities", List.of("pathling:export", "pathling:read:Patient"))
            .issuedAt(Instant.now())
            .expiresAt(Instant.now().plusSeconds(3600))
            .build();
    when(jwtDecoder.decode(ACCESS_TOKEN)).thenReturn(jwt);

    authenticatedClient =
        webTestClient
            .mutate()
            .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + ACCESS_TOKEN)
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .build();
  }

  @AfterEach
  void cleanup() throws IOException {
    // Only clean up the jobs directory, preserving the delta tables for reuse.
    final Path jobsDir = warehouseDir.resolve("delta").resolve("jobs");
    if (jobsDir.toFile().exists()) {
      FileUtils.cleanDirectory(jobsDir.toFile());
    }
  }

  @AfterAll
  static void cleanupAll() throws IOException {
    // Clean the entire temp directory before JUnit's @TempDir cleanup runs, so Spark/Delta file
    // handles do not prevent directory deletion.
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  @Test
  void systemExportIsSilentlyNarrowedToPermittedResourceTypes() {
    // A system-level export with no _type parameter must be narrowed to the types the caller is
    // permitted to read: Patient files appear in the manifest, Encounter files must not.
    final String uri =
        "http://localhost:" + port + "/fhir/$export?_outputFormat=application/fhir+ndjson";
    final String pollUrl = kickOffRequest(authenticatedClient, uri);
    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () ->
                doPolling(
                    authenticatedClient,
                    pollUrl,
                    result -> {
                      assertNotNull(result.getResponseBody());
                      assertManifestNarrowedToPatient(result.getResponseBody());
                    }));
  }

  /**
   * Asserts that a completed export manifest contains output for the Patient type only, that the
   * manifest declares an access token as required, and that each output file downloads successfully
   * with the caller's token and parses as Patient resources.
   */
  private void assertManifestNarrowedToPatient(final String responseBody) {
    final JsonNode node;
    try {
      node = new ObjectMapper().readTree(responseBody);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    assertThat(node.get("resourceType").asText()).isEqualTo("Parameters");
    final JsonNode parameters = node.get("parameter");
    assertThat(parameters).isNotNull();

    // With authorisation enabled, the manifest must declare that downloads require a token.
    assertThat(getRequiresAccessToken(parameters)).isTrue();

    final List<FileInformation> fileInfos = extractFileInfos(parameters);
    assertThat(fileInfos).isNotEmpty();

    // The output must be narrowed to the permitted type only: the Encounter table is present in
    // the warehouse but the caller holds no authority to read it.
    final Set<String> resourceTypes =
        fileInfos.stream().map(FileInformation::fhirResourceType).collect(Collectors.toSet());
    assertThat(resourceTypes).containsExactly("Patient");

    // Each output file must be downloadable with the caller's token and contain Patient
    // resources.
    for (final FileInformation fileInfo : fileInfos) {
      final EntityExchangeResult<byte[]> result =
          authenticatedClient
              .get()
              .uri(fileInfo.absoluteUrl())
              .exchange()
              .expectStatus()
              .isOk()
              .expectBody()
              .returnResult();
      final byte[] bytes = result.getResponseBodyContent();
      assertThat(bytes).isNotNull();
      final String content = new String(bytes, StandardCharsets.UTF_8);
      final List<Resource> resources = ExportOperationUtil.parseNdjson(parser, content, "Patient");
      assertThat(resources).isNotEmpty();
    }
  }

  /** Extracts the output file information entries from a manifest's parameter array. */
  private static List<FileInformation> extractFileInfos(final JsonNode parameters) {
    return StreamSupport.stream(parameters.spliterator(), false)
        .filter(param -> "output".equals(param.get("name").asText()))
        .map(
            outputParam -> {
              String type = null;
              String url = null;
              for (final JsonNode part : outputParam.get("part")) {
                final String partName = part.get("name").asText();
                if ("type".equals(partName)) {
                  type =
                      part.has("valueCode")
                          ? part.get("valueCode").asText()
                          : part.get("valueString").asText();
                } else if ("url".equals(partName)) {
                  url = part.get("valueUri").asText();
                }
              }
              assertNotNull(type);
              assertNotNull(url);
              return new FileInformation(type, url);
            })
        .toList();
  }

  /** Reads the requiresAccessToken boolean parameter from a manifest's parameter array. */
  private static boolean getRequiresAccessToken(final JsonNode parameters) {
    return StreamSupport.stream(parameters.spliterator(), false)
        .filter(param -> "requiresAccessToken".equals(param.get("name").asText()))
        .findFirst()
        .map(param -> param.get("valueBoolean").asBoolean())
        .orElse(false);
  }
}
