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

package au.csiro.pathling.operations.compartment;

import static org.mockito.Mockito.when;

import au.csiro.pathling.security.OidcConfiguration;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
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
import org.springframework.test.web.reactive.server.WebTestClient.ResponseSpec;

/**
 * End-to-end coverage of the read authority required to resolve a Group or to check that a Patient
 * exists, over HTTP against a server with authorisation enabled. Group-level {@code $export} and
 * the {@code group} filter of {@code $sql-run} and {@code $sql-export} resolve a Group to its
 * members, and the {@code patient} filter checks that each named Patient exists. Each of these
 * reveals stored data, so each must be refused with a 403 for a caller without read access to the
 * type, whether or not the named resource exists.
 *
 * <p>Every refusal is paired with a positive control that differs only in the authority or filter
 * under test, so that a 403 caused by something else cannot pass for the check.
 *
 * @author John Grimes
 */
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
class FilterReadAuthorisationIT {

  private static final Gson GSON = new Gson();

  private static final String PATIENT_ID = "p1";

  private static final String GROUP_ID = "g1";

  /** Holds every authority, used only to write the fixture data. */
  private static final String ADMIN_TOKEN = "admin-token";

  /** The caller from the issue: export plus read access to Patient, but not to Group. */
  private static final String PATIENT_READER_TOKEN = "patient-reader-token";

  /** The same caller with read access to Group added. */
  private static final String GROUP_READER_TOKEN = "group-reader-token";

  /** A caller that can read Observation but not Patient. */
  private static final String OBSERVATION_READER_TOKEN = "observation-reader-token";

  @TempDir private static Path warehouseDir;

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @MockitoBean private JwtDecoder jwtDecoder;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) throws IOException {
    // An empty database: the fixture data is written over the API before each test.
    Files.createDirectories(warehouseDir.resolve("delta"));
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  @AfterAll
  static void cleanupAll() throws IOException {
    // Clean the directory before JUnit's @TempDir cleanup runs, so Spark and Delta file handles do
    // not prevent its deletion.
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  @BeforeEach
  void setUp() {
    // The mocked decoder maps each test token to a JWT carrying its authorities. Everything below
    // the decoder in the authorisation chain is exercised for real.
    stubToken(ADMIN_TOKEN, "pathling");
    stubToken(
        PATIENT_READER_TOKEN,
        "pathling:export",
        "pathling:sql-run",
        "pathling:sql-export",
        "pathling:read:Patient");
    stubToken(
        GROUP_READER_TOKEN,
        "pathling:export",
        "pathling:sql-run",
        "pathling:sql-export",
        "pathling:read:Patient",
        "pathling:read:Group");
    stubToken(OBSERVATION_READER_TOKEN, "pathling:sql-run", "pathling:read:Observation");

    put(
        "Patient",
        PATIENT_ID,
        """
        { "resourceType": "Patient", "id": "%s" }
        """
            .formatted(PATIENT_ID));
    put(
        "Group",
        GROUP_ID,
        """
        {
          "resourceType": "Group",
          "id": "%s",
          "type": "person",
          "actual": true,
          "member": [ { "entity": { "reference": "Patient/%s" } } ]
        }
        """
            .formatted(GROUP_ID, PATIENT_ID));
    put(
        "Observation",
        "o1",
        """
        {
          "resourceType": "Observation",
          "id": "o1",
          "status": "final",
          "code": { "text": "test" },
          "subject": { "reference": "Patient/%s" }
        }
        """
            .formatted(PATIENT_ID));
  }

  // ---------------------------------------------------------------------------
  // Group-level $export.
  // ---------------------------------------------------------------------------

  // Before the fix this was accepted, and the output contained the group's member.
  @Test
  void groupExportOfExistingGroupIsForbiddenWithoutGroupRead() {
    groupExport(PATIENT_READER_TOKEN, GROUP_ID).expectStatus().isForbidden();
  }

  // Before the fix this was a 404 naming the group, which revealed that it does not exist.
  @Test
  void groupExportOfMissingGroupIsForbiddenWithoutGroupRead() {
    groupExport(PATIENT_READER_TOKEN, "nope").expectStatus().isForbidden();
  }

  // Positive control: adding read access to Group lets the export start.
  @Test
  void groupExportIsAcceptedWithGroupRead() {
    groupExport(GROUP_READER_TOKEN, GROUP_ID).expectStatus().isAccepted();
  }

  // ---------------------------------------------------------------------------
  // The group filter of $sql-run.
  // ---------------------------------------------------------------------------

  @Test
  void sqlRunGroupFilterIsForbiddenWithoutGroupRead() {
    sqlRun(PATIENT_READER_TOKEN, "Patient", referencePart("group", "Group/" + GROUP_ID))
        .expectStatus()
        .isForbidden();
  }

  // A group that does not exist is refused in the same way, rather than reported as not found.
  @Test
  void sqlRunGroupFilterOfMissingGroupIsForbiddenWithoutGroupRead() {
    sqlRun(PATIENT_READER_TOKEN, "Patient", referencePart("group", "Group/nope"))
        .expectStatus()
        .isForbidden();
  }

  // Positive control: the same caller can run the same view without the group filter.
  @Test
  void sqlRunWithoutGroupFilterIsAllowedWithoutGroupRead() {
    sqlRun(PATIENT_READER_TOKEN, "Patient").expectStatus().isOk();
  }

  // Positive control: adding read access to Group lets the group filter resolve.
  @Test
  void sqlRunGroupFilterIsAllowedWithGroupRead() {
    sqlRun(GROUP_READER_TOKEN, "Patient", referencePart("group", "Group/" + GROUP_ID))
        .expectStatus()
        .isOk();
  }

  // ---------------------------------------------------------------------------
  // The patient filter of $sql-run.
  // ---------------------------------------------------------------------------

  // A view over Observation needs no read access to Patient, but narrowing it by patient checks
  // that the patient exists, which does.
  @Test
  void sqlRunPatientFilterIsForbiddenWithoutPatientRead() {
    sqlRun(
            OBSERVATION_READER_TOKEN,
            "Observation",
            referencePart("patient", "Patient/" + PATIENT_ID))
        .expectStatus()
        .isForbidden();
  }

  // Positive control: the same caller can run the same view without the patient filter.
  @Test
  void sqlRunWithoutPatientFilterIsAllowedWithoutPatientRead() {
    sqlRun(OBSERVATION_READER_TOKEN, "Observation").expectStatus().isOk();
  }

  // ---------------------------------------------------------------------------
  // The group filter of $sql-export.
  // ---------------------------------------------------------------------------

  // The kick-off resolves the filters during validation, so the refusal comes before a job starts.
  @Test
  void sqlExportGroupFilterIsForbiddenWithoutGroupRead() {
    sqlExport(PATIENT_READER_TOKEN, referencePart("group", "Group/" + GROUP_ID))
        .expectStatus()
        .isForbidden();
  }

  // Positive control: adding read access to Group lets the export start.
  @Test
  void sqlExportGroupFilterIsAcceptedWithGroupRead() {
    sqlExport(GROUP_READER_TOKEN, referencePart("group", "Group/" + GROUP_ID))
        .expectStatus()
        .isAccepted();
  }

  // ---- helpers ----

  private void stubToken(@Nonnull final String token, @Nonnull final String... authorities) {
    final Jwt jwt =
        Jwt.withTokenValue(token)
            .header("alg", "none")
            .subject(token)
            .claim("authorities", List.of(authorities))
            .issuedAt(Instant.now())
            .expiresAt(Instant.now().plusSeconds(3600))
            .build();
    when(jwtDecoder.decode(token)).thenReturn(jwt);
  }

  @Nonnull
  private WebTestClient client(@Nonnull final String token) {
    return webTestClient
        .mutate()
        .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token)
        .responseTimeout(Duration.ofSeconds(60))
        .build();
  }

  private void put(
      @Nonnull final String resourceType, @Nonnull final String id, @Nonnull final String body) {
    client(ADMIN_TOKEN)
        .put()
        .uri("http://localhost:" + port + "/fhir/" + resourceType + "/" + id)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  @Nonnull
  private ResponseSpec groupExport(@Nonnull final String token, @Nonnull final String groupId) {
    return client(token)
        .get()
        .uri(
            "http://localhost:"
                + port
                + "/fhir/Group/"
                + groupId
                + "/$export?_type=Patient&_outputFormat=application/fhir+ndjson")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .exchange();
  }

  /** Runs an inline ViewDefinition projecting the ids of the given type, with any filters. */
  @SafeVarargs
  @Nonnull
  private ResponseSpec sqlRun(
      @Nonnull final String token,
      @Nonnull final String resourceType,
      @Nonnull final Map<String, Object>... filters) {
    final List<Map<String, Object>> parameters = new ArrayList<>();
    parameters.add(Map.of("name", "subjectResource", "resource", idView(resourceType)));
    parameters.add(Map.of("name", "_format", "valueString", "ndjson"));
    parameters.addAll(List.of(filters));
    final Map<String, Object> body = new LinkedHashMap<>();
    body.put("resourceType", "Parameters");
    body.put("parameter", parameters);
    return client(token)
        .post()
        .uri("http://localhost:" + port + "/fhir/$sql-run")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/x-ndjson")
        .bodyValue(GSON.toJson(body))
        .exchange();
  }

  /** Kicks off an export of an inline ViewDefinition over Patient, with any filters. */
  @SafeVarargs
  @Nonnull
  private ResponseSpec sqlExport(
      @Nonnull final String token, @Nonnull final Map<String, Object>... filters) {
    final List<Map<String, Object>> parameters = new ArrayList<>();
    parameters.add(
        Map.of(
            "name",
            "subject",
            "part",
            List.of(
                Map.of("name", "name", "valueString", "patients"),
                Map.of("name", "subjectResource", "resource", idView("Patient")))));
    parameters.addAll(List.of(filters));
    final Map<String, Object> body = new LinkedHashMap<>();
    body.put("resourceType", "Parameters");
    body.put("parameter", parameters);
    return client(token)
        .post()
        .uri("http://localhost:" + port + "/fhir/$sql-export")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(GSON.toJson(body))
        .exchange();
  }

  /** A minimal ViewDefinition projecting the id of every resource of the given type. */
  @Nonnull
  private static Map<String, Object> idView(@Nonnull final String resourceType) {
    return Map.of(
        "resourceType",
        "ViewDefinition",
        "status",
        "active",
        "resource",
        resourceType,
        "select",
        List.of(Map.of("column", List.of(Map.of("name", "id", "path", "id")))));
  }

  @Nonnull
  private static Map<String, Object> referencePart(
      @Nonnull final String name, @Nonnull final String reference) {
    return Map.of("name", name, "valueReference", Map.of("reference", reference));
  }
}
