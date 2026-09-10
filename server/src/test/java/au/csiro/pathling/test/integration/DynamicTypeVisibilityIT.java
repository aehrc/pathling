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

import static au.csiro.pathling.util.ExportOperationUtil.doPolling;
import static au.csiro.pathling.util.ExportOperationUtil.kickOffRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import au.csiro.pathling.util.TestDataSetup;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * End-to-end reproduction of issue #2709: a resource type whose Delta table is created after the
 * server started must be visible to reads that narrow by {@code patient}, {@code group} or {@code
 * _since}, and to bulk exports, without a restart.
 *
 * <p>The server starts against an <b>empty</b> warehouse, so every table these tests read is
 * created after startup. Before the fix, the unfiltered {@code $sql-run} returned the row while
 * every narrowed variant failed with {@code No data found for resource type: Patient}.
 *
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class DynamicTypeVisibilityIT {

  private static final Gson GSON = new Gson();

  private static final String PATIENT_ID = "dynamic-visibility-patient";

  private static final String OTHER_PATIENT_ID = "dynamic-visibility-other-patient";

  private static final String GROUP_ID = "dynamic-visibility-group";

  /**
   * A type used only by the enumeration scenario, so that no other test in this class reads it and
   * brings it into the source's type set through dynamic discovery.
   */
  private static final String NEVER_READ_TYPE = "Organization";

  @TempDir private static Path warehouseDir;

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) throws IOException {
    // An empty database directory: the server starts knowing about no resource types at all.
    Files.createDirectories(warehouseDir.resolve("delta"));
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  @BeforeEach
  void setUp() {
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .responseTimeout(Duration.ofSeconds(60))
            .build();
    putPatient(PATIENT_ID);
  }

  // -------------------------------------------------------------------------
  // User story 1: filtered $sql-run.
  // -------------------------------------------------------------------------

  // The baseline: without narrowing, the post-startup type is already visible, because the live
  // source discovers its table on demand.
  @Test
  void unfilteredRunSeesPostStartupType() {
    assertThat(runIds()).contains(PATIENT_ID);
  }

  // The defect as reported: narrowing by patient used to fail because the derived source was built
  // over the startup resource map.
  @Test
  void patientFilteredRunSeesPostStartupType() {
    assertThat(runIds(referencePart("patient", "Patient/" + PATIENT_ID)))
        .containsExactly(PATIENT_ID);
  }

  // The narrowing still narrows: with a second Patient stored, filtering to the first returns only
  // that one. A patient id naming nothing is not used here, because the operation answers that
  // with a 400 by design rather than an empty result.
  @Test
  void patientFilteredRunExcludesOtherPatients() {
    putPatient(OTHER_PATIENT_ID);

    assertThat(runIds()).contains(PATIENT_ID, OTHER_PATIENT_ID);
    assertThat(runIds(referencePart("patient", "Patient/" + PATIENT_ID)))
        .containsExactly(PATIENT_ID);
  }

  // The _since variant of the same defect.
  @Test
  void sinceFilteredRunSeesPostStartupType() {
    assertThat(runIds(simplePart("_since", "valueInstant", "2000-01-01T00:00:00Z")))
        .contains(PATIENT_ID);
  }

  // The group variant of the same defect: the group's member resolves to the patient, and the
  // compartment filter is applied over a type discovered after startup.
  @Test
  void groupFilteredRunSeesPostStartupType() {
    putGroup();

    assertThat(runIds(referencePart("group", "Group/" + GROUP_ID))).containsExactly(PATIENT_ID);
  }

  // -------------------------------------------------------------------------
  // User story 2: filtered bulk export.
  // -------------------------------------------------------------------------

  // A patient-level export derives the source through the compartment filter, so it reached the
  // same defect.
  @Test
  void patientLevelExportIncludesPostStartupType() {
    final Map<String, List<String>> output =
        exportToCompletion(
            "http://localhost:"
                + port
                + "/fhir/Patient/"
                + PATIENT_ID
                + "/$export?_outputFormat=application/fhir+ndjson");

    assertThat(output).containsKey("Patient");
    assertThat(downloadAll(output.get("Patient"))).contains(PATIENT_ID);
  }

  // -------------------------------------------------------------------------
  // User story 3: enumeration of a never-read type.
  // -------------------------------------------------------------------------

  // A table written straight into the warehouse by another process, which this server has never
  // read, must still appear in an unnarrowed system-level export.
  @Test
  void systemLevelExportIncludesNeverReadType() {
    // Stands in for another process sharing the warehouse: the table appears on disk without the
    // server having created or read it.
    TestDataSetup.copyTestDataToTempDir(warehouseDir.resolve("delta"), NEVER_READ_TYPE);

    final Map<String, List<String>> output =
        exportToCompletion(
            "http://localhost:" + port + "/fhir/$export?_outputFormat=application/fhir+ndjson");

    assertThat(output).containsKey(NEVER_READ_TYPE);
    assertThat(downloadAll(output.get(NEVER_READ_TYPE)))
        .contains("\"resourceType\":\"Organization\"");
  }

  // ---- helpers ----

  /** Creates a Patient, in a table that appears only after the server has started. */
  private void putPatient(@Nonnull final String id) {
    final String patient =
        """
        {
          "resourceType": "Patient",
          "id": "%s",
          "gender": "female"
        }
        """
            .formatted(id);
    webTestClient
        .put()
        .uri("http://localhost:" + port + "/fhir/Patient/" + id)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(patient)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  /** Creates a Group whose only member is the Patient created in setup. */
  private void putGroup() {
    final String group =
        """
        {
          "resourceType": "Group",
          "id": "%s",
          "type": "person",
          "actual": true,
          "member": [ { "entity": { "reference": "Patient/%s" } } ]
        }
        """
            .formatted(GROUP_ID, PATIENT_ID);
    webTestClient
        .put()
        .uri("http://localhost:" + port + "/fhir/Group/" + GROUP_ID)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(group)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  /**
   * Runs an inline ViewDefinition over Patient through {@code $sql-run} with the given narrowing
   * parameters, and returns the ids it projected.
   */
  @SafeVarargs
  @Nonnull
  private List<String> runIds(@Nonnull final Map<String, Object>... narrowing) {
    final List<Map<String, Object>> params = new ArrayList<>();
    params.add(resourcePart("subjectResource", patientView()));
    params.add(simplePart("_format", "valueString", "ndjson"));
    params.addAll(List.of(narrowing));
    final Map<String, Object> body = new LinkedHashMap<>();
    body.put("resourceType", "Parameters");
    body.put("parameter", params);

    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$sql-run")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/x-ndjson")
            .bodyValue(GSON.toJson(body))
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult();
    final String text =
        new String(
            Objects.requireNonNullElse(result.getResponseBodyContent(), new byte[0]),
            StandardCharsets.UTF_8);
    return text.lines()
        .filter(line -> !line.isBlank())
        .map(line -> (String) GSON.fromJson(line, Map.class).get("id"))
        .toList();
  }

  /** A minimal ViewDefinition projecting the id of every Patient. */
  @Nonnull
  private static Map<String, Object> patientView() {
    return Map.of(
        "resourceType",
        "ViewDefinition",
        "status",
        "active",
        "resource",
        "Patient",
        "select",
        List.of(Map.of("column", List.of(Map.of("name", "id", "path", "id")))));
  }

  @Nonnull
  private static Map<String, Object> simplePart(
      @Nonnull final String name, @Nonnull final String valueKey, @Nonnull final Object value) {
    final Map<String, Object> part = new LinkedHashMap<>();
    part.put("name", name);
    part.put(valueKey, value);
    return part;
  }

  @Nonnull
  private static Map<String, Object> referencePart(
      @Nonnull final String name, @Nonnull final String reference) {
    final Map<String, Object> part = new LinkedHashMap<>();
    part.put("name", name);
    part.put("valueReference", Map.of("reference", reference));
    return part;
  }

  @Nonnull
  private static Map<String, Object> resourcePart(
      @Nonnull final String name, @Nonnull final Map<String, Object> resource) {
    final Map<String, Object> part = new LinkedHashMap<>();
    part.put("name", name);
    part.put("resource", resource);
    return part;
  }

  /**
   * Kicks off a bulk export, polls it to completion, and returns the download URLs of the output
   * files grouped by resource type.
   */
  @Nonnull
  private Map<String, List<String>> exportToCompletion(@Nonnull final String uri) {
    final String pollUrl = kickOffRequest(webTestClient, uri);
    final AtomicReference<Map<String, List<String>>> outputs = new AtomicReference<>();
    await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () ->
                doPolling(
                    webTestClient,
                    pollUrl,
                    result -> outputs.set(outputsOf(result.getResponseBody()))));
    return outputs.get();
  }

  /** Extracts the {@code output} parameters of a completion manifest, grouped by resource type. */
  @SuppressWarnings("unchecked")
  @Nonnull
  private static Map<String, List<String>> outputsOf(@Nonnull final String manifest) {
    final Map<String, Object> parsed = GSON.fromJson(manifest, Map.class);
    final List<Map<String, Object>> parameters =
        (List<Map<String, Object>>) parsed.get("parameter");
    final Map<String, List<String>> outputs = new LinkedHashMap<>();
    for (final Map<String, Object> parameter : parameters) {
      if (!"output".equals(parameter.get("name"))) {
        continue;
      }
      final List<Map<String, Object>> parts = (List<Map<String, Object>>) parameter.get("part");
      String type = null;
      String url = null;
      for (final Map<String, Object> part : parts) {
        if ("type".equals(part.get("name"))) {
          type =
              (String) Objects.requireNonNullElse(part.get("valueCode"), part.get("valueString"));
        } else if ("url".equals(part.get("name"))) {
          url = (String) part.get("valueUri");
        }
      }
      if (type != null && url != null) {
        outputs.computeIfAbsent(type, key -> new ArrayList<>()).add(url);
      }
    }
    return outputs;
  }

  /** Downloads and concatenates every file at the given URLs. */
  @Nonnull
  private String downloadAll(@Nonnull final List<String> urls) {
    final StringBuilder content = new StringBuilder();
    for (final String url : urls) {
      final byte[] bytes =
          webTestClient
              .get()
              .uri(url)
              .exchange()
              .expectStatus()
              .isOk()
              .expectBody()
              .returnResult()
              .getResponseBodyContent();
      content.append(
          new String(Objects.requireNonNullElse(bytes, new byte[0]), StandardCharsets.UTF_8));
    }
    return content.toString();
  }
}
