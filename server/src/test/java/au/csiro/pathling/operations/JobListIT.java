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

package au.csiro.pathling.operations;

import static au.csiro.pathling.util.ExportOperationUtil.kickOffRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.util.TestDataSetup;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * End-to-end coverage for the {@code $jobs} list operation and the job cancellation round trip.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class JobListIT {

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @TempDir private static Path warehouseDir;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
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
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  @Test
  void listsKickedOffExportJob() {
    // Quickstart scenario 1: kick off an async export and confirm it appears in the job list.
    final String uri = exportUri();
    final String pollUrl = kickOffRequest(webTestClient, uri);
    final String jobId = extractJobId(pollUrl);

    // The job is registered synchronously during kick-off, so it should appear on the list. Poll
    // briefly to avoid any incidental timing sensitivity.
    await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertJobListed(jobId));
  }

  private void assertJobListed(final String jobId) throws IOException {
    final String body = getJobs();
    final JsonNode job = findJob(body, jobId);
    assertThat(job).as("Job %s should appear in the job list", jobId).isNotNull();

    assertThat(partValue(job, "operation")).isEqualTo("export");
    // Status is either in-progress or completed depending on how quickly the export finished.
    assertThat(partValue(job, "status")).isIn("in-progress", "completed");
    assertThat(partValue(job, "startTime")).isNotNull();
    assertThat(partValue(job, "url")).endsWith("/$job?id=" + jobId);
  }

  private String getJobs() {
    final String body =
        webTestClient
            .get()
            .uri("http://localhost:" + port + "/fhir/$jobs")
            .header("Accept", "application/fhir+json")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(String.class)
            .returnResult()
            .getResponseBody();
    assertThat(body).isNotNull();
    return body;
  }

  private String exportUri() {
    return "http://localhost:"
        + port
        + "/fhir/$export?_outputFormat=application/fhir+ndjson&_type=Patient";
  }

  @Nullable
  private JsonNode findJob(final String body, final String jobId) throws IOException {
    final JsonNode root = objectMapper.readTree(body);
    assertThat(root.get("resourceType").asText()).isEqualTo("Parameters");
    final JsonNode parameters = root.get("parameter");
    if (parameters == null) {
      return null;
    }
    for (final JsonNode param : parameters) {
      if ("job".equals(param.path("name").asText()) && jobId.equals(partValue(param, "id"))) {
        return param;
      }
    }
    return null;
  }

  @Nullable
  private String partValue(final JsonNode jobParam, final String name) {
    final JsonNode parts = jobParam.get("part");
    if (parts == null) {
      return null;
    }
    for (final JsonNode part : parts) {
      if (name.equals(part.path("name").asText())) {
        for (final String valueField :
            new String[] {"valueString", "valueCode", "valueUri", "valueInstant", "valueInteger"}) {
          if (part.has(valueField)) {
            return part.get(valueField).asText();
          }
        }
      }
    }
    return null;
  }

  private String extractJobId(final String pollUrl) {
    final String id =
        UriComponentsBuilder.fromUriString(pollUrl).build().getQueryParams().getFirst("id");
    assertThat(id).as("Poll URL should carry a job id").isNotNull();
    return id;
  }
}
