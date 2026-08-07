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

import au.csiro.pathling.io.DynamicDeltaSource;
import au.csiro.pathling.library.io.sink.FileInformation;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.util.ExportOperationUtil;
import au.csiro.pathling.util.TestDataSetup;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * End-to-end coverage for the system-level $export path that narrows the output by the _since
 * parameter. The standard test data carries no meta.lastUpdated values, so this test stamps
 * distinct timestamps onto known partitions of the Patient table before exporting, and then asserts
 * the exact set of resources that crosses the cut-off - including that resources with no
 * lastUpdated value are always exported.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class ExportSinceFilterIT {

  /** The _since value used in the export request, between the two stamped timestamps. */
  private static final String SINCE_CUTOFF = "2020-01-01T00:00:00Z";

  /** Stamped onto the first partition: well before the cut-off, in any time zone. */
  private static final String BEFORE_CUTOFF_TIMESTAMP = "2015-06-15 00:00:00";

  /** Stamped onto the second partition: well after the cut-off, in any time zone. */
  private static final String AFTER_CUTOFF_TIMESTAMP = "2025-06-15 00:00:00";

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @TempDir private static Path warehouseDir;

  @Autowired private FhirContext fhirContext;

  @Autowired private SparkSession sparkSession;

  @Autowired private QueryableDataSource deltaLake;

  private IParser parser;

  // The ID partitions are computed once for the class. Static state is safe here because failsafe
  // runs each integration test class in its own JVM.
  private static Set<String> beforeCutoffIds;
  private static Set<String> afterCutoffIds;
  private static Set<String> nullLastUpdatedIds;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    // Only the Patient table is needed; the test partitions it by lastUpdated value.
    TestDataSetup.copyTestDataToTempDir(warehouseDir.resolve("delta"), "Patient");
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  @BeforeEach
  void stampLastUpdatedValues() {
    // The stamping runs once for the class; a static guard is used because the autowired Spark
    // session is not available in a static @BeforeAll method.
    if (beforeCutoffIds != null) {
      return;
    }
    final String tablePath =
        "file://" + warehouseDir.resolve("delta").resolve("Patient.parquet").toAbsolutePath();

    // Partition the patient IDs deterministically into three groups: updated before the cut-off,
    // updated after the cut-off, and never stamped (lastUpdated remains null).
    final List<String> ids =
        sparkSession
            .read()
            .format("delta")
            .load(tablePath)
            .select("id")
            .orderBy("id")
            .as(Encoders.STRING())
            .collectAsList();
    assertThat(ids).hasSizeGreaterThanOrEqualTo(3);
    final int third = ids.size() / 3;
    beforeCutoffIds = new HashSet<>(ids.subList(0, third));
    afterCutoffIds = new HashSet<>(ids.subList(third, 2 * third));
    nullLastUpdatedIds = new HashSet<>(ids.subList(2 * third, ids.size()));

    stampLastUpdated(tablePath, beforeCutoffIds, BEFORE_CUTOFF_TIMESTAMP);
    stampLastUpdated(tablePath, afterCutoffIds, AFTER_CUTOFF_TIMESTAMP);

    // The table was modified outside the server's own write path, so the server's dataset for the
    // type is refreshed to make sure it observes the stamped timestamps.
    ((DynamicDeltaSource) deltaLake).refresh("Patient");
  }

  @BeforeEach
  void setup() {
    parser = fhirContext.newJsonParser();
    webTestClient =
        webTestClient
            .mutate()
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

  /** Updates meta.lastUpdated on the Delta table for the given resource IDs. */
  private void stampLastUpdated(
      final String tablePath, final Set<String> ids, final String timestamp) {
    final String idList = ids.stream().map(id -> "'" + id + "'").collect(Collectors.joining(","));
    sparkSession.sql(
        "UPDATE delta.`%s` SET meta.lastUpdated = timestamp'%s' WHERE id IN (%s)"
            .formatted(tablePath, timestamp, idList));
  }

  @Test
  void sinceExportExcludesResourcesUpdatedBeforeCutoff() {
    // A system-level export with _since must export exactly the resources whose lastUpdated is on
    // or after the cut-off, plus those with no lastUpdated value at all. The equality assertion
    // below is self-controlling: if the stamped timestamps had not taken effect, every patient
    // would be exported and the comparison would fail.
    final String uri =
        "http://localhost:"
            + port
            + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since="
            + SINCE_CUTOFF;
    final String pollUrl = kickOffRequest(webTestClient, uri);
    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () ->
                doPolling(
                    webTestClient,
                    pollUrl,
                    result -> {
                      assertNotNull(result.getResponseBody());
                      assertSinceFilteredResult(result.getResponseBody());
                    }));
  }

  /**
   * Asserts that the completed export contains exactly the patients stamped after the cut-off plus
   * the patients that carry no lastUpdated value.
   */
  private void assertSinceFilteredResult(final String responseBody) {
    final JsonNode node;
    try {
      node = new ObjectMapper().readTree(responseBody);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    assertThat(node.get("resourceType").asText()).isEqualTo("Parameters");
    final JsonNode parameters = node.get("parameter");
    assertThat(parameters).isNotNull();

    final List<FileInformation> fileInfos = extractFileInfos(parameters);
    assertThat(fileInfos).isNotEmpty();
    assertThat(fileInfos).allMatch(fileInfo -> "Patient".equals(fileInfo.fhirResourceType()));

    // Download every output file and collect the IDs of the exported patients.
    final Set<String> exportedIds = new HashSet<>();
    for (final FileInformation fileInfo : fileInfos) {
      final EntityExchangeResult<byte[]> result =
          webTestClient
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
      resources.forEach(resource -> exportedIds.add(resource.getIdPart()));
    }

    // The export must contain exactly the resources at or after the cut-off, plus the resources
    // with no lastUpdated value, and none of the resources updated before the cut-off.
    final Set<String> expectedIds = new HashSet<>(afterCutoffIds);
    expectedIds.addAll(nullLastUpdatedIds);
    assertThat(exportedIds).containsExactlyInAnyOrderElementsOf(expectedIds);
    assertThat(exportedIds).doesNotContainAnyElementsOf(beforeCutoffIds);
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
}
