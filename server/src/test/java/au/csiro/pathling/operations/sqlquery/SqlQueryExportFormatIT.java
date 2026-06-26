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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Integration tests for {@code $sqlquery-export} output formats, the CSV header toggle, filter
 * scoping, the synchronous rejections (unsupported {@code _format} and {@code source}), and the
 * all-or-nothing multi-query failure behaviour.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
@Import(SqlQueryExportTestConfiguration.class)
class SqlQueryExportFormatIT extends AbstractSqlQueryExportIT {

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
  }

  // -------------------------------------------------------------------------
  // Formats and CSV header
  // -------------------------------------------------------------------------

  @Test
  void defaultFormatIsNdjson() throws InterruptedException {
    final Map<String, Object> manifest =
        exportToCompletion(
            systemLevelUri(), storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, null));
    final String location =
        partValue(paramsByName(manifest, "output").get(0), "location", "valueUri");
    assertThat(location).endsWith(".ndjson");
    assertThat(findParamValue(manifest, "_format", "valueCode")).isEqualTo("ndjson");
  }

  @Test
  void csvFormatIncludesHeaderByDefault() throws InterruptedException {
    final Map<String, Object> manifest =
        exportToCompletion(
            systemLevelUri(), storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, "csv"));
    final String location =
        partValue(paramsByName(manifest, "output").get(0), "location", "valueUri");
    final String content = download(location);
    assertThat(content).startsWith("id,family_name");
  }

  @Test
  void csvFormatOmitsHeaderWhenHeaderFalse() throws InterruptedException {
    final Map<String, Object> body =
        storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, "csv");
    addSimpleParam(body, "header", "valueBoolean", Boolean.FALSE);

    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), body);
    final String content =
        download(partValue(paramsByName(manifest, "output").get(0), "location", "valueUri"));
    assertThat(content).doesNotContain("id,family_name");
    assertThat(content).contains("Smith");
  }

  @Test
  void parquetFormatProducesParquetFiles() throws InterruptedException {
    final Map<String, Object> manifest =
        exportToCompletion(
            systemLevelUri(),
            storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, "parquet"));
    final String location =
        partValue(paramsByName(manifest, "output").get(0), "location", "valueUri");
    assertThat(location).endsWith(".parquet");
    assertThat(findParamValue(manifest, "_format", "valueCode")).isEqualTo("parquet");
  }

  // -------------------------------------------------------------------------
  // Synchronous rejections
  // -------------------------------------------------------------------------

  @Test
  void jsonFormatRejectedAtKickOff() {
    kickOff(systemLevelUri(), storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, "json"))
        .expectStatus()
        .isBadRequest();
  }

  @Test
  void fhirFormatRejectedAtKickOff() {
    kickOff(systemLevelUri(), storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, "fhir"))
        .expectStatus()
        .isBadRequest();
  }

  @Test
  void sourceParameterRejectedAtKickOff() {
    final Map<String, Object> body =
        storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, null);
    addSimpleParam(body, "source", "valueString", "s3://bucket/data");

    final byte[] outcome =
        kickOff(systemLevelUri(), body)
            .expectStatus()
            .isBadRequest()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    assertThat(new String(outcome == null ? new byte[0] : outcome, StandardCharsets.UTF_8))
        .contains("source");
  }

  // -------------------------------------------------------------------------
  // Filtering
  // -------------------------------------------------------------------------

  @Test
  void patientFilterScopesExportedRows() throws InterruptedException {
    final Map<String, Object> body =
        storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, null);
    addParam(body, referencePart("patient", "Patient/p1"));

    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), body);
    final String content =
        download(partValue(paramsByName(manifest, "output").get(0), "location", "valueUri"));
    assertThat(content).contains("\"id\":\"p1\"");
    assertThat(content).doesNotContain("\"id\":\"p2\"").doesNotContain("\"id\":\"p3\"");
  }

  @Test
  void groupFilterScopesExportedRows() throws InterruptedException {
    final Map<String, Object> body =
        storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, null);
    // The group's sole member is p1, so only p1 should be exported.
    addParam(body, referencePart("group", "Group/" + SqlQueryExportTestConfiguration.GROUP_ID));

    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), body);
    final String content =
        download(partValue(paramsByName(manifest, "output").get(0), "location", "valueUri"));
    assertThat(content).contains("\"id\":\"p1\"");
    assertThat(content).doesNotContain("\"id\":\"p2\"").doesNotContain("\"id\":\"p3\"");
  }

  @Test
  void sinceFilterScopesExportedRows() throws InterruptedException {
    final Map<String, Object> body =
        storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, null);
    // p1 was last updated in 2020; p2 and p3 in 2025. A _since of 2023 excludes p1.
    addSimpleParam(body, "_since", "valueInstant", "2023-01-01T00:00:00.000Z");

    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), body);
    final String content =
        download(partValue(paramsByName(manifest, "output").get(0), "location", "valueUri"));
    assertThat(content).doesNotContain("\"id\":\"p1\"");
    assertThat(content).contains("\"id\":\"p2\"").contains("\"id\":\"p3\"");
  }

  // -------------------------------------------------------------------------
  // All-or-nothing multi-query failure
  // -------------------------------------------------------------------------

  @Test
  void failingQueryFailsTheWholeExportWithNoManifest() throws InterruptedException {
    final Map<String, Object> body = new LinkedHashMap<>();
    body.put("resourceType", "Parameters");
    final Map<String, Object> goodQuery =
        queryPartWithReference(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID);
    final Map<String, Object> badQuery = new LinkedHashMap<>();
    badQuery.put("name", "query");
    badQuery.put("part", new ArrayList<>(List.of(resourcePart("queryResource", failingLibrary()))));
    body.put("parameter", new ArrayList<>(List.of(goodQuery, badQuery)));

    final byte[] result =
        resultOfFailedExport(systemLevelUri(), body)
            .expectStatus()
            .value(status -> assertThat(status).isGreaterThanOrEqualTo(400))
            .expectBody()
            .returnResult()
            .getResponseBodyContent();

    final Map<String, Object> parsed = parse(result);
    // The failure is reported as an OperationOutcome, not a completion manifest.
    assertThat(parsed.get("resourceType")).isEqualTo("OperationOutcome");
    assertThat(paramsByName(parsed, "output")).isEmpty();
  }

  /** An inline SQLQuery Library that parses but fails at execution (unresolved column). */
  @Nonnull
  private Map<String, Object> failingLibrary() {
    final Map<String, Object> library = new LinkedHashMap<>();
    library.put("resourceType", "Library");
    library.put("status", "active");
    library.put(
        "type",
        Map.of(
            "coding",
            List.of(
                Map.of(
                    "system",
                    SqlLibraryParser.LIBRARY_TYPE_SYSTEM,
                    "code",
                    SqlLibraryParser.SQL_QUERY_TYPE_CODE))));
    final String sql = "SELECT nonexistent_column FROM patients";
    library.put(
        "content",
        List.of(
            Map.of(
                "contentType",
                "application/sql",
                "data",
                java.util.Base64.getEncoder()
                    .encodeToString(sql.getBytes(StandardCharsets.UTF_8)))));
    library.put(
        "relatedArtifact",
        List.of(
            Map.of(
                "type",
                "depends-on",
                "label",
                "patients",
                "resource",
                "ViewDefinition/" + SqlQueryExportTestConfiguration.PATIENT_VIEW_ID)));
    return library;
  }
}
