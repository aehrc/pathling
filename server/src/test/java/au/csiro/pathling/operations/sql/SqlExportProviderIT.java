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

package au.csiro.pathling.operations.sql;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
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
 * End-to-end integration tests for the system-level {@code $sql-export} operation: the asynchronous
 * flow over a job mixing subject kinds, the manifest it produces, cancellation, and the kick-off
 * rejections that must be answered synchronously.
 *
 * <p>Backed by {@link SqlRunTestConfiguration}, which supplies the stored ViewDefinition, SQLQuery
 * and SQLView subjects together with the Patient data they project.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
@Import(SqlRunTestConfiguration.class)
class SqlExportProviderIT extends AbstractAsyncExportIT {

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
  }

  // -------------------------------------------------------------------------
  // A mixed job, end to end.
  // -------------------------------------------------------------------------

  @Test
  void kickOffReturns202WithAnAcceptedAcknowledgement() {
    final byte[] body =
        kickOff(systemLevelUri(), mixedJob(null))
            .expectStatus()
            .isAccepted()
            .expectHeader()
            .exists("Content-Location")
            .expectBody()
            .returnResult()
            .getResponseBodyContent();

    final Map<String, Object> acknowledgement = parse(body);
    assertThat(acknowledgement.get("resourceType")).isEqualTo("Parameters");
    assertThat(findParamValue(acknowledgement, "status", "valueCode")).isEqualTo("accepted");
    assertThat(findParamValue(acknowledgement, "exportId", "valueString")).isNotNull();
  }

  @Test
  void exportsAMixedJobWithOneOutputPerSubject() throws InterruptedException {
    // A ViewDefinition and a SQLQuery in one job: two outputs, correlated by the names the request
    // gave them, each downloadable in the requested format.
    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), mixedJob("track-1"));

    assertThat(findParamValue(manifest, "status", "valueCode")).isEqualTo("completed");
    assertThat(findParamValue(manifest, "clientTrackingId", "valueString")).isEqualTo("track-1");

    final List<Map<String, Object>> outputs = paramsByName(manifest, "output");
    assertThat(outputs).hasSize(2);
    assertThat(outputs)
        .extracting(o -> partValue(o, "name", "valueString"))
        .containsExactlyInAnyOrder("demographics", "johnsons");

    final Map<String, Object> demographics = outputNamed(outputs, "demographics");
    final String demographicsContent = downloadAll(demographics);
    assertThat(demographicsContent).contains("Smith").contains("Johnson").contains("Williams");

    // The SQL subject's bound parameter narrowed its result to the one matching patient.
    final String johnsonsContent = downloadAll(outputNamed(outputs, "johnsons"));
    assertThat(johnsonsContent).contains("Johnson").doesNotContain("Smith");
  }

  // An inline query whose table source the server does not hold, satisfied by a job-wide context
  // entry. Without this the query fails as an undeclared table (aehrc/pathling#2663).
  @Test
  void exportsAnInlineQueryAgainstAnInlineContextEntry() throws InterruptedException {
    final Map<String, Object> body =
        parameters(
            subject(nameOf("ad_hoc"), resourcePart("subjectResource", inlineQueryOverAdHocView())),
            resourcePart("context", adHocView()));

    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), body);

    assertThat(findParamValue(manifest, "status", "valueCode")).isEqualTo("completed");
    final List<Map<String, Object>> outputs = paramsByName(manifest, "output");
    assertThat(outputs).hasSize(1);
    final String content = downloadAll(outputNamed(outputs, "ad_hoc"));
    assertThat(content).contains("Smith").contains("Johnson").contains("Williams");
  }

  @Test
  void writesTheRequestedFormat() throws InterruptedException {
    final Map<String, Object> body = mixedJob(null);
    addParam(body, simpleParam("_format", "valueString", "csv"));

    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), body);
    final List<Map<String, Object>> outputs = paramsByName(manifest, "output");

    assertThat(findParamValue(manifest, "_format", "valueCode")).isEqualTo("csv");
    final String content = downloadAll(outputNamed(outputs, "demographics"));
    assertThat(content).contains("id,family_name");
  }

  // The plain-JSON manifest is what a client that sends no Accept header receives, and it must
  // name every file the FHIR form names: a location it omits is a file nothing can reach.
  @Test
  void thePlainJsonManifestNamesEveryFileTheFhirFormDoes() throws InterruptedException {
    final String resultLocation = resultLocationOf(systemLevelUri(), mixedJob(null));

    final Map<String, Object> fhirManifest =
        parse(
            webTestClient
                .get()
                .uri(resultLocation)
                .header("Accept", "application/fhir+json")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .returnResult()
                .getResponseBodyContent());
    final Map<String, Integer> fhirCounts = new java.util.LinkedHashMap<>();
    for (final Map<String, Object> output : paramsByName(fhirManifest, "output")) {
      fhirCounts.put(
          partValue(output, "name", "valueString"),
          partValues(output, "location", "valueUri").size());
    }
    assertThat(fhirCounts).hasSize(2);

    final Map<String, Object> plainManifest =
        parse(
            webTestClient
                .get()
                .uri(resultLocation)
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .returnResult()
                .getResponseBodyContent());

    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> plainOutputs =
        (List<Map<String, Object>>) plainManifest.get("output");
    assertThat(plainOutputs).hasSize(2);
    for (final Map<String, Object> output : plainOutputs) {
      final String name = (String) output.get("name");
      final Object location = output.get("location");
      final int count = location instanceof final List<?> list ? list.size() : 1;
      assertThat(count)
          .as("The plain-JSON manifest must name every file of output '%s'", name)
          .isEqualTo(fhirCounts.get(name));
    }
  }

  @Test
  void listsTheJobUnderTheSqlExportOperation() throws InterruptedException {
    exportToCompletion(systemLevelUri(), mixedJob(null));

    final byte[] body =
        webTestClient
            .get()
            .uri("http://localhost:" + port + "/fhir/$jobs")
            .header("Accept", "application/fhir+json")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();

    final String listing = new String(body == null ? new byte[0] : body, StandardCharsets.UTF_8);
    assertThat(listing).contains("sql-export");
  }

  // -------------------------------------------------------------------------
  // Cancellation.
  // -------------------------------------------------------------------------

  @Test
  void cancellingAJobMakesItsStatusUrlReturn404() {
    final String contentLocation = contentLocationOf(systemLevelUri(), mixedJob(null));

    webTestClient.delete().uri(contentLocation).exchange().expectStatus().isAccepted();

    webTestClient
        .get()
        .uri(contentLocation)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isNotFound();
  }

  // -------------------------------------------------------------------------
  // Kick-off rejections, all answered synchronously.
  // -------------------------------------------------------------------------

  @Test
  void rejectsAJobWithNoSubject() {
    expectRejection(parameters(), 400, "subject");
  }

  @Test
  void rejectsTwoSubjectsWithTheSameName() {
    expectRejection(
        parameters(
            subject(nameOf("same"), canonicalOf(SqlRunTestConfiguration.PATIENT_VIEW_URL)),
            subject(
                nameOf("same"),
                canonicalOf(
                    SqlRunTestConfiguration.libraryUrl(SqlRunTestConfiguration.ALL_PATIENTS_ID)))),
        400,
        "subject");
  }

  @Test
  void rejectsParametersOnAViewDefinitionSubject() {
    expectRejection(
        parameters(
            subject(
                canonicalOf(SqlRunTestConfiguration.PATIENT_VIEW_URL),
                resourcePart("parameters", Map.of("resourceType", "Parameters")))),
        400,
        "parameters");
  }

  @Test
  void rejectsTheJsonFormatAndTheLimitParameter() {
    expectRejection(
        parameters(
            subject(canonicalOf(SqlRunTestConfiguration.PATIENT_VIEW_URL)),
            simpleParam("_format", "valueString", "json")),
        400,
        "_format");

    expectRejection(
        parameters(
            subject(canonicalOf(SqlRunTestConfiguration.PATIENT_VIEW_URL)),
            simpleParam("_limit", "valueInteger", 10)),
        400,
        "_limit");
  }

  @Test
  void rejectsAMissingRespondAsyncPreference() {
    webTestClient
        .post()
        .uri(systemLevelUri())
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(
            gson.toJson(parameters(subject(canonicalOf(SqlRunTestConfiguration.PATIENT_VIEW_URL)))))
        .exchange()
        .expectStatus()
        .isBadRequest();
  }

  @Test
  void reportsAnUnresolvableSubjectAsA404EvenWhenAnotherResolves() {
    final byte[] body =
        kickOff(
                systemLevelUri(),
                parameters(
                    subject(canonicalOf(SqlRunTestConfiguration.PATIENT_VIEW_URL)),
                    subject(canonicalOf("https://example.org/nothing-here"))))
            .expectStatus()
            .isNotFound()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();

    final Map<String, Object> outcome = parse(body);
    assertThat(outcome.get("resourceType")).isEqualTo("OperationOutcome");
    assertThat((List<?>) outcome.get("issue")).hasSize(1);
  }

  // ---- helpers ----

  /** Builds a job mixing a ViewDefinition subject and a parameter-bound SQLQuery subject. */
  @Nonnull
  private Map<String, Object> mixedJob(final String clientTrackingId) {
    final Map<String, Object> bindings =
        Map.of(
            "resourceType",
            "Parameters",
            "parameter",
            List.of(Map.of("name", "family", "valueString", "Johnson")));

    final Map<String, Object> body =
        parameters(
            subject(nameOf("demographics"), canonicalOf(SqlRunTestConfiguration.PATIENT_VIEW_URL)),
            subject(
                nameOf("johnsons"),
                referencePart(
                    "subjectReference", "Library/" + SqlRunTestConfiguration.PATIENTS_BY_FAMILY_ID),
                resourcePart("parameters", bindings)));
    if (clientTrackingId != null) {
      addParam(body, simpleParam("clientTrackingId", "valueString", clientTrackingId));
    }
    return body;
  }

  /** A ViewDefinition the server does not hold, supplied as a context entry. */
  @Nonnull
  private Map<String, Object> adHocView() {
    return Map.of(
        "resourceType",
        "ViewDefinition",
        "url",
        "https://pathling.csiro.au/test/ViewDefinition/ExportAdHoc",
        "name",
        "export_ad_hoc",
        "status",
        "active",
        "resource",
        "Patient",
        "select",
        List.of(
            Map.of(
                "column",
                List.of(
                    Map.of("name", "id", "path", "id"),
                    Map.of("name", "family_name", "path", "name.first().family")))));
  }

  /** An inline SQLQuery whose only table source is the ad-hoc view above. */
  @Nonnull
  private Map<String, Object> inlineQueryOverAdHocView() {
    final String sql =
        java.util.Base64.getEncoder()
            .encodeToString(
                "SELECT id, family_name FROM adh ORDER BY id"
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    return Map.of(
        "resourceType",
        "Library",
        "status",
        "active",
        "type",
        Map.of(
            "coding",
            List.of(
                Map.of(
                    "system",
                    au.csiro.pathling.operations.sqlquery.SqlLibraryParser.LIBRARY_TYPE_SYSTEM,
                    "code",
                    au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_QUERY_TYPE_CODE))),
        "relatedArtifact",
        List.of(
            Map.of(
                "type",
                "depends-on",
                "label",
                "adh",
                "resource",
                "https://pathling.csiro.au/test/ViewDefinition/ExportAdHoc")),
        "content",
        List.of(Map.of("contentType", "application/sql", "data", sql)));
  }

  @Nonnull
  private Map<String, Object> nameOf(@Nonnull final String name) {
    return simpleParam("name", "valueString", name);
  }

  @Nonnull
  private Map<String, Object> canonicalOf(@Nonnull final String url) {
    return simpleParam("subjectCanonical", "valueCanonical", url);
  }

  @Nonnull
  private static Map<String, Object> outputNamed(
      @Nonnull final List<Map<String, Object>> outputs, @Nonnull final String name) {
    return outputs.stream()
        .filter(o -> name.equals(partValue(o, "name", "valueString")))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No output named " + name));
  }

  /** Kicks off a request expected to be rejected, asserting the status and the named parameter. */
  private void expectRejection(
      @Nonnull final Map<String, Object> body, final int status, @Nonnull final String expression) {
    final byte[] content =
        kickOff(systemLevelUri(), body)
            .expectStatus()
            .isEqualTo(status)
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    assertThat(new String(content == null ? new byte[0] : content, StandardCharsets.UTF_8))
        .contains(expression);
  }
}
