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
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Integration tests for the system-level {@code $sqlquery-export} asynchronous flow: stored and
 * inline queries, request-supplied views, multiple queries, and the manifest shape, lifetime, and
 * cancellation.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
@Import(SqlQueryExportTestConfiguration.class)
class SqlQueryExportProviderIT extends AbstractSqlQueryExportIT {

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
  }

  // -------------------------------------------------------------------------
  // US1 - stored single-query export, full async flow
  // -------------------------------------------------------------------------

  @Test
  void kickOffReturns202WithAcceptedAcknowledgement() {
    final byte[] body =
        kickOff(
                systemLevelUri(),
                storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, null))
            .expectStatus()
            .isAccepted()
            .expectHeader()
            .exists("Content-Location")
            .expectBody()
            .returnResult()
            .getResponseBodyContent();

    final Map<String, Object> ack = parse(body);
    assertThat(ack.get("resourceType")).isEqualTo("Parameters");
    assertThat(findParamValue(ack, "status", "valueCode")).isEqualTo("accepted");
    assertThat(findParamValue(ack, "exportId", "valueString")).isNotNull();
  }

  @Test
  void storedQueryExportsToNdjsonEndToEnd() throws InterruptedException {
    final Map<String, Object> manifest =
        exportToCompletion(
            systemLevelUri(), storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, null));

    assertThat(findParamValue(manifest, "status", "valueCode")).isEqualTo("completed");
    final List<Map<String, Object>> outputs = paramsByName(manifest, "output");
    assertThat(outputs).hasSize(1);
    final String location = partValue(outputs.get(0), "location", "valueUri");
    assertThat(location).contains("$result");

    final String content = download(location);
    final String[] lines = content.trim().split("\n");
    assertThat(lines).hasSize(3);
    assertThat(content)
        .contains("\"family_name\":\"Smith\"")
        .contains("\"family_name\":\"Johnson\"")
        .contains("\"family_name\":\"Williams\"");
  }

  @Test
  void echoesClientTrackingIdInAcknowledgementAndManifest() throws InterruptedException {
    final Map<String, Object> body =
        storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, null);
    addSimpleParam(body, "clientTrackingId", "valueString", "track-1");

    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), body);
    assertThat(findParamValue(manifest, "clientTrackingId", "valueString")).isEqualTo("track-1");
  }

  @Test
  void noQueryAtSystemLevelRejectedWith400() {
    kickOff(systemLevelUri(), emptyParameters()).expectStatus().isBadRequest();
  }

  @Test
  void queryWithBothSourcesRejectedWith400() {
    final Map<String, Object> queryPart =
        queryPartWithReference(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID);
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> parts = (List<Map<String, Object>>) queryPart.get("part");
    parts.add(resourcePart("queryResource", inlineSqlLibrary()));

    kickOff(systemLevelUri(), parametersWith(queryPart)).expectStatus().isBadRequest();
  }

  @Test
  void queryReferenceToMissingLibraryRejectedWith404() {
    kickOff(systemLevelUri(), storedQuery("does-not-exist", null)).expectStatus().isNotFound();
  }

  // -------------------------------------------------------------------------
  // US2 - inline query and request-supplied views
  // -------------------------------------------------------------------------

  @Test
  void inlineQueryWithInlineViewExportsRows() throws InterruptedException {
    final Map<String, Object> body = parametersWith(queryPartWithResource(inlineSqlLibrary()));
    addParam(body, viewPartWithResource(inlineViewDefinition()));

    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), body);
    final List<Map<String, Object>> outputs = paramsByName(manifest, "output");
    assertThat(outputs).hasSize(1);
    final String content = download(partValue(outputs.get(0), "location", "valueUri"));
    assertThat(content).contains("\"family_name\":\"Smith\"");
  }

  @Test
  void inlineQueryWithViewReferenceExportsRows() throws InterruptedException {
    final Map<String, Object> body = parametersWith(queryPartWithResource(inlineSqlLibrary()));
    addParam(
        body,
        viewPartWithReference("ViewDefinition/" + SqlQueryExportTestConfiguration.PATIENT_VIEW_ID));

    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), body);
    assertThat(paramsByName(manifest, "output")).hasSize(1);
  }

  @Test
  void viewPartWithBothSourcesRejectedWith400() {
    final Map<String, Object> viewPart = viewPartWithResource(inlineViewDefinition());
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> parts = (List<Map<String, Object>>) viewPart.get("part");
    parts.add(
        referencePart(
            "viewReference", "ViewDefinition/" + SqlQueryExportTestConfiguration.PATIENT_VIEW_ID));

    final Map<String, Object> body = parametersWith(queryPartWithResource(inlineSqlLibrary()));
    addParam(body, viewPart);
    kickOff(systemLevelUri(), body).expectStatus().isBadRequest();
  }

  @Test
  void viewPartWithNeitherSourceRejectedWith400() {
    final Map<String, Object> viewPart = new LinkedHashMap<>();
    viewPart.put("name", "view");
    viewPart.put("part", new ArrayList<>(List.of(simplePart("name", "valueString", "empty-view"))));

    final Map<String, Object> body = parametersWith(queryPartWithResource(inlineSqlLibrary()));
    addParam(body, viewPart);
    kickOff(systemLevelUri(), body).expectStatus().isBadRequest();
  }

  @Test
  void semanticallyInvalidSuppliedViewRejectedWith422() {
    final Map<String, Object> invalidView = inlineViewDefinition();
    invalidView.put("select", new ArrayList<>()); // Empty select is semantically invalid.

    final Map<String, Object> body = parametersWith(queryPartWithResource(inlineSqlLibrary()));
    addParam(body, viewPartWithResource(invalidView));
    kickOff(systemLevelUri(), body).expectStatus().isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
  }

  // -------------------------------------------------------------------------
  // US3 - multiple queries in one export
  // -------------------------------------------------------------------------

  @Test
  void twoQueriesProduceTwoNamedOutputs() throws InterruptedException {
    final Map<String, Object> body = new LinkedHashMap<>();
    body.put("resourceType", "Parameters");
    final Map<String, Object> q1 =
        queryPartWithReference(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID);
    addPartTo(q1, simplePart("name", "valueString", "people"));
    final Map<String, Object> q2 =
        queryPartWithReference(SqlQueryExportTestConfiguration.OBSERVATION_QUERY_ID);
    body.put("parameter", new ArrayList<>(List.of(q1, q2)));

    final Map<String, Object> manifest = exportToCompletion(systemLevelUri(), body);
    final List<Map<String, Object>> outputs = paramsByName(manifest, "output");
    assertThat(outputs).hasSize(2);
    assertThat(partValue(outputs.get(0), "name", "valueString")).isEqualTo("people");
    assertThat(partValue(outputs.get(1), "name", "valueString"))
        .isEqualTo("observation_weight_query");
  }

  @Test
  void perQueryParameterBindingScopesRows() throws InterruptedException {
    final Map<String, Object> queryPart =
        queryPartWithReference(SqlQueryExportTestConfiguration.PARAM_QUERY_ID);
    final Map<String, Object> parametersResource = new LinkedHashMap<>();
    parametersResource.put("resourceType", "Parameters");
    parametersResource.put(
        "parameter", List.of(Map.of("name", "familyName", "valueString", "Smith")));
    addPartTo(queryPart, resourcePart("parameters", parametersResource));

    final Map<String, Object> manifest =
        exportToCompletion(systemLevelUri(), parametersWith(queryPart));
    final String content =
        download(partValue(paramsByName(manifest, "output").get(0), "location", "valueUri"));
    assertThat(content).contains("\"family_name\":\"Smith\"");
    assertThat(content).doesNotContain("Johnson").doesNotContain("Williams");
  }

  // -------------------------------------------------------------------------
  // US6 - manifest shape, lifetime, and cancellation
  // -------------------------------------------------------------------------

  @Test
  void manifestOmitsCancelUrlAndEstimatedTimeRemaining() throws InterruptedException {
    final Map<String, Object> manifest =
        exportToCompletion(
            systemLevelUri(), storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, null));
    assertThat(findParam(manifest, "cancelUrl")).isNull();
    assertThat(findParam(manifest, "estimatedTimeRemaining")).isNull();
    assertThat(findParam(manifest, "exportStartTime")).isNotNull();
    assertThat(findParam(manifest, "exportDuration")).isNotNull();
  }

  @Test
  void resultUrlSupportsRepeatRetrievalWithExpiresHeader() throws InterruptedException {
    final String resultLocation =
        resultLocationOf(
            systemLevelUri(), storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, null));

    for (int i = 0; i < 2; i++) {
      webTestClient
          .get()
          .uri(resultLocation)
          .header("Accept", "application/fhir+json")
          .exchange()
          .expectStatus()
          .isOk()
          .expectHeader()
          .exists("Expires");
    }
  }

  @Test
  void deleteCancelsInProgressExportThenPollReturns404() {
    final String contentLocation =
        kickOff(
                systemLevelUri(),
                storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, null))
            .expectStatus()
            .isAccepted()
            .returnResult(String.class)
            .getResponseHeaders()
            .getFirst("Content-Location");
    assertThat(contentLocation).isNotNull();

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
  // Request builders specific to this suite
  // -------------------------------------------------------------------------

  @Nonnull
  private Map<String, Object> queryPartWithResource(@Nonnull final Map<String, Object> library) {
    final Map<String, Object> queryParam = new LinkedHashMap<>();
    queryParam.put("name", "query");
    queryParam.put("part", new ArrayList<>(List.of(resourcePart("queryResource", library))));
    return queryParam;
  }

  /** An inline SQLQuery Library referencing the stored Patient ViewDefinition by relative id. */
  @Nonnull
  private Map<String, Object> inlineSqlLibrary() {
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
    final String sql = "SELECT id, family_name FROM patients ORDER BY id";
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

  /** An inline Patient ViewDefinition whose id matches the inline query's relatedArtifact. */
  @Nonnull
  private Map<String, Object> inlineViewDefinition() {
    final Map<String, Object> view = new LinkedHashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("id", SqlQueryExportTestConfiguration.PATIENT_VIEW_ID);
    view.put("name", "supplied_patient_view");
    view.put("resource", "Patient");
    view.put("status", "active");
    view.put(
        "select",
        new ArrayList<>(
            List.of(
                Map.of(
                    "column",
                    List.of(
                        Map.of("name", "id", "path", "id"),
                        Map.of("name", "family_name", "path", "name.first().family"))))));
    return view;
  }

  @Nonnull
  private Map<String, Object> viewPartWithResource(@Nonnull final Map<String, Object> view) {
    final Map<String, Object> viewParam = new LinkedHashMap<>();
    viewParam.put("name", "view");
    viewParam.put("part", new ArrayList<>(List.of(resourcePart("viewResource", view))));
    return viewParam;
  }

  @Nonnull
  private Map<String, Object> viewPartWithReference(@Nonnull final String reference) {
    final Map<String, Object> viewParam = new LinkedHashMap<>();
    viewParam.put("name", "view");
    viewParam.put("part", new ArrayList<>(List.of(referencePart("viewReference", reference))));
    return viewParam;
  }

  @SuppressWarnings("unchecked")
  private void addPartTo(
      @Nonnull final Map<String, Object> param, @Nonnull final Map<String, Object> part) {
    ((List<Map<String, Object>>) param.get("part")).add(part);
  }

  @Nonnull
  private Map<String, Object> simplePart(
      @Nonnull final String name, @Nonnull final String valueKey, @Nonnull final String value) {
    final Map<String, Object> part = new LinkedHashMap<>();
    part.put("name", name);
    part.put(valueKey, value);
    return part;
  }
}
