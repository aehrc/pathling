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

import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.LIBRARY_TYPE_SYSTEM;
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_QUERY_TYPE_CODE;
import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * End-to-end integration test for the {@code $sqlquery-run} operation against stored SQLView {@code
 * Library} resources. Drives the full pipeline - dependency resolution, graph materialisation, and
 * result streaming - for a SQLQuery that composes a SQLView (US1) and for nested and cyclic graphs
 * (US2).
 *
 * <p>Backed by {@link SqlViewTestConfiguration}, which substitutes an in-memory data source holding
 * the ViewDefinitions, SQLViews, and FHIR data the queries resolve against.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
@Import(SqlViewTestConfiguration.class)
class SqlViewRunProviderIT {

  private static final Gson GSON = new Gson();

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @Autowired private FhirContext fhirContext;

  private IParser jsonParser;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
  }

  @BeforeEach
  void setup() {
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            // Nested SQLView graphs plan a deeper Spark query, so allow more than the 5s default.
            .responseTimeout(java.time.Duration.ofSeconds(60))
            .build();
    jsonParser = fhirContext.newJsonParser();
  }

  @Test
  void runsSqlQueryComposingAStoredSqlView() {
    // A SQLQuery that selects from a SQLView (which itself selects from a ViewDefinition) returns
    // the composed rows.
    final Library library =
        sqlQueryLibrary(
            "SELECT id, family_name FROM ap ORDER BY id",
            "ap",
            SqlViewTestConfiguration.libraryUrl(SqlViewTestConfiguration.ACTIVE_PATIENTS_ID));

    final String body = postOk(parametersJson(library));

    final String[] lines = body.trim().split("\n");
    assertThat(lines).hasSize(3);
    assertThat(body)
        .contains("\"id\":\"p1\"")
        .contains("\"family_name\":\"Smith\"")
        .contains("\"id\":\"p2\"")
        .contains("\"family_name\":\"Johnson\"")
        .contains("\"id\":\"p3\"")
        .contains("\"family_name\":\"Williams\"");
  }

  @Test
  void runsSqlQueryComposingANestedSqlViewChain() {
    // SQLQuery -> refined-patients (SQLView) -> active-patients (SQLView) -> patient-view (VD).
    // refined-patients filters out Johnson, so two rows remain.
    final Library library =
        sqlQueryLibrary(
            "SELECT id, family_name FROM rp ORDER BY id",
            "rp",
            SqlViewTestConfiguration.libraryUrl(SqlViewTestConfiguration.REFINED_PATIENTS_ID));

    final String body = postOk(parametersJson(library));

    final String[] lines = body.trim().split("\n");
    assertThat(lines).hasSize(2);
    assertThat(body).contains("Smith").contains("Williams").doesNotContain("Johnson");
  }

  @Test
  void runsSqlQueryOverADiamondOfSqlViews() {
    // left and right both depend on the shared SQLView; the join returns one row per patient,
    // confirming both arms observe the same shared materialisation.
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_QUERY_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(
        "SELECT l.id, l.family_name FROM l JOIN r ON l.id = r.id ORDER BY l.id"
            .getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel("l")
            .setResource(
                SqlViewTestConfiguration.libraryUrl(SqlViewTestConfiguration.LEFT_PATIENTS_ID)));
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel("r")
            .setResource(
                SqlViewTestConfiguration.libraryUrl(SqlViewTestConfiguration.RIGHT_PATIENTS_ID)));

    final String body = postOk(parametersJson(library));

    final String[] lines = body.trim().split("\n");
    assertThat(lines).hasSize(3);
    assertThat(body).contains("Smith").contains("Johnson").contains("Williams");
  }

  @Test
  void runsStoredSqlViewAtInstanceLevel() {
    // A stored SQLView supplied as the top-level resource of the instance-level operation executes
    // as a parameter-less query and returns its rows.
    final String body =
        getOk(
            "/fhir/Library/"
                + SqlViewTestConfiguration.ACTIVE_PATIENTS_ID
                + "/$sqlquery-run?_format=ndjson");

    final String[] lines = body.trim().split("\n");
    assertThat(lines).hasSize(3);
    assertThat(body).contains("Smith").contains("Johnson").contains("Williams");
  }

  @Test
  void runsSqlViewByQueryReferenceAtSystemLevel() {
    // A SQLView supplied as a top-level queryReference at the system level executes and returns its
    // rows.
    final String body =
        postOk(
            queryReferenceParametersJson("Library/" + SqlViewTestConfiguration.ACTIVE_PATIENTS_ID));

    final String[] lines = body.trim().split("\n");
    assertThat(lines).hasSize(3);
    assertThat(body).contains("Smith").contains("Johnson").contains("Williams");
  }

  @Test
  void returnsErrorWhenReferencedSqlViewDoesNotExist() {
    final Library library =
        sqlQueryLibrary(
            "SELECT id FROM missing",
            "missing",
            SqlViewTestConfiguration.libraryUrl("does-not-exist"));

    // The error names the failing label and reference so the client can act on it.
    final String body = postExpect4xx(parametersJson(library));
    assertThat(body).contains("missing").contains("does-not-exist");
  }

  @Test
  void rejectsCyclicSqlViewGraphWith400() {
    // cycle-a -> cycle-b -> cycle-a must be rejected before any SQL executes.
    final Library library =
        sqlQueryLibrary(
            "SELECT * FROM a",
            "a",
            SqlViewTestConfiguration.libraryUrl(SqlViewTestConfiguration.CYCLE_A_ID));

    final String body = postExpect4xx(parametersJson(library));
    assertThat(body).containsIgnoringCase("cycl");
  }

  @Nonnull
  private String postExpect4xx(@Nonnull final String body) {
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$sqlquery-run")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .is4xxClientError()
            .expectBody()
            .returnResult();
    final byte[] payload = result.getResponseBodyContent();
    return payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
  }

  @Nonnull
  private String getOk(@Nonnull final String path) {
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .get()
            .uri("http://localhost:" + port + path)
            .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult();
    return new String(
        Objects.requireNonNull(result.getResponseBodyContent()), StandardCharsets.UTF_8);
  }

  @Nonnull
  private String queryReferenceParametersJson(@Nonnull final String reference) {
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final List<Map<String, Object>> parameterList = new ArrayList<>();

    final Map<String, Object> queryReferenceParam = new LinkedHashMap<>();
    queryReferenceParam.put("name", "queryReference");
    queryReferenceParam.put("valueReference", Map.of("reference", reference));
    parameterList.add(queryReferenceParam);

    final Map<String, Object> formatParam = new LinkedHashMap<>();
    formatParam.put("name", "_format");
    formatParam.put("valueString", SqlQueryOutputFormat.NDJSON.getCode());
    parameterList.add(formatParam);

    parameters.put("parameter", parameterList);
    return GSON.toJson(parameters);
  }

  @Nonnull
  private String postOk(@Nonnull final String body) {
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$sqlquery-run")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentTypeCompatibleWith(
                MediaType.parseMediaType(SqlQueryOutputFormat.NDJSON.getContentType()))
            .expectBody()
            .returnResult();
    return new String(
        Objects.requireNonNull(result.getResponseBodyContent()), StandardCharsets.UTF_8);
  }

  @Nonnull
  private Library sqlQueryLibrary(
      @Nonnull final String sql, @Nonnull final String label, @Nonnull final String resource) {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_QUERY_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(sql.getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel(label)
            .setResource(resource));
    return library;
  }

  @Nonnull
  private String parametersJson(@Nonnull final Library library) {
    final String libraryJson = jsonParser.encodeResourceToString(library);
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final List<Map<String, Object>> parameterList = new ArrayList<>();

    final Map<String, Object> queryResourceParam = new LinkedHashMap<>();
    queryResourceParam.put("name", "queryResource");
    queryResourceParam.put("resource", GSON.fromJson(libraryJson, Map.class));
    parameterList.add(queryResourceParam);

    final Map<String, Object> formatParam = new LinkedHashMap<>();
    formatParam.put("name", "_format");
    formatParam.put("valueString", SqlQueryOutputFormat.NDJSON.getCode());
    parameterList.add(formatParam);

    parameters.put("parameter", parameterList);
    return GSON.toJson(parameters);
  }
}
