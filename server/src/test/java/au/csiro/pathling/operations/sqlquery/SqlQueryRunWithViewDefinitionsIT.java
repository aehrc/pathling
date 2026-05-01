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

import static au.csiro.pathling.operations.sqlquery.SqlQueryLibraryParser.LIBRARY_TYPE_CODE;
import static au.csiro.pathling.operations.sqlquery.SqlQueryLibraryParser.LIBRARY_TYPE_SYSTEM;
import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
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
 * End-to-end integration test for the {@code $sqlquery-run} operation against a referenced {@link
 * au.csiro.pathling.encoders.ViewDefinitionResource}. Drives the full pipeline — Library parsing,
 * ViewDefinition lookup, FhirView execution, request-scoped temp-view registration, SQL execution,
 * and result streaming — using SQL that names the registered view.
 *
 * <p>Backed by {@link SqlQueryViewDefinitionTestConfiguration} which substitutes an in-memory
 * {@link au.csiro.pathling.util.CustomObjectDataSource} for the production Delta Lake source so the
 * pre-loaded ViewDefinition is visible at startup, sidestepping the table-list caching that
 * disables the equivalent {@code ViewDefinitionRunProvider} instance-level tests.
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
@Import(SqlQueryViewDefinitionTestConfiguration.class)
class SqlQueryRunWithViewDefinitionsIT {

  private static final Gson GSON = new Gson();

  private static final String VIEW_REFERENCE =
      "ViewDefinition/" + SqlQueryViewDefinitionTestConfiguration.PATIENT_VIEW_ID;

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
            .build();
    jsonParser = fhirContext.newJsonParser();
  }

  @Test
  void runsSqlAgainstReferencedViewDefinition() {
    final Library library =
        sqlQueryLibrary(
            "SELECT id, family_name FROM patients ORDER BY id", "patients", VIEW_REFERENCE);

    final String body =
        postOk(
            "/fhir/$sqlquery-run",
            parametersJson(library, SqlQueryOutputFormat.NDJSON),
            SqlQueryOutputFormat.NDJSON);
    log.debug("End-to-end NDJSON response:\n{}", body);

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
  void appliesLimitParameter() {
    final Library library =
        sqlQueryLibrary(
            "SELECT id, family_name FROM patients ORDER BY id", "patients", VIEW_REFERENCE);

    final String body =
        postOk(
            "/fhir/$sqlquery-run",
            parametersJson(library, SqlQueryOutputFormat.NDJSON, 2),
            SqlQueryOutputFormat.NDJSON);

    final String[] lines = body.trim().split("\n");
    assertThat(lines).hasSize(2);
  }

  @Test
  void returns400WhenReferencedViewDefinitionDoesNotExist() {
    final Library library =
        sqlQueryLibrary("SELECT id FROM patients", "patients", "ViewDefinition/does-not-exist");

    webTestClient
        .post()
        .uri("http://localhost:" + port + "/fhir/$sqlquery-run")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
        .bodyValue(parametersJson(library, SqlQueryOutputFormat.NDJSON))
        .exchange()
        .expectStatus()
        .is4xxClientError();
  }

  @Nonnull
  private String postOk(
      @Nonnull final String path,
      @Nonnull final String body,
      @Nonnull final SqlQueryOutputFormat format) {
    final String contentType = format.getContentType();
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + path)
            .header("Content-Type", "application/fhir+json")
            .header("Accept", contentType)
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentTypeCompatibleWith(MediaType.parseMediaType(contentType))
            .expectBody()
            .returnResult();
    return new String(
        Objects.requireNonNull(result.getResponseBodyContent()), StandardCharsets.UTF_8);
  }

  @Nonnull
  private Library sqlQueryLibrary(
      @Nonnull final String sql, @Nonnull final String label, @Nonnull final String viewReference) {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(LIBRARY_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(sql.getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel(label)
            .setResource(viewReference));
    return library;
  }

  @Nonnull
  private String parametersJson(
      @Nonnull final Library library, @Nonnull final SqlQueryOutputFormat format) {
    return parametersJson(library, format, null);
  }

  @Nonnull
  private String parametersJson(
      @Nonnull final Library library,
      @Nonnull final SqlQueryOutputFormat format,
      @Nullable final Integer limit) {
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
    formatParam.put("valueString", format.getCode());
    parameterList.add(formatParam);

    if (limit != null) {
      final Map<String, Object> limitParam = new LinkedHashMap<>();
      limitParam.put("name", "_limit");
      limitParam.put("valueInteger", limit);
      parameterList.add(limitParam);
    }

    parameters.put("parameter", parameterList);
    return GSON.toJson(parameters);
  }
}
