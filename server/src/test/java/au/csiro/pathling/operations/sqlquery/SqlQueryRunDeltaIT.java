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

import au.csiro.pathling.util.TestDataSetup;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
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
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * End-to-end integration test for the {@code $sqlquery-run} operation against the production Delta
 * Lake data source. Drives the full pipeline - Library parsing, ViewDefinition lookup, FhirView
 * execution, request-scoped temp-view registration, SQL execution, and result streaming - using SQL
 * that names a registered view backed by a real Delta table.
 *
 * <p>This complements {@link SqlQueryRunWithViewDefinitionsIT}, which substitutes an in-memory data
 * source for the production one. The Delta-backed path produces a different analyzed plan (the leaf
 * is a {@link org.apache.spark.sql.execution.datasources.LogicalRelation} reading from a Delta
 * table rather than a {@code LocalRelation} over an in-memory dataset), and the {@link
 * SqlValidator} must accept that shape when it appears as a descendant of a registered request-
 * scoped temp view.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class SqlQueryRunDeltaIT {

  private static final Gson GSON = new Gson();

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @Autowired private FhirContext fhirContext;

  private IParser jsonParser;

  @TempDir private static Path warehouseDir;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  @BeforeEach
  void setup() {
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .responseTimeout(Duration.ofSeconds(60))
            .build();
    jsonParser = fhirContext.newJsonParser();
  }

  @AfterEach
  void cleanup() throws IOException {
    // Wipe and re-seed the temp warehouse between tests so writes from earlier tests
    // (e.g. ViewDefinition.parquet) do not leak into later ones.
    FileUtils.cleanDirectory(warehouseDir.toFile());
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
  }

  /**
   * Reproduces the case that fails in production but passes in {@link
   * SqlQueryRunWithViewDefinitionsIT}: a simple {@code SELECT} against a stored ViewDefinition
   * whose underlying table is a Delta-backed Patient table. The validator must allow the {@code
   * LogicalRelation} appearing under the registered temp view's plan.
   */
  @Test
  void runsSqlAgainstDeltaBackedViewDefinition() {
    final String viewId = createPatientViewDefinition();
    final Library library =
        sqlQueryLibrary(
            "SELECT id FROM patients ORDER BY id LIMIT 5", "patients", "ViewDefinition/" + viewId);

    final String body =
        postOk(
            "/fhir/$sqlquery-run",
            parametersJson(library, SqlQueryOutputFormat.NDJSON),
            SqlQueryOutputFormat.NDJSON);
    log.debug("Delta-backed end-to-end NDJSON response:\n{}", body);

    // The seeded test dataset has at least five Patient resources, so we expect five rows
    // each with a non-empty "id" field.
    final String[] lines = body.trim().split("\n");
    assertThat(lines).hasSize(5);
    for (final String line : lines) {
      assertThat(line).startsWith("{").contains("\"id\":\"").endsWith("}");
    }
  }

  @Nonnull
  private String createPatientViewDefinition() {
    final Map<String, Object> view = new LinkedHashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("name", "patients");
    view.put("resource", "Patient");
    view.put("status", "active");
    final Map<String, Object> column = new LinkedHashMap<>();
    column.put("name", "id");
    column.put("path", "id");
    final Map<String, Object> select = new LinkedHashMap<>();
    select.put("column", List.of(column));
    view.put("select", List.of(select));

    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/ViewDefinition")
            .header("Content-Type", "application/fhir+json")
            .bodyValue(GSON.toJson(view))
            .exchange()
            .expectStatus()
            .isCreated()
            .expectBody()
            .returnResult();
    final String location =
        Objects.requireNonNull(result.getResponseHeaders().getFirst("Location"));
    final String[] parts = location.split("/");
    return parts[parts.length - 1];
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
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_QUERY_TYPE_CODE)));
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

    parameters.put("parameter", parameterList);
    return GSON.toJson(parameters);
  }
}
